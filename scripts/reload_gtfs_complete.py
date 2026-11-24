"""
Complete GTFS Data Reload Script with Versioning

Downloads an agency's static GTFS feed and applies it to the database in
a single transaction: either the new snapshot fully replaces the current
one, or nothing changes. Versioned tables (routes / stops / trips /
stop_times / calendar / calendar_dates) get their current rows marked
is_current=False and a fresh snapshot inserted; agencies are upserted by
agency_id so the FK target stays stable for historical route rows; the
remaining unversioned tables (feed_info / timepoints / timepoint_times /
route_service_profile) are truncated and reinserted.

The download step lives in `_download_and_parse_gtfs`; the DB-side logic
lives in `apply_gtfs_to_db(db, gtfs_data)` so tests can exercise it
without network or postgres-specific upserts.

Multi-agency (NOTES-100): pass `--agency` (matching a
`config/agencies/<agency>.yaml`) to load a non-WMATA agency's static
GTFS into that agency's own database. The download URL, auth style
(header vs query), and static-GTFS query params (e.g. SFMTA/511.org's
`operator_id=SF` — a DIFFERENT param name than the real-time feeds'
`agency=SF`, see config/agencies/sfmta.yaml) all come from the agency
config. `timepoints.txt` / `timepoint_times.txt` are a WMATA-only
GTFS-Plus extension; other agencies' zips simply won't have them, and
that's tolerated (empty lists) rather than an error — every other GTFS
file is still required.

Raw-zip archive + unchanged-feed skip (issue #258 PR): before this, the
downloaded zip only ever lived in memory, so the database was the sole
copy of every past schedule — and every reload inserted a full new
snapshot even when the agency had republished nothing (by 2026-11
WMATA's DB held 15 snapshots spanning only 4 feed_versions, each a full
copy of stop_times). Now each reload:

  1. Downloads the zip and computes a *content* hash
     (`gtfs_content_sha256`: sha256 over the sorted member names and
     member bytes, so a server that re-packs byte-identical files with
     fresh zip timestamps still hashes the same).
  2. Compares it with the hash recorded on the current snapshot (stored
     as a ``content_sha256=<hex>`` token in `gtfs_snapshots.notes` — an
     existing free-text column, so no schema migration is needed). A
     match means the feed is unchanged: exit 0 without touching the DB
     or S3. feed_version + row counts are deliberately NOT used as the
     identity test: WMATA snapshots 11 and 12 share feed_version
     S1000249 and every count, yet 12's calendar runs a week longer.
     A snapshot loaded before this change carries no hash, so the first
     reload after it always loads (once) and records one.
  3. Uploads the exact downloaded bytes to
     ``s3://<cfg.s3_bucket>/gtfs-archive/<agency>/<UTC ts>_<feed_version>_<sha12>.zip``
     and verifies the stored byte count — BEFORE any DB write. An upload
     failure aborts the reload with the DB untouched (still on the
     previous snapshot; `run_gtfs_reload.py` fires its failure
     notification and the next run retries). Rationale: the archive
     exists so the DB is never again the only copy of a schedule; a
     snapshot that loads without its archive silently recreates exactly
     that gap, while failing costs ~nothing in practice
     (`bin/pull-and-derive.sh` needs the same AWS credentials for its S3
     sync and cannot run without them anyway). `--no-archive` is the
     deliberate, logged operator escape hatch.
  4. Applies the snapshot in one transaction exactly as before,
     recording the content hash and archive URI in the snapshot's notes.

`--force` loads a new snapshot even when the content hash matches.

Usage:
  uv run python scripts/reload_gtfs_complete.py
  uv run python scripts/reload_gtfs_complete.py --agency sfmta
  uv run python scripts/reload_gtfs_complete.py --force        # load even if unchanged
  uv run python scripts/reload_gtfs_complete.py --no-archive   # skip the S3 copy (logged)
"""

import argparse
import csv
import hashlib
import io
import os
import re
import sys
import zipfile
from datetime import datetime

import requests
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

from pipelines.refresh_corridors import refresh_corridors
from src.agency_config import AgencyConfig, load_agency_config, resolve_agency_db_url
from src.database import get_session
from src.models import (
    Agency,
    Calendar,
    CalendarDate,
    FeedInfo,
    GTFSSnapshot,
    Route,
    RouteServiceProfile,
    Shape,
    Stop,
    StopTime,
    Timepoint,
    TimepointTime,
    Trip,
)
from src.service_profile import compute_route_service_profile
from src.timezones import utcnow_naive

load_dotenv()

DEFAULT_AGENCY = "wmata"

# Files we read out of the GTFS zip. Order is informational; parse order
# does not constrain DB write order.
GTFS_FILES = [
    "agency.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "feed_info.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
    "shapes.txt",
    "timepoints.txt",
    "timepoint_times.txt",
]

# GTFS-Plus extension files WMATA publishes that standard GTFS agencies
# (e.g. SFMTA/511.org) simply don't have. Absence is expected, not an
# error — every other file in GTFS_FILES is still required.
OPTIONAL_GTFS_FILES = {"timepoints.txt", "timepoint_times.txt"}

# S3 key prefix for the raw downloaded zips; the bucket comes from the
# agency config (`s3.bucket`, shared with the raw-jsonl-archive prefixes).
GTFS_ARCHIVE_PREFIX = "gtfs-archive"

# Token recorded in gtfs_snapshots.notes carrying the feed's content hash.
CONTENT_HASH_NOTE_RE = re.compile(r"content_sha256=([0-9a-f]{64})")


class GTFSArchiveError(RuntimeError):
    """Raised when the downloaded zip could not be archived to S3 (and verified)."""


def parse_csv(zip_file: zipfile.ZipFile, filename: str) -> list[dict]:
    """Parse a CSV file out of the GTFS zip into a list of dicts."""
    content = zip_file.read(filename).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def _require_field(row: dict, field: str, context: str) -> str:
    """Fetch a required GTFS field, raising a clear error naming what's missing.

    A bare ``row[field]`` raises a raw, contextless ``KeyError: 'field'``
    if an agency's GTFS export omits the whole column -- e.g. `agency_id`
    in `agency.txt`, which GTFS treats as only *conditionally* required
    (optional for a single-agency feed, which some non-WMATA exports are).
    Wrapping the lookup turns that into an actionable error naming the
    field and the row it came from, instead of a bare stack trace pointing
    at a dict literal deep in `apply_gtfs_to_db`.
    """
    try:
        return row[field]
    except KeyError:
        raise RuntimeError(
            f"GTFS row missing required field {field!r} ({context}). "
            f"Row keys present: {sorted(row.keys())}"
        ) from None


def _download_gtfs_zip(cfg: AgencyConfig) -> bytes:
    """Download ``cfg``'s static GTFS zip and return its exact bytes.

    Auth style (header vs query) and the download URL/params come from
    ``cfg`` (NOTES-100 multi-agency) — see the module docstring.
    """
    api_key = os.getenv(cfg.api_key_env)
    if not api_key:
        raise RuntimeError(f"{cfg.api_key_env} not found in environment variables")

    print(f"\nDownloading {cfg.display_name} GTFS data...")
    if cfg.auth_style == "header":
        response = requests.get(
            cfg.static_gtfs_url,
            headers={"api_key": api_key},
            timeout=30,
        )
    else:
        response = requests.get(
            cfg.static_gtfs_url,
            params={"api_key": api_key, **cfg.static_gtfs_params},
            timeout=30,
        )
    response.raise_for_status()
    print(f"✓ Downloaded GTFS data ({len(response.content):,} bytes)")
    return response.content


def parse_gtfs_zip(zip_bytes: bytes, cfg: AgencyConfig) -> dict[str, list[dict]]:
    """Parse a GTFS zip's bytes into a dict of row lists keyed by filename stem.

    Optional GTFS-Plus files (``OPTIONAL_GTFS_FILES``) absent from the zip
    become empty lists; any other missing file raises ``KeyError``.
    """
    print("\nParsing GTFS files...")
    zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))
    zip_names = set(zip_file.namelist())

    gtfs_data: dict[str, list[dict]] = {}
    for filename in GTFS_FILES:
        print(f"  - {filename}...", end="")
        sys.stdout.flush()
        key = filename.replace(".txt", "")
        if filename in OPTIONAL_GTFS_FILES and filename not in zip_names:
            gtfs_data[key] = []
            print(f" not present (optional GTFS-Plus extension for {cfg.name!r}) — 0 records")
            continue
        gtfs_data[key] = parse_csv(zip_file, filename)
        print(f" {len(gtfs_data[key])} records")

    print("✓ All GTFS files parsed")
    return gtfs_data


def _download_and_parse_gtfs(cfg: AgencyConfig) -> dict[str, list[dict]]:
    """Download ``cfg``'s static GTFS zip and return a dict keyed by filename stem."""
    return parse_gtfs_zip(_download_gtfs_zip(cfg), cfg)


def gtfs_content_sha256(zip_bytes: bytes) -> str:
    """Return a hash of a GTFS zip's *contents*, independent of zip packaging.

    Hashes each member's name and the sha256 of its bytes, in sorted name
    order. Zip container metadata (member timestamps, compression level,
    member order) is ignored, so an agency endpoint that re-packs
    byte-identical files on every request still hashes identically —
    which is what the unchanged-feed skip needs. Directory entries are
    skipped.
    """
    outer = hashlib.sha256()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in sorted(n for n in zf.namelist() if not n.endswith("/")):
            outer.update(name.encode("utf-8"))
            outer.update(b"\0")
            outer.update(hashlib.sha256(zf.read(name)).hexdigest().encode("ascii"))
            outer.update(b"\n")
    return outer.hexdigest()


def feed_version_from_gtfs(gtfs_data: dict[str, list[dict]]) -> str | None:
    """Return ``feed_info.feed_version`` from parsed GTFS data, or None if absent."""
    feed_info = gtfs_data.get("feed_info") or []
    return feed_info[0].get("feed_version") if feed_info else None


def current_snapshot_content_hash(db: Session) -> str | None:
    """Return the content hash recorded on the current GTFS snapshot, if any.

    The current snapshot is the one owning the ``is_current`` routes rows
    (the same definition `pipelines/refresh_corridors.py` uses). Returns
    None when there is no current snapshot or it predates hash recording.
    """
    current_id = (
        db.query(Route.snapshot_id)
        .filter(Route.is_current, Route.snapshot_id.isnot(None))
        .order_by(Route.snapshot_id.desc())
        .limit(1)
        .scalar()
    )
    if current_id is None:
        return None
    notes = db.query(GTFSSnapshot.notes).filter(GTFSSnapshot.snapshot_id == current_id).scalar()
    match = CONTENT_HASH_NOTE_RE.search(notes or "")
    return match.group(1) if match else None


def gtfs_archive_key(
    agency: str, when: datetime, feed_version: str | None, content_sha: str
) -> str:
    """Build the S3 key for one downloaded GTFS zip.

    ``gtfs-archive/<agency>/<YYYYMMDDTHHMMSSZ>_<feed_version>_<sha12>.zip``
    (``when`` is naive UTC, per the project's storage convention). The UTC
    timestamp sorts keys chronologically and makes every download unique;
    feed_version (sanitised to ``[A-Za-z0-9.-]``, ``unknown`` when absent)
    and the first 12 hex chars of the content hash make a listing
    readable without opening any object.
    """
    version = re.sub(r"[^A-Za-z0-9.-]", "-", feed_version or "") or "unknown"
    return (
        f"{GTFS_ARCHIVE_PREFIX}/{agency}/{when.strftime('%Y%m%dT%H%M%SZ')}_"
        f"{version}_{content_sha[:12]}.zip"
    )


def archive_gtfs_zip(s3_client, bucket: str, key: str, zip_bytes: bytes, metadata: dict) -> str:
    """Upload the exact zip bytes to ``s3://bucket/key`` and verify the stored size.

    Returns the ``s3://`` URI. Raises `GTFSArchiveError` on any upload
    error or on a byte-count mismatch reported by ``head_object`` —
    callers treat that as fatal for the reload (see the module docstring).
    """
    uri = f"s3://{bucket}/{key}"
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=zip_bytes,
            ContentType="application/zip",
            Metadata=metadata,
        )
        head = s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:  # boto3/botocore raise a wide family of errors
        raise GTFSArchiveError(f"failed to archive GTFS zip to {uri}: {exc}") from exc
    stored = head.get("ContentLength")
    if stored != len(zip_bytes):
        raise GTFSArchiveError(
            f"archived GTFS zip at {uri} has {stored} bytes, expected {len(zip_bytes)}"
        )
    return uri


def apply_gtfs_to_db(
    db: Session,
    gtfs_data: dict[str, list[dict]],
    content_sha256: str | None = None,
    archive_uri: str | None = None,
) -> int:
    """
    Apply parsed GTFS data to the DB inside the caller's transaction.

    Caller owns commit/rollback. On any exception, callers should rollback
    so the DB returns to its pre-call state.

    ``content_sha256`` / ``archive_uri``, when given, are recorded in the
    snapshot's ``notes`` (``content_sha256=<hex>`` is what the next
    reload's unchanged-feed check reads back).

    Returns the snapshot_id of the newly created GTFSSnapshot row.
    """
    now = utcnow_naive()

    print("\nCreating new GTFS snapshot...")
    print("-" * 70)

    feed_version = feed_version_from_gtfs(gtfs_data)
    notes = f"Auto-reload at {now.isoformat()}"
    if content_sha256:
        notes += f"; content_sha256={content_sha256}"
    if archive_uri:
        notes += f"; archive={archive_uri}"
    snapshot = GTFSSnapshot(
        snapshot_date=now,
        feed_version=feed_version,
        routes_count=len(gtfs_data["routes"]),
        stops_count=len(gtfs_data["stops"]),
        trips_count=len(gtfs_data.get("trips", [])),
        stop_times_count=len(gtfs_data["stop_times"]),
        shapes_count=len(gtfs_data.get("shapes", [])),
        calendar_entries=len(gtfs_data["calendar"]),
        calendar_exceptions=len(gtfs_data["calendar_dates"]),
        notes=notes,
    )
    db.add(snapshot)
    db.flush()
    snapshot_id = snapshot.snapshot_id
    print(f"✓ Created snapshot {snapshot_id} (version: {feed_version or 'unknown'})")

    print("\n→ Marking old versioned records as inactive...")
    for model in (Route, Stop, Trip, StopTime, Calendar, CalendarDate):
        db.query(model).filter(model.is_current).update(
            {"valid_to": now, "is_current": False}, synchronize_session=False
        )
    print("  ✓ Old records marked inactive")

    # Agencies are FK targets from routes.agency_id (both old and new rows
    # reference them), so a DELETE-then-INSERT cycle can never succeed in a
    # populated DB. Upsert by agency_id instead — the row stays put, fields
    # get refreshed.
    print("\n→ Upserting agencies...")
    for agency_data in gtfs_data["agency"]:
        # agency_id is GTFS-"conditionally required" -- optional when
        # agency.txt has exactly one row, required otherwise. A raw
        # `agency_data["agency_id"]` KeyError with no context is a common
        # single-agency-feed footgun; _require_field names the field.
        agency_id = _require_field(agency_data, "agency_id", "agency.txt")
        agency_name = _require_field(agency_data, "agency_name", "agency.txt")
        existing = db.query(Agency).filter_by(agency_id=agency_id).one_or_none()
        if existing is not None:
            existing.agency_name = agency_name
            existing.agency_url = agency_data.get("agency_url")
            existing.agency_timezone = agency_data.get("agency_timezone")
            existing.agency_lang = agency_data.get("agency_lang")
            existing.agency_phone = agency_data.get("agency_phone")
            existing.agency_fare_url = agency_data.get("agency_fare_url")
            existing.agency_email = agency_data.get("agency_email")
        else:
            db.add(
                Agency(
                    agency_id=agency_id,
                    agency_name=agency_name,
                    agency_url=agency_data.get("agency_url"),
                    agency_timezone=agency_data.get("agency_timezone"),
                    agency_lang=agency_data.get("agency_lang"),
                    agency_phone=agency_data.get("agency_phone"),
                    agency_fare_url=agency_data.get("agency_fare_url"),
                    agency_email=agency_data.get("agency_email"),
                )
            )
    db.flush()
    print(f"  ✓ Upserted {len(gtfs_data['agency'])} agencies")

    print("\nLoading new GTFS data with versioning...")
    print("-" * 70)

    print("→ Loading routes...")
    for route_data in gtfs_data["routes"]:
        db.add(
            Route(
                route_id=route_data["route_id"],
                agency_id=route_data.get("agency_id"),
                route_short_name=route_data.get("route_short_name", ""),
                route_long_name=route_data.get("route_long_name"),
                route_desc=route_data.get("route_desc"),
                route_type=route_data.get("route_type"),
                route_url=route_data.get("route_url"),
                route_color=route_data.get("route_color"),
                route_text_color=route_data.get("route_text_color"),
                snapshot_id=snapshot_id,
                valid_from=now,
                valid_to=None,
                is_current=True,
            )
        )
    db.flush()
    print(f"  ✓ Loaded {len(gtfs_data['routes'])} routes")

    print("→ Loading stops...")
    for stop_data in gtfs_data["stops"]:
        stop_id = stop_data["stop_id"]
        context = f"stops.txt, stop_id={stop_id!r}"
        db.add(
            Stop(
                stop_id=stop_id,
                stop_code=stop_data.get("stop_code"),
                stop_name=_require_field(stop_data, "stop_name", context),
                stop_desc=stop_data.get("stop_desc"),
                stop_lat=float(_require_field(stop_data, "stop_lat", context)),
                stop_lon=float(_require_field(stop_data, "stop_lon", context)),
                zone_id=stop_data.get("zone_id"),
                stop_url=stop_data.get("stop_url"),
                snapshot_id=snapshot_id,
                valid_from=now,
                valid_to=None,
                is_current=True,
            )
        )
    db.flush()
    print(f"  ✓ Loaded {len(gtfs_data['stops'])} stops")

    print("→ Loading trips...")
    for trip_data in gtfs_data["trips"]:
        db.add(
            Trip(
                trip_id=trip_data["trip_id"],
                route_id=trip_data["route_id"],
                service_id=trip_data.get("service_id"),
                trip_headsign=trip_data.get("trip_headsign"),
                direction_id=int(trip_data["direction_id"])
                if trip_data.get("direction_id")
                else None,
                block_id=trip_data.get("block_id"),
                shape_id=trip_data.get("shape_id"),
                snapshot_id=snapshot_id,
                valid_from=now,
                valid_to=None,
                is_current=True,
            )
        )
    db.flush()
    print(f"  ✓ Loaded {len(gtfs_data['trips'])} trips")

    print("→ Loading stop_times (this will take 3-5 minutes)...")
    batch: list = []
    total = len(gtfs_data["stop_times"])
    for i, st_data in enumerate(gtfs_data["stop_times"]):
        batch.append(
            StopTime(
                trip_id=st_data["trip_id"],
                stop_id=st_data["stop_id"],
                arrival_time=st_data["arrival_time"],
                departure_time=st_data["departure_time"],
                stop_sequence=int(st_data["stop_sequence"]),
                stop_headsign=st_data.get("stop_headsign"),
                pickup_type=int(st_data["pickup_type"]) if st_data.get("pickup_type") else None,
                drop_off_type=int(st_data["drop_off_type"])
                if st_data.get("drop_off_type")
                else None,
                shape_dist_traveled=float(st_data["shape_dist_traveled"])
                if st_data.get("shape_dist_traveled")
                else None,
                timepoint=int(st_data["timepoint"]) if st_data.get("timepoint") else None,
                snapshot_id=snapshot_id,
                valid_from=now,
                valid_to=None,
                is_current=True,
            )
        )
        if len(batch) >= 10000:
            db.bulk_save_objects(batch)
            batch = []
            percent = ((i + 1) / total) * 100
            print(f"\r    Progress: {percent:.1f}% ({i + 1:,}/{total:,})", end="")
            sys.stdout.flush()
    if batch:
        db.bulk_save_objects(batch)
    print(f"\r  ✓ Loaded {total:,} stop_times")

    print("→ Loading calendar...")
    for cal_data in gtfs_data["calendar"]:
        db.add(
            Calendar(
                service_id=cal_data["service_id"],
                monday=int(cal_data["monday"]),
                tuesday=int(cal_data["tuesday"]),
                wednesday=int(cal_data["wednesday"]),
                thursday=int(cal_data["thursday"]),
                friday=int(cal_data["friday"]),
                saturday=int(cal_data["saturday"]),
                sunday=int(cal_data["sunday"]),
                start_date=cal_data["start_date"],
                end_date=cal_data["end_date"],
                snapshot_id=snapshot_id,
                valid_from=now,
                valid_to=None,
                is_current=True,
            )
        )
    print(f"  ✓ Loaded {len(gtfs_data['calendar'])} calendar entries")

    print("→ Loading calendar_dates...")
    for cal_date_data in gtfs_data["calendar_dates"]:
        db.add(
            CalendarDate(
                service_id=cal_date_data["service_id"],
                date=cal_date_data["date"],
                exception_type=int(cal_date_data["exception_type"]),
                snapshot_id=snapshot_id,
                valid_from=now,
                valid_to=None,
                is_current=True,
            )
        )
    db.flush()
    print(f"  ✓ Loaded {len(gtfs_data['calendar_dates'])} calendar exceptions")

    # Unversioned tables with no inbound FKs: safe to DELETE-then-INSERT
    # inside the same transaction. (`agencies` is the one exception, handled
    # above via upsert.)
    print("→ Loading feed_info...")
    db.execute(text("DELETE FROM feed_info"))
    for feed_data in gtfs_data["feed_info"]:
        db.add(
            FeedInfo(
                feed_publisher_name=feed_data["feed_publisher_name"],
                feed_publisher_url=feed_data.get("feed_publisher_url"),
                feed_lang=feed_data.get("feed_lang"),
                feed_start_date=feed_data.get("feed_start_date"),
                feed_end_date=feed_data.get("feed_end_date"),
                feed_version=feed_data.get("feed_version"),
                feed_contact_email=feed_data.get("feed_contact_email"),
                feed_contact_url=feed_data.get("feed_contact_url"),
            )
        )
    print(f"  ✓ Loaded {len(gtfs_data['feed_info'])} feed_info records")

    # `shapes` has no snapshot_id/is_current columns (unversioned, same as
    # feed_info/timepoints) -- truncate-then-reinsert, NOT the
    # `init_database.py` one-time "skip if any exist" seed behavior. Before
    # this, a database that only ever went through `reload_gtfs_complete.py`
    # (never `init_database.py`) had a permanently empty `shapes` table with
    # no recovery path -- `init_database.py` refuses to run once a snapshot
    # exists (NOTES-100 follow-up).
    print("→ Loading shapes...")
    db.execute(text("DELETE FROM shapes"))
    batch = []
    total_shapes = len(gtfs_data["shapes"])
    for i, shape_data in enumerate(gtfs_data["shapes"]):
        batch.append(
            Shape(
                shape_id=shape_data["shape_id"],
                shape_pt_lat=float(shape_data["shape_pt_lat"]),
                shape_pt_lon=float(shape_data["shape_pt_lon"]),
                shape_pt_sequence=int(shape_data["shape_pt_sequence"]),
                shape_dist_traveled=float(shape_data["shape_dist_traveled"])
                if shape_data.get("shape_dist_traveled")
                else None,
            )
        )
        if len(batch) >= 10000:
            db.bulk_save_objects(batch)
            batch = []
            percent = ((i + 1) / total_shapes) * 100
            print(f"\r    Progress: {percent:.1f}% ({i + 1:,}/{total_shapes:,})", end="")
            sys.stdout.flush()
    if batch:
        db.bulk_save_objects(batch)
    print(f"\r  ✓ Loaded {total_shapes:,} shapes")

    print("→ Loading timepoints...")
    db.execute(text("DELETE FROM timepoint_times"))
    db.execute(text("DELETE FROM timepoints"))
    for tp_data in gtfs_data["timepoints"]:
        db.add(
            Timepoint(
                stop_id=tp_data["stop_id"],
                stop_code=tp_data.get("stop_code"),
                stop_name=tp_data["stop_name"],
                stop_desc=tp_data.get("stop_desc"),
                stop_lat=float(tp_data["stop_lat"]),
                stop_lon=float(tp_data["stop_lon"]),
                zone_id=tp_data.get("zone_id"),
                stop_url=tp_data.get("stop_url"),
            )
        )
    db.flush()
    print(f"  ✓ Loaded {len(gtfs_data['timepoints'])} timepoints")

    print("→ Loading timepoint_times (this may take 1-2 minutes)...")
    batch = []
    total_tp = len(gtfs_data["timepoint_times"])
    for i, tpt_data in enumerate(gtfs_data["timepoint_times"]):
        batch.append(
            TimepointTime(
                trip_id=tpt_data["trip_id"],
                stop_id=tpt_data["stop_id"],
                arrival_time=tpt_data["arrival_time"],
                departure_time=tpt_data["departure_time"],
                stop_sequence=int(tpt_data["stop_sequence"]),
                stop_headsign=tpt_data.get("stop_headsign"),
                pickup_type=int(tpt_data["pickup_type"]) if tpt_data.get("pickup_type") else None,
                drop_off_type=int(tpt_data["drop_off_type"])
                if tpt_data.get("drop_off_type")
                else None,
                shape_dist_traveled=float(tpt_data["shape_dist_traveled"])
                if tpt_data.get("shape_dist_traveled")
                else None,
                timepoint=int(tpt_data["timepoint"]) if tpt_data.get("timepoint") else None,
            )
        )
        if len(batch) >= 10000:
            db.bulk_save_objects(batch)
            batch = []
            percent = ((i + 1) / total_tp) * 100
            print(f"\r    Progress: {percent:.1f}% ({i + 1:,}/{total_tp:,})", end="")
            sys.stdout.flush()
    if batch:
        db.bulk_save_objects(batch)
    print(f"\r  ✓ Loaded {total_tp:,} timepoint_times")

    # route_service_profile is derived from the rows we just flushed; the
    # session can read its own uncommitted writes via the open transaction.
    print("→ Computing route_service_profile...")
    db.execute(text("DELETE FROM route_service_profile"))
    profile_rows = compute_route_service_profile(db)
    for row in profile_rows:
        db.add(RouteServiceProfile(snapshot_id=snapshot_id, **row))
    db.flush()
    print(f"  ✓ Loaded {len(profile_rows):,} route_service_profile rows")

    # Cross-route corridors (NOTES-62) — full rebuild from the trips +
    # shapes + stops we just wrote. Runs inside this transaction so the
    # corridor identity tables either advance with the new snapshot or
    # roll back together with everything else.
    print("→ Rebuilding cross-route corridors...")
    corridor_counts = refresh_corridors(session=db, gtfs_snapshot_id=snapshot_id)
    print(f"  ✓ {corridor_counts}")

    return snapshot_id


def reload_complete_gtfs(
    agency: str = DEFAULT_AGENCY,
    force: bool = False,
    archive: bool = True,
    s3_client=None,
) -> int | None:
    """Download an agency's GTFS feed, archive it, and apply it in one transaction.

    Args:
        agency: Agency name matching ``config/agencies/<agency>.yaml``
            (NOTES-100 multi-agency; default ``"wmata"``). Selects the
            download source, the archive bucket, and the target database.
        force: Load a new snapshot even when the feed's content hash
            matches the current snapshot's.
        archive: Upload the downloaded zip to S3 before loading (default).
            ``False`` skips the upload — an explicit operator choice that
            is logged loudly.
        s3_client: Injected boto3 S3 client (tests); built lazily when None.

    Returns:
        The new snapshot_id, or None when the feed was unchanged and no
        snapshot was created.
    """
    cfg = load_agency_config(agency)
    print("=" * 70)
    print(f"Complete GTFS Data Reload ({cfg.display_name})")
    print("=" * 70)

    zip_bytes = _download_gtfs_zip(cfg)
    content_sha = gtfs_content_sha256(zip_bytes)
    print(f"  content_sha256={content_sha}")

    db = get_session(db_url=resolve_agency_db_url(cfg))
    try:
        if not force and current_snapshot_content_hash(db) == content_sha:
            print(
                "\n✓ GTFS feed unchanged (content hash matches the current snapshot); "
                "no new snapshot created. Pass --force to load anyway."
            )
            return None

        gtfs_data = parse_gtfs_zip(zip_bytes, cfg)
        feed_version = feed_version_from_gtfs(gtfs_data)

        # Archive BEFORE any DB write: an upload failure raises out of here
        # with the DB untouched (see module docstring for why that's fatal).
        archive_uri = None
        if archive:
            key = gtfs_archive_key(cfg.name, utcnow_naive(), feed_version, content_sha)
            if s3_client is None:
                import boto3

                s3_client = boto3.client("s3")
            print(f"\nArchiving GTFS zip to s3://{cfg.s3_bucket}/{key} ...")
            archive_uri = archive_gtfs_zip(
                s3_client,
                cfg.s3_bucket,
                key,
                zip_bytes,
                metadata={
                    "agency": cfg.name,
                    "feed-version": feed_version or "unknown",
                    "content-sha256": content_sha,
                    "zip-sha256": hashlib.sha256(zip_bytes).hexdigest(),
                },
            )
            print(f"✓ Archived {len(zip_bytes):,} bytes to {archive_uri}")
        else:
            print(
                "\n!! --no-archive: the downloaded GTFS zip is NOT being saved to S3; "
                "the database will be the only copy of this snapshot.",
                file=sys.stderr,
            )

        snapshot_id = apply_gtfs_to_db(
            db, gtfs_data, content_sha256=content_sha, archive_uri=archive_uri
        )
        db.commit()

        print("\n" + "=" * 70)
        print(f"✓ Complete GTFS Reload Successful! (snapshot_id={snapshot_id})")
        print("=" * 70)

        print("\nDatabase Summary:")
        print(f"  Agencies:        {len(gtfs_data['agency'])}")
        print(f"  Routes:          {len(gtfs_data['routes'])}")
        print(f"  Stops:           {len(gtfs_data['stops'])}")
        print(f"  Trips:           {len(gtfs_data.get('trips', []))}")
        print(f"  Stop Times:      {len(gtfs_data['stop_times'])}")
        print(f"  Shapes:          {len(gtfs_data.get('shapes', []))}")
        print(f"  Calendar:        {len(gtfs_data['calendar'])}")
        print(f"  Calendar Dates:  {len(gtfs_data['calendar_dates'])}")
        print(f"  Timepoints:      {len(gtfs_data['timepoints'])}")
        print(f"  Timepoint Times: {len(gtfs_data['timepoint_times'])}")
        return snapshot_id
    except Exception as e:
        db.rollback()
        print(f"\n✗ Reload failed; transaction rolled back. DB state is unchanged.\n  {e}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--agency",
        default=DEFAULT_AGENCY,
        help=(f"Agency name matching config/agencies/<agency>.yaml (default: {DEFAULT_AGENCY!r})."),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Load a new snapshot even if the feed's content hash matches the current one.",
    )
    parser.add_argument(
        "--no-archive",
        action="store_true",
        help="Skip uploading the downloaded zip to S3 (the DB becomes the only copy).",
    )
    cli_args = parser.parse_args()
    reload_complete_gtfs(
        agency=cli_args.agency, force=cli_args.force, archive=not cli_args.no_archive
    )
