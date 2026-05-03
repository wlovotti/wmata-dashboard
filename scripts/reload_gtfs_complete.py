"""
Complete GTFS Data Reload Script with Versioning

Downloads the WMATA GTFS feed and applies it to the database in a single
transaction: either the new snapshot fully replaces the current one, or
nothing changes. Versioned tables (routes / stops / trips / stop_times /
calendar / calendar_dates) get their current rows marked is_current=False
and a fresh snapshot inserted; agencies are upserted by agency_id so the
FK target stays stable for historical route rows; the remaining unversioned
tables (feed_info / timepoints / timepoint_times / route_service_profile)
are truncated and reinserted.

The download step lives in `_download_and_parse_gtfs`; the DB-side logic
lives in `apply_gtfs_to_db(db, gtfs_data)` so tests can exercise it
without network or postgres-specific upserts.
"""

import csv
import io
import os
import sys
import zipfile
from datetime import datetime

import requests
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

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

load_dotenv()

BASE_URL = "https://api.wmata.com/gtfs"

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


def parse_csv(zip_file: zipfile.ZipFile, filename: str) -> list[dict]:
    """Parse a CSV file out of the GTFS zip into a list of dicts."""
    content = zip_file.read(filename).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def _download_and_parse_gtfs() -> dict[str, list[dict]]:
    """Download the WMATA GTFS zip and return a dict keyed by filename stem."""
    api_key = os.getenv("WMATA_API_KEY")
    if not api_key:
        raise RuntimeError("WMATA_API_KEY not found in environment variables")

    print("\nDownloading GTFS data...")
    response = requests.get(
        f"{BASE_URL}/bus-gtfs-static.zip",
        headers={"api_key": api_key},
        timeout=30,
    )
    response.raise_for_status()
    print("✓ Downloaded GTFS data")

    print("\nParsing GTFS files...")
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))

    gtfs_data: dict[str, list[dict]] = {}
    for filename in GTFS_FILES:
        print(f"  - {filename}...", end="")
        sys.stdout.flush()
        key = filename.replace(".txt", "")
        gtfs_data[key] = parse_csv(zip_file, filename)
        print(f" {len(gtfs_data[key])} records")

    print("✓ All GTFS files parsed")
    return gtfs_data


def apply_gtfs_to_db(db: Session, gtfs_data: dict[str, list[dict]]) -> int:
    """
    Apply parsed GTFS data to the DB inside the caller's transaction.

    Caller owns commit/rollback. On any exception, callers should rollback
    so the DB returns to its pre-call state.

    Returns the snapshot_id of the newly created GTFSSnapshot row.
    """
    now = datetime.utcnow()

    print("\nCreating new GTFS snapshot...")
    print("-" * 70)

    feed_version = (
        gtfs_data["feed_info"][0].get("feed_version") if gtfs_data.get("feed_info") else None
    )
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
        notes=f"Auto-reload at {now.isoformat()}",
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
        existing = db.query(Agency).filter_by(agency_id=agency_data["agency_id"]).one_or_none()
        if existing is not None:
            existing.agency_name = agency_data["agency_name"]
            existing.agency_url = agency_data.get("agency_url")
            existing.agency_timezone = agency_data.get("agency_timezone")
            existing.agency_lang = agency_data.get("agency_lang")
            existing.agency_phone = agency_data.get("agency_phone")
            existing.agency_fare_url = agency_data.get("agency_fare_url")
            existing.agency_email = agency_data.get("agency_email")
        else:
            db.add(
                Agency(
                    agency_id=agency_data["agency_id"],
                    agency_name=agency_data["agency_name"],
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
        db.add(
            Stop(
                stop_id=stop_data["stop_id"],
                stop_code=stop_data.get("stop_code"),
                stop_name=stop_data["stop_name"],
                stop_desc=stop_data.get("stop_desc"),
                stop_lat=float(stop_data["stop_lat"]),
                stop_lon=float(stop_data["stop_lon"]),
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

    return snapshot_id


def reload_complete_gtfs():
    """Download the WMATA GTFS feed and apply it in a single transaction."""
    print("=" * 70)
    print("Complete GTFS Data Reload")
    print("=" * 70)

    gtfs_data = _download_and_parse_gtfs()

    db = get_session()
    try:
        snapshot_id = apply_gtfs_to_db(db, gtfs_data)
        db.commit()

        print("\n" + "=" * 70)
        print(f"✓ Complete GTFS Reload Successful! (snapshot_id={snapshot_id})")
        print("=" * 70)

        print("\nDatabase Summary:")
        print(f"  Agencies:        {len(gtfs_data['agency'])}")
        print(f"  Routes:          {len(gtfs_data['routes'])}")
        print(f"  Stops:           {len(gtfs_data['stops'])}")
        print(f"  Trips:           {db.query(Trip).count()}")
        print(f"  Stop Times:      {len(gtfs_data['stop_times'])}")
        print(f"  Shapes:          {db.query(Shape).count()}")
        print(f"  Calendar:        {len(gtfs_data['calendar'])}")
        print(f"  Calendar Dates:  {len(gtfs_data['calendar_dates'])}")
        print(f"  Timepoints:      {len(gtfs_data['timepoints'])}")
        print(f"  Timepoint Times: {len(gtfs_data['timepoint_times'])}")
    except Exception as e:
        db.rollback()
        print(f"\n✗ Reload failed; transaction rolled back. DB state is unchanged.\n  {e}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    reload_complete_gtfs()
