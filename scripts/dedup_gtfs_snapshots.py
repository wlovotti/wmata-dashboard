"""De-duplicate GTFS snapshots by true content (dry-run by default).

Every weekly reload before PR #263 inserted a full copy of the feed even
when nothing changed, so WMATA's DB holds 16 snapshots spanning ~4 feed
versions (``stop_times`` ~24 GB, ~95% superseded copies). Old snapshots
have no archived zip — the DB is the only copy — so this tool never
deletes a snapshot unless an identical one survives, and proves it row for
row first.

Identity is CONTENT across all six versioned tables (``routes``, ``stops``,
``trips``, ``stop_times``, ``calendar``, ``calendar_dates``; every GTFS
column, excluding ``id`` / ``snapshot_id`` / ``valid_from`` / ``valid_to`` /
``is_current`` / ``created_at``), never ``feed_version`` + row counts:
WMATA snapshots 11 and 12 share a feed_version and every count, yet 12's
calendar runs a week longer.

Steps (each subcommand includes the ones before it):

1. **Fingerprint** each snapshot per table: ``count(*)`` and two
   order-independent sums of ``hashtextextended`` over the row's content
   text (one aggregate pass over that snapshot's rows via the
   ``snapshot_id`` index). Snapshots whose six fingerprints all match form
   a candidate group. ``--metadata-only`` instead groups by feed_version +
   stored counts — instant, labelled UNVERIFIED, never used by
   ``--apply``.
2. **Survivor** per group: the ``is_current`` member if any, else a member
   carrying an ``archive=`` token, else the newest (latest
   ``snapshot_date``, then highest id). Newest matters because
   ``resolve_gtfs_snapshot_for_date`` picks the newest *covering*
   snapshot: identical members cover the same dates, so the newest member
   is exactly the one resolution already picks. The tool then re-resolves
   every service date from the first ``system_metrics_daily`` date to
   today with the duplicates excluded and refuses the group if any date
   would land on different content (the fallback rules can depend on
   load order).
3. **Prove** (``--prove`` / ``--apply``): for each duplicate vs its
   survivor and each table, ``(A EXCEPT ALL B) UNION ALL (B EXCEPT ALL A)``
   must be empty — exact multiset equality, NULL-safe. ``trips`` and
   ``stop_times`` are compared in ``--piece-trips`` trip_id ranges (bounds
   taken from the survivor's trips), so each query touches a bounded slice;
   equality on every piece of a partition is equality of the whole.
4. **Delete** (``--apply``, run lock held): per duplicate, in one
   transaction repoint ``corridors.gtfs_snapshot_id`` (not an FK) and
   ``route_service_profile.snapshot_id`` (FK) from the duplicate to the
   survivor, delete its ``calendar_dates`` + ``calendar`` (so a half-deleted
   snapshot can never be chosen — resolution skips calendar-less
   snapshots) and tag its notes ``dedup_into=<survivor>``; then delete
   ``stop_times`` / ``trips`` in ``--batch-size`` batches, ``stops``,
   ``routes`` and finally the ``gtfs_snapshots`` row (the versioned tables'
   FKs require that order). A rerun resumes tagged snapshots without
   re-proving (they were proven before tagging).

``export``: write each surviving snapshot that has no ``archive=`` token
(or ``--snapshot N``) to ``<out-dir>/<agency>_snapshot<N>_<feed_version>.zip``
— one CSV per versioned table (via ``COPY``) plus ``manifest.json`` with
the snapshot row, per-file row counts and sha256 — as a safety copy. The
tool only writes locally; uploading is the owner's ``aws s3 cp`` step
(printed). Run it BEFORE ``--apply``.

Usage:
    uv run python scripts/dedup_gtfs_snapshots.py --agency wmata --metadata-only
    uv run python scripts/dedup_gtfs_snapshots.py --agency wmata            # fingerprints (heavy)
    uv run python scripts/dedup_gtfs_snapshots.py --agency wmata --prove
    uv run python scripts/dedup_gtfs_snapshots.py --agency wmata export --out-dir ~/gtfs-db-export
    uv run python scripts/dedup_gtfs_snapshots.py --agency wmata --apply

Postgres only. Run fingerprints/prove/apply only when no backfill is
running (they read every snapshot's ``stop_times``).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src import run_lock  # noqa: E402
from src.agency_config import load_agency_config, resolve_agency_db_url  # noqa: E402
from src.gtfs_versioning import resolve_gtfs_snapshot_for_date  # noqa: E402

# GTFS content columns per versioned table (everything but bookkeeping).
CONTENT_COLUMNS: dict[str, tuple[str, ...]] = {
    "routes": (
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_desc",
        "route_type",
        "route_url",
        "route_color",
        "route_text_color",
    ),
    "stops": (
        "stop_id",
        "stop_code",
        "stop_name",
        "stop_desc",
        "stop_lat",
        "stop_lon",
        "zone_id",
        "stop_url",
    ),
    "trips": (
        "trip_id",
        "route_id",
        "service_id",
        "trip_headsign",
        "direction_id",
        "block_id",
        "shape_id",
    ),
    "stop_times": (
        "trip_id",
        "stop_id",
        "arrival_time",
        "departure_time",
        "stop_sequence",
        "stop_headsign",
        "pickup_type",
        "drop_off_type",
        "shape_dist_traveled",
        "timepoint",
    ),
    "calendar": (
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "start_date",
        "end_date",
    ),
    "calendar_dates": ("service_id", "date", "exception_type"),
}
TABLES = tuple(CONTENT_COLUMNS)
# Delete order honours the FKs to gtfs_snapshots (all six reference it; no
# FKs between them). calendar* first: a snapshot without calendar rows is
# never chosen by resolve_gtfs_snapshot_for_date.
DELETE_ORDER = ("calendar_dates", "calendar", "stop_times", "trips", "stops", "routes")
PIECED_TABLES = ("trips", "stop_times")  # compared in trip_id ranges
DEDUP_TOKEN_RE = re.compile(r"dedup_into=(\d+)")
ARCHIVE_TOKEN_RE = re.compile(r"archive=(s3://[^;\s]+)")
DEFAULT_BATCH_SIZE = 50_000
DEFAULT_PIECE_TRIPS = 5_000


def _row_text_sql(table: str) -> str:
    """SQL expression rendering a row's content as unambiguous text (NULL-marked)."""
    parts = [f"coalesce({c}::text, E'\\\\N')" for c in CONTENT_COLUMNS[table]]
    return "concat_ws(E'\\x1f', " + ", ".join(parts) + ")"


@dataclass
class Snapshot:
    """One gtfs_snapshots row plus what the tool learned about it."""

    snapshot_id: int
    snapshot_date: object
    feed_version: str | None
    notes: str
    counts: dict[str, int] = field(default_factory=dict)
    fingerprint: tuple | None = None
    is_current: bool = False

    @property
    def archived(self) -> bool:
        """True when the snapshot's zip is archived in S3 (``archive=`` token)."""
        return bool(ARCHIVE_TOKEN_RE.search(self.notes or ""))

    @property
    def dedup_into(self) -> int | None:
        """Survivor id when this snapshot is a half-deleted duplicate (resume)."""
        m = DEDUP_TOKEN_RE.search(self.notes or "")
        return int(m.group(1)) if m else None


@dataclass
class Group:
    """Snapshots with identical content; ``survivor`` is kept, ``duplicates`` deleted."""

    survivor: Snapshot
    duplicates: list[Snapshot]
    survivor_reason: str
    resolution_problem: str | None = None


# gtfs_snapshots metadata columns holding each table's row count at load time.
_STORED_COUNT_COLUMNS = {
    "routes": "routes_count",
    "stops": "stops_count",
    "trips": "trips_count",
    "stop_times": "stop_times_count",
    "calendar": "calendar_entries",
    "calendar_dates": "calendar_exceptions",
}


def load_snapshots(session: Session, exact: bool = True) -> list[Snapshot]:
    """All snapshots, oldest first, with per-table row counts and ``is_current``.

    ``exact=True`` counts each snapshot's rows (one ``snapshot_id``-indexed
    count per snapshot and table); ``exact=False`` uses the counts stored in
    ``gtfs_snapshots`` at load time (instant — for ``--metadata-only``).
    """
    stored = ", ".join(_STORED_COUNT_COLUMNS.values())
    rows = session.execute(
        text(
            f"SELECT snapshot_id, snapshot_date, feed_version, notes, {stored} "
            "FROM gtfs_snapshots ORDER BY snapshot_date, snapshot_id"
        )
    ).mappings()
    snaps = []
    for r in rows:
        s = Snapshot(r["snapshot_id"], r["snapshot_date"], r["feed_version"], r["notes"] or "")
        s.counts = {t: r[c] or 0 for t, c in _STORED_COUNT_COLUMNS.items()}
        snaps.append(s)
    if exact:
        for s in snaps:
            for table in TABLES:
                s.counts[table] = session.execute(
                    text(f"SELECT count(*) FROM {table} WHERE snapshot_id = :sid"),
                    {"sid": s.snapshot_id},
                ).scalar_one()
    current = session.execute(
        text("SELECT max(snapshot_id) FROM calendar WHERE is_current")
    ).scalar()
    for s in snaps:
        s.is_current = s.snapshot_id == current
    return snaps


def fingerprint(session: Session, snapshot_id: int, log: Callable[[str], None]) -> tuple:
    """Per-table ``(count, sum(hash seed 0), sum(hash seed 1))`` for one snapshot."""
    out = []
    for table in TABLES:
        rt = _row_text_sql(table)
        row = session.execute(
            text(
                f"SELECT count(*), coalesce(sum(hashtextextended({rt}, 0)), 0), "
                f"coalesce(sum(hashtextextended({rt}, 1)), 0) FROM {table} "
                "WHERE snapshot_id = :sid"
            ),
            {"sid": snapshot_id},
        ).one()
        out.append((table, int(row[0]), int(row[1]), int(row[2])))
    log(f"  fingerprinted snapshot {snapshot_id}")
    return tuple(out)


def metadata_key(s: Snapshot) -> tuple:
    """Cheap UNVERIFIED identity: feed_version + per-table counts."""
    return (s.feed_version, tuple(s.counts[t] for t in TABLES))


def choose_survivor(members: list[Snapshot]) -> tuple[Snapshot, str]:
    """Pick the member to keep: is_current > archived zip > newest load."""
    current = [m for m in members if m.is_current]
    if current:
        return current[0], "is_current"
    archived = [m for m in members if m.archived]
    pool, why = (archived, "newest with archive= token") if archived else (members, "newest load")
    return max(pool, key=lambda m: (m.snapshot_date, m.snapshot_id)), why


def group_snapshots(snaps: list[Snapshot], key: Callable[[Snapshot], tuple]) -> list[Group]:
    """Group live (non-resuming) snapshots by ``key``; singleton groups have no duplicates."""
    buckets: dict[tuple, list[Snapshot]] = {}
    for s in snaps:
        if s.dedup_into is None:
            buckets.setdefault(key(s), []).append(s)
    groups = []
    for members in buckets.values():
        survivor, why = choose_survivor(members)
        dups = [m for m in members if m is not survivor]
        groups.append(Group(survivor, dups, why))
    return sorted(groups, key=lambda g: g.survivor.snapshot_id)


def check_resolution(
    session: Session, groups: list[Group], tz_name: str, start: date, end: date
) -> None:
    """Set ``resolution_problem`` on any group whose de-dup changes a date's resolved content.

    Content id = the survivor id of the group a snapshot belongs to. Every
    date in ``[start, end]`` is resolved with all snapshots present and
    with the duplicates excluded — first each group's alone, then every
    remaining group's together (the fallback rules compare load times
    across groups); before and after must name the same content.
    """
    content_of = {}
    for g in groups:
        content_of[g.survivor.snapshot_id] = g.survivor.snapshot_id
        for d in g.duplicates:
            content_of[d.snapshot_id] = g.survivor.snapshot_id

    def _first_change(excluded: frozenset[int]) -> str | None:
        """First date whose resolved content changes with ``excluded`` removed, or None."""
        d = start
        while d <= end:
            before = resolve_gtfs_snapshot_for_date(session, d, tz_name).snapshot_id
            after = resolve_gtfs_snapshot_for_date(
                session, d, tz_name, exclude_snapshot_ids=excluded
            ).snapshot_id
            if content_of.get(before, before) != content_of.get(after, after):
                return f"{d} resolves to snapshot {before} now but {after} after the de-dup"
            d += timedelta(days=1)
        return None

    for g in groups:
        if g.duplicates:
            g.resolution_problem = _first_change(frozenset(x.snapshot_id for x in g.duplicates))
    ok = [g for g in groups if g.duplicates and not g.resolution_problem]
    if len(ok) > 1:
        problem = _first_change(frozenset(x.snapshot_id for g in ok for x in g.duplicates))
        if problem:
            for g in ok:
                g.resolution_problem = f"combined de-dup: {problem}"


def _piece_bounds(session: Session, snapshot_id: int, piece_trips: int) -> list[str | None]:
    """trip_id boundaries splitting a snapshot's trips into ``piece_trips``-sized ranges.

    Returns ``[None, b1, b2, ..., None]``: piece i covers
    ``bounds[i] <= trip_id < bounds[i+1]`` with None meaning unbounded.
    """
    ids = [
        r[0]
        for r in session.execute(
            text(
                "SELECT trip_id FROM (SELECT trip_id, row_number() OVER (ORDER BY trip_id) rn "
                "FROM (SELECT DISTINCT trip_id FROM trips WHERE snapshot_id = :sid) t) x "
                "WHERE rn % :n = 1 AND rn > 1 ORDER BY trip_id"
            ),
            {"sid": snapshot_id, "n": piece_trips},
        )
    ]
    return [None, *ids, None]


def _diff_count(session: Session, table: str, a: int, b: int, lo=None, hi=None) -> int:
    """Rows in the symmetric multiset difference of snapshots a and b (optionally a trip_id range)."""
    cols = ", ".join(CONTENT_COLUMNS[table])
    rng = ""
    params = {"a": a, "b": b}
    if lo is not None:
        rng += " AND trip_id >= :lo"
        params["lo"] = lo
    if hi is not None:
        rng += " AND trip_id < :hi"
        params["hi"] = hi
    sa = f"SELECT {cols} FROM {table} WHERE snapshot_id = :a{rng}"
    sb = f"SELECT {cols} FROM {table} WHERE snapshot_id = :b{rng}"
    return session.execute(
        text(f"SELECT count(*) FROM (({sa} EXCEPT ALL {sb}) UNION ALL ({sb} EXCEPT ALL {sa})) d"),
        params,
    ).scalar_one()


def prove_identical(
    session: Session,
    dup: Snapshot,
    survivor: Snapshot,
    piece_trips: int = DEFAULT_PIECE_TRIPS,
    log: Callable[[str], None] = print,
) -> bool:
    """Exact row-for-row content equality of ``dup`` and ``survivor`` on all six tables."""
    ok = True
    bounds = _piece_bounds(session, survivor.snapshot_id, piece_trips)
    for table in TABLES:
        if dup.counts.get(table) != survivor.counts.get(table):
            log(f"    {table}: row counts differ ({dup.counts[table]} vs {survivor.counts[table]})")
            ok = False
            continue
        if table in PIECED_TABLES:
            diff = 0
            for lo, hi in zip(bounds[:-1], bounds[1:], strict=True):
                diff += _diff_count(session, table, dup.snapshot_id, survivor.snapshot_id, lo, hi)
                session.rollback()  # end each bounded read
            pieces = len(bounds) - 1
        else:
            diff = _diff_count(session, table, dup.snapshot_id, survivor.snapshot_id)
            session.rollback()
            pieces = 1
        status = "identical" if diff == 0 else f"DIFFER ({diff} rows)"
        log(f"    {table}: {status} — {dup.counts[table]:,} rows, {pieces} piece(s)")
        ok &= diff == 0
    return ok


def _avg_row_bytes(session: Session) -> dict[str, float | None]:
    """``pg_total_relation_size / reltuples`` per versioned table (estimate)."""
    out = {}
    for table in TABLES:
        row = session.execute(
            text(
                "SELECT pg_total_relation_size(c.oid)::float8, c.reltuples::float8 "
                "FROM pg_class c WHERE c.oid = to_regclass(:t)"
            ),
            {"t": table},
        ).first()
        out[table] = row[0] / row[1] if row and row[1] and row[1] > 0 else None
    return out


def _snapshot_bytes(s: Snapshot, per_row: dict[str, float | None]) -> float:
    """Estimated bytes held by one snapshot's versioned rows."""
    return sum(s.counts[t] * (per_row.get(t) or 0) for t in TABLES)


def delete_duplicate(
    session: Session,
    dup: Snapshot,
    survivor_id: int,
    batch_size: int = DEFAULT_BATCH_SIZE,
    log: Callable[[str], None] = print,
) -> None:
    """Delete one proven (or already tagged) duplicate snapshot, resumably."""
    if dup.is_current:
        raise RuntimeError(f"snapshot {dup.snapshot_id} is is_current — never deleted")
    if dup.dedup_into is None:
        session.execute(
            text("UPDATE corridors SET gtfs_snapshot_id = :s WHERE gtfs_snapshot_id = :d"),
            {"s": survivor_id, "d": dup.snapshot_id},
        )
        session.execute(
            text("UPDATE route_service_profile SET snapshot_id = :s WHERE snapshot_id = :d"),
            {"s": survivor_id, "d": dup.snapshot_id},
        )
        for table in ("calendar_dates", "calendar"):
            session.execute(
                text(f"DELETE FROM {table} WHERE snapshot_id = :d"), {"d": dup.snapshot_id}
            )
        session.execute(
            text(
                "UPDATE gtfs_snapshots SET notes = coalesce(notes, '') || :tag "
                "WHERE snapshot_id = :d"
            ),
            {"tag": f"; dedup_into={survivor_id}", "d": dup.snapshot_id},
        )
        session.commit()
        log(f"  snapshot {dup.snapshot_id}: calendar removed, tagged dedup_into={survivor_id}")
    for table in ("stop_times", "trips", "stops", "routes"):
        total = 0
        while True:
            n = session.execute(
                text(
                    f"DELETE FROM {table} WHERE id IN (SELECT id FROM {table} "
                    "WHERE snapshot_id = :d LIMIT :n)"
                ),
                {"d": dup.snapshot_id, "n": batch_size},
            ).rowcount
            session.commit()
            total += n
            if n < batch_size:
                break
        log(f"  snapshot {dup.snapshot_id}: deleted {total:,} {table}")
    session.execute(
        text("DELETE FROM gtfs_snapshots WHERE snapshot_id = :d"), {"d": dup.snapshot_id}
    )
    session.commit()
    log(f"  snapshot {dup.snapshot_id}: gtfs_snapshots row deleted")


def export_snapshot(session: Session, s: Snapshot, agency: str, out_dir: Path) -> Path:
    """Write one snapshot's six tables as CSVs + manifest.json into a local zip; return its path."""
    out_dir.mkdir(parents=True, exist_ok=True)
    fv = re.sub(r"[^A-Za-z0-9_.-]", "_", s.feed_version or "unknown")
    path = out_dir / f"{agency}_snapshot{s.snapshot_id}_{fv}.zip"
    raw = session.connection().connection.dbapi_connection
    manifest = {
        "agency": agency,
        "snapshot_id": s.snapshot_id,
        "snapshot_date": str(s.snapshot_date),
        "feed_version": s.feed_version,
        "notes": s.notes,
        "files": {},
    }
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for table in TABLES:
            cols = ", ".join(CONTENT_COLUMNS[table])
            order = ", ".join(CONTENT_COLUMNS[table])
            sql = (
                f"COPY (SELECT {cols} FROM {table} WHERE snapshot_id = {int(s.snapshot_id)} "
                f"ORDER BY {order}) TO STDOUT WITH (FORMAT csv, HEADER true)"
            )
            digest = _HashingWriter()
            with zf.open(f"{table}.txt", "w", force_zip64=True) as member:
                digest.target = member
                with raw.cursor() as cur:
                    cur.copy_expert(sql, digest)
            manifest["files"][f"{table}.txt"] = {
                "rows": s.counts[table],
                "sha256": digest.hexdigest(),
            }
        zf.writestr("manifest.json", json.dumps(manifest, indent=1, default=str))
    return path


class _HashingWriter:
    """File-like sink for ``copy_expert`` that hashes and forwards bytes."""

    def __init__(self):
        """Start an empty sha256; ``target`` is set before writing."""
        self._h = hashlib.sha256()
        self.target = None

    def write(self, data):
        """Hash and forward one chunk (str or bytes)."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._h.update(data)
        self.target.write(data)
        return len(data)

    def hexdigest(self) -> str:
        """sha256 of everything written."""
        return self._h.hexdigest()


def _fmt_gb(n: float) -> str:
    """Bytes as ``~x.y GB``."""
    return f"~{n / 1e9:.1f} GB"


def run(args, session: Session, log: Callable[[str], None] = print) -> int:
    """Plan / prove / apply / export for one agency on ``session``; returns the exit code."""
    cfg = load_agency_config(args.agency)
    exporting = getattr(args, "command", None) == "export"
    if exporting and args.metadata_only and not args.snapshot:
        log("ERROR: export needs content fingerprints (or explicit --snapshot ids)")
        return 2
    # Fingerprints carry exact counts; metadata-only / export use stored ones first.
    snaps = load_snapshots(session, exact=False)
    session.rollback()
    per_row = _avg_row_bytes(session)
    resuming = [s for s in snaps if s.dedup_into is not None]
    live = [s for s in snaps if s.dedup_into is None]

    if not args.metadata_only and not (exporting and args.snapshot):
        for s in live:
            s.fingerprint = fingerprint(session, s.snapshot_id, log)
            s.counts = {t: n for t, n, _h0, _h1 in s.fingerprint}
            session.rollback()

    if exporting:
        if args.snapshot:
            chosen = [s for s in live if s.snapshot_id in set(args.snapshot)]
            exact = {s.snapshot_id: s for s in load_snapshots(session, exact=True)}
            for s in chosen:
                s.counts = exact[s.snapshot_id].counts
        else:
            # One representative (the de-dup survivor) per distinct content
            # whose zip was never archived — exactly what --apply would keep.
            chosen = [
                g.survivor
                for g in group_snapshots(live, lambda s: s.fingerprint)
                if not g.survivor.archived
            ]
        for s in chosen:
            path = export_snapshot(session, s, args.agency, args.out_dir)
            log(f"exported snapshot {s.snapshot_id} → {path} ({path.stat().st_size / 1e6:.1f} MB)")
        log(
            "Upload (owner step), e.g.:\n"
            f"  aws s3 cp {args.out_dir}/ s3://{cfg.s3_bucket}/gtfs-archive/{args.agency}/db-export/ "
            "--recursive --exclude '*' --include '*.zip'"
        )
        return 0

    if args.metadata_only:
        groups = group_snapshots(snaps, metadata_key)
        log("UNVERIFIED grouping by feed_version + stored row counts (never used by --apply):")
    else:
        groups = group_snapshots(snaps, lambda s: s.fingerprint)
        log("Grouping by content fingerprint (all six versioned tables):")
    first = session.execute(text("SELECT min(service_date) FROM system_metrics_daily")).scalar()
    session.rollback()
    from src.timezones import local_today

    today = local_today(cfg.timezone)
    start = date.fromisoformat(first) if first else today
    check_resolution(session, groups, cfg.timezone, start, today)
    session.rollback()

    total_bytes = 0.0
    for g in groups:
        sv = g.survivor
        label = "un-archived (DB is the only copy)" if not sv.archived else "archived"
        log(
            f"- keep snapshot {sv.snapshot_id} ({sv.feed_version}, {sv.snapshot_date:%Y-%m-%d}, "
            f"{g.survivor_reason}; {label}; {sv.counts['stop_times']:,} stop_times)"
        )
        for d in g.duplicates:
            b = _snapshot_bytes(d, per_row)
            total_bytes += b
            log(
                f"    duplicate {d.snapshot_id} ({d.snapshot_date:%Y-%m-%d}): "
                + ", ".join(f"{t}={d.counts[t]:,}" for t in TABLES)
                + f" {_fmt_gb(b)}"
            )
        if g.resolution_problem:
            log(f"    REFUSED: {g.resolution_problem}")
    for s in resuming:
        log(f"- snapshot {s.snapshot_id}: half-deleted duplicate of {s.dedup_into} (will resume)")
    deletable = [g for g in groups if g.duplicates and not g.resolution_problem]
    log(
        f"would delete {sum(len(g.duplicates) for g in deletable)} duplicate snapshot(s), "
        f"{_fmt_gb(total_bytes)} estimated; {len(groups)} distinct content(s) kept"
    )
    if not (args.prove or args.apply):
        log("DRY RUN — nothing compared row-for-row or deleted (use --prove / --apply).")
        return 0
    if args.metadata_only:
        log("ERROR: --prove/--apply need content fingerprints; drop --metadata-only")
        return 2

    rc = 0
    for g in deletable:
        for d in g.duplicates:
            log(f"proving snapshot {d.snapshot_id} == {g.survivor.snapshot_id}:")
            if not prove_identical(session, d, g.survivor, args.piece_trips, log):
                log(f"  NOT identical — snapshot {d.snapshot_id} kept")
                rc = 1
                continue
            if args.apply:
                delete_duplicate(session, d, g.survivor.snapshot_id, args.batch_size, log)
    if args.apply:
        for s in resuming:
            delete_duplicate(session, s, s.dedup_into, args.batch_size, log)
    if args.apply:
        log(
            "Done. Space is reusable now; `VACUUM (ANALYZE)` the six tables, and see "
            "docs/DEPLOYMENT.md §13 for when VACUUM FULL is worth it."
        )
    return rc


def build_parser() -> argparse.ArgumentParser:
    """CLI definition (``export`` is a subcommand; the rest are flags)."""
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--agency", required=True, choices=("wmata", "sfmta"))
    p.add_argument("--metadata-only", action="store_true", help="instant, UNVERIFIED grouping")
    p.add_argument("--prove", action="store_true", help="run the exact comparison; delete nothing")
    p.add_argument("--apply", action="store_true", help="prove, then delete proven duplicates")
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument("--piece-trips", type=int, default=DEFAULT_PIECE_TRIPS)
    sub = p.add_subparsers(dest="command")
    ex = sub.add_parser("export", help="write un-archived snapshots to local zips")
    ex.add_argument("--out-dir", type=Path, required=True)
    ex.add_argument("--snapshot", type=int, action="append", help="export exactly these ids")
    return p


def main(argv=None) -> int:
    """CLI entry point; ``--apply`` holds the refresh run lock."""
    args = build_parser().parse_args(argv)
    load_dotenv()
    from src.database import get_session

    cfg = load_agency_config(args.agency)
    session = get_session(db_url=resolve_agency_db_url(cfg))
    if session.get_bind().dialect.name != "postgresql":
        print("ERROR: Postgres only", file=sys.stderr)
        return 2
    own_lock = False
    try:
        if args.apply and not run_lock.held_by_parent(run_lock.LOCK_DIR):
            try:
                run_lock.acquire(os.getpid(), "dedup_gtfs_snapshots --apply", run_lock.LOCK_DIR)
                own_lock = True
            except run_lock.LockHeldError as exc:
                print(f"ERROR: {exc}", file=sys.stderr)
                return run_lock.EXIT_LOCKED
        return run(args, session)
    finally:
        session.close()
        if own_lock:
            run_lock.release(os.getpid(), run_lock.LOCK_DIR)


if __name__ == "__main__":
    sys.exit(main())
