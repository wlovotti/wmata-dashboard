"""
Archive-then-DELETE retention for `trip_update_snapshots` (closes NOTES-21).

Why this exists
---------------
`trip_update_snapshots` is an append-only evidence trail for the
`derive_stop_events_trip_updates` pipeline. Measured storage as of
2026-05-03: ~247 bytes/row including indexes, ~7,150 rows per 30s tick →
~20.6M rows/day (~4.74 GB/day, ~33 GB/week). Without retention, the laptop
disk fills in roughly six weeks. After the trip_update derivation pipeline
(PR #43) materializes one `stop_event` per actual arrival, the underlying
raw rows for that (trip, stop) pair can be discarded.

Why archive-then-DELETE rather than bare DELETE
-----------------------------------------------
The WMATA TripUpdates feed has no replay window — a hard DELETE is
irrecoverable. The user explicitly chose archive-then-DELETE: stream
expired rows to compressed parquet on local disk first, verify the parquet
matches the queried row count, only then DELETE. Compressed parquet of
this schema runs ~30–60 bytes/row, putting full-rate archives at ~50 GB/year
— trivial on disk, fully recoverable via `polars.read_parquet`,
`pandas.read_parquet`, or Postgres `COPY FROM` if a future audit needs
raw evidence.

Logic
-----
1. Compute `cutoff = utcnow() - timedelta(days=retention_days)` (default 14).
2. Count rows with `snapshot_ts < cutoff`. Exit cleanly if zero.
3. For each distinct UTC date < cutoff, stream those rows to
   `archive/trip_update_snapshots/<YYYY-MM-DD>.parquet` (zstd compression).
   Skip if the file already exists — the rows stay in-place for the next run.
4. Verify by reading the parquet back and asserting row count matches the
   pre-archive count. If mismatch, raise — DO NOT delete.
5. DELETE the verified rows by `snapshot_ts` range (one DELETE per day).
6. After all DELETEs succeed, run `VACUUM trip_update_snapshots` (regular,
   not FULL — keeps bloat in check without locks). VACUUM cannot run inside
   a transaction, so it uses AUTOCOMMIT isolation (same pattern as
   `scripts/add_trip_update_trip_snap_index.py`).

Usage
-----
    uv run python pipelines/archive_trip_update_snapshots.py --dry-run
    uv run python pipelines/archive_trip_update_snapshots.py
    uv run python pipelines/archive_trip_update_snapshots.py \
        --retention-days 14 --archive-dir archive/trip_update_snapshots
"""

import argparse
import sys
from datetime import date as date_type
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import text

from src.database import get_engine
from src.timezones import utcnow_naive

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ARCHIVE_DIR = REPO_ROOT / "archive" / "trip_update_snapshots"
TABLE_NAME = "trip_update_snapshots"

# Columns archived. Mirrors the `TripUpdateSnapshot` SQLAlchemy model so
# every persisted field round-trips. `id` is included so re-loaded rows
# can be cross-referenced against derived stop_events if needed.
ARCHIVE_COLUMNS = [
    "id",
    "snapshot_ts",
    "trip_id",
    "route_id",
    "vehicle_id",
    "stop_id",
    "stop_sequence",
    "predicted_arrival_ts",
    "predicted_departure_ts",
    "schedule_relationship",
    "collected_at",
]

# Explicit pyarrow schema mirroring the Postgres column types. Declared
# rather than inferred so we don't repeat the polars row-by-row inference
# bug that crashed the prior implementation on day 1 (~32M rows).
ARCHIVE_SCHEMA = pa.schema(
    [
        ("id", pa.int32()),
        ("snapshot_ts", pa.timestamp("us")),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("vehicle_id", pa.string()),
        ("stop_id", pa.string()),
        ("stop_sequence", pa.int32()),
        ("predicted_arrival_ts", pa.timestamp("us")),
        ("predicted_departure_ts", pa.timestamp("us")),
        ("schedule_relationship", pa.string()),
        ("collected_at", pa.timestamp("us")),
    ]
)

# Rows fetched per server-side cursor batch. ~50K rows × 11 columns of
# strings/ints/timestamps lands at ~25 MB Python-side memory per batch —
# well below the user's laptop pressure threshold, and large enough that
# the per-batch overhead doesn't dominate end-to-end throughput.
STREAM_BATCH_SIZE = 50_000


def compute_cutoff(retention_days: int) -> datetime:
    """Return the UTC cutoff: rows with `snapshot_ts < cutoff` are expired.

    Naive UTC matches the project-wide datetime convention (see CLAUDE.md);
    every `DateTime` column in the DB is naive UTC.
    """
    return utcnow_naive() - timedelta(days=retention_days)


def count_rows_before(engine, cutoff: datetime) -> int:
    """Return the count of rows with `snapshot_ts < cutoff`."""
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE snapshot_ts < :cutoff"),
            {"cutoff": cutoff},
        ).scalar()
    return int(result or 0)


def list_expired_dates(engine, cutoff: datetime) -> list[date_type]:
    """Return distinct UTC dates with at least one row expired vs `cutoff`.

    `DATE(snapshot_ts)` interprets the timestamp as UTC because the column
    is naive UTC — there's no timezone conversion to worry about. Returned
    in ascending order so the per-date archive loop processes oldest first.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT DISTINCT DATE(snapshot_ts) AS d FROM {TABLE_NAME} "
                "WHERE snapshot_ts < :cutoff ORDER BY d"
            ),
            {"cutoff": cutoff},
        ).all()
    return [r[0] for r in rows]


def count_rows_for_date(engine, day: date_type) -> int:
    """Return the count of rows whose `snapshot_ts` falls on `day` (UTC)."""
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    with engine.connect() as conn:
        result = conn.execute(
            text(
                f"SELECT COUNT(*) FROM {TABLE_NAME} "
                "WHERE snapshot_ts >= :start AND snapshot_ts < :end"
            ),
            {"start": start, "end": end},
        ).scalar()
    return int(result or 0)


def archive_date(engine, day: date_type, archive_path: Path) -> int:
    """Stream all rows for `day` (UTC) to `archive_path` as zstd parquet.

    Uses a psycopg2 server-side named cursor to fetch in fixed-size batches
    and pyarrow's ParquetWriter to write each batch as a row group. Memory
    is bounded by ``STREAM_BATCH_SIZE`` (~25 MB per batch) rather than the
    full day, which avoids the OOM/swap behavior the prior polars-based
    implementation showed on this dev laptop. The explicit ``ARCHIVE_SCHEMA``
    bypasses schema-inference entirely, which was the failure mode polars'
    row-by-row reader hit (couldn't reconcile NULL prediction columns with
    later non-null datetime values).

    Returns the row count that was written.
    """
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    columns_sql = ", ".join(ARCHIVE_COLUMNS)
    rows_written = 0

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor(name=f"archive_stream_{day.isoformat()}") as cursor:
            cursor.itersize = STREAM_BATCH_SIZE
            cursor.execute(
                f"SELECT {columns_sql} FROM {TABLE_NAME} "
                "WHERE snapshot_ts >= %s AND snapshot_ts < %s "
                "ORDER BY snapshot_ts, id",
                (start, end),
            )
            with pq.ParquetWriter(archive_path, ARCHIVE_SCHEMA, compression="zstd") as writer:
                while True:
                    rows = cursor.fetchmany(STREAM_BATCH_SIZE)
                    if not rows:
                        break
                    # Transpose row tuples to per-column lists and build a
                    # RecordBatch with the explicit schema. pyarrow coerces
                    # Python None → null and naive datetime → timestamp[us].
                    columns_data = list(zip(*rows, strict=True))
                    arrays = [
                        pa.array(col, type=ARCHIVE_SCHEMA.field(i).type)
                        for i, col in enumerate(columns_data)
                    ]
                    writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=ARCHIVE_SCHEMA))
                    rows_written += len(rows)
    finally:
        raw_conn.close()

    return rows_written


def verify_archive(archive_path: Path, expected_rows: int) -> tuple[bool, int]:
    """Confirm parquet row count matches ``expected_rows``.

    Reads only ParquetFile metadata so no row data is materialized.
    """
    actual = pq.ParquetFile(archive_path).metadata.num_rows
    return actual == expected_rows, int(actual)


def delete_date(engine, day: date_type) -> int:
    """DELETE all rows whose `snapshot_ts` falls on `day` (UTC).

    Caller MUST have verified the corresponding parquet first — this is the
    irreversible step. Uses an explicit transaction so the row count we
    return is the count actually deleted.
    """
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {TABLE_NAME} WHERE snapshot_ts >= :start AND snapshot_ts < :end"),
            {"start": start, "end": end},
        )
    return int(result.rowcount or 0)


def vacuum_table(engine) -> None:
    """Run `VACUUM trip_update_snapshots` (NOT FULL).

    Regular vacuum reclaims space from the just-deleted rows for reuse by
    future INSERTs and updates statistics, all under a SHARE UPDATE
    EXCLUSIVE lock that doesn't block concurrent reads or writes. VACUUM
    FULL would rewrite the table and acquire an ACCESS EXCLUSIVE lock —
    inappropriate for a high-churn table the collector is writing to every
    30s.

    VACUUM cannot run inside a transaction; use AUTOCOMMIT isolation. Same
    pattern as `scripts/add_trip_update_trip_snap_index.py` for CREATE
    INDEX CONCURRENTLY.
    """
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"VACUUM {TABLE_NAME}"))


def format_size(path: Path) -> str:
    """Return a human-readable size string (MB) for `path`."""
    bytes_ = path.stat().st_size
    mb = bytes_ / (1024 * 1024)
    return f"{mb:,.1f} MB"


def archive_and_delete(
    retention_days: int = 14,
    archive_dir: Path = DEFAULT_ARCHIVE_DIR,
    dry_run: bool = False,
) -> int:
    """Drive one archive-then-DELETE cycle. Returns 0 on success, non-zero on failure.

    Failure modes that produce a non-zero return:
        - Verification mismatch (parquet row count != queried row count) for
          any day — DELETE is skipped for that day and the function returns 1
          after attempting the remaining days.

    A skipped day (parquet already exists) is not a failure — those rows
    stay in-place until next run.
    """
    cutoff = compute_cutoff(retention_days)
    print(
        f"archive_trip_update_snapshots: retention={retention_days}d, "
        f"cutoff={cutoff.isoformat()}, archive_dir={archive_dir}"
    )
    if dry_run:
        print("(dry-run mode — no parquet files will be written, no rows deleted)")

    engine = get_engine()
    total_expired = count_rows_before(engine, cutoff)
    if total_expired == 0:
        print("No rows older than cutoff. Nothing to do.")
        return 0
    print(f"Total expired rows: {total_expired:,}")

    expired_dates = list_expired_dates(engine, cutoff)
    print(f"Distinct expired dates: {len(expired_dates)}")

    if not dry_run:
        archive_dir.mkdir(parents=True, exist_ok=True)

    failures = 0
    archived_dates: list[date_type] = []

    for day in expired_dates:
        archive_path = archive_dir / f"{day.isoformat()}.parquet"
        expected = count_rows_for_date(engine, day)

        if archive_path.exists():
            print(
                f"skip {day.isoformat()}: archive already exists at "
                f"{archive_path} ({format_size(archive_path)}); rows left in-place"
            )
            continue

        if dry_run:
            print(
                f"DRY-RUN {day.isoformat()}: would archive {expected:,} rows "
                f"→ {archive_path} (then DELETE from {TABLE_NAME})"
            )
            continue

        # Archive.
        written = archive_date(engine, day, archive_path)
        if written != expected:
            # Race between count and read shouldn't happen — there's no
            # writer producing rows older than 14 days — but if it does,
            # surface it loudly.
            print(
                f"FAIL {day.isoformat()}: archive_date wrote {written:,} rows "
                f"but COUNT(*) was {expected:,}. Refusing to DELETE.",
                file=sys.stderr,
            )
            failures += 1
            continue

        # Verify.
        ok, actual = verify_archive(archive_path, expected)
        if not ok:
            print(
                f"FAIL {day.isoformat()}: parquet has {actual:,} rows, "
                f"expected {expected:,}. Refusing to DELETE.",
                file=sys.stderr,
            )
            failures += 1
            continue

        # DELETE.
        deleted = delete_date(engine, day)
        archived_dates.append(day)
        print(
            f"archived {day.isoformat()}: {written:,} rows → "
            f"{archive_path} ({format_size(archive_path)}), "
            f"deleted {deleted:,} from {TABLE_NAME}"
        )

    if archived_dates and not dry_run:
        print(f"running VACUUM {TABLE_NAME}...")
        vacuum_table(engine)
        print("VACUUM complete.")

    if failures:
        print(f"{failures} day(s) failed verification — see errors above", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description=(
            "Archive expired rows from trip_update_snapshots to compressed "
            "parquet, then DELETE. Verifies parquet round-trip before any "
            "destructive operation."
        )
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=14,
        help="Rows with snapshot_ts older than this many days are archived (default: 14).",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=DEFAULT_ARCHIVE_DIR,
        help=f"Directory for archive parquet files (default: {DEFAULT_ARCHIVE_DIR}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan without writing parquet or deleting rows.",
    )
    args = parser.parse_args()

    load_dotenv()
    return archive_and_delete(
        retention_days=args.retention_days,
        archive_dir=args.archive_dir,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())
