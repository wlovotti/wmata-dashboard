"""Tier-3 retention: archive aged `vehicle_positions` to S3 parquet, then DELETE (NOTES-48 §3.5).

`vehicle_positions` is the raw GPS feed and the project's largest table. It is
the only tier with a SHORT in-DB window (30 days) because its single live
consumer — the nightly proximity derivation (`pipelines/derive_stop_events.py`)
— runs within ~1 day of collection. Older rows live only as compressed parquet
in S3 and are reloaded only for re-derivation.

Flow, per UTC date older than the cutoff (oldest first):
  1. Stage the day's rows to a local zstd parquet via a server-side cursor
     (bounded memory — the streaming pattern from PR #131 / the
     trip_update_snapshots archiver; never polars row-by-row inference).
  2. Verify the parquet row count == the queried count.
  3. Upload to S3 and verify the uploaded object size == local size.
  4. ONLY THEN DELETE the day's rows from Postgres.
  5. Delete the local staging parquet.
After all days, run a regular (non-FULL) VACUUM so the collector's concurrent
writes are never blocked.

Columns: only the 14 the pruned model persists (PR #152). The five dropped
GTFS-RT fields are intentionally absent from the archive.

S3, not B2: spec §3.4 chose AWS S3 for one-bill/one-console consistency. This
job uses the standard boto3 AWS credential chain (env / shared config /
instance profile) and `S3_ARCHIVE_BUCKET`. `S3_ENDPOINT_URL`, if set, overrides
the endpoint (point at a local S3-compatible store in development); unset means
real AWS. The existing `rotate_archive.py` B2 flow is separate and unchanged.

Partitioning is by UTC date (`DATE(timestamp)`), matching the
trip_update_snapshots archiver. An Eastern service day therefore spans two
UTC-date parquet files; re-derivation reads both — an accepted trade for
pattern consistency.

Usage:
    uv run python pipelines/archive_vehicle_positions.py --dry-run
    uv run python pipelines/archive_vehicle_positions.py
    uv run python pipelines/archive_vehicle_positions.py --retention-days 30
"""

import argparse
import os
import sys
from datetime import date as date_type
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.database import get_engine
from src.timezones import utcnow_naive

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_STAGING_DIR = REPO_ROOT / "archive" / "vehicle_positions"
TABLE_NAME = "vehicle_positions"
TS_COLUMN = "timestamp"
# Must stay under the wmata-vp-archive/ prefix — the wmata-vm-backup IAM
# user (deployment/aws/s3-backup-policy.json) grants PutObject only there.
KEY_PREFIX = "wmata-vp-archive"

# The 14 columns the pruned VehiclePosition model persists (PR #152). The five
# dropped GTFS-RT fields are intentionally excluded.
ARCHIVE_COLUMNS = [
    "id",
    "vehicle_id",
    "route_id",
    "trip_id",
    "latitude",
    "longitude",
    "speed",
    "current_stop_sequence",
    "stop_id",
    "current_status",
    "direction_id",
    "trip_start_date",
    "timestamp",
    "collected_at",
]

# Explicit pyarrow schema mirroring the Postgres column types. Declared, not
# inferred, to avoid the polars row-by-row inference failure (NULL columns
# reconciled against later non-null values) that motivated PR #131. `id` is
# int64: at ~900K rows/day the int32 ceiling (~2.1B) would be reached in ~6
# years, so int64 is the safe choice for an append-only archive.
ARCHIVE_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("vehicle_id", pa.string()),
        ("route_id", pa.string()),
        ("trip_id", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("speed", pa.float64()),
        ("current_stop_sequence", pa.int32()),
        ("stop_id", pa.string()),
        ("current_status", pa.int32()),
        ("direction_id", pa.int32()),
        ("trip_start_date", pa.string()),
        ("timestamp", pa.timestamp("us")),
        ("collected_at", pa.timestamp("us")),
    ]
)

# Rows per server-side fetch batch. ~50K rows × 14 mostly-narrow columns lands
# well under the laptop's memory-pressure threshold while amortising per-batch
# overhead.
STREAM_BATCH_SIZE = 50_000


def compute_cutoff(retention_days: int = 30) -> datetime:
    """Return the naive-UTC cutoff: rows with `timestamp < cutoff` are expired.

    Naive UTC matches the project-wide storage convention — every `DateTime`
    column in the DB holds naive UTC (CLAUDE.md).
    """
    return utcnow_naive() - timedelta(days=retention_days)


def verify_parquet(parquet_path: Path, expected_rows: int) -> tuple[bool, int]:
    """Confirm a parquet file's row count matches ``expected_rows``.

    Reads only the ParquetFile metadata, so no row data is materialised.
    Returns ``(matches, actual_row_count)``.
    """
    actual = pq.ParquetFile(parquet_path).metadata.num_rows
    return actual == expected_rows, int(actual)


def _make_s3_client():
    """Build a boto3 S3 client for AWS S3 (spec §3.4).

    Uses the standard AWS credential chain (env vars / shared config /
    instance profile on the VM). If ``S3_ENDPOINT_URL`` is set it overrides the
    endpoint — useful to target a local S3-compatible store in development;
    unset means real AWS S3.
    """
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    kwargs = {"endpoint_url": endpoint} if endpoint else {}
    return boto3.client("s3", **kwargs)


def upload_and_verify(s3, parquet_path: Path, bucket: str, key: str) -> None:
    """Upload ``parquet_path`` to ``s3://bucket/key`` and verify the object size.

    Raises ``RuntimeError`` if the uploaded object's ``ContentLength`` does not
    match the local file size. The caller MUST treat this as "do not DELETE" —
    the rows stay in Postgres for the next run.
    """
    local_size = parquet_path.stat().st_size
    s3.upload_file(str(parquet_path), bucket, key)
    remote_size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if remote_size != local_size:
        raise RuntimeError(
            f"Upload size mismatch for s3://{bucket}/{key}: "
            f"local={local_size}, remote={remote_size}"
        )


def count_rows_before(conn: Connection, cutoff: datetime) -> int:
    """Return the count of rows with `timestamp < cutoff` on ``conn``'s transaction."""
    result = conn.execute(
        text(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {TS_COLUMN} < :cutoff"),
        {"cutoff": cutoff},
    ).scalar()
    return int(result or 0)


def list_expired_dates(conn: Connection, cutoff: datetime) -> list[date_type]:
    """Return distinct UTC dates (`DATE(timestamp)`) with ≥1 row older than ``cutoff``.

    `timestamp` is naive UTC, so `DATE(timestamp)` is the UTC calendar date with
    no timezone conversion. Ascending order so the archive loop processes the
    oldest day first.
    """
    rows = conn.execute(
        text(
            f"SELECT DISTINCT DATE({TS_COLUMN}) AS d FROM {TABLE_NAME} "
            f"WHERE {TS_COLUMN} < :cutoff ORDER BY d"
        ),
        {"cutoff": cutoff},
    ).all()
    return [r[0] for r in rows]


def count_rows_for_date(conn: Connection, day: date_type) -> int:
    """Return the count of rows whose `timestamp` falls on `day` (UTC)."""
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    result = conn.execute(
        text(
            f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {TS_COLUMN} >= :start AND {TS_COLUMN} < :end"
        ),
        {"start": start, "end": end},
    ).scalar()
    return int(result or 0)


def archive_day_to_parquet(conn: Connection, day: date_type, parquet_path: Path) -> int:
    """Stream all rows for `day` (UTC) on ``conn`` to ``parquet_path`` as zstd parquet.

    Uses a psycopg2 named server-side cursor (``conn.connection.cursor(name=...)``) so
    memory is bounded by ``STREAM_BATCH_SIZE`` rather than the whole day — the same
    bounded-memory streaming as PR #131. Accessing the raw DBAPI connection via
    ``conn.connection`` (rather than using SQLAlchemy's ``stream_results=True`` on the
    Connection object) avoids permanently tainting ``conn``'s execution options:
    ``Connection.execution_options()`` mutates in-place and would cause all subsequent
    DML on the same ``conn`` (DELETE, ROLLBACK TO SAVEPOINT) to be wrapped in a server-
    side cursor declaration — which Postgres rejects for non-SELECT statements.
    The named cursor runs inside the caller's transaction so the savepoint-based rollback
    in ``pg_session`` teardown still cleans up. Rows are written with the explicit
    ``ARCHIVE_SCHEMA`` (no inference). Returns the row count written.
    """
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    columns_sql = ", ".join(ARCHIVE_COLUMNS)
    sql = (
        f"SELECT {columns_sql} FROM {TABLE_NAME} "
        f"WHERE {TS_COLUMN} >= %s AND {TS_COLUMN} < %s "
        f"ORDER BY {TS_COLUMN}, id"
    )
    rows_written = 0
    # `conn.connection` is the underlying psycopg2 connection; the named cursor
    # opens a server-side cursor within the caller's transaction without modifying
    # the SQLAlchemy Connection object.
    raw_conn = conn.connection
    with raw_conn.cursor(name=f"archive_vp_{day.isoformat()}") as cursor:
        cursor.itersize = STREAM_BATCH_SIZE
        cursor.execute(sql, (start, end))
        with pq.ParquetWriter(parquet_path, ARCHIVE_SCHEMA, compression="zstd") as writer:
            while True:
                rows = cursor.fetchmany(STREAM_BATCH_SIZE)
                if not rows:
                    break
                # Transpose row tuples to per-column lists, coerce to the explicit
                # schema (None → null, naive datetime → timestamp[us]).
                columns_data = list(zip(*rows, strict=True))
                arrays = [
                    pa.array(col, type=ARCHIVE_SCHEMA.field(i).type)
                    for i, col in enumerate(columns_data)
                ]
                writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=ARCHIVE_SCHEMA))
                rows_written += len(rows)
    return rows_written


def delete_day(conn: Connection, day: date_type) -> int:
    """DELETE all rows whose `timestamp` falls on `day` (UTC) on ``conn``.

    The caller MUST have archived + verified + uploaded the day first — this is
    the irreversible step. Does NOT commit; the caller (the orchestrator)
    commits per day so a later failure cannot lose already-archived days.
    Returns the number of rows deleted.
    """
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    result = conn.execute(
        text(f"DELETE FROM {TABLE_NAME} WHERE {TS_COLUMN} >= :start AND {TS_COLUMN} < :end"),
        {"start": start, "end": end},
    )
    return int(result.rowcount or 0)


def vacuum_table(engine) -> None:
    """Run a regular `VACUUM vehicle_positions` (NOT FULL).

    Reclaims the just-deleted rows for reuse and refreshes statistics under a
    SHARE UPDATE EXCLUSIVE lock that does not block the collector's concurrent
    INSERTs. VACUUM cannot run inside a transaction, so it uses AUTOCOMMIT —
    the same pattern as the trip_update_snapshots archiver.
    """
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"VACUUM {TABLE_NAME}"))


def _format_size(path: Path) -> str:
    """Return a human-readable MB size string for ``path``."""
    return f"{path.stat().st_size / (1024 * 1024):,.1f} MB"


def archive_and_delete(
    retention_days: int = 30,
    staging_dir: Path = DEFAULT_STAGING_DIR,
    bucket: str | None = None,
    dry_run: bool = False,
) -> int:
    """Drive one archive→verify→upload→delete cycle. Returns 0 on success, 1 on any failure.

    For each UTC date older than the cutoff (oldest first): stage to local
    parquet, verify the row count, upload to S3 and verify the object size, then
    DELETE the day and remove the local staging file. A day that fails archive,
    verify, or upload is left intact (rows stay in Postgres) and counts as a
    failure. After all successful deletes, a regular VACUUM runs.

    Args:
        retention_days: Rows with `timestamp` older than this are archived
            (default 30, spec §3.5).
        staging_dir: Local directory for the transient parquet files (deleted
            after a successful upload).
        bucket: S3 bucket name; falls back to ``S3_ARCHIVE_BUCKET`` when None.
        dry_run: Print the plan without writing parquet, uploading, or deleting.

    Returns:
        0 if every expired day was archived+deleted (or there was nothing to
        do, or dry-run); 1 if any day failed verification or upload.
    """
    cutoff = compute_cutoff(retention_days)
    bucket = bucket or os.environ.get("S3_ARCHIVE_BUCKET")
    print(
        f"archive_vehicle_positions: retention={retention_days}d, "
        f"cutoff={cutoff.isoformat()}, staging_dir={staging_dir}, bucket={bucket}"
    )
    if dry_run:
        print("(dry-run mode — no parquet written, no uploads, no rows deleted)")
    if not dry_run and not bucket:
        print("ERROR: no S3 bucket (set --bucket or S3_ARCHIVE_BUCKET).", file=sys.stderr)
        return 1

    engine = get_engine()
    failures = 0
    archived_dates: list[date_type] = []

    with engine.connect() as conn:
        total = count_rows_before(conn, cutoff)
        if total == 0:
            print("No rows older than cutoff. Nothing to do.")
            return 0
        print(f"Total expired rows: {total:,}")
        expired_dates = list_expired_dates(conn, cutoff)
        print(f"Distinct expired UTC dates: {len(expired_dates)}")

        if not dry_run:
            staging_dir.mkdir(parents=True, exist_ok=True)
            s3 = _make_s3_client()

        for day in expired_dates:
            expected = count_rows_for_date(conn, day)
            parquet_path = staging_dir / f"{day.isoformat()}.parquet"
            key = f"{KEY_PREFIX}/{day.isoformat()}.parquet"

            if dry_run:
                print(
                    f"DRY-RUN {day.isoformat()}: would archive {expected:,} rows "
                    f"→ s3://{bucket}/{key}, then DELETE from {TABLE_NAME}"
                )
                continue

            written = archive_day_to_parquet(conn, day, parquet_path)
            ok, actual = verify_parquet(parquet_path, expected)
            if written != expected or not ok:
                print(
                    f"FAIL {day.isoformat()}: wrote {written:,}, parquet has {actual:,}, "
                    f"expected {expected:,}. Refusing to upload/DELETE.",
                    file=sys.stderr,
                )
                failures += 1
                parquet_path.unlink(missing_ok=True)
                continue

            try:
                upload_and_verify(s3, parquet_path, bucket, key)
            except Exception as exc:
                # Broad on purpose: any S3 failure for THIS day — the
                # size-mismatch RuntimeError, a botocore ClientError (auth,
                # throttling, missing bucket), or a network drop — must degrade
                # to "this day failed, keep going", never abort the whole run or
                # fall through to delete_day. Rows stay in Postgres for retry.
                print(
                    f"FAIL {day.isoformat()}: S3 upload failed ({exc}). Refusing to DELETE.",
                    file=sys.stderr,
                )
                failures += 1
                parquet_path.unlink(missing_ok=True)
                continue

            size_str = _format_size(parquet_path) if parquet_path.exists() else "n/a"
            deleted = delete_day(conn, day)
            conn.commit()  # commit per day: a later failure cannot lose this day
            archived_dates.append(day)
            print(
                f"archived {day.isoformat()}: {written:,} rows → s3://{bucket}/{key} "
                f"({size_str}), deleted {deleted:,} from {TABLE_NAME}"
            )
            parquet_path.unlink(missing_ok=True)

    if archived_dates and not dry_run:
        print(f"running VACUUM {TABLE_NAME}...")
        vacuum_table(engine)
        print("VACUUM complete.")

    if failures:
        print(f"{failures} day(s) failed — see errors above", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description=(
            "Archive expired vehicle_positions rows to S3 parquet, then DELETE. "
            "Verifies the parquet round-trip and the S3 object size before any "
            "destructive operation."
        )
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=30,
        help="Rows with timestamp older than this many days are archived (default: 30).",
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=DEFAULT_STAGING_DIR,
        help=f"Local staging directory for transient parquet (default: {DEFAULT_STAGING_DIR}).",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="S3 bucket (overrides S3_ARCHIVE_BUCKET).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan without writing parquet, uploading, or deleting.",
    )
    args = parser.parse_args()

    load_dotenv()
    return archive_and_delete(
        retention_days=args.retention_days,
        staging_dir=args.staging_dir,
        bucket=args.bucket,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())
