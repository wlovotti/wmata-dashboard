"""Restore a day of ``vehicle_positions`` from the S3 parquet archive.

Inverse of ``pipelines/archive_vehicle_positions.py``: downloads
``s3://<bucket>/wmata-vp-archive/<date>.parquet`` (or reads ``--file``)
and inserts the rows back with their original ids, skipping ids already
present (``ON CONFLICT DO NOTHING`` — safe to re-run, safe on overlap
with synced data). Built for the 6/11–6/12 fold-in of the 2026-07
recovery (spec 2026-07-14, Phase 2 step 5).

Usage:
    uv run python pipelines/load_vp_from_parquet.py --date 2026-06-11
    uv run python pipelines/load_vp_from_parquet.py --date 2026-06-11 --file /tmp/x.parquet
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from pipelines.archive_vehicle_positions import KEY_PREFIX
from src.database import get_session
from src.models import VehiclePosition

BATCH_SIZE = 50_000


def load_parquet_into_vp(db: Session, table: pa.Table) -> int:
    """Insert archived rows into ``vehicle_positions``; return rows inserted.

    Original ``id`` values are preserved; conflicts are skipped so the
    loader composes with synced data and with itself. Note: this is a
    restore tool, not a pipeline upsert — ``upsert_rows`` (the standard
    helper) always updates on conflict, which is wrong here, hence the
    direct ``ON CONFLICT DO NOTHING``. Commits once per BATCH_SIZE chunk.
    """
    inserted = 0
    rows = table.to_pylist()
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        stmt = (
            pg_insert(VehiclePosition).values(chunk).on_conflict_do_nothing(index_elements=["id"])
        )
        result = db.execute(stmt)
        inserted += result.rowcount
        db.commit()
    # Keep the sequence ahead of explicit ids so future inserts can't collide.
    db.execute(
        text(
            "SELECT setval('vehicle_positions_id_seq', "
            "(SELECT COALESCE(MAX(id), 1) FROM vehicle_positions))"
        )
    )
    db.commit()
    return inserted


def main() -> int:
    """CLI entry: fetch the day's parquet (S3 or --file) and load it."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC archive day)")
    parser.add_argument("--file", help="Local parquet path (skips S3 download)")
    parser.add_argument("--bucket", default=os.environ.get("S3_ARCHIVE_BUCKET"))
    args = parser.parse_args()

    if args.file:
        path = Path(args.file)
    else:
        if not args.bucket:
            print("No --bucket and S3_ARCHIVE_BUCKET unset", file=sys.stderr)
            return 2
        key = f"{KEY_PREFIX}/{args.date}.parquet"
        path = Path(tempfile.mkdtemp()) / f"{args.date}.parquet"
        print(f"Downloading s3://{args.bucket}/{key} ...")
        boto3.client("s3").download_file(args.bucket, key, str(path))

    table = pq.read_table(path)
    db = get_session()
    try:
        inserted = load_parquet_into_vp(db, table)
    finally:
        db.close()
    print(f"{args.date}: {table.num_rows} rows in file, {inserted} inserted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
