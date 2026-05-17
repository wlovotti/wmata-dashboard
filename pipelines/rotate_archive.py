"""Nightly rotation: yesterday's JSONL archive -> parquet -> B2.

Reads ``archive/raw_snapshots/<date>.jsonl.zst``, converts to parquet
with zstd compression, uploads to the configured B2 bucket, verifies the
upload object size, and deletes local files only after a successful
verification.

Designed for idempotent re-runs: if the parquet has already been
uploaded for a date, re-running uploads again (overwriting) and deletes
local files (no-op if already gone). If the JSONL is missing for the
target date, exits 0 — nothing to do.

Usage:
    uv run python pipelines/rotate_archive.py --date 2026-05-17
    uv run python pipelines/rotate_archive.py            # defaults to yesterday UTC
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
import polars as pl
import zstandard as zstd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ARCHIVE_DIR = REPO_ROOT / "archive" / "raw_snapshots"


def _make_s3_client():
    """Build a boto3 S3 client pointed at B2.

    Reads B2_ACCESS_KEY_ID / B2_SECRET_ACCESS_KEY / B2_ENDPOINT_URL from
    environment (loaded via dotenv at main()).
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ["B2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["B2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["B2_SECRET_ACCESS_KEY"],
    )


def jsonl_to_parquet(jsonl_path: Path, parquet_path: Path) -> int:
    """Read ZSTD-compressed JSONL (streaming format), write zstd-compressed parquet.

    Uses ``stream_reader`` (not ``decompress``) so it works on the
    streaming-frame output produced by JsonlArchiveWriter, which has no
    content-size header that ``decompress`` would require.

    Returns the row count written. Uses polars for the actual conversion
    so the schema is inferred from the JSONL contents.
    """
    dctx = zstd.ZstdDecompressor()
    with open(jsonl_path, "rb") as f:
        with dctx.stream_reader(f) as reader:
            decompressed = reader.read().decode("utf-8")
    df = pl.read_ndjson(decompressed.encode("utf-8"))
    df.write_parquet(parquet_path, compression="zstd")
    return df.height


def rotate_one_day(
    archive_dir: Path,
    target_date: date,
    bucket: str,
    key_prefix: str,
) -> None:
    """Convert one day's JSONL to parquet, upload, verify, clean up.

    Raises ``RuntimeError`` if the uploaded object size doesn't match
    local — caller can retry without losing data.
    """
    jsonl_path = archive_dir / f"{target_date.isoformat()}.jsonl.zst"
    parquet_path = archive_dir / f"{target_date.isoformat()}.parquet"

    if not jsonl_path.exists():
        print(f"No JSONL archive for {target_date.isoformat()}, nothing to rotate.")
        return

    rows = jsonl_to_parquet(jsonl_path, parquet_path)
    local_size = parquet_path.stat().st_size
    print(f"Wrote {parquet_path.name}: {rows:,} rows, {local_size:,} bytes")

    s3 = _make_s3_client()
    key = f"{key_prefix}/{target_date.isoformat()}.parquet"
    s3.upload_file(str(parquet_path), bucket, key)
    print(f"Uploaded to s3://{bucket}/{key}")

    head = s3.head_object(Bucket=bucket, Key=key)
    remote_size = head["ContentLength"]
    if remote_size != local_size:
        raise RuntimeError(
            f"Upload size mismatch for {key}: local={local_size}, remote={remote_size}"
        )

    # Verified — delete local files.
    jsonl_path.unlink()
    parquet_path.unlink()
    print(f"Cleaned up local files for {target_date.isoformat()}")


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description=("Rotate yesterday's JSONL archive to parquet and upload to B2.")
    )
    parser.add_argument(
        "--date",
        help="UTC date to rotate (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=DEFAULT_ARCHIVE_DIR,
        help=f"Path to archive directory (default: {DEFAULT_ARCHIVE_DIR})",
    )
    args = parser.parse_args()

    load_dotenv()
    bucket = os.environ["B2_ARCHIVE_BUCKET"]

    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target = (datetime.utcnow() - timedelta(days=1)).date()

    rotate_one_day(
        archive_dir=args.archive_dir,
        target_date=target,
        bucket=bucket,
        key_prefix="raw_snapshots",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
