"""Nightly rotation: yesterday's JSONL archive -> parquet -> B2.

Discovers all JSONL source files for a given date, converts them to a
single merged parquet, uploads to the configured B2 bucket, verifies the
upload object size, and deletes local files only after a successful
verification.

**Source file naming:**

The writer produces per-process files:
``YYYY-MM-DD.<pid>.<startup_unix_ts>.jsonl.zst``

For backward-compatibility the rotator also handles the legacy single-day
format ``YYYY-MM-DD.jsonl.zst`` that was written before the per-process
scheme was introduced. Both patterns are discovered by
``_find_jsonl_files_for_date``, so nothing is orphaned mid-transition.

Each source file is decompressed with ``read_across_frames=True`` so a
single-process file with a trailing partial frame (ungraceful shutdown) is
still decoded as far as possible. After all source files are merged into
parquet, each is size-verified and deleted individually — a partial failure
leaves the remaining sources intact for retry.

Designed for idempotent re-runs: if the parquet has already been
uploaded for a date, re-running uploads again (overwriting) and deletes
local files (no-op if already gone). If no JSONL exists for the
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

from src.timezones import utcnow_naive

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


def _find_jsonl_files_for_date(archive_dir: Path, target_date: date) -> list[Path]:
    """Return all JSONL source files for ``target_date``, sorted by filename.

    Matches both formats:
    - Legacy single-day:   ``YYYY-MM-DD.jsonl.zst``
    - Per-process (current): ``YYYY-MM-DD.<pid>.<startup_ts>.jsonl.zst``

    Files are returned in ascending filename order so earlier-started
    processes are merged first (deterministic across runs).
    """
    date_str = target_date.isoformat()
    # Glob picks up both ``2026-05-17.jsonl.zst`` and
    # ``2026-05-17.12345.1747440000.jsonl.zst``.
    matches = sorted(archive_dir.glob(f"{date_str}*.jsonl.zst"))
    return matches


def _read_jsonl_zst(jsonl_path: Path) -> bytes:
    """Decompress a ZSTD-compressed JSONL file and return raw bytes.

    Uses ``stream_reader`` (not ``decompress``) because the streaming
    compressor used by JsonlArchiveWriter does not embed a content-size
    header. ``read_across_frames=True`` handles the case where a process
    flushed multiple complete frames before exiting (e.g., a very long-lived
    run), or where the final frame is missing its footer due to an ungraceful
    shutdown — the decoder reads as far as it can and does not raise.
    """
    dctx = zstd.ZstdDecompressor()
    with open(jsonl_path, "rb") as f:
        with dctx.stream_reader(f, read_across_frames=True) as reader:
            return reader.read()


def jsonl_to_parquet(jsonl_path: Path, parquet_path: Path) -> int:
    """Read a single ZSTD-compressed JSONL file, write zstd-compressed parquet.

    Returns the row count written. Kept for backward compatibility and
    direct testing; ``rotate_one_day`` uses ``_merge_jsonl_files_to_parquet``
    when multiple source files may exist.
    """
    decompressed = _read_jsonl_zst(jsonl_path)
    df = pl.read_ndjson(decompressed)
    df.write_parquet(parquet_path, compression="zstd")
    return df.height


def _merge_jsonl_files_to_parquet(jsonl_paths: list[Path], parquet_path: Path) -> int:
    """Decompress and merge all ``jsonl_paths`` into a single parquet file.

    Each source file is decompressed in order (ascending filename = ascending
    startup time for per-process files). The resulting DataFrames are
    concatenated and written as a single zstd-compressed parquet.

    Returns the total row count across all source files.
    """
    frames = []
    for p in jsonl_paths:
        decompressed = _read_jsonl_zst(p)
        if decompressed:
            frames.append(pl.read_ndjson(decompressed))
        else:
            print(f"  Warning: {p.name} decompressed to empty bytes — skipping")
    if not frames:
        df = pl.DataFrame()
    else:
        df = pl.concat(frames, how="diagonal_relaxed")
    df.write_parquet(parquet_path, compression="zstd")
    return df.height


def rotate_one_day(
    archive_dir: Path,
    target_date: date,
    bucket: str,
    key_prefix: str,
) -> None:
    """Convert one day's JSONL source files to parquet, upload, verify, clean up.

    Discovers all JSONL files for ``target_date`` (both legacy single-file
    and new per-process filenames), merges them into a single parquet,
    uploads to S3/B2, verifies the upload size, then deletes each source
    file and the local parquet individually.

    Raises ``RuntimeError`` if the uploaded object size doesn't match
    local — caller can retry without losing data. Any source files not yet
    deleted are left intact for the retry.
    """
    jsonl_paths = _find_jsonl_files_for_date(archive_dir, target_date)
    parquet_path = archive_dir / f"{target_date.isoformat()}.parquet"

    if not jsonl_paths:
        print(f"No JSONL archive for {target_date.isoformat()}, nothing to rotate.")
        return

    print(f"Found {len(jsonl_paths)} JSONL source file(s) for {target_date.isoformat()}:")
    for p in jsonl_paths:
        print(f"  {p.name} ({p.stat().st_size:,} bytes)")

    rows = _merge_jsonl_files_to_parquet(jsonl_paths, parquet_path)
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

    # Verified — delete each source JSONL and the local parquet.
    for p in jsonl_paths:
        p.unlink()
        print(f"  Deleted {p.name}")
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
        target = (utcnow_naive() - timedelta(days=1)).date()

    rotate_one_day(
        archive_dir=args.archive_dir,
        target_date=target,
        bucket=bucket,
        key_prefix="raw_snapshots",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
