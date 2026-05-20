"""Replay archived JSONL snapshots into ``trip_update_state``.

The JSONL archive (``archive/raw_snapshots/<date>.*.jsonl.zst``) is the
only source of truth for any historical service_date once the
``trip_update_state`` row for the trip has been overwritten by a later
day's snapshot. This tool reads those files and replays each line
through the same UPSERT helper (``upsert_trip_update_state``) the live
collector uses.

The tool is idempotent: re-running for the same date produces the same
end state, because the UPSERT formulas are deterministic functions of
the input sequence.

The tool is cross-date safe: rows for service_dates other than the
target are silently skipped, so backfilling 2026-05-18 cannot corrupt
the 2026-05-19 rows the running collector is writing.

Usage:
    uv run python pipelines/replay_archive_to_state.py --date 2026-05-18
    uv run python pipelines/replay_archive_to_state.py --date 2026-05-18 \\
        --archive-root /path/to/archive/raw_snapshots
"""

import argparse
import json
import sys
from datetime import date as date_type
from datetime import datetime
from pathlib import Path

import zstandard as zstd
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from src.database import get_session
from src.upsert_helpers import upsert_trip_update_state
from src.wmata_collector import _service_date_for_row

DEFAULT_ARCHIVE_ROOT = Path("archive/raw_snapshots")
BATCH_SIZE = 5000


def _parse_dt(s: str | None) -> datetime | None:
    """Parse an archive datetime string ("2026-05-18 22:00:00") to a naive datetime.

    The live ``JsonlArchiveWriter`` serialises datetimes via ``str()``,
    producing a space-separated form that Python's ``fromisoformat``
    accepts since 3.11. Returns ``None`` for ``None`` or empty input
    (some snapshots have null ``predicted_arrival_ts``).
    """
    if not s:
        return None
    return datetime.fromisoformat(s)


def _iter_jsonl_zst(path: Path):
    """Yield decoded dicts from a zstd-compressed JSONL file.

    Uses streaming decompression so multi-frame files (one frame per
    collector flush) decode correctly — see PR #131 for the analogous
    fix on the parquet reader side. Lines split on ``\\n``; the final
    partial-line is yielded only if it has non-whitespace content.
    """
    dctx = zstd.ZstdDecompressor()
    with path.open("rb") as fh:
        with dctx.stream_reader(fh) as reader:
            buf = b""
            while True:
                chunk = reader.read(65536)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if line:
                        yield json.loads(line)
            if buf.strip():
                yield json.loads(buf)


def replay_archive_for_date(
    db: Session,
    target_date: date_type,
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
) -> int:
    """Replay all archive files for ``target_date`` into trip_update_state.

    Globs both the per-process pattern (``{date}.*.jsonl.zst`` — current,
    from PR #132) and the legacy single-file pattern
    (``{date}.jsonl.zst`` — pre-PR #132). Each line is decoded, parsed
    into the collector's row shape, and pushed through
    ``upsert_trip_update_state`` in batches of ``BATCH_SIZE`` for memory
    bounds.

    Rows whose computed service_date doesn't match ``target_date`` are
    silently skipped — defensive against midnight-crossing files that
    might contain a few rows belonging to the adjacent service-day.

    Args:
        db: Active SQLAlchemy session bound to PostgreSQL. Caller is
            responsible for committing or rolling back.
        target_date: The service date to replay (Eastern calendar day).
        archive_root: Directory holding the JSONL.zst files.

    Returns:
        The number of snapshot lines that matched ``target_date`` and
        were enqueued for UPSERT (note: not the row-count in state —
        many snapshots collapse to one state row by design).
    """
    pattern_per_proc = f"{target_date.isoformat()}.*.jsonl.zst"
    pattern_legacy = f"{target_date.isoformat()}.jsonl.zst"
    paths = sorted(
        set(archive_root.glob(pattern_per_proc)) | set(archive_root.glob(pattern_legacy))
    )
    if not paths:
        print(f"No archive files found for {target_date} under {archive_root}")
        return 0

    print(f"Replaying {len(paths)} archive file(s) for {target_date}:")
    for p in paths:
        print(f"  - {p.name}")

    total = 0
    batch: list[dict] = []
    for p in paths:
        for raw in _iter_jsonl_zst(p):
            if raw.get("stop_sequence") is None:
                continue
            snapshot_ts = _parse_dt(raw["snapshot_ts"])
            row = {
                "trip_id": raw["trip_id"],
                "stop_sequence": raw["stop_sequence"],
                "service_date": _service_date_for_row(
                    {
                        "trip_start_date": raw.get("trip_start_date"),
                        "snapshot_ts": snapshot_ts,
                    }
                ),
                "stop_id": raw["stop_id"],
                "vehicle_id": raw.get("vehicle_id"),
                "snapshot_ts": snapshot_ts,
                "predicted_arrival_ts": _parse_dt(raw.get("predicted_arrival_ts")),
                "schedule_relationship": raw.get("schedule_relationship"),
            }
            if row["service_date"] != target_date:
                continue
            batch.append(row)
            total += 1
            if len(batch) >= BATCH_SIZE:
                upsert_trip_update_state(db, batch)
                batch = []

    if batch:
        upsert_trip_update_state(db, batch)

    print(f"Replayed {total} snapshot rows for {target_date}.")
    return total


def main() -> int:
    """CLI entry point."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="Service date (YYYY-MM-DD)")
    parser.add_argument(
        "--archive-root",
        default=str(DEFAULT_ARCHIVE_ROOT),
        help=f"Archive directory (default: {DEFAULT_ARCHIVE_ROOT})",
    )
    args = parser.parse_args()
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    archive_root = Path(args.archive_root)

    db = get_session()
    try:
        replay_archive_for_date(db, target_date, archive_root)
        db.commit()
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
