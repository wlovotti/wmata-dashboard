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


class NoArchiveFilesFoundError(RuntimeError):
    """Raised when the archive glob for a date matches zero files.

    A replay with no input is almost always an operator error (the
    JSONL archive hasn't been rsynced yet, a typo'd date, a wrong
    ``--archive-root``) rather than an intentional no-op. NOTES-93:
    this used to return 0 silently, which during the recovery driver's
    fold-in phase turned "the archive isn't here yet" into a
    clean-looking success — the failure guard never tripped, and
    derivation ran against empty state. Callers that genuinely expect
    an empty date must pass ``allow_empty=True`` explicitly.
    """


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
    chunk_size: int = 5000,
    allow_empty: bool = False,
) -> int:
    """Replay all archive files for ``target_date`` into trip_update_state.

    Globs both the per-process pattern (``{date}.*.jsonl.zst`` — current,
    from PR #132) and the legacy single-file pattern
    (``{date}.jsonl.zst`` — pre-PR #132).

    **Fold-then-upsert:** a full day of the WMATA TripUpdates feed is
    ~10M raw lines (every future stop republished on every ~30s poll),
    but only ~150k distinct (trip_id, stop_sequence) keys. Replaying
    line-by-line through per-snapshot UPSERT statements takes hours per
    date (measured ~7h on the production Lightsail box, 2026-07-12).
    Instead, the accumulation semantics — which the live collector
    expresses as ON CONFLICT CASE expressions applied poll-by-poll —
    are a deterministic fold, so this replays them in memory:

    - ``final_snapshot_ts`` / ``final_schedule_relationship`` /
      ``stop_id``: from the row with the greatest snapshot_ts.
    - ``last_pred_snapshot_ts`` / ``last_predicted_arrival_ts``: from
      the greatest-snapshot_ts row whose predicted_arrival_ts is
      non-null (WMATA nullifies predictions at arrival; the last
      meaningful estimate must survive).
    - ``vehicle_id``: last non-null across the sequence.

    Rows are ordered by snapshot_ts (not file order) so overlapping
    per-process files from a collector restart fold correctly. The
    folded rows (one per key) then go through the same
    ``upsert_trip_update_state`` helper in chunks, preserving the
    conditional ON CONFLICT semantics against any pre-existing DB rows.

    Rows whose computed service_date doesn't match ``target_date`` are
    silently skipped — defensive against midnight-crossing files that
    might contain a few rows belonging to the adjacent service-day.

    Args:
        db: Active SQLAlchemy session bound to PostgreSQL. Caller is
            responsible for committing or rolling back.
        target_date: The service date to replay (Eastern calendar day).
        archive_root: Directory holding the JSONL.zst files.
        chunk_size: Folded rows per UPSERT statement.
        allow_empty: When ``True``, a zero-file glob match returns 0
            instead of raising — see ``NoArchiveFilesFoundError``
            (NOTES-93).

    Returns:
        The number of snapshot lines that matched ``target_date`` and
        were folded (note: not the row-count in state — many snapshots
        collapse to one state row by design).

    Raises:
        NoArchiveFilesFoundError: if the glob matches zero files and
            ``allow_empty`` is ``False``.
    """
    pattern_per_proc = f"{target_date.isoformat()}.*.jsonl.zst"
    pattern_legacy = f"{target_date.isoformat()}.jsonl.zst"
    paths = sorted(
        set(archive_root.glob(pattern_per_proc)) | set(archive_root.glob(pattern_legacy))
    )
    if not paths:
        if allow_empty:
            print(
                f"No archive files found for {target_date} under {archive_root} "
                "(--allow-empty set, continuing)"
            )
            return 0
        raise NoArchiveFilesFoundError(
            f"No archive files found for {target_date} under {archive_root}. "
            "This usually means the JSONL archive hasn't been synced yet — "
            "check the source before assuming the date is genuinely empty. "
            "Pass --allow-empty to proceed anyway."
        )

    print(f"Replaying {len(paths)} archive file(s) for {target_date}:")
    for p in paths:
        print(f"  - {p.name}")

    total = 0
    # Fold state per (trip_id, stop_sequence); service_date is fixed to
    # target_date by the filter below. "_vehicle_ts" / "_pred_ts" track
    # which snapshot each conditional field last came from so ordering
    # is by snapshot_ts, not file iteration order.
    folded: dict[tuple[str, int], dict] = {}

    for p in paths:
        for raw in _iter_jsonl_zst(p):
            if raw.get("stop_sequence") is None:
                continue
            snapshot_ts = _parse_dt(raw["snapshot_ts"])
            service_date = _service_date_for_row(
                {
                    "trip_start_date": raw.get("trip_start_date"),
                    "snapshot_ts": snapshot_ts,
                }
            )
            if service_date != target_date:
                continue

            vehicle_id = raw.get("vehicle_id")
            predicted_arrival_ts = _parse_dt(raw.get("predicted_arrival_ts"))
            key = (raw["trip_id"], raw["stop_sequence"])
            cur = folded.get(key)
            if cur is None:
                cur = {
                    "trip_id": raw["trip_id"],
                    "stop_sequence": raw["stop_sequence"],
                    "service_date": target_date,
                    "stop_id": raw["stop_id"],
                    "vehicle_id": vehicle_id,
                    "snapshot_ts": snapshot_ts,
                    "schedule_relationship": raw.get("schedule_relationship"),
                    "predicted_arrival_ts": None,
                    "last_pred_snapshot_ts": None,
                    "_vehicle_ts": snapshot_ts if vehicle_id is not None else None,
                }
                folded[key] = cur
            else:
                if snapshot_ts >= cur["snapshot_ts"]:
                    cur["snapshot_ts"] = snapshot_ts
                    cur["stop_id"] = raw["stop_id"]
                    cur["schedule_relationship"] = raw.get("schedule_relationship")
                if vehicle_id is not None and (
                    cur["_vehicle_ts"] is None or snapshot_ts >= cur["_vehicle_ts"]
                ):
                    cur["vehicle_id"] = vehicle_id
                    cur["_vehicle_ts"] = snapshot_ts
            if predicted_arrival_ts is not None and (
                cur["last_pred_snapshot_ts"] is None or snapshot_ts >= cur["last_pred_snapshot_ts"]
            ):
                cur["predicted_arrival_ts"] = predicted_arrival_ts
                cur["last_pred_snapshot_ts"] = snapshot_ts
            total += 1

    rows_out = [{k: v for k, v in cur.items() if not k.startswith("_")} for cur in folded.values()]
    for i in range(0, len(rows_out), chunk_size):
        upsert_trip_update_state(db, rows_out[i : i + chunk_size])

    print(f"Replayed {total} snapshot rows for {target_date} ({len(rows_out)} state rows).")
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
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help=(
            "Exit 0 instead of erroring when the archive glob matches zero "
            "files for --date. Off by default — see NoArchiveFilesFoundError "
            "(NOTES-93)."
        ),
    )
    args = parser.parse_args()
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    archive_root = Path(args.archive_root)

    db = get_session()
    try:
        replay_archive_for_date(db, target_date, archive_root, allow_empty=args.allow_empty)
        db.commit()
    except NoArchiveFilesFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
