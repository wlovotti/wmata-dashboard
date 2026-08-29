"""Load raw VP JSONL archives into vehicle_positions (spec 2026-08-22 §3).

Replaces the retired \\copy-over-tunnel VP delta: the stateless collector
archives VehiclePositions as jsonl.zst in S3; this loader parses each
synced file once (manifest-table idempotency) and applies the
phantom-timestamp guard at load time — raw files stay raw.

    uv run python pipelines/load_vp_archive.py --agency wmata
    uv run python pipelines/load_vp_archive.py --agency sfmta
"""

import argparse
import io
import json
import sys
from datetime import datetime
from pathlib import Path

import zstandard as zstd
from dotenv import load_dotenv
from sqlalchemy import insert

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_session
from src.models import VehiclePosition, VpArchiveLoadedFile
from src.timezones import from_epoch_naive_utc, utcnow_naive

REPO_ROOT = Path(__file__).resolve().parent.parent

# Drop rows whose vehicle-reported timestamp is >30 min from the row's own
# poll time: the spec's "±15 min outside the 15-min rotation window" with
# row-level precision. Catches the documented +20-24h phantom-timestamp
# rows (stale AVL clocks); tolerates ordinary AVL lag.
GUARD_SEC = 1800
INSERT_CHUNK = 5000


def parse_vp_line(obj: dict) -> dict | None:
    """Map one archived VP JSON line to VehiclePosition insert kwargs.

    Returns None when the row should be dropped: either the
    phantom-timestamp guard fires, or the row is missing a value one of
    ``VehiclePosition``'s NOT NULL columns requires (``vehicle_id``,
    ``latitude``, ``longitude`` — the feed can emit vehicles mid-assignment
    with no ID or fix yet). Passing a null through to the insert would
    raise an IntegrityError at chunk-flush time and abort the whole file,
    so these are filtered here instead. A missing/null ``timestamp`` falls
    back to ``collected_at`` (legacy collector behavior for
    un-timestamped vehicles).
    """
    if obj.get("vehicle_id") is None or obj.get("latitude") is None or obj.get("longitude") is None:
        return None

    collected_at = datetime.fromisoformat(obj["collected_at"])
    epoch = obj.get("timestamp")
    if epoch:
        ts = from_epoch_naive_utc(epoch)
        if abs((ts - collected_at).total_seconds()) > GUARD_SEC:
            return None
    else:
        ts = collected_at
    return {
        "vehicle_id": obj.get("vehicle_id"),
        "route_id": obj.get("route_id"),
        "trip_id": obj.get("trip_id"),
        "direction_id": obj.get("direction_id"),
        "trip_start_date": obj.get("trip_start_date"),
        "latitude": obj.get("latitude"),
        "longitude": obj.get("longitude"),
        "speed": obj.get("speed"),
        "current_stop_sequence": obj.get("current_stop_sequence"),
        "stop_id": obj.get("stop_id"),
        "current_status": obj.get("current_status"),
        "timestamp": ts,
        "collected_at": collected_at,
    }


def load_vp_file(session, path: Path) -> tuple[int, int]:
    """Load one archive file exactly once; returns (inserted, dropped).

    Rows + the manifest row commit in a single transaction, so a crash
    mid-file rolls back cleanly and the file re-loads next run. The zstd
    stream_reader tolerates a missing frame footer (crash-cut files). A
    line that fails to decode (JSON or UTF-8) OR that fails to *parse*
    into a row (e.g. ``parse_vp_line`` raising on an absurd epoch or a
    missing ``collected_at`` key) is dropped and counted rather than
    raised — one bad line must not poison the whole file.

    The stream is read as raw bytes (``io.BufferedReader`` over the zstd
    stream_reader), split into lines, and decoded by hand per line —
    NOT via ``io.TextIOWrapper``, whose ``for line in text_stream:``
    would raise ``UnicodeDecodeError`` from the iteration itself, outside
    any per-line try/except, escaping this function on real byte
    corruption (review round 2 caught this: the wrapper decodes eagerly
    before ``json.loads`` ever runs, so a try/except around only
    ``json.loads`` never actually saw a ``UnicodeDecodeError``).

    ``parse_vp_line(obj)`` runs INSIDE the same try as the decode (review
    round 3): well-formed JSON can still raise there —
    ``from_epoch_naive_utc`` on an out-of-range epoch raises ``ValueError``
    / ``OverflowError`` / ``OSError`` (exactly the documented phantom
    pattern, just extreme enough to overflow ``datetime`` instead of only
    tripping ``GUARD_SEC``), and ``datetime.fromisoformat`` or a missing
    ``collected_at`` key can raise ``ValueError`` / ``TypeError`` /
    ``KeyError``. Any of those must drop the one bad line, not the file.
    """
    already = session.get(VpArchiveLoadedFile, path.name)
    if already is not None:
        print(f"  {path.name}: already loaded ({already.row_count} rows), skipping")
        return 0, 0

    inserted = dropped = 0
    batch: list[dict] = []
    with open(path, "rb") as fh:
        buffered = io.BufferedReader(zstd.ZstdDecompressor().stream_reader(fh))
        for raw in buffered:
            try:
                obj = json.loads(raw.decode("utf-8"))
                row = parse_vp_line(obj)
            except (
                json.JSONDecodeError,
                UnicodeDecodeError,
                ValueError,
                TypeError,
                KeyError,
                OverflowError,
                OSError,
            ):
                dropped += 1
                continue
            if row is None:
                dropped += 1
                continue
            batch.append(row)
            if len(batch) >= INSERT_CHUNK:
                session.execute(insert(VehiclePosition), batch)
                inserted += len(batch)
                batch = []
    if batch:
        session.execute(insert(VehiclePosition), batch)
        inserted += len(batch)

    session.add(
        VpArchiveLoadedFile(
            filename=path.name,
            row_count=inserted,
            dropped_count=dropped,
            loaded_at=utcnow_naive(),
        )
    )
    session.commit()
    print(f"  {path.name}: inserted {inserted}, dropped {dropped}")
    return inserted, dropped


def main(argv=None) -> int:
    """Load every not-yet-loaded VP archive file for one agency.

    Each file is loaded in isolation: an exception from ``load_vp_file``
    (any DB- or file-level failure a line-level guard didn't already
    catch) is logged and the run continues to the next file rather than
    aborting the whole batch — one bad file must not block every other
    file behind it. Returns 1 if any file failed to load, so callers
    (cron, CI) can detect a partial run; the manifest table is the source
    of truth for exactly which files still need attention.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", default="wmata", choices=("wmata", "sfmta"))
    parser.add_argument("--archive-root", type=Path, default=None)
    args = parser.parse_args(argv)
    load_dotenv()

    cfg = load_agency_config(args.agency)
    archive_root = args.archive_root or REPO_ROOT / cfg.vp_archive_dir
    if not archive_root.is_dir():
        print(f"Error: --archive-root {archive_root} is not a directory.")
        return 1

    session = get_session(db_url=resolve_agency_db_url(cfg))
    try:
        total_ins = total_drop = 0
        failed_files: list[str] = []
        for path in sorted(archive_root.glob("*.jsonl.zst")):
            try:
                ins, drop = load_vp_file(session, path)
            except Exception as exc:  # isolate one bad file, keep going
                print(f"  {path.name}: FAILED to load ({exc!r}), continuing")
                session.rollback()
                failed_files.append(path.name)
                continue
            total_ins += ins
            total_drop += drop
        print(f"Done: {total_ins} rows inserted, {total_drop} dropped (guard + unusable rows).")
        if failed_files:
            print(f"  {len(failed_files)} file(s) failed to load: {', '.join(failed_files)}")
    finally:
        session.close()
    return 1 if failed_files else 0


if __name__ == "__main__":
    sys.exit(main())
