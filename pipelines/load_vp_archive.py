"""Load raw VP JSONL archives into vehicle_positions (NOTES-95, spec §3).

Replaces the retired \\copy-over-tunnel VP delta: the stateless collector
archives VehiclePositions as jsonl.zst in S3; this loader parses each
synced file once (manifest-table idempotency) and applies the NOTES-81
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
# row-level precision. Catches NOTES-81's +20-24h phantoms; tolerates AVL lag.
GUARD_SEC = 1800
INSERT_CHUNK = 5000


def parse_vp_line(obj: dict) -> dict | None:
    """Map one archived VP JSON line to VehiclePosition insert kwargs.

    Returns None when the row should be dropped: either the NOTES-81
    phantom-timestamp guard fires, or the vehicle has no reported position
    (``latitude``/``longitude`` null — ``VehiclePosition`` requires both
    NOT NULL, but the feed can emit vehicles mid-assignment with no fix
    yet). A missing/null ``timestamp`` falls back to ``collected_at``
    (legacy collector behavior for un-timestamped vehicles).
    """
    if obj.get("latitude") is None or obj.get("longitude") is None:
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
    stream_reader tolerates a missing frame footer (crash-cut files).
    """
    already = session.get(VpArchiveLoadedFile, path.name)
    if already is not None:
        print(f"  {path.name}: already loaded ({already.row_count} rows), skipping")
        return 0, 0

    inserted = dropped = 0
    batch: list[dict] = []
    with open(path, "rb") as fh:
        text_stream = io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(fh), encoding="utf-8")
        for line in text_stream:
            row = parse_vp_line(json.loads(line))
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
    """Load every not-yet-loaded VP archive file for one agency."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", default="wmata", choices=("wmata", "sfmta"))
    parser.add_argument("--archive-root", type=Path, default=None)
    args = parser.parse_args(argv)
    load_dotenv()

    cfg = load_agency_config(args.agency)
    archive_root = args.archive_root or REPO_ROOT / cfg.vp_archive_dir
    session = get_session(db_url=resolve_agency_db_url(cfg))
    try:
        total_ins = total_drop = 0
        for path in sorted(archive_root.glob("*.jsonl.zst")):
            ins, drop = load_vp_file(session, path)
            total_ins += ins
            total_drop += drop
        print(f"Done: {total_ins} rows inserted, {total_drop} dropped (NOTES-81 guard).")
    finally:
        session.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
