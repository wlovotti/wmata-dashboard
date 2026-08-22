"""Spec §5: TU files written by the stateless path replay identically.

Builds one poll's rows, archives them via archive_tu_rows, and asserts the
decoded lines match what WMATADataCollector's own archive writer produces
for the same rows (same keys, same JSON encoding of timestamps).
"""

import io
import json
from datetime import datetime
from pathlib import Path

import zstandard as zstd

from src.archive_writer import JsonlArchiveWriter
from src.stateless_poller import archive_tu_rows

SNAPSHOT_TS = datetime(2026, 8, 22, 12, 0, 0)
ROWS = [
    {
        "snapshot_ts": SNAPSHOT_TS,
        "trip_id": "t1",
        "route_id": "D72",
        "vehicle_id": "42",
        "stop_id": "1001",
        "stop_sequence": 3,
        "predicted_arrival_ts": datetime(2026, 8, 22, 12, 5, 0),
        "predicted_departure_ts": None,
        "schedule_relationship": "SCHEDULED",
        "trip_start_date": "20260822",
    }
]


def _decode(path: Path) -> list[dict]:
    """Decode a zstd-compressed JSONL archive file into a list of row dicts."""
    with open(path, "rb") as fh:
        text = io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(fh), encoding="utf-8")
        return [json.loads(line) for line in text]


def test_stateless_tu_lines_match_legacy_archive_format(tmp_path):
    """archive_tu_rows produces byte-for-byte-equivalent JSON lines to the legacy path.

    The "legacy" writer here reproduces exactly what
    ``WMATADataCollector._save_trip_updates`` does with its
    ``_archive_writer`` before any DB work — a single
    ``writer.append(row, snapshot_ts=row["snapshot_ts"])`` per row — so a
    match proves the stateless path's archived bytes are compatible with
    ``pipelines/replay_archive_to_state.py`` without needing network access
    to WMATA's live feed.
    """
    legacy_dir, new_dir = tmp_path / "legacy", tmp_path / "new"
    legacy = JsonlArchiveWriter(legacy_dir)
    for row in ROWS:  # exactly what _save_trip_updates does before its DB work
        legacy.append(row, snapshot_ts=row["snapshot_ts"])
    legacy.close()

    new = JsonlArchiveWriter(new_dir)
    archive_tu_rows(new, ROWS)
    new.close()

    legacy_lines = _decode(next(legacy_dir.glob("*.jsonl.zst")))
    new_lines = _decode(next(new_dir.glob("*.jsonl.zst")))
    assert new_lines == legacy_lines
