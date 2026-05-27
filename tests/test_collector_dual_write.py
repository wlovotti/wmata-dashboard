"""Tests for ``_save_trip_updates`` after the Phase E.2 (NOTES-72) collector cutover.

Phase E.2 removed the ``TripUpdateSnapshot`` dual-write and the ``_tu_dedup_cache``
from ``_save_trip_updates``. The method now:
  - Upserts into ``trip_update_state`` (unchanged from Phase E.1).
  - Writes one ``CollectorHeartbeat`` row per tick (replaces snapshot as the
    coverage signal for ``src/data_completeness.py``).
  - No longer writes to ``trip_update_snapshots``.
"""

from datetime import datetime

import pytest
from sqlalchemy import func, select

from src.models import CollectorHeartbeat, TripUpdateSnapshot, TripUpdateState


@pytest.mark.integration
def test_save_trip_updates_writes_state_and_heartbeat(pg_session, tmp_path):
    """Each call to _save_trip_updates writes to trip_update_state and
    collector_heartbeats, and does NOT write to trip_update_snapshots.
    """
    from src.wmata_collector import WMATADataCollector

    collector = WMATADataCollector(api_key="unused", db_session=pg_session, archive_root=tmp_path)

    rows = [
        {
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 1,
            "route_id": "R1",
            "vehicle_id": "V1",
            "snapshot_ts": datetime(2026, 5, 17, 14, 0, 0),
            "predicted_arrival_ts": datetime(2026, 5, 17, 14, 5, 0),
            "predicted_departure_ts": datetime(2026, 5, 17, 14, 5, 30),
            "schedule_relationship": "SCHEDULED",
            "collected_at": datetime(2026, 5, 17, 14, 0, 5),
            "trip_start_date": "20260517",
        }
    ]
    try:
        collector._save_trip_updates(rows)

        # trip_update_state must be written (Phase E.1 path, unchanged).
        state = (pg_session.execute(select(TripUpdateState).filter_by(trip_id="T1"))).scalar_one()
        assert state.trip_id == "T1"
        assert state.stop_sequence == 1
        assert state.final_snapshot_ts == datetime(2026, 5, 17, 14, 0, 0)
        assert state.last_predicted_arrival_ts == datetime(2026, 5, 17, 14, 5, 0)

        # collector_heartbeats must have exactly one row for this tick.
        hb_count = pg_session.execute(
            select(func.count()).select_from(CollectorHeartbeat).filter(
                CollectorHeartbeat.ts == datetime(2026, 5, 17, 14, 0, 0)
            )
        ).scalar()
        assert hb_count == 1

        # trip_update_snapshots must NOT be written (Phase E.2 cutover).
        snap_count = pg_session.execute(
            select(func.count()).select_from(TripUpdateSnapshot).filter(
                TripUpdateSnapshot.trip_id == "T1"
            )
        ).scalar()
        assert snap_count == 0, (
            "snapshot write was not removed: TripUpdateSnapshot has rows for T1"
        )
    finally:
        collector.close()


@pytest.mark.integration
def test_collector_writes_jsonl_archive(pg_session, tmp_path):
    """_save_trip_updates appends rows to the JSONL archive."""
    from src.wmata_collector import WMATADataCollector

    collector = WMATADataCollector(api_key="unused", db_session=pg_session, archive_root=tmp_path)

    rows = [
        {
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 1,
            "route_id": "R1",
            "vehicle_id": "V1",
            "snapshot_ts": datetime(2026, 5, 17, 14, 0, 0),
            "predicted_arrival_ts": datetime(2026, 5, 17, 14, 5, 0),
            "predicted_departure_ts": datetime(2026, 5, 17, 14, 5, 30),
            "schedule_relationship": "SCHEDULED",
            "collected_at": datetime(2026, 5, 17, 14, 0, 5),
            "trip_start_date": "20260517",
        }
    ]
    try:
        collector._save_trip_updates(rows)
    finally:
        collector.close()

    # Per-process filenames: YYYY-MM-DD.<pid>.<startup_ts>.jsonl.zst.
    # Glob for the date prefix rather than an exact name.
    matches = list(tmp_path.glob("2026-05-17.*.jsonl.zst"))
    assert len(matches) == 1, f"Expected 1 archive file, found: {matches}"
