"""Tests that _save_trip_updates writes to both tables."""

from datetime import datetime

import pytest
from sqlalchemy import select

from src.models import TripUpdateSnapshot, TripUpdateState


@pytest.mark.integration
def test_save_trip_updates_writes_to_both_tables(pg_session, tmp_path):
    """Each call to _save_trip_updates writes the row to BOTH tables."""
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
            # trip_start_date carries the GTFS-RT tripDescriptor.start_date
            # forward to the upsert/archive paths. TripUpdateSnapshot has no
            # such column, so the snapshot constructor must NOT receive it.
            # Without this key in the test row, the dual-write test misses
            # the regression where ``**row`` splats into TripUpdateSnapshot.
            "trip_start_date": "20260517",
        }
    ]
    try:
        collector._save_trip_updates(rows)

        # Filter by the test's trip_id so the assertion is portable against
        # a dev DB that already holds production rows (same pattern as
        # PR #133 / NOTES-70).
        snapshot = (
            pg_session.execute(select(TripUpdateSnapshot).filter_by(trip_id="T1"))
        ).scalar_one()
        assert snapshot.trip_id == "T1"

        state = (pg_session.execute(select(TripUpdateState).filter_by(trip_id="T1"))).scalar_one()
        assert state.trip_id == "T1"
        assert state.stop_sequence == 1
        assert state.final_snapshot_ts == datetime(2026, 5, 17, 14, 0, 0)
        assert state.last_predicted_arrival_ts == datetime(2026, 5, 17, 14, 5, 0)
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
