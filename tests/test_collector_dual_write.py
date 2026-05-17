"""Tests that _save_trip_updates writes to both tables."""

from datetime import datetime

import pytest
from sqlalchemy import select

from src.models import TripUpdateSnapshot, TripUpdateState


@pytest.mark.integration
def test_save_trip_updates_writes_to_both_tables(pg_session):
    """Each call to _save_trip_updates writes the row to BOTH tables."""
    from src.wmata_collector import WMATADataCollector

    collector = WMATADataCollector(api_key="unused", db_session=pg_session)

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
        }
    ]
    collector._save_trip_updates(rows)

    # Old path: row exists in trip_update_snapshots.
    snapshot = pg_session.execute(select(TripUpdateSnapshot)).scalar_one()
    assert snapshot.trip_id == "T1"

    # New path: row exists in trip_update_state with the right final-state.
    state = pg_session.execute(select(TripUpdateState)).scalar_one()
    assert state.trip_id == "T1"
    assert state.stop_sequence == 1
    assert state.final_snapshot_ts == datetime(2026, 5, 17, 14, 0, 0)
    assert state.last_predicted_arrival_ts == datetime(2026, 5, 17, 14, 5, 0)


@pytest.mark.integration
def test_collector_writes_jsonl_archive(pg_session, tmp_path):
    """_save_trip_updates appends rows to the JSONL archive."""
    from src.archive_writer import JsonlArchiveWriter
    from src.wmata_collector import WMATADataCollector

    collector = WMATADataCollector(api_key="unused", db_session=pg_session)
    # Redirect the archive to a tmpdir.
    collector._archive_writer = JsonlArchiveWriter(archive_dir=tmp_path)

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
        }
    ]
    collector._save_trip_updates(rows)
    collector._archive_writer.close()

    assert (tmp_path / "2026-05-17.jsonl.zst").exists()
