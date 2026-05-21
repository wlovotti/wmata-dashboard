"""Tests for src.upsert_helpers.upsert_trip_update_state."""

from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import select

from src.models import TripUpdateState


def _make_row(
    trip_id: str = "T1",
    stop_sequence: int = 1,
    service_date: date | None = None,
    stop_id: str = "S1",
    vehicle_id: str | None = "V1",
    snapshot_ts: datetime | None = None,
    predicted_arrival_ts: datetime | None = None,
    schedule_relationship: str | None = "SCHEDULED",
) -> dict:
    """Build a row dict in the shape upsert_trip_update_state expects.

    ``service_date`` defaults to 2026-05-17 to match the default
    ``snapshot_ts``; tests that hold trip_id constant across snapshots
    can rely on the default to land both rows on the same PK.
    """
    return {
        "trip_id": trip_id,
        "stop_sequence": stop_sequence,
        "service_date": service_date or date(2026, 5, 17),
        "stop_id": stop_id,
        "vehicle_id": vehicle_id,
        "snapshot_ts": snapshot_ts or datetime(2026, 5, 17, 14, 0, 0),
        "predicted_arrival_ts": predicted_arrival_ts,
        "schedule_relationship": schedule_relationship,
    }


@pytest.mark.integration
def test_first_insert_creates_row(pg_session):
    """An UPSERT against an empty table inserts a new row."""
    from src.upsert_helpers import upsert_trip_update_state

    pred = datetime(2026, 5, 17, 14, 5, 0)
    upsert_trip_update_state(pg_session, [_make_row(predicted_arrival_ts=pred)])
    pg_session.commit()

    row = pg_session.execute(select(TripUpdateState)).scalar_one()
    assert row.trip_id == "T1"
    assert row.stop_sequence == 1
    assert row.final_snapshot_ts == datetime(2026, 5, 17, 14, 0, 0)
    assert row.last_pred_snapshot_ts == datetime(2026, 5, 17, 14, 0, 0)
    assert row.last_predicted_arrival_ts == pred


@pytest.mark.integration
def test_upsert_overwrites_final_fields_always(pg_session):
    """final_* fields always reflect the most recent snapshot."""
    from src.upsert_helpers import upsert_trip_update_state

    t1 = datetime(2026, 5, 17, 14, 0, 0)
    t2 = t1 + timedelta(minutes=1)

    upsert_trip_update_state(pg_session, [_make_row(snapshot_ts=t1)])
    upsert_trip_update_state(
        pg_session,
        [_make_row(snapshot_ts=t2, schedule_relationship="SKIPPED")],
    )
    pg_session.commit()

    row = pg_session.execute(select(TripUpdateState)).scalar_one()
    assert row.final_snapshot_ts == t2
    assert row.final_schedule_relationship == "SKIPPED"


@pytest.mark.integration
def test_last_pred_updates_only_when_prediction_is_non_null(pg_session):
    """last_pred_* fields stick on the most recent NON-NULL prediction."""
    from src.upsert_helpers import upsert_trip_update_state

    t1 = datetime(2026, 5, 17, 14, 0, 0)
    t2 = t1 + timedelta(minutes=1)
    pred1 = datetime(2026, 5, 17, 14, 5, 0)

    upsert_trip_update_state(pg_session, [_make_row(snapshot_ts=t1, predicted_arrival_ts=pred1)])
    # Second snapshot has a NULL prediction — should NOT overwrite last_pred_*.
    upsert_trip_update_state(pg_session, [_make_row(snapshot_ts=t2, predicted_arrival_ts=None)])
    pg_session.commit()

    row = pg_session.execute(select(TripUpdateState)).scalar_one()
    assert row.final_snapshot_ts == t2  # final_ moved forward
    assert row.last_pred_snapshot_ts == t1  # but last_pred_ stuck on t1
    assert row.last_predicted_arrival_ts == pred1


@pytest.mark.integration
def test_vehicle_id_coalesces_to_latest_non_null(pg_session):
    """vehicle_id keeps the last non-null value (can come and go in WMATA feed)."""
    from src.upsert_helpers import upsert_trip_update_state

    t1 = datetime(2026, 5, 17, 14, 0, 0)
    t2 = t1 + timedelta(minutes=1)

    upsert_trip_update_state(pg_session, [_make_row(snapshot_ts=t1, vehicle_id="V1")])
    upsert_trip_update_state(pg_session, [_make_row(snapshot_ts=t2, vehicle_id=None)])
    pg_session.commit()

    row = pg_session.execute(select(TripUpdateState)).scalar_one()
    assert row.vehicle_id == "V1"  # preserved from the earlier snapshot
