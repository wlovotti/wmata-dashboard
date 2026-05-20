"""Regression: two service_dates for the same (trip_id, stop_sequence) must coexist.

Without service_date in the trip_update_state PK, WMATA's repeating
day-over-day trip_ids would overwrite themselves and historical
re-derivation would be impossible (the root cause of the Phase D
recovery gap from 2026-05-18 → 19). This test pins the new behavior:
two rows with different service_dates for the same (trip_id,
stop_sequence) can be persisted simultaneously.

Uses the SQLite ``db_session`` fixture (not pg_session) because the
behavior under test is the *schema* — three-column PK semantics — which
both SQLite and Postgres honor identically once the model declares them.
"""

from datetime import date, datetime

import pytest
from sqlalchemy.exc import IntegrityError

from src.models import TripUpdateState


@pytest.mark.smoke
def test_two_service_dates_coexist(db_session):
    """A trip_id that ran on 5/18 and 5/19 keeps both rows in state."""
    db_session.add_all(
        [
            TripUpdateState(
                trip_id="T_COEXIST",
                stop_sequence=1,
                service_date=date(2026, 5, 18),
                stop_id="S1",
                vehicle_id="V_18",
                final_snapshot_ts=datetime(2026, 5, 18, 18, 0),
                last_predicted_arrival_ts=datetime(2026, 5, 18, 18, 5),
            ),
            TripUpdateState(
                trip_id="T_COEXIST",
                stop_sequence=1,
                service_date=date(2026, 5, 19),
                stop_id="S1",
                vehicle_id="V_19",
                final_snapshot_ts=datetime(2026, 5, 19, 18, 0),
                last_predicted_arrival_ts=datetime(2026, 5, 19, 18, 7),
            ),
        ]
    )
    db_session.flush()

    rows_18 = (
        db_session.query(TripUpdateState)
        .filter(TripUpdateState.trip_id == "T_COEXIST")
        .filter(TripUpdateState.service_date == date(2026, 5, 18))
        .all()
    )
    assert len(rows_18) == 1
    assert rows_18[0].vehicle_id == "V_18"

    rows_19 = (
        db_session.query(TripUpdateState)
        .filter(TripUpdateState.trip_id == "T_COEXIST")
        .filter(TripUpdateState.service_date == date(2026, 5, 19))
        .all()
    )
    assert len(rows_19) == 1
    assert rows_19[0].vehicle_id == "V_19"


@pytest.mark.smoke
def test_same_service_date_pk_collision(db_session):
    """Same (trip_id, stop_sequence, service_date) twice is a PK collision.

    Smoke check that the PK still enforces uniqueness on the original
    two dimensions when service_date is held constant. Without this
    guard, a regression that drops service_date from the PK would let
    every (trip, stop) collide silently.
    """
    db_session.add(
        TripUpdateState(
            trip_id="T_PK",
            stop_sequence=1,
            service_date=date(2026, 5, 18),
            stop_id="S1",
            final_snapshot_ts=datetime(2026, 5, 18, 18, 0),
        )
    )
    db_session.flush()

    db_session.add(
        TripUpdateState(
            trip_id="T_PK",
            stop_sequence=1,
            service_date=date(2026, 5, 18),
            stop_id="S1",
            final_snapshot_ts=datetime(2026, 5, 18, 19, 0),
        )
    )
    with pytest.raises(IntegrityError):
        db_session.flush()
