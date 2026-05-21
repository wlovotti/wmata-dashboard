"""Tests for pipelines.derive_stop_events_from_state."""

from datetime import date, datetime

import pytest
from sqlalchemy import select

from src.models import StopEvent, StopTime, Trip, TripUpdateState, VehiclePosition


@pytest.mark.parametrize(
    "items, size, expected",
    [
        ([1, 2, 3, 4, 5], 2, [[1, 2], [3, 4], [5]]),
        ([1, 2, 3, 4], 2, [[1, 2], [3, 4]]),
        ([1, 2], 5, [[1, 2]]),
        ([], 3, []),
    ],
)
def test_iter_chunks_partitions_input(items, size, expected):
    """_iter_chunks splits a sequence into successive lists of length ``size``,
    with a possibly-short final chunk."""
    from pipelines.derive_stop_events_from_state import _iter_chunks

    assert [list(c) for c in _iter_chunks(items, size)] == expected


def test_iter_chunks_rejects_non_positive_size():
    """_iter_chunks raises ValueError for size <= 0 (a silent infinite loop
    is worse than a clear error)."""
    from pipelines.derive_stop_events_from_state import _iter_chunks

    with pytest.raises(ValueError):
        list(_iter_chunks([1, 2, 3], 0))


def _seed_minimal_route(pg_session, *, route_id="R1", trip_id="T1"):
    """Seed the minimum DB state for a single-stop derivation test."""
    pg_session.add_all(
        [
            Trip(trip_id=trip_id, route_id=route_id, direction_id=0, is_current=True),
            StopTime(
                trip_id=trip_id,
                stop_sequence=1,
                stop_id="S1",
                arrival_time="14:05:00",
                departure_time="14:05:30",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id=trip_id,
                route_id=route_id,
                trip_start_date="20260517",
                latitude=0,
                longitude=0,
                timestamp=datetime(2026, 5, 17, 14, 0, 0),
            ),
        ]
    )


@pytest.mark.integration
def test_derive_produces_stop_event_with_correct_observed_arrival(pg_session):
    """A trip_update_state row produces a stop_event with last_predicted_arrival_ts as observed."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    _seed_minimal_route(pg_session)
    pred = datetime(2026, 5, 17, 14, 6, 30)  # 90s late vs 14:05 schedule
    pg_session.add(
        TripUpdateState(
            trip_id="T1",
            stop_sequence=1,
            service_date=date(2026, 5, 17),
            stop_id="S1",
            vehicle_id="V1",
            final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
            final_schedule_relationship="SCHEDULED",
            last_pred_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
            last_predicted_arrival_ts=pred,
        )
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 5, 17),
        target_table_name="stop_events",
    )
    pg_session.commit()

    event = pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T1")).scalar_one()
    assert event.trip_id == "T1"
    assert event.observed_arrival_ts == pred
    assert event.schedule_relationship == "SCHEDULED"
    assert event.source == "trip_update"


@pytest.mark.integration
def test_derive_emits_skipped_stops(pg_session):
    """A SKIPPED final_schedule_relationship produces a stop_event with observed_arrival_ts=None."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    _seed_minimal_route(pg_session)
    pg_session.add(
        TripUpdateState(
            trip_id="T1",
            stop_sequence=1,
            service_date=date(2026, 5, 17),
            stop_id="S1",
            vehicle_id="V1",
            final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
            final_schedule_relationship="SKIPPED",
            last_pred_snapshot_ts=None,
            last_predicted_arrival_ts=None,
        )
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 5, 17),
        target_table_name="stop_events",
    )
    pg_session.commit()

    event = pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T1")).scalar_one()
    assert event.schedule_relationship == "SKIPPED"
    assert event.observed_arrival_ts is None


@pytest.mark.integration
def test_derive_sets_derived_at_on_state_rows(pg_session):
    """After derivation, the source state rows have derived_at set."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    _seed_minimal_route(pg_session)
    pg_session.add(
        TripUpdateState(
            trip_id="T1",
            stop_sequence=1,
            service_date=date(2026, 5, 17),
            stop_id="S1",
            vehicle_id="V1",
            final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
            final_schedule_relationship="SCHEDULED",
            last_pred_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
            last_predicted_arrival_ts=datetime(2026, 5, 17, 14, 6, 30),
        )
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 5, 17),
        target_table_name="stop_events",
    )
    pg_session.commit()

    row = pg_session.execute(
        select(TripUpdateState).where(TripUpdateState.trip_id == "T1")
    ).scalar_one()
    assert row.derived_at is not None
