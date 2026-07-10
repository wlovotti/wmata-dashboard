"""Tests for pipelines.derive_stop_events (proximity derivation).

Regression coverage for the 2026-06/07 production incident: the
vehicle_positions query must be bounded by timestamp (so Postgres can use
idx_route_timestamp instead of seq-scanning the whole table) WITHOUT
dropping legitimate past-midnight positions, while excluding rows whose
timestamp is wildly outside the service day (the NOTES-81 phantom rows).
"""

from datetime import date, datetime

import pytest
from sqlalchemy import select

from src.models import Stop, StopEvent, StopTime, Trip, VehiclePosition

# Coordinates shared by the test stop and all test positions — within the
# 50m proximity threshold by construction.
STOP_LAT = 38.9072
STOP_LON = -77.0369


def _seed_route_with_two_trips(pg_session):
    """Seed one route with two current trips, each serving stop S1 once.

    T_LATE will get a legitimate past-midnight position; T_PHANTOM will get
    only a position with a garbage timestamp months in the past.
    """
    pg_session.add_all(
        [
            Trip(trip_id="T_LATE", route_id="R1", direction_id=0, is_current=True),
            Trip(trip_id="T_PHANTOM", route_id="R1", direction_id=0, is_current=True),
            Stop(
                stop_id="S1",
                stop_name="Test Stop",
                stop_lat=STOP_LAT,
                stop_lon=STOP_LON,
                is_current=True,
            ),
            StopTime(
                trip_id="T_LATE",
                stop_sequence=1,
                stop_id="S1",
                arrival_time="24:25:00",
                departure_time="24:25:30",
                is_current=True,
            ),
            StopTime(
                trip_id="T_PHANTOM",
                stop_sequence=1,
                stop_id="S1",
                arrival_time="12:00:00",
                departure_time="12:00:30",
                is_current=True,
            ),
        ]
    )


@pytest.mark.integration
def test_derive_keeps_past_midnight_positions_and_drops_phantom_timestamps(pg_session):
    """Positions after midnight of the service day still derive stop_events;
    positions with timestamps months outside the service day do not.

    Both positions carry trip_start_date='20260702', so before the
    timestamp-window fix the phantom row was matched and produced a
    stop_event with a 2025 observed_arrival_ts.
    """
    from pipelines.derive_stop_events import derive_proximity_stop_events

    _seed_route_with_two_trips(pg_session)
    late_ts = datetime(2026, 7, 3, 4, 30, 0)  # 00:30 ET on Jul 3 — past midnight
    pg_session.add_all(
        [
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T_LATE",
                route_id="R1",
                trip_start_date="20260702",
                latitude=STOP_LAT,
                longitude=STOP_LON,
                timestamp=late_ts,
            ),
            VehiclePosition(
                vehicle_id="V2",
                trip_id="T_PHANTOM",
                route_id="R1",
                trip_start_date="20260702",
                latitude=STOP_LAT,
                longitude=STOP_LON,
                timestamp=datetime(2025, 10, 15, 12, 0, 0),  # NOTES-81 phantom
            ),
        ]
    )
    pg_session.commit()

    derive_proximity_stop_events(pg_session, route_id="R1", service_date=date(2026, 7, 2))
    pg_session.commit()

    late_event = pg_session.execute(
        select(StopEvent).where(StopEvent.trip_id == "T_LATE")
    ).scalar_one()
    assert late_event.observed_arrival_ts == late_ts
    assert late_event.source == "proximity"

    phantom_events = (
        pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T_PHANTOM"))
        .scalars()
        .all()
    )
    assert phantom_events == []
