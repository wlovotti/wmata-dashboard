"""Tests for pipelines.derive_stop_events (proximity derivation).

Regression coverage for the 2026-06/07 production incident: the
vehicle_positions query must be bounded by timestamp (so Postgres can use
idx_route_timestamp instead of seq-scanning the whole table) WITHOUT
dropping legitimate past-midnight positions, while excluding rows whose
timestamp is wildly outside the service day (documented phantom rows from
stale AVL clocks).
"""

from datetime import date, datetime

import pytest
from sqlalchemy import select

from src.models import GTFSSnapshot, Stop, StopEvent, StopTime, Trip, VehiclePosition

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
                timestamp=datetime(2025, 10, 15, 12, 0, 0),  # phantom timestamp
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
    # NOTES-110: no anchor shift needed here (the same-day anchor already
    # lands close to the observation), so service_date must stay the
    # plain outer-loop value passed in.
    assert late_event.service_date == "2026-07-02"

    phantom_events = (
        pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T_PHANTOM"))
        .scalars()
        .all()
    )
    assert phantom_events == []


@pytest.mark.integration
def test_derive_corrects_owl_route_service_date_off_by_one(pg_session):
    """NOTES-110: the proximity pipeline's stop_events.service_date must be
    the anchor day resolve_stop_time actually resolved (misattributed
    service_date minus one day for SFMTA owl routes), not the raw
    outer-loop service_date argument.

    Same production shape as the trip_update-source sibling test in
    test_derive_stop_events_from_state.py: trip 12097226_M11-style stop
    with GTFS static arrival_time "24:09:40" Pacific, expressed here as
    the UTC-equivalent "31:09:40" purely to avoid DST arithmetic.
    """
    from pipelines.derive_stop_events import derive_proximity_stop_events

    pg_session.add_all(
        [
            Trip(trip_id="T_OWL", route_id="R14", direction_id=0, is_current=True),
            Stop(
                stop_id="S1",
                stop_name="Test Stop",
                stop_lat=STOP_LAT,
                stop_lon=STOP_LON,
                is_current=True,
            ),
            StopTime(
                trip_id="T_OWL",
                stop_sequence=2,
                stop_id="S1",
                arrival_time="31:09:40",  # UTC-equivalent of 24:09:40 Pacific
                departure_time="31:09:40",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T_OWL",
                route_id="R14",
                trip_start_date="20260723",  # misattributed +1 day
                latitude=STOP_LAT,
                longitude=STOP_LON,
                timestamp=datetime(2026, 7, 23, 7, 7, 22),
            ),
        ]
    )
    pg_session.commit()

    derive_proximity_stop_events(
        pg_session, route_id="R14", service_date=date(2026, 7, 23), tz_name="UTC"
    )
    pg_session.commit()

    event = pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T_OWL")).scalar_one()
    assert event.scheduled_arrival_ts == datetime(2026, 7, 23, 7, 9, 40)
    assert abs(event.deviation_sec) < 300
    assert event.service_date == "2026-07-22"


@pytest.mark.integration
def test_derive_supersedes_misattributed_row_on_reresolve(pg_session):
    """PR #193 review: a stop_events row previously written under the
    misattributed service_date (pre-NOTES-110 code, or a prior run before
    this fix) must be deleted when re-deriving lands the corrected row on
    the adjacent date -- otherwise both copies survive (service_date is
    part of uq_stop_events_run_stop_source, so the corrected row is a
    distinct upsert target, not a conflict against the stale one) and the
    same real-world event double-counts across two service dates.
    """
    from pipelines.derive_stop_events import derive_proximity_stop_events

    pg_session.add_all(
        [
            Trip(trip_id="T_OWL", route_id="R14", direction_id=0, is_current=True),
            Stop(
                stop_id="S1",
                stop_name="Test Stop",
                stop_lat=STOP_LAT,
                stop_lon=STOP_LON,
                is_current=True,
            ),
            StopTime(
                trip_id="T_OWL",
                stop_sequence=2,
                stop_id="S1",
                arrival_time="31:09:40",  # UTC-equivalent of 24:09:40 Pacific
                departure_time="31:09:40",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T_OWL",
                route_id="R14",
                trip_start_date="20260723",  # misattributed +1 day
                latitude=STOP_LAT,
                longitude=STOP_LON,
                timestamp=datetime(2026, 7, 23, 7, 7, 22),
            ),
            # Simulates a stop_event a prior derivation run (pre-NOTES-110,
            # or a subsequent PR #193 review round without the dedup fix) wrote under the
            # misattributed date.
            StopEvent(
                service_date="2026-07-23",
                trip_id="T_OWL",
                route_id="R14",
                direction_id=0,
                vehicle_id="V1",
                stop_id="S1",
                stop_sequence=2,
                scheduled_arrival_ts=datetime(2026, 7, 24, 7, 9, 40),
                scheduled_departure_ts=datetime(2026, 7, 24, 7, 9, 40),
                observed_arrival_ts=datetime(2026, 7, 23, 7, 7, 22),
                deviation_sec=-86538,
                source="proximity",
                schedule_relationship="SCHEDULED",
                match_distance_m=1.0,
                derived_at=datetime(2026, 7, 23, 8, 0, 0),
            ),
        ]
    )
    pg_session.commit()

    derive_proximity_stop_events(
        pg_session, route_id="R14", service_date=date(2026, 7, 23), tz_name="UTC"
    )
    pg_session.commit()

    events = (
        pg_session.execute(
            select(StopEvent).where(
                StopEvent.trip_id == "T_OWL",
                StopEvent.stop_sequence == 2,
                StopEvent.source == "proximity",
            )
        )
        .scalars()
        .all()
    )
    assert len(events) == 1
    assert events[0].service_date == "2026-07-22"
    assert abs(events[0].deviation_sec) < 300


@pytest.mark.integration
def test_derive_with_gtfs_snapshot_id_uses_historical_schedule(pg_session):
    """gtfs_snapshot_id derives against a superseded GTFS snapshot; without
    it, trips that exist only in that old snapshot are invisible.

    This is the backfill path for service dates whose schedule has since
    been replaced by a reload (e.g. 2026-06-11..20 against snapshot 12).
    """
    from pipelines.derive_stop_events import derive_proximity_stop_events

    pg_session.add_all(
        [
            GTFSSnapshot(snapshot_id=1, snapshot_date=datetime(2026, 5, 3, 18, 30, 0)),
            Trip(
                trip_id="T_HIST",
                route_id="R1",
                direction_id=0,
                snapshot_id=1,
                is_current=False,
            ),
            Stop(
                stop_id="S1",
                stop_name="Old Stop",
                stop_lat=STOP_LAT,
                stop_lon=STOP_LON,
                snapshot_id=1,
                is_current=False,
            ),
            StopTime(
                trip_id="T_HIST",
                stop_sequence=1,
                stop_id="S1",
                arrival_time="10:00:00",
                departure_time="10:00:30",
                snapshot_id=1,
                is_current=False,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T_HIST",
                route_id="R1",
                trip_start_date="20260611",
                latitude=STOP_LAT,
                longitude=STOP_LON,
                timestamp=datetime(2026, 6, 11, 14, 0, 0),
            ),
        ]
    )
    pg_session.commit()

    # Without the flag, the old-snapshot trip is invisible (no current GTFS).
    result = derive_proximity_stop_events(pg_session, route_id="R1", service_date=date(2026, 6, 11))
    assert result["rows_written"] == 0

    # With the flag, the historical schedule is used and the event lands.
    result = derive_proximity_stop_events(
        pg_session, route_id="R1", service_date=date(2026, 6, 11), gtfs_snapshot_id=1
    )
    pg_session.commit()
    assert result["rows_written"] == 1
    event = pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T_HIST")).scalar_one()
    assert event.observed_arrival_ts == datetime(2026, 6, 11, 14, 0, 0)


@pytest.mark.integration
def test_derive_tz_name_widens_window_for_late_pacific_positions(pg_session):
    """NOTES-100: ``tz_name`` genuinely changes the position-window bounds,
    not just cosmetically.

    A position at 2026-07-04 05:00 UTC is inside the Pacific 48h window
    for service_date=2026-07-02 (which runs 07-02 07:00 UTC .. 07-04
    07:00 UTC, PDT = UTC-7) but past the end of the Eastern window for
    the same service_date (07-02 04:00 UTC .. 07-04 04:00 UTC, EDT =
    UTC-4) -- Pacific's window closes 3h later in UTC than Eastern's.
    Without ``tz_name`` threaded through to the window helper, this
    position would be silently dropped for an SFMTA-style Pacific route.
    """
    from pipelines.derive_stop_events import derive_proximity_stop_events

    _seed_route_with_two_trips(pg_session)
    late_pacific_ts = datetime(2026, 7, 4, 5, 0, 0)
    pg_session.add(
        VehiclePosition(
            vehicle_id="V1",
            trip_id="T_LATE",
            route_id="R1",
            trip_start_date="20260702",
            latitude=STOP_LAT,
            longitude=STOP_LON,
            timestamp=late_pacific_ts,
        )
    )
    pg_session.commit()

    # Default (Eastern) window misses it.
    result_eastern = derive_proximity_stop_events(
        pg_session, route_id="R1", service_date=date(2026, 7, 2)
    )
    assert result_eastern["rows_written"] == 0

    # Pacific window catches it.
    result_pacific = derive_proximity_stop_events(
        pg_session,
        route_id="R1",
        service_date=date(2026, 7, 2),
        tz_name="America/Los_Angeles",
    )
    pg_session.commit()
    assert result_pacific["rows_written"] == 1
