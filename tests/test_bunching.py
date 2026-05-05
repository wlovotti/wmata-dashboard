"""
Unit and integration tests for src/bunching.py.

Covers the threshold formula edges and the end-to-end
compute_bunching_for_route_date pipeline against an in-memory DB with
stop_events, stop_times, trips, and calendar fixtures. Also exercises the
filters that distinguish bunching from EWT — schedule_relationship gating
and the > 120-min service-break drop.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from src.bunching import (
    BUNCHING_ABSOLUTE_FLOOR_SEC,
    BUNCHING_RATIO,
    _cell_hour_threshold_sec,
    compute_bunching_for_route_date,
    compute_bunching_for_routes,
)
from src.ewt import EWT_TIME_PERIODS
from src.models import Calendar, Route, Stop, StopEvent, StopTime, Trip

ROUTE = "TEST1"
SERVICE_DATE = date(2026, 4, 14)  # Tuesday, EDT
SERVICE_DATE_STR = SERVICE_DATE.isoformat()
SERVICE_ID = "WK"


class TestCellHourThresholdSec:
    """Pure threshold logic — no DB."""

    def test_empty_returns_none(self):
        assert _cell_hour_threshold_sec([]) is None

    def test_ratio_dominates_for_long_scheduled(self):
        # mean = 600s, ratio × 600 = 150s, floor = 120s → ratio wins.
        assert _cell_hour_threshold_sec([600.0]) == pytest.approx(150.0)

    def test_floor_dominates_for_short_scheduled(self):
        # mean = 240s, ratio × 240 = 60s, floor = 120s → floor wins.
        assert _cell_hour_threshold_sec([240.0]) == pytest.approx(120.0)

    def test_at_crossover_floor_and_ratio_match(self):
        # mean = 480s → ratio × 480 = 120s = floor exactly.
        assert _cell_hour_threshold_sec([480.0]) == pytest.approx(120.0)

    def test_constants_match_documented_values(self):
        # Wired sanity check: if these change, the module docstring needs updating.
        assert BUNCHING_RATIO == 0.25
        assert BUNCHING_ABSOLUTE_FLOOR_SEC == 120.0


def _seed_route(db_session, route_id: str = ROUTE) -> Route:
    """Insert a current Route row."""
    route = Route(
        route_id=route_id,
        route_short_name=route_id,
        route_long_name=f"Test Route {route_id}",
        route_type=3,
        is_current=True,
    )
    db_session.add(route)
    db_session.commit()
    return route


def _seed_calendar(db_session, service_id: str = SERVICE_ID) -> None:
    """Insert a Tuesday-active calendar row (covers the `weekday` day_type)."""
    cal = Calendar(
        service_id=service_id,
        monday=1,
        tuesday=1,
        wednesday=1,
        thursday=1,
        friday=1,
        saturday=0,
        sunday=0,
        start_date="20260101",
        end_date="20261231",
        is_current=True,
    )
    db_session.add(cal)
    db_session.commit()


def _seed_stop(db_session, stop_id: str) -> None:
    """Minimal Stop row — not required by bunching but kept for fixture parity."""
    db_session.add(Stop(stop_id=stop_id, stop_name=stop_id, stop_lat=0.0, stop_lon=0.0))
    db_session.commit()


def _seed_trip(db_session, trip_id: str, route_id: str, direction_id: int = 0) -> None:
    """Insert a current Trip row tied to SERVICE_ID."""
    db_session.add(
        Trip(
            trip_id=trip_id,
            route_id=route_id,
            service_id=SERVICE_ID,
            direction_id=direction_id,
            trip_headsign="Downtown",
            is_current=True,
        )
    )
    db_session.commit()


def _seed_stop_time(
    db_session,
    trip_id: str,
    stop_id: str,
    arrival_time: str,
    stop_sequence: int = 1,
) -> None:
    """Insert one StopTime row in current-version state."""
    db_session.add(
        StopTime(
            trip_id=trip_id,
            stop_id=stop_id,
            arrival_time=arrival_time,
            departure_time=arrival_time,
            stop_sequence=stop_sequence,
            is_current=True,
        )
    )
    db_session.commit()


def _seed_stop_event(
    db_session,
    trip_id: str,
    route_id: str,
    stop_id: str,
    observed_arrival_ts: datetime | None,
    direction_id: int = 0,
    source: str = "trip_update",
    schedule_relationship: str = "SCHEDULED",
    stop_sequence: int = 1,
) -> None:
    """Insert one StopEvent row. observed_arrival_ts is naive UTC."""
    db_session.add(
        StopEvent(
            service_date=SERVICE_DATE_STR,
            trip_id=trip_id,
            route_id=route_id,
            direction_id=direction_id,
            stop_id=stop_id,
            stop_sequence=stop_sequence,
            observed_arrival_ts=observed_arrival_ts,
            source=source,
            schedule_relationship=schedule_relationship,
        )
    )
    db_session.commit()


def _seed_schedule_at_7am(db_session, stop_id: str = "S1", direction_id: int = 0) -> None:
    """Four scheduled trips at 7:00, 7:10, 7:20, 7:30 — mean headway 600s.

    Threshold for this cell-hour: max(0.25 × 600, 120) = 150s. Pairs below
    150s observed will be marked bunched.
    """
    for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00", "07:30:00"]):
        trip_id = f"T_sched_{stop_id}_{direction_id}_{i + 1}"
        _seed_trip(db_session, trip_id, ROUTE, direction_id=direction_id)
        _seed_stop_time(db_session, trip_id, stop_id, t)


class TestComputeBunchingForRouteDate:
    """End-to-end: stop_events + stop_times + calendar → per-period rows."""

    def test_no_schedule_returns_all_empty_periods(self, db_session):
        """No GTFS schedule ⇒ no threshold ⇒ no eligible pairs in any period."""
        _seed_route(db_session)
        _seed_calendar(db_session)
        rows = compute_bunching_for_route_date(db_session, ROUTE, SERVICE_DATE)
        assert [r["time_period"] for r in rows] == [p[0] for p in EWT_TIME_PERIODS]
        for r in rows:
            assert r["bunching_count"] == 0
            assert r["total_headways"] == 0
            assert r["bunching_rate"] is None
            assert r["day_type"] == "weekday"

    def test_uniform_observed_matches_schedule_zero_bunching(self, db_session):
        """On-schedule observations: 600s headways, threshold 150s → none bunched."""
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S1")
        _seed_schedule_at_7am(db_session)
        # Observed exactly on schedule (11:00 UTC = 7:00 EDT, …).
        for i, minute in enumerate([0, 10, 20, 30]):
            _seed_stop_event(
                db_session,
                trip_id=f"T_obs_{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=datetime(2026, 4, 14, 11, minute, 0),
            )

        rows = compute_bunching_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["total_headways"] == 3  # four arrivals → three pairs
        assert am["bunching_count"] == 0
        assert am["bunching_rate"] == pytest.approx(0.0)

    def test_tight_observations_all_bunched(self, db_session):
        """Three pairs each 60s apart on a 600s schedule → all three bunched."""
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S1")
        _seed_schedule_at_7am(db_session)
        # 11:00, 11:01, 11:02, 11:03 UTC → three 60s pairs (< 150s threshold).
        for i, minute in enumerate([0, 1, 2, 3]):
            _seed_stop_event(
                db_session,
                trip_id=f"T_obs_{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=datetime(2026, 4, 14, 11, minute, 0),
            )

        rows = compute_bunching_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["total_headways"] == 3
        assert am["bunching_count"] == 3
        assert am["bunching_rate"] == pytest.approx(1.0)

    def test_mixed_observations_partial_rate(self, db_session):
        """Two pairs bunched, one not → bunching_rate = 2/3."""
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S1")
        _seed_schedule_at_7am(db_session)
        # 11:00, 11:01 (60s), 11:02 (60s), 11:12 (600s) → two bunched, one not.
        for i, (h, m) in enumerate([(11, 0), (11, 1), (11, 2), (11, 12)]):
            _seed_stop_event(
                db_session,
                trip_id=f"T_obs_{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=datetime(2026, 4, 14, h, m, 0),
            )

        rows = compute_bunching_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["total_headways"] == 3
        assert am["bunching_count"] == 2
        assert am["bunching_rate"] == pytest.approx(2 / 3, rel=1e-4)

    def test_direction_split_no_double_count(self, db_session):
        """Same stop_id served by both directions: pairs computed within direction.

        Tight pair (60s) in dir 0 must not pair with a near-simultaneous
        arrival in dir 1 — directions are separate cells. With one arrival in
        each direction at the same time and a tight pair only in dir 0, total
        should be 1 pair (the dir-0 tight pair), 1 bunched.
        """
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "SHARED")
        # Schedule both directions at the same stop, 7:00/7:10/7:20/7:30.
        _seed_schedule_at_7am(db_session, stop_id="SHARED", direction_id=0)
        _seed_schedule_at_7am(db_session, stop_id="SHARED", direction_id=1)
        # Direction 0: tight pair at 11:00 + 11:01.
        _seed_stop_event(
            db_session,
            trip_id="T_d0_a",
            route_id=ROUTE,
            stop_id="SHARED",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 0, 0),
            direction_id=0,
        )
        _seed_stop_event(
            db_session,
            trip_id="T_d0_b",
            route_id=ROUTE,
            stop_id="SHARED",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 1, 0),
            direction_id=0,
        )
        # Direction 1: single arrival at 11:00:30 — no pair.
        _seed_stop_event(
            db_session,
            trip_id="T_d1_a",
            route_id=ROUTE,
            stop_id="SHARED",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 0, 30),
            direction_id=1,
        )

        rows = compute_bunching_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["total_headways"] == 1
        assert am["bunching_count"] == 1

    def test_service_break_drops_pair(self, db_session):
        """Pair > 120 min apart is a service break, not a headway."""
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S1")
        _seed_schedule_at_7am(db_session)
        # 11:00 then 14:00 (= 7:00→10:00 EDT, 3-hour gap).
        _seed_stop_event(
            db_session,
            trip_id="T_obs_1",
            route_id=ROUTE,
            stop_id="S1",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 0, 0),
        )
        _seed_stop_event(
            db_session,
            trip_id="T_obs_2",
            route_id=ROUTE,
            stop_id="S1",
            observed_arrival_ts=datetime(2026, 4, 14, 14, 0, 0),
        )

        rows = compute_bunching_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["total_headways"] == 0
        assert am["bunching_rate"] is None

    def test_added_trips_excluded(self, db_session):
        """schedule_relationship='ADDED' rows should not enter the observed pool."""
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S1")
        _seed_schedule_at_7am(db_session)
        # SCHEDULED at 11:00 and 11:10 (one 600s pair, not bunched), plus an
        # ADDED arrival at 11:01 that would create two 60s bunched pairs if
        # incorrectly included.
        _seed_stop_event(
            db_session,
            trip_id="T_sch_a",
            route_id=ROUTE,
            stop_id="S1",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 0, 0),
        )
        _seed_stop_event(
            db_session,
            trip_id="T_added",
            route_id=ROUTE,
            stop_id="S1",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 1, 0),
            schedule_relationship="ADDED",
        )
        _seed_stop_event(
            db_session,
            trip_id="T_sch_b",
            route_id=ROUTE,
            stop_id="S1",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 10, 0),
        )

        rows = compute_bunching_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["total_headways"] == 1
        assert am["bunching_count"] == 0


class TestComputeBunchingForRoutes:
    """Driver: enumerates routes from stop_events, optionally restricts."""

    def test_no_routes_returns_empty(self, db_session):
        assert compute_bunching_for_routes(db_session, SERVICE_DATE) == []

    def test_explicit_route_filter(self, db_session):
        """Restricting to an unknown route still emits five placeholder rows."""
        _seed_route(db_session)
        _seed_calendar(db_session)
        rows = compute_bunching_for_routes(db_session, SERVICE_DATE, route_ids=["NOPE"])
        assert len(rows) == 5
        assert {r["time_period"] for r in rows} == {p[0] for p in EWT_TIME_PERIODS}
        for r in rows:
            assert r["route_id"] == "NOPE"
            assert r["total_headways"] == 0
