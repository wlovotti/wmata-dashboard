"""
Unit and integration tests for src/ewt.py.

Covers the AWT formula edge cases, period/day_type bucketing, and the full
compute_ewt_for_route_date pipeline against an in-memory DB with stop_events,
stop_times, trips, and calendar fixtures. The frequent-cell-hour gate is
classifier-driven (mean scheduled headway ≤ 15 min at the cell-hour), so
tests don't seed `route_service_profile` rows — `is_frequent` is no longer
the gate.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from src.ewt import (
    EWT_TIME_PERIODS,
    _day_type_for,
    _eastern_hour,
    _is_cell_hour_frequent,
    _period_for_hour,
    compute_awt,
    compute_ewt_for_route_date,
    compute_ewt_for_routes,
)
from src.models import Calendar, Route, Stop, StopEvent, StopTime, Trip

ROUTE = "TEST1"
SERVICE_DATE = date(2026, 4, 14)  # Tuesday, EDT
SERVICE_DATE_STR = SERVICE_DATE.isoformat()
SERVICE_ID = "WK"


class TestComputeAwt:
    """Pure-formula tests — no DB."""

    def test_empty_returns_none(self):
        assert compute_awt([]) is None

    def test_single_headway_is_h_over_2(self):
        # AWT = h²/(2h) = h/2 — even with a single sample.
        assert compute_awt([600.0]) == 300.0

    def test_uniform_headways_reduce_to_h_over_2(self):
        # All 600s → mean(h²)/(2·mean(h)) = 360000/1200 = 300.
        assert compute_awt([600.0, 600.0, 600.0, 600.0]) == 300.0

    def test_bunching_pushes_awt_above_naive_half(self):
        # mean(h) = 600 (so naive h/2 = 300), but variance lifts AWT above that.
        # h = [60, 60, 1680]: sum = 1800, sum_sq = 3600+3600+2_822_400 = 2_829_600
        # AWT = 2_829_600 / 3600 = 786.0
        assert compute_awt([60.0, 60.0, 1680.0]) == pytest.approx(786.0)

    def test_all_zero_returns_none(self):
        assert compute_awt([0.0, 0.0]) is None

    def test_negative_total_returns_none(self):
        # Pathological — guards against weird input rather than a real case.
        assert compute_awt([-100.0]) is None


class TestPeriodForHour:
    """Eastern-hour to time_period boundary checks."""

    @pytest.mark.parametrize(
        "hour,expected",
        [
            (0, "Night (0-6)"),
            (5, "Night (0-6)"),
            (6, "AM Peak (6-9)"),
            (8, "AM Peak (6-9)"),
            (9, "Midday (9-15)"),
            (14, "Midday (9-15)"),
            (15, "PM Peak (15-19)"),
            (18, "PM Peak (15-19)"),
            (19, "Evening (19-24)"),
            (23, "Evening (19-24)"),
        ],
    )
    def test_known_hour_maps_to_period(self, hour, expected):
        assert _period_for_hour(hour) == expected

    @pytest.mark.parametrize("hour", [-1, 24])
    def test_out_of_range_raises(self, hour):
        with pytest.raises(ValueError):
            _period_for_hour(hour)


class TestDayTypeFor:
    """Calendar mapping matches service_profile.py / service_delivered.py."""

    def test_tuesday_is_weekday(self):
        assert _day_type_for(date(2026, 4, 14)) == "weekday"

    def test_friday_is_weekday(self):
        assert _day_type_for(date(2026, 4, 17)) == "weekday"

    def test_saturday(self):
        assert _day_type_for(date(2026, 4, 18)) == "saturday"

    def test_sunday(self):
        assert _day_type_for(date(2026, 4, 19)) == "sunday"


class TestEasternHour:
    """Naive-UTC → Eastern hour conversion."""

    def test_edt_summer(self):
        # 2026-04-14 11:00 UTC = 7:00 AM EDT (UTC-4 in April).
        assert _eastern_hour(datetime(2026, 4, 14, 11, 0, 0)) == 7

    def test_est_winter(self):
        # 2026-01-14 12:00 UTC = 7:00 AM EST (UTC-5 in January).
        assert _eastern_hour(datetime(2026, 1, 14, 12, 0, 0)) == 7

    def test_midnight_eastern_wrap(self):
        # 2026-04-14 04:00 UTC = 0:00 AM EDT — boundary into Night bucket.
        assert _eastern_hour(datetime(2026, 4, 14, 4, 0, 0)) == 0


class TestIsCellHourFrequent:
    """Cell-hour frequent classifier — mean scheduled headway ≤ 15 min (= 900s)."""

    def test_empty_is_not_frequent(self):
        assert _is_cell_hour_frequent([]) is False

    def test_at_threshold_is_frequent(self):
        # 900 s = 15 min exactly — boundary is inclusive.
        assert _is_cell_hour_frequent([900.0]) is True

    def test_above_threshold_is_not_frequent(self):
        # 901 s > 15 min — just over.
        assert _is_cell_hour_frequent([901.0]) is False

    def test_mean_above_threshold_excludes_cell_with_one_short_gap(self):
        # 60s + 1740s averages to 900 — borderline frequent. 60s + 1741s tips it.
        assert _is_cell_hour_frequent([60.0, 1741.0]) is False

    def test_short_uniform_headways_are_frequent(self):
        # 5-min headways — clearly frequent.
        assert _is_cell_hour_frequent([300.0, 300.0, 300.0]) is True


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
    """Minimal Stop row — not strictly required by EWT, kept for cross-test consistency."""
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
    stop_sequence: int = 1,
) -> None:
    """Insert one StopEvent row. observed_arrival_ts is naive UTC by convention."""
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
            schedule_relationship="SCHEDULED",
        )
    )
    db_session.commit()


class TestComputeEwtForRouteDate:
    """End-to-end: stop_events + stop_times + calendar → per-period rows."""

    def _seed_frequent_cell_at_7am(self, db_session) -> None:
        """One frequent cell at S1 dir 0 in hour 7 — 4 trips spaced 10 min apart.

        Mean scheduled headway = 600s = 10 min ≤ 15 min ⇒ cell-hour is frequent.
        """
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S1")
        # Scheduled: 7:00, 7:10, 7:20, 7:30 — three 600s headways.
        for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00", "07:30:00"]):
            trip_id = f"T{i + 1}"
            _seed_trip(db_session, trip_id, ROUTE)
            _seed_stop_time(db_session, trip_id, "S1", t)

    def test_no_schedule_returns_all_empty_periods(self, db_session):
        """A route with no scheduled trips has no frequent cells — all periods empty."""
        _seed_route(db_session)
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        assert [r["time_period"] for r in rows] == [p[0] for p in EWT_TIME_PERIODS]
        for r in rows:
            assert r["frequent_cell_hours"] == 0
            assert r["awt_seconds"] is None
            assert r["swt_seconds"] is None
            assert r["ewt_seconds"] is None
            assert r["n_observed_headways"] == 0
            assert r["n_scheduled_headways"] == 0
            assert r["day_type"] == "weekday"

    def test_uniform_observed_matches_swt_so_ewt_zero(self, db_session):
        """Perfectly even observations → AWT = SWT = 300 → EWT = 0."""
        self._seed_frequent_cell_at_7am(db_session)
        # Observed exactly on schedule at S1 dir 0 (11:00 UTC = 7:00 EDT, etc.).
        for i, minute in enumerate([0, 10, 20, 30]):
            _seed_stop_event(
                db_session,
                trip_id=f"T{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=datetime(2026, 4, 14, 11, minute, 0),
            )

        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["awt_seconds"] == pytest.approx(300.0)
        assert am["swt_seconds"] == pytest.approx(300.0)
        assert am["ewt_seconds"] == pytest.approx(0.0)
        assert am["n_observed_headways"] == 3
        assert am["n_scheduled_headways"] == 3
        assert am["frequent_cell_hours"] == 1

    def test_bunched_observations_lift_awt_above_swt(self, db_session):
        """Bunched observed arrivals → EWT > 0; exact value matches the formula."""
        self._seed_frequent_cell_at_7am(db_session)
        # Observed: 7:00, 7:01, 7:02, 7:30 → headways 60, 60, 1680.
        # AWT = (60² + 60² + 1680²) / (2 · 1800) = 2_829_600 / 3600 = 786.0
        for i, ts in enumerate(
            [
                datetime(2026, 4, 14, 11, 0, 0),
                datetime(2026, 4, 14, 11, 1, 0),
                datetime(2026, 4, 14, 11, 2, 0),
                datetime(2026, 4, 14, 11, 30, 0),
            ]
        ):
            _seed_stop_event(
                db_session,
                trip_id=f"T{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=ts,
            )

        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["awt_seconds"] == pytest.approx(786.0)
        assert am["swt_seconds"] == pytest.approx(300.0)
        assert am["ewt_seconds"] == pytest.approx(486.0)

    def test_no_observations_swt_only(self, db_session):
        """Schedule present, no observed events → AWT/EWT None, SWT populated."""
        self._seed_frequent_cell_at_7am(db_session)
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["awt_seconds"] is None
        assert am["swt_seconds"] == pytest.approx(300.0)
        assert am["ewt_seconds"] is None
        assert am["n_observed_headways"] == 0
        assert am["n_scheduled_headways"] == 3

    def test_proximity_source_excluded_from_observed(self, db_session):
        """Proximity-source events shouldn't double-count or contaminate observed."""
        self._seed_frequent_cell_at_7am(db_session)
        # Single trip_update arrival → no observed headways.
        _seed_stop_event(
            db_session,
            trip_id="T1",
            route_id=ROUTE,
            stop_id="S1",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 0, 0),
            source="trip_update",
        )
        # Three proximity arrivals; if we mistakenly included them we'd get headways.
        for i, minute in enumerate([5, 10, 15]):
            _seed_stop_event(
                db_session,
                trip_id=f"P{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=datetime(2026, 4, 14, 11, minute, 0),
                source="proximity",
            )
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["n_observed_headways"] == 0

    def test_direction_grouping_prevents_bidirectional_pooling(self, db_session):
        """Two arrivals at the same stop in opposite directions don't form a headway."""
        self._seed_frequent_cell_at_7am(db_session)
        # Direction 0 arrival.
        _seed_stop_event(
            db_session,
            trip_id="T1",
            route_id=ROUTE,
            stop_id="S1",
            direction_id=0,
            observed_arrival_ts=datetime(2026, 4, 14, 11, 0, 0),
        )
        # Direction 1 arrival 60s later — different cell, must not pair with the dir-0 row.
        _seed_stop_event(
            db_session,
            trip_id="T_OPP",
            route_id=ROUTE,
            stop_id="S1",
            direction_id=1,
            observed_arrival_ts=datetime(2026, 4, 14, 11, 1, 0),
        )
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        # No within-direction pair — zero observed headways.
        assert am["n_observed_headways"] == 0

    def test_sparse_cell_excluded_even_when_observed(self, db_session):
        """A cell-hour with mean scheduled headway > 15 min is dropped — even if
        observed has bunched arrivals that would otherwise inflate AWT.

        This is the regression that motivated the cell-level gate: under the
        old route-level rule, any sparse stop on a frequent route polluted SWT.
        """
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S_SPARSE")
        # Two scheduled arrivals 30 min apart at the same cell-hour (hour 14):
        # 14:00 and 14:30 → one 1800s headway, mean = 1800 > 900 ⇒ NOT frequent.
        for i, t in enumerate(["14:00:00", "14:30:00"]):
            tid = f"S{i + 1}"
            _seed_trip(db_session, tid, ROUTE)
            _seed_stop_time(db_session, tid, "S_SPARSE", t)
        # Observed bunched at 14:00, 14:01, 14:02 — would give 60s, 60s headways
        # if pooled. The cell must be excluded entirely.
        for i, minute in enumerate([0, 1, 2]):
            _seed_stop_event(
                db_session,
                trip_id=f"S{i + 1}",
                route_id=ROUTE,
                stop_id="S_SPARSE",
                observed_arrival_ts=datetime(2026, 4, 14, 18, minute, 0),  # 14:00 EDT
            )
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        midday = next(r for r in rows if r["time_period"] == "Midday (9-15)")
        assert midday["frequent_cell_hours"] == 0
        assert midday["n_observed_headways"] == 0
        assert midday["n_scheduled_headways"] == 0
        assert midday["awt_seconds"] is None
        assert midday["swt_seconds"] is None

    def test_branch_cell_excluded_trunk_cell_kept(self, db_session):
        """When one cell is frequent and another is sparse, only the frequent one pools."""
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S_TRUNK")
        _seed_stop(db_session, "S_BRANCH")
        # Trunk cell (S_TRUNK) — every 10 min in hour 7.
        for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00", "07:30:00"]):
            tid = f"TR{i + 1}"
            _seed_trip(db_session, tid, ROUTE)
            _seed_stop_time(db_session, tid, "S_TRUNK", t)
        # Branch cell (S_BRANCH) — only 7:00 and 7:50 (50 min apart, sparse).
        for i, t in enumerate(["07:00:00", "07:50:00"]):
            tid = f"BR{i + 1}"
            _seed_trip(db_session, tid, ROUTE)
            _seed_stop_time(db_session, tid, "S_BRANCH", t)
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        # Only the trunk cell-hour should pool: 3 headways at 600s each.
        assert am["frequent_cell_hours"] == 1
        assert am["n_scheduled_headways"] == 3
        assert am["swt_seconds"] == pytest.approx(300.0)


class TestCoverageRatio:
    """`coverage_ratio` is the observed-to-scheduled-headway gauge surfaced
    alongside EWT so the frontend can flag thin-data periods where the
    trip-update derivation missed enough arrivals to make AWT unreliable."""

    def _seed_frequent_cell_at_7am(self, db_session):
        """Identical setup to TestComputeEwtForRouteDate's helper of the same name."""
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S1")
        # Four scheduled arrivals at S1 in hour 7 → three 600s headways.
        for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00", "07:30:00"]):
            trip_id = f"T{i + 1}"
            _seed_trip(db_session, trip_id, ROUTE)
            _seed_stop_time(db_session, trip_id, "S1", t)

    def test_full_coverage_is_one(self, db_session):
        """Three observed headways out of three scheduled → coverage_ratio = 1.0."""
        self._seed_frequent_cell_at_7am(db_session)
        for i, minute in enumerate([0, 10, 20, 30]):
            _seed_stop_event(
                db_session,
                trip_id=f"T{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=datetime(2026, 4, 14, 11, minute, 0),
            )
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["coverage_ratio"] == pytest.approx(1.0)

    def test_partial_coverage_below_threshold(self, db_session):
        """One observed headway of three scheduled → coverage_ratio = 1/3."""
        self._seed_frequent_cell_at_7am(db_session)
        # Only two arrivals → one observed headway.
        _seed_stop_event(
            db_session,
            trip_id="T1",
            route_id=ROUTE,
            stop_id="S1",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 0, 0),
        )
        _seed_stop_event(
            db_session,
            trip_id="T2",
            route_id=ROUTE,
            stop_id="S1",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 10, 0),
        )
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["coverage_ratio"] == pytest.approx(1.0 / 3.0)
        # Sanity: this is what the UI threshold (<0.5) would flag.
        assert am["coverage_ratio"] < 0.5

    def test_no_observed_is_zero(self, db_session):
        """Schedule present, no observed → coverage_ratio = 0.0 (not None)."""
        self._seed_frequent_cell_at_7am(db_session)
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["coverage_ratio"] == 0.0

    def test_no_schedule_is_none(self, db_session):
        """No frequent cell-hours in a period → coverage_ratio = None (undefined)."""
        _seed_route(db_session)
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        for r in rows:
            assert r["coverage_ratio"] is None

    def test_clamped_at_one(self, db_session):
        """ADDED real-time-only trips can push observed > scheduled; clamp at 1.0."""
        self._seed_frequent_cell_at_7am(db_session)
        # Six observed arrivals → five observed headways vs three scheduled.
        for i, minute in enumerate([0, 5, 10, 15, 20, 25]):
            _seed_stop_event(
                db_session,
                trip_id=f"X{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=datetime(2026, 4, 14, 11, minute, 0),
            )
        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["coverage_ratio"] == 1.0
        # Raw counts unchanged — the clamp only affects the published ratio.
        assert am["n_observed_headways"] == 5
        assert am["n_scheduled_headways"] == 3


class TestComputeEwtForRoutes:
    """Batch helper enumerates routes from stop_events on the date."""

    def test_default_scans_routes_with_events(self, db_session):
        # R_A and R_B both have events on the date.
        _seed_route(db_session, "R_A")
        _seed_route(db_session, "R_B")
        _seed_stop(db_session, "S2")
        _seed_stop_event(
            db_session,
            trip_id="TA1",
            route_id="R_A",
            stop_id="S2",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 0, 0),
        )
        _seed_stop_event(
            db_session,
            trip_id="TB1",
            route_id="R_B",
            stop_id="S2",
            observed_arrival_ts=datetime(2026, 4, 14, 11, 0, 0),
        )

        rows = compute_ewt_for_routes(db_session, SERVICE_DATE)
        route_ids = sorted({r["route_id"] for r in rows})
        assert route_ids == ["R_A", "R_B"]
        assert len(rows) == 2 * len(EWT_TIME_PERIODS)

    def test_explicit_route_ids_restricts(self, db_session):
        _seed_route(db_session, "R_OTHER")
        rows = compute_ewt_for_routes(db_session, SERVICE_DATE, route_ids=["R_NONE"])
        # Even an unknown route gets evaluated (and emits empty placeholders).
        assert {r["route_id"] for r in rows} == {"R_NONE"}
        assert len(rows) == len(EWT_TIME_PERIODS)
