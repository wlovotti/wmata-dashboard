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
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.ewt import (
    EWT_TIME_PERIODS,
    _dates_matching_weekday,
    _day_type_for,
    _eastern_hour,
    _feed_validity_window,
    _is_cell_hour_frequent,
    _period_for_hour,
    _resolve_service_ids_for_day_type,
    _scheduled_headways_by_cell_hour,
    compute_awt,
    compute_ewt_for_route_date,
    compute_ewt_for_routes,
    compute_ewt_headline_for_route,
    fetch_scheduled_cell_hours_for_routes,
)
from src.models import (
    Base,
    Calendar,
    CalendarDate,
    GTFSSnapshot,
    Route,
    Stop,
    StopEvent,
    StopTime,
    Trip,
)

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

    def test_defaults_to_eastern_when_tz_name_omitted(self):
        # Same as test_edt_summer — confirms the new tz_name param defaults
        # to Eastern so every existing WMATA call site is byte-for-byte
        # unaffected by NOTES-103.
        assert _eastern_hour(datetime(2026, 4, 14, 11, 0, 0)) == 7

    def test_pacific_late_afternoon_buckets_correctly(self):
        # 2026-04-15 00:00 UTC = 17:00 (5pm) PDT the prior day (UTC-7 in
        # April) — a late-afternoon Pacific hour that would land somewhere
        # else entirely (20:00 / 8pm) if this were mistakenly bucketed as
        # Eastern. This is the NOTES-103 regression case.
        assert _eastern_hour(datetime(2026, 4, 15, 0, 0, 0), tz_name="America/Los_Angeles") == 17
        # Sanity: the same instant read as Eastern (the old hardcoded
        # behavior) gives a different, wrong hour — proving the two zones
        # actually diverge for this timestamp rather than coincidentally
        # agreeing.
        assert _eastern_hour(datetime(2026, 4, 15, 0, 0, 0)) == 20

    def test_pacific_pdt_dst_boundary(self):
        # 2026-01-15 20:00 UTC = 12:00 PM PST (UTC-8 in January, before PDT
        # starts) — confirms zoneinfo's DST handling isn't hardcoded either.
        assert _eastern_hour(datetime(2026, 1, 15, 20, 0, 0), tz_name="America/Los_Angeles") == 12


class TestIsCellHourFrequent:
    """Cell-hour frequent classifier — mean scheduled headway ≤ gate_sec (default 900s)."""

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

    def test_custom_gate_includes_medium_freq_headways(self):
        # 18-min mean is excluded by the 15-min default gate but included
        # by the 20-min medium-freq gate. Mirrors the per-route gate
        # lookup used in production.
        eighteen_min_secs = [18 * 60]
        assert _is_cell_hour_frequent(eighteen_min_secs) is False
        assert _is_cell_hour_frequent(eighteen_min_secs, gate_sec=20 * 60) is True

    def test_custom_gate_boundary_is_inclusive(self):
        # 20-min headway under a 20-min gate is in (≤ is inclusive),
        # 20-min-and-one-second is out.
        assert _is_cell_hour_frequent([20 * 60], gate_sec=20 * 60) is True
        assert _is_cell_hour_frequent([20 * 60 + 1], gate_sec=20 * 60) is False


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


def _seed_calendar_flag(
    db_session,
    service_id: str,
    *,
    monday: int = 0,
    tuesday: int = 0,
    wednesday: int = 0,
    thursday: int = 0,
    friday: int = 0,
    saturday: int = 0,
    sunday: int = 0,
    start_date: str = "20260101",
    end_date: str = "20261231",
) -> None:
    """Insert a `calendar` row with explicit per-weekday flags (default all 0).

    Unlike the module-level `_seed_calendar` helper (which always seeds a
    Mon-Fri weekday row), this lets a test model a `calendar` with NO
    weekday coverage at all — the Muni/SFMTA shape NOTES-106 fixes.
    """
    db_session.add(
        Calendar(
            service_id=service_id,
            monday=monday,
            tuesday=tuesday,
            wednesday=wednesday,
            thursday=thursday,
            friday=friday,
            saturday=saturday,
            sunday=sunday,
            start_date=start_date,
            end_date=end_date,
            is_current=True,
        )
    )
    db_session.commit()


def _seed_calendar_date(db_session, service_id: str, date_str: str, exception_type: int) -> None:
    """Insert a `calendar_dates` exception row (1=added, 2=removed)."""
    db_session.add(
        CalendarDate(
            service_id=service_id,
            date=date_str,
            exception_type=exception_type,
            is_current=True,
        )
    )
    db_session.commit()


def _seed_trip_with_service(
    db_session, trip_id: str, route_id: str, service_id: str, direction_id: int = 0
) -> None:
    """Insert a current Trip row tied to an arbitrary `service_id` (unlike the
    module-level `_seed_trip`, which always uses the global `SERVICE_ID`)."""
    db_session.add(
        Trip(
            trip_id=trip_id,
            route_id=route_id,
            service_id=service_id,
            direction_id=direction_id,
            trip_headsign="Downtown",
            is_current=True,
        )
    )
    db_session.commit()


class TestFeedValidityWindow:
    """Direct tests of `_feed_validity_window` — the union of `calendar`
    start/end ranges and `calendar_dates` dates that bounds which dates
    modal resolution samples."""

    def test_union_of_calendar_and_calendar_dates_ranges(self, db_session):
        _seed_calendar_flag(db_session, "WK", tuesday=1, start_date="20260301", end_date="20260601")
        _seed_calendar_date(
            db_session, "EXTRA", "20260901", exception_type=1
        )  # later than end_date
        assert _feed_validity_window(db_session) == (date(2026, 3, 1), date(2026, 9, 1))

    def test_calendar_dates_only_feed(self, db_session):
        """No `calendar` rows at all — window comes entirely from
        `calendar_dates` (the Muni/SFMTA shape)."""
        _seed_calendar_date(db_session, "A", "20260723", exception_type=1)
        _seed_calendar_date(db_session, "B", "20260828", exception_type=1)
        assert _feed_validity_window(db_session) == (date(2026, 7, 23), date(2026, 8, 28))

    def test_returns_none_when_no_data(self, db_session):
        assert _feed_validity_window(db_session) is None

    def test_malformed_calendar_end_date_is_skipped(self, db_session):
        """A malformed `calendar.end_date` must not raise — the window
        falls back to whatever other evidence (here, `calendar_dates`) is
        parseable, per the finding-C hardening (`_try_parse_yyyymmdd`)."""
        _seed_calendar_flag(
            db_session, "WK", tuesday=1, start_date="20260301", end_date="not-a-date"
        )
        _seed_calendar_date(db_session, "WK", "20260414", exception_type=1)
        # start_date parses fine; end_date doesn't, so the max comes from
        # calendar_dates instead of raising.
        assert _feed_validity_window(db_session) == (date(2026, 3, 1), date(2026, 4, 14))


class TestDatesMatchingWeekday:
    """Direct tests of `_dates_matching_weekday`."""

    def test_enumerates_every_matching_weekday_in_range(self, db_session):
        # 2026-03-31, 04-07, 04-14 are consecutive Tuesdays (weekday index 1).
        assert _dates_matching_weekday(date(2026, 3, 28), date(2026, 4, 16), 1) == [
            date(2026, 3, 31),
            date(2026, 4, 7),
            date(2026, 4, 14),
        ]

    def test_empty_when_start_after_end(self):
        assert _dates_matching_weekday(date(2026, 4, 14), date(2026, 4, 1), 1) == []

    def test_single_day_range_matching(self):
        assert _dates_matching_weekday(date(2026, 4, 14), date(2026, 4, 14), 1) == [
            date(2026, 4, 14)
        ]

    def test_single_day_range_not_matching(self):
        assert _dates_matching_weekday(date(2026, 4, 14), date(2026, 4, 14), 5) == []


class TestResolveServiceIdsForDayTypeModal:
    """Direct tests of `_resolve_service_ids_for_day_type`'s modal-resolution
    core, isolated from the `_scheduled_headways_by_cell_hour` wiring.
    Reference weekdays (all Tuesdays, 7 days apart, so no independent
    verification needed beyond the first): 2026-03-31, 04-07, 04-14, 04-21,
    04-28, 05-05.
    """

    def test_single_sample_wins_trivially(self, db_session):
        _seed_calendar_date(db_session, "WKADD", "20260414", exception_type=1)
        assert _resolve_service_ids_for_day_type(db_session, "weekday") == {"WKADD"}

    def test_majority_wins_over_terminal_edge_exceptions(self, db_session):
        """The exact shape of the real WMATA bug: a base service_id covers
        most Tuesdays in the window, but the LAST few Tuesdays each carry
        their own distinct type=1/type=2 substitution pair (a
        schedule-transition artifact stacked at the feed's terminal edge).
        No single substitute earns more than one vote, so the base
        service_id's 4-vote majority wins cleanly over 1-vote-each
        substitutes."""
        _seed_calendar_flag(
            db_session, "MTWT_SID", tuesday=1, start_date="20260325", end_date="20260510"
        )
        # Tuesdays in [2026-03-25, 2026-05-10]: 3/31, 4/7, 4/14, 4/21, 4/28, 5/5 (6 total).
        # Terminal two (4/28, 5/5) each get a one-off substitution.
        _seed_calendar_date(db_session, "MTWT_SID", "20260428", exception_type=2)
        _seed_calendar_date(db_session, "SUB1", "20260428", exception_type=1)
        _seed_calendar_date(db_session, "MTWT_SID", "20260505", exception_type=2)
        _seed_calendar_date(db_session, "SUB2", "20260505", exception_type=1)

        assert _resolve_service_ids_for_day_type(db_session, "weekday") == {"MTWT_SID"}

    def test_exception_on_the_most_recent_date_does_not_win(self, db_session):
        """Minimal case of the same bug: ONLY the most recent sampled date
        carries an exception — proving modal resolution doesn't special-case
        "most recent" the way the rejected anchor design did."""
        _seed_calendar_flag(
            db_session, "BASE", tuesday=1, start_date="20260325", end_date="20260428"
        )
        # Tuesdays: 3/31, 4/7, 4/14, 4/21, 4/28 (5 total) — only the last one substituted.
        _seed_calendar_date(db_session, "BASE", "20260428", exception_type=2)
        _seed_calendar_date(db_session, "SUB", "20260428", exception_type=1)

        assert _resolve_service_ids_for_day_type(db_session, "weekday") == {"BASE"}

    def test_majority_schedule_revision_era_wins_over_more_recent_minority_era(self, db_session):
        """The real SFMTA shape: two calendar_dates-only weekday service_ids
        on non-overlapping date ranges (no `calendar` row at all). Era A
        covers 3 Tuesdays, era B (the more recent revision) covers only 2.
        Modal resolution picks era A — the MAJORITY era, not the most
        recent one. This is intended: era A is what the schedule looked
        like on most of the sampled weekdays."""
        _seed_calendar_date(db_session, "ERA_A", "20260331", exception_type=1)
        _seed_calendar_date(db_session, "ERA_A", "20260407", exception_type=1)
        _seed_calendar_date(db_session, "ERA_A", "20260414", exception_type=1)
        _seed_calendar_date(db_session, "ERA_B", "20260421", exception_type=1)
        _seed_calendar_date(db_session, "ERA_B", "20260428", exception_type=1)

        assert _resolve_service_ids_for_day_type(db_session, "weekday") == {"ERA_A"}

    def test_tiebreak_prefers_set_with_more_recent_contributing_date(self, db_session):
        """Two eras tied 2-2 on vote count — the tiebreak picks the era
        whose most recent contributing date is later."""
        _seed_calendar_date(db_session, "ERA_A", "20260331", exception_type=1)
        _seed_calendar_date(db_session, "ERA_A", "20260407", exception_type=1)
        _seed_calendar_date(db_session, "ERA_B", "20260414", exception_type=1)
        _seed_calendar_date(db_session, "ERA_B", "20260421", exception_type=1)

        assert _resolve_service_ids_for_day_type(db_session, "weekday") == {"ERA_B"}

    def test_empty_sampled_dates_are_skipped_not_counted_as_modal(self, db_session):
        """A Tuesday that resolves to nothing (e.g. a removal with no
        replacement — a genuine one-off outage) must not out-vote a real,
        non-empty majority just by being another sampled date."""
        _seed_calendar_flag(
            db_session, "BASE", tuesday=1, start_date="20260325", end_date="20260421"
        )
        # Tuesdays: 3/31, 4/7, 4/14, 4/21 (4 total) — one pure removal, no substitute added.
        _seed_calendar_date(db_session, "BASE", "20260421", exception_type=2)

        assert _resolve_service_ids_for_day_type(db_session, "weekday") == {"BASE"}

    def test_all_empty_stays_empty(self, db_session):
        _seed_calendar_flag(
            db_session, "BASE", tuesday=1, start_date="20260325", end_date="20260414"
        )
        # Every Tuesday in range (3/31, 4/7, 4/14) is removed with no replacement.
        _seed_calendar_date(db_session, "BASE", "20260331", exception_type=2)
        _seed_calendar_date(db_session, "BASE", "20260407", exception_type=2)
        _seed_calendar_date(db_session, "BASE", "20260414", exception_type=2)

        assert _resolve_service_ids_for_day_type(db_session, "weekday") == set()

    def test_returns_empty_when_no_calendar_data_at_all(self, db_session):
        assert _resolve_service_ids_for_day_type(db_session, "weekday") == set()


class TestResolveServiceIdsForDayTypeMemoization:
    """`_resolve_service_ids_for_day_type` is called fresh per route from
    `_scheduled_headways_by_cell_hour` (no upstream caching the way
    `fetch_scheduled_cell_hours_for_routes`'s vectorized path has) — modal
    resolution costs ~4 window queries + 3 per sampled representative
    date, so unmemoized it multiplies into thousands of extra round-trips
    across a route loop. These tests assert the module-level memo
    (`_service_id_resolution_cache`) actually avoids re-running that work,
    and that it doesn't cross-contaminate across snapshot_ids — mirroring
    `_schedule_cache`'s exact `(db_identity, day_type, resolved
    snapshot_id)` keying and eviction semantics. Cross-database isolation
    (the `db_identity` component, NOTES-108) is covered separately by
    `TestScheduleCacheAgencyIsolation` below.
    """

    def test_second_call_for_same_day_type_and_snapshot_reuses_cached_result(
        self, db_session, monkeypatch
    ):
        """The expensive per-date resolution (`scheduled_service_ids_for_date`)
        must not run again on a cache hit — monkeypatched to count calls
        rather than asserting on raw SQL statement counts, since the
        cheap snapshot-id-resolution query legitimately still runs on
        every call (mirrors `_schedule_cache`'s own behavior)."""
        import src.ewt as ewt_module

        _seed_calendar_flag(db_session, "WK", tuesday=1, start_date="20260301", end_date="20260401")
        calls = {"n": 0}
        original = ewt_module.scheduled_service_ids_for_date

        def _counting(*args, **kwargs):
            calls["n"] += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(ewt_module, "scheduled_service_ids_for_date", _counting)

        first = _resolve_service_ids_for_day_type(db_session, "weekday")
        assert calls["n"] > 0, "the first call must actually do the per-date resolution work"

        calls["n"] = 0
        second = _resolve_service_ids_for_day_type(db_session, "weekday")

        assert calls["n"] == 0, "a cache hit must not re-run per-date resolution"
        assert second == first == {"WK"}

    def test_different_snapshot_ids_do_not_cross_contaminate(self, db_session):
        """Two explicit `gtfs_snapshot_id`s with different underlying
        calendars must each resolve to their OWN service_id, never the
        other's — including after the eviction that storing the second
        snapshot's entry triggers for the first (mirrors `_schedule_cache`:
        storing evicts every entry keyed to a different snapshot_id)."""
        db_session.add(
            Calendar(
                service_id="S1",
                monday=0,
                tuesday=1,
                wednesday=0,
                thursday=0,
                friday=0,
                saturday=0,
                sunday=0,
                start_date="20260301",
                end_date="20260401",
                snapshot_id=1,
                is_current=True,
            )
        )
        db_session.add(
            Calendar(
                service_id="S2",
                monday=0,
                tuesday=1,
                wednesday=0,
                thursday=0,
                friday=0,
                saturday=0,
                sunday=0,
                start_date="20260301",
                end_date="20260401",
                snapshot_id=2,
                is_current=True,
            )
        )
        db_session.commit()

        assert _resolve_service_ids_for_day_type(db_session, "weekday", gtfs_snapshot_id=1) == {
            "S1"
        }
        assert _resolve_service_ids_for_day_type(db_session, "weekday", gtfs_snapshot_id=2) == {
            "S2"
        }
        # Re-fetch both — snapshot 1's cache entry was evicted by storing
        # snapshot 2's, forcing a recompute, but it must still resolve
        # correctly (not silently return snapshot 2's cached value).
        assert _resolve_service_ids_for_day_type(db_session, "weekday", gtfs_snapshot_id=1) == {
            "S1"
        }
        assert _resolve_service_ids_for_day_type(db_session, "weekday", gtfs_snapshot_id=2) == {
            "S2"
        }


class TestScheduledHeadwaysCalendarDatesResolution:
    """NOTES-106 (+ two review follow-ups): `_scheduled_headways_by_cell_hour`
    must honor `calendar_dates` when `calendar` itself defines no
    day-of-week service for the day_type — the Muni/SFMTA shape, where
    weekday service exists purely as `calendar_dates` exception_type=1
    (added) rows. The resolver samples every representative-weekday date
    in the feed's validity window and takes the MODAL (most common)
    resolved service_id set (`_resolve_service_ids_for_day_type`, tested
    directly above) — these tests check that resolver is wired correctly
    into the scheduled-headway pool end-to-end, not the modal arithmetic
    itself.

    Tests call the private `_scheduled_headways_by_cell_hour` directly —
    same pattern the file already uses for `_day_type_for` / `_eastern_hour`
    / `_is_cell_hour_frequent` — to isolate the calendar-resolution logic
    from the AWT/SWT pooling machinery.
    """

    def test_weekday_only_via_calendar_dates_addition_is_resolved(self, db_session):
        """Muni shape: `calendar` has ONLY a Saturday row (no weekday flags
        anywhere), and the real weekday service exists purely as a single
        `calendar_dates` exception_type=1 addition. Before the original
        NOTES-106 fix this returned an empty dict for day_type='weekday'."""
        _seed_route(db_session)
        _seed_calendar_flag(db_session, "SAT", saturday=1)
        _seed_calendar_date(db_session, "WKADD", "20260414", exception_type=1)  # Tue
        _seed_stop(db_session, "S1")
        for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00"]):
            trip_id = f"WT{i + 1}"
            _seed_trip_with_service(db_session, trip_id, ROUTE, "WKADD")
            _seed_stop_time(db_session, trip_id, "S1", t)

        sched = _scheduled_headways_by_cell_hour(db_session, ROUTE, "weekday")

        assert sched, "weekday scheduled headways must not be empty on a calendar_dates-only feed"
        assert sched[(0, "S1", 7)] == [600.0, 600.0]

    def test_wmata_shaped_terminal_exceptions_do_not_override_the_majority_schedule(
        self, db_session
    ):
        """Real-shape regression (re-review finding A.1): split calendar
        rows (a Mon-Thu service_id with `tuesday=1`, plus a separate
        Friday-only service_id with `tuesday=0` — WMATA's actual split,
        see NOTES-51) PLUS type=1/type=2 exception pairs stacked on the
        terminal Tuesdays of the window. The resolved schedule must still
        match the Mon-Thu service_id's trips — not an exception
        substitute — because it's the majority across the sampled window."""
        _seed_route(db_session)
        _seed_calendar_flag(
            db_session,
            "MTWT_SID",
            monday=1,
            tuesday=1,
            wednesday=1,
            thursday=1,
            start_date="20260325",
            end_date="20260510",
        )
        _seed_calendar_flag(
            db_session, "FRI_SID", friday=1, start_date="20260325", end_date="20260510"
        )
        # Terminal-edge exception pairs on the last two Tuesdays (4/28, 5/5).
        _seed_calendar_date(db_session, "MTWT_SID", "20260428", exception_type=2)
        _seed_calendar_date(db_session, "SUB1", "20260428", exception_type=1)
        _seed_calendar_date(db_session, "MTWT_SID", "20260505", exception_type=2)
        _seed_calendar_date(db_session, "SUB2", "20260505", exception_type=1)
        _seed_stop(db_session, "S1")
        for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00"]):
            trip_id = f"T{i + 1}"
            _seed_trip_with_service(db_session, trip_id, ROUTE, "MTWT_SID")
            _seed_stop_time(db_session, trip_id, "S1", t)
        # A substitute trip that must NOT show up in the resolved schedule.
        _seed_trip_with_service(db_session, "SUBTRIP", ROUTE, "SUB1")
        _seed_stop_time(db_session, "SUBTRIP", "S1", "23:00:00")

        sched = _scheduled_headways_by_cell_hour(db_session, ROUTE, "weekday")

        assert sched == {(0, "S1", 7): [600.0, 600.0]}

    def test_disjoint_schedule_revision_eras_resolve_to_the_majority_era_not_an_interleave(
        self, db_session
    ):
        """The real SFMTA shape (service_id `78968` covers 7/23-8/14 —
        3 Tuesdays in a small window — `82660` takes over 8/17-8/28 — 2
        Tuesdays): two calendar_dates-only weekday service_ids with
        DIFFERENT arrival times, added on non-overlapping dates. The
        resolved pool must match exactly the MAJORITY era's timetable —
        not a blended interleave of both (the bug the original,
        since-rejected feed-wide-pooling fallback had)."""
        _seed_route(db_session)
        for d in ("20260331", "20260407", "20260414"):  # ERA_A: 3 Tuesdays (majority)
            _seed_calendar_date(db_session, "ERA_A", d, exception_type=1)
        for d in ("20260421", "20260428"):  # ERA_B: 2 Tuesdays (minority, more recent)
            _seed_calendar_date(db_session, "ERA_B", d, exception_type=1)
        _seed_stop(db_session, "S1")
        # ERA_A: 10-minute headway (600s) — the majority, expected winner.
        for i, t in enumerate(["07:00:00", "07:10:00"]):
            trip_id = f"A{i + 1}"
            _seed_trip_with_service(db_session, trip_id, ROUTE, "ERA_A")
            _seed_stop_time(db_session, trip_id, "S1", t)
        # ERA_B: 20-minute headway (1200s) — would pollute the pool if blended in.
        for i, t in enumerate(["07:00:00", "07:20:00"]):
            trip_id = f"B{i + 1}"
            _seed_trip_with_service(db_session, trip_id, ROUTE, "ERA_B")
            _seed_stop_time(db_session, trip_id, "S1", t)

        sched = _scheduled_headways_by_cell_hour(db_session, ROUTE, "weekday")

        assert sched == {(0, "S1", 7): [600.0]}, "must resolve to ERA_A only, not a blend"

    def test_weekend_base_calendar_unaffected_by_a_single_one_off_swap(self, db_session):
        """The real SFMTA shape found in NOTES-106: a Saturday base
        service_id is removed via calendar_dates type=2 AND a substitute
        service_id is added via type=1 on exactly ONE Saturday out of
        several in the window (a one-off holiday schedule swap). The base
        service_id's majority vote must win — the acceptance criterion is
        'weekend values unchanged'."""
        _seed_route(db_session)
        _seed_calendar_flag(
            db_session, "SAT_BASE", saturday=1, start_date="20260404", end_date="20260425"
        )
        # Saturdays in range: 4/4, 4/11, 4/18, 4/25 (4 total) — only 4/18 swapped.
        _seed_calendar_date(db_session, "SAT_BASE", "20260418", exception_type=2)
        _seed_calendar_date(db_session, "SAT_SWAP", "20260418", exception_type=1)
        _seed_stop(db_session, "S1")
        _seed_stop(db_session, "S2")
        for i, t in enumerate(["09:00:00", "09:15:00", "09:30:00"]):
            trip_id = f"BASE{i + 1}"
            _seed_trip_with_service(db_session, trip_id, ROUTE, "SAT_BASE")
            _seed_stop_time(db_session, trip_id, "S1", t)
        for i, t in enumerate(["10:00:00", "10:05:00"]):
            trip_id = f"SWAP{i + 1}"
            _seed_trip_with_service(db_session, trip_id, ROUTE, "SAT_SWAP")
            _seed_stop_time(db_session, trip_id, "S2", t)

        sched = _scheduled_headways_by_cell_hour(db_session, ROUTE, "saturday")

        assert sched == {(0, "S1", 9): [900.0, 900.0]}
        assert (0, "S2", 10) not in sched

    def test_end_to_end_ewt_weekday_non_null_on_calendar_dates_only_feed(self, db_session):
        """Integration check through `compute_ewt_for_route_date`: the Muni
        shape from `test_weekday_only_via_calendar_dates_addition_is_resolved`
        must produce a non-None, non-degenerate weekday EWT row when there's
        matching observed service, not just a non-empty scheduled pool."""
        _seed_route(db_session)
        _seed_calendar_flag(db_session, "SAT", saturday=1)
        _seed_calendar_date(db_session, "WKADD", "20260414", exception_type=1)  # Tue
        _seed_stop(db_session, "S1")
        for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00", "07:30:00"]):
            trip_id = f"WT{i + 1}"
            _seed_trip_with_service(db_session, trip_id, ROUTE, "WKADD")
            _seed_stop_time(db_session, trip_id, "S1", t)
        for i, minute in enumerate([0, 10, 20, 30]):
            _seed_stop_event(
                db_session,
                trip_id=f"WT{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=datetime(2026, 4, 14, 11, minute, 0),
            )

        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["frequent_cell_hours"] == 1
        assert am["n_scheduled_headways"] == 3
        assert am["swt_seconds"] == pytest.approx(300.0)
        assert am["awt_seconds"] == pytest.approx(300.0)
        assert am["ewt_seconds"] == pytest.approx(0.0)


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


class TestComputeEwtHeadlineForRoutePeriodFilter:
    """`period_key` (NOTES-41) restricts the headline to one Eastern-hour bucket.

    Verifies the filter excludes non-matching cell-hours from the EWT pool —
    required for the RouteDetail filter to actually re-slice the headline.
    """

    def _seed_two_frequent_cells_at_7am_and_3pm(self, db_session) -> None:
        """One frequent cell at S_AM dir 0 in hour 7, another at S_PM dir 0 in hour 15.

        Different stops are used so the consecutive-headway computation
        (per-cell, then bucket-by-earlier-arrival's-hour) doesn't bridge the
        two cells via a phantom 7:30→15:00 mid-day gap that would dominate
        the cell mean and disqualify the 7am cell from the frequent gate.
        Mirrors what real GTFS data looks like: stops with morning service
        and a separate set of stops with afternoon service.
        """
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S_AM")
        _seed_stop(db_session, "S_PM")
        # Hour 7 cell at S_AM (7:00, 7:10, 7:20, 7:30) — three 600s headways.
        for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00", "07:30:00"]):
            trip_id = f"TA{i + 1}"
            _seed_trip(db_session, trip_id, ROUTE)
            _seed_stop_time(db_session, trip_id, "S_AM", t, stop_sequence=1)
        # Hour 15 cell at S_PM (15:00, 15:10, 15:20, 15:30) — three 600s headways.
        for i, t in enumerate(["15:00:00", "15:10:00", "15:20:00", "15:30:00"]):
            trip_id = f"TP{i + 1}"
            _seed_trip(db_session, trip_id, ROUTE)
            _seed_stop_time(db_session, trip_id, "S_PM", t, stop_sequence=1)

    def test_no_filter_pools_both_periods(self, db_session):
        """`period_key='all'` keeps both cell-hours in the pool."""
        self._seed_two_frequent_cells_at_7am_and_3pm(db_session)
        # Observed at 7:00, 7:10, 7:20, 7:30 EDT (= 11:00, 11:10, 11:20, 11:30 UTC)
        for i, minute in enumerate([0, 10, 20, 30]):
            _seed_stop_event(
                db_session,
                trip_id=f"TA{i + 1}",
                route_id=ROUTE,
                stop_id="S_AM",
                observed_arrival_ts=datetime(2026, 4, 14, 11, minute, 0),
            )
        # Observed at 15:00, 15:10, 15:20, 15:30 EDT (= 19:00, 19:10, 19:20, 19:30 UTC)
        for i, minute in enumerate([0, 10, 20, 30]):
            _seed_stop_event(
                db_session,
                trip_id=f"TP{i + 1}",
                route_id=ROUTE,
                stop_id="S_PM",
                observed_arrival_ts=datetime(2026, 4, 14, 19, minute, 0),
            )

        result = compute_ewt_headline_for_route(db_session, ROUTE, SERVICE_DATE, period_key="all")
        # Two frequent cell-hours, three observed headways each → 6 total.
        assert result["frequent_cell_hours"] == 2
        assert result["n_observed_headways"] == 6
        assert result["n_scheduled_headways"] == 6

    def test_am_peak_filter_excludes_pm_cell(self, db_session):
        """`period_key='am_peak'` keeps only the 7am cell — half the pool."""
        self._seed_two_frequent_cells_at_7am_and_3pm(db_session)
        for i, minute in enumerate([0, 10, 20, 30]):
            _seed_stop_event(
                db_session,
                trip_id=f"TA{i + 1}",
                route_id=ROUTE,
                stop_id="S_AM",
                observed_arrival_ts=datetime(2026, 4, 14, 11, minute, 0),
            )
        for i, minute in enumerate([0, 10, 20, 30]):
            _seed_stop_event(
                db_session,
                trip_id=f"TP{i + 1}",
                route_id=ROUTE,
                stop_id="S_PM",
                observed_arrival_ts=datetime(2026, 4, 14, 19, minute, 0),
            )

        result = compute_ewt_headline_for_route(
            db_session, ROUTE, SERVICE_DATE, period_key="am_peak"
        )
        # Only the 7am cell-hour qualifies — three headways from each side.
        assert result["frequent_cell_hours"] == 1
        assert result["n_observed_headways"] == 3
        assert result["n_scheduled_headways"] == 3

    def test_late_filter_wraps_midnight(self, db_session):
        """`late` (22-6) wraps midnight; a 5am cell qualifies, a 7am one does not."""
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S_LATE")
        _seed_stop(db_session, "S_MORN")
        # 5am-ish frequent cell at its own stop — wraps into "Late" via
        # the start-hour-or-end-hour rule (5 < 6).
        for i, t in enumerate(["05:00:00", "05:10:00", "05:20:00", "05:30:00"]):
            trip_id = f"TL{i + 1}"
            _seed_trip(db_session, trip_id, ROUTE)
            _seed_stop_time(db_session, trip_id, "S_LATE", t, stop_sequence=1)
        # 7am frequent cell at a separate stop (outside Late).
        for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00", "07:30:00"]):
            trip_id = f"TM{i + 1}"
            _seed_trip(db_session, trip_id, ROUTE)
            _seed_stop_time(db_session, trip_id, "S_MORN", t, stop_sequence=1)

        result = compute_ewt_headline_for_route(db_session, ROUTE, SERVICE_DATE, period_key="late")
        # Only the 5am cell qualifies → 1 frequent cell-hour, 3 scheduled headways.
        assert result["frequent_cell_hours"] == 1
        assert result["n_scheduled_headways"] == 3


class TestSfmtaShapedPacificHourBucketing:
    """NOTES-103 regression: an SFMTA-shaped fixture (GTFS schedule and
    observed arrivals both anchored to a real 7am *Pacific* service, the
    way SFMTA's actual data looks) must produce a non-degenerate EWT
    cell-hour join when `tz_name="America/Los_Angeles"` is passed through.

    Before the fix, the scheduled side was always agency-local (GTFS clock
    time, hour 7) while the observed side was bucketed by hardcoded
    Eastern — for an April UTC offset of Pacific(-7)/Eastern(-4), the two
    sides disagree by 3 hours and the cell-hour join silently produces
    zero observed headways, even though the schedule and the observed
    arrivals are, in wall-clock reality, the same 7am service.
    """

    def _seed_pacific_frequent_cell_at_7am(self, db_session) -> None:
        """Schedule identical in shape to `_seed_frequent_cell_at_7am` — GTFS
        clock time is agency-local by construction, so the fixture doesn't
        need to change for Pacific; only the *observed* UTC offset does.
        """
        _seed_route(db_session)
        _seed_calendar(db_session)
        _seed_stop(db_session, "S1")
        for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00", "07:30:00"]):
            trip_id = f"T{i + 1}"
            _seed_trip(db_session, trip_id, ROUTE)
            _seed_stop_time(db_session, trip_id, "S1", t)

    def test_pacific_tz_name_produces_non_degenerate_join(self, db_session):
        """Observed arrivals at 7:00/7:10/7:20/7:30 PDT (= 14:00/14:10/14:20/
        14:30 UTC, April so PDT is UTC-7) must pair into 3 observed
        headways in the hour-7 cell when `tz_name="America/Los_Angeles"`.
        """
        self._seed_pacific_frequent_cell_at_7am(db_session)
        for i, minute in enumerate([0, 10, 20, 30]):
            _seed_stop_event(
                db_session,
                trip_id=f"T{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=datetime(2026, 4, 14, 14, minute, 0),
            )

        rows = compute_ewt_for_route_date(
            db_session, ROUTE, SERVICE_DATE, tz_name="America/Los_Angeles"
        )
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        # Non-degenerate: the observed side actually joined the scheduled
        # side's hour-7 cell, not zero.
        assert am["frequent_cell_hours"] == 1
        assert am["n_observed_headways"] == 3
        assert am["n_scheduled_headways"] == 3
        assert am["awt_seconds"] == pytest.approx(300.0)
        assert am["ewt_seconds"] == pytest.approx(0.0)

    def test_eastern_default_on_same_fixture_is_degenerate(self, db_session):
        """Same fixture, but *without* passing `tz_name` — reproduces the
        NOTES-103 bug: the observed side buckets by Eastern hour (10, not
        7), misses the scheduled side's hour-7 cell entirely, and the join
        collapses to zero observed headways despite real, on-time service.
        This is the regression the fix closes — pinned here so it can't
        silently come back.
        """
        self._seed_pacific_frequent_cell_at_7am(db_session)
        for i, minute in enumerate([0, 10, 20, 30]):
            _seed_stop_event(
                db_session,
                trip_id=f"T{i + 1}",
                route_id=ROUTE,
                stop_id="S1",
                observed_arrival_ts=datetime(2026, 4, 14, 14, minute, 0),
            )

        rows = compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        am = next(r for r in rows if r["time_period"] == "AM Peak (6-9)")
        assert am["frequent_cell_hours"] == 1
        assert am["n_observed_headways"] == 0
        assert am["awt_seconds"] is None


@pytest.fixture
def other_agency_db_session(tmp_path):
    """A second, physically distinct database — simulates a different
    agency's database (e.g. `sfmta_dashboard`) whose `gtfs_snapshots`
    table has its own independent `snapshot_id` sequence that can reach
    the same integer value as the primary `db_session` fixture's, purely
    by coincidence (NOTES-108). File-backed (not `:memory:`) so its bind
    URL is guaranteed to differ from `db_session`'s.
    """
    db_path = tmp_path / "other_agency.db"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    yield session
    session.close()
    engine.dispose()


class TestScheduleCacheAgencyIsolation:
    """NOTES-108: `_service_id_resolution_cache` and `_schedule_cache` are
    both keyed `(day_type, snapshot_id)` with no database/agency
    component. `gtfs_snapshot_id` is `MAX(GTFSSnapshot.snapshot_id)`
    scoped to whichever `db` session is passed in — a per-database
    sequence — so two different databases whose sequences happen to
    collide on the same integer must never share a cache entry.

    Both tests seed `db_session` and `other_agency_db_session` (a
    physically separate SQLite file — see fixture above) with
    `gtfs_snapshots.snapshot_id=1` in EACH database independently, so the
    resolved cache key collides on `("weekday", 1)` pre-fix. Each database
    is seeded with agency-distinguishing schedule data; a query against
    the second database must return the SECOND database's data, never a
    cached result served from the first.
    """

    def test_service_id_resolution_cache_does_not_leak_across_databases(
        self, db_session, other_agency_db_session
    ):
        db_session.add(GTFSSnapshot(snapshot_id=1, snapshot_date=datetime(2026, 5, 1)))
        db_session.add(
            Calendar(
                service_id="WMATA_SVC",
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
        )
        db_session.commit()

        other_agency_db_session.add(GTFSSnapshot(snapshot_id=1, snapshot_date=datetime(2026, 5, 1)))
        other_agency_db_session.add(
            Calendar(
                service_id="SFMTA_SVC",
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
        )
        other_agency_db_session.commit()

        assert _resolve_service_ids_for_day_type(db_session, "weekday") == {"WMATA_SVC"}
        # Pre-fix, this collides on cache key ("weekday", 1) and would
        # incorrectly return db_session's cached {"WMATA_SVC"}.
        assert _resolve_service_ids_for_day_type(other_agency_db_session, "weekday") == {
            "SFMTA_SVC"
        }
        # Re-querying the first database must still return its own result
        # — not evicted or clobbered by the second database's store.
        assert _resolve_service_ids_for_day_type(db_session, "weekday") == {"WMATA_SVC"}

    def test_schedule_cache_does_not_leak_across_databases(
        self, db_session, other_agency_db_session
    ):
        db_session.add(GTFSSnapshot(snapshot_id=1, snapshot_date=datetime(2026, 5, 1)))
        _seed_route(db_session, "RTE")
        _seed_calendar(db_session)
        _seed_stop(db_session, "S1")
        _seed_trip(db_session, "T1", "RTE")
        _seed_trip(db_session, "T2", "RTE")
        _seed_stop_time(db_session, "T1", "S1", "7:00:00")
        _seed_stop_time(db_session, "T2", "S1", "7:10:00")

        other_agency_db_session.add(GTFSSnapshot(snapshot_id=1, snapshot_date=datetime(2026, 5, 1)))
        _seed_route(other_agency_db_session, "RTE")
        _seed_calendar(other_agency_db_session)
        _seed_stop(other_agency_db_session, "S1")
        _seed_trip(other_agency_db_session, "T1", "RTE")
        _seed_trip(other_agency_db_session, "T2", "RTE")
        _seed_stop_time(other_agency_db_session, "T1", "S1", "7:00:00")
        _seed_stop_time(other_agency_db_session, "T2", "S1", "7:05:00")

        sched_a = fetch_scheduled_cell_hours_for_routes(db_session, "weekday")
        assert sched_a["RTE"][(0, "S1", 7)] == [600.0]

        # Pre-fix, this collides on cache key ("weekday", 1) and would
        # incorrectly return db_session's cached 600.0-second headway.
        sched_b = fetch_scheduled_cell_hours_for_routes(other_agency_db_session, "weekday")
        assert sched_b["RTE"][(0, "S1", 7)] == [300.0]

        # Re-querying the first database must still return its own result.
        sched_a_again = fetch_scheduled_cell_hours_for_routes(db_session, "weekday")
        assert sched_a_again["RTE"][(0, "S1", 7)] == [600.0]
