"""
Unit tests for API aggregation functions

Tests the business logic in api/aggregations.py including:
- Utility functions (sanitize_float)
- Route scorecard generation
- Route detail metrics
- Trend data generation

Run with: pytest tests/test_aggregations.py
"""

from datetime import date, timedelta

import pytest

from api.aggregations import (
    EWT_SCORE_FLOOR_SEC,
    EWT_SCORE_TARGET_SEC,
    _aggregate_bunching_window,
    _aggregate_ewt_window,
    _aggregate_otp_split_window,
    _aggregate_service_delivered_window,
    _make_otp_block,
    compute_route_grade,
    get_all_routes_scorecard,
    get_route_detail_metrics,
    get_route_trend_data,
    get_system_trend_data,
    sanitize_float,
)
from src.timezones import eastern_today


class TestSanitizeFloat:
    """Tests for sanitize_float utility function"""

    def test_sanitize_float_valid_number(self):
        """Test sanitize_float with valid numbers"""
        assert sanitize_float(42.5) == 42.5
        assert sanitize_float(0.0) == 0.0
        assert sanitize_float(-15.3) == -15.3

    def test_sanitize_float_nan(self):
        """Test sanitize_float with NaN returns None"""
        assert sanitize_float(float("nan")) is None

    def test_sanitize_float_infinity(self):
        """Test sanitize_float with infinity returns None"""
        assert sanitize_float(float("inf")) is None
        assert sanitize_float(float("-inf")) is None

    def test_sanitize_float_none(self):
        """Test sanitize_float with None returns None"""
        assert sanitize_float(None) is None

    def test_sanitize_float_string_number(self):
        """Test sanitize_float with string representation of number"""
        assert sanitize_float("42.5") == 42.5

    def test_sanitize_float_invalid_string(self):
        """Test sanitize_float with invalid string returns None"""
        assert sanitize_float("not a number") is None

    def test_sanitize_float_integer(self):
        """Test sanitize_float with integer"""
        assert sanitize_float(42) == 42.0


class TestComputeRouteGrade:
    """Tests for the NOTES-18 composite grade rubric.

    Covers the weighted-composite math for both frequent (EWT included)
    and non-frequent (EWT absent) routes, the EWT-to-score interpolation,
    bucket boundaries, and the missing-input N/A path.
    """

    def test_na_when_otp_missing(self):
        """OTP is required for any grade — null returns N/A."""
        assert compute_route_grade(None, 0.9, 60.0) == "N/A"

    def test_na_when_service_delivered_missing(self):
        """Service-delivered is required for any grade — null returns N/A."""
        assert compute_route_grade(85.0, None, 60.0) == "N/A"

    def test_frequent_route_perfect_a(self):
        """100% OTP, 100% SD, EWT at TfL target → composite 100 → A."""
        assert compute_route_grade(100.0, 1.0, EWT_SCORE_TARGET_SEC) == "A"

    def test_frequent_route_terrible_f(self):
        """0% OTP, 0% SD, EWT past floor → composite 0 → F."""
        assert compute_route_grade(0.0, 0.0, EWT_SCORE_FLOOR_SEC + 60) == "F"

    def test_frequent_route_weights_30_50_20(self):
        """Composite math: OTP 70 + SD 0.8 + EWT 60s → 30*70 + 50*80 + 20*100 = 21+40+20 = 81 → A."""
        # 70 * 0.30 = 21.0; 80 * 0.50 = 40.0; 100 * 0.20 = 20.0; total 81.0
        assert compute_route_grade(70.0, 0.80, 60.0) == "A"

    def test_non_frequent_route_weights_40_60(self):
        """No EWT → composite uses 40/60. OTP 70 + SD 0.8 → 28 + 48 = 76 → B."""
        # 70 * 0.40 = 28.0; 80 * 0.60 = 48.0; total 76.0
        assert compute_route_grade(70.0, 0.80, None) == "B"

    def test_ewt_score_clamps_below_target(self):
        """EWT below TfL target gets full 100 — sub-target time isn't extra credit."""
        # 100 OTP, 100 SD, EWT 30s (below 60s target) → 30+50+20 = 100 → A
        assert compute_route_grade(100.0, 1.0, 30.0) == "A"

    def test_ewt_score_clamps_above_floor(self):
        """EWT above the 5-min floor gets 0 — caps the penalty so the grade reflects OTP+SD."""
        # 80 OTP, 0.8 SD, EWT 600s (above 300s floor) → 24+40+0 = 64 → B
        assert compute_route_grade(80.0, 0.80, 600.0) == "B"

    def test_ewt_score_linear_interpolation(self):
        """EWT at midpoint (180s, halfway between 60 and 300) → score 50."""
        # 100 OTP, 1.0 SD, EWT 180s → score 50 → 30+50+10 = 90 → A
        # Verify the EWT contribution by checking a borderline case:
        # 50 OTP, 0.40 SD, EWT 180s → 15+20+10 = 45 → C
        assert compute_route_grade(50.0, 0.40, 180.0) == "C"

    def test_grade_b_boundary_at_60(self):
        """Composite exactly 60 → B (>= boundary)."""
        # Non-frequent: OTP 60, SD 0.6 → 24+36 = 60 → B
        assert compute_route_grade(60.0, 0.60, None) == "B"

    def test_grade_c_boundary_at_40(self):
        """Composite exactly 40 → C."""
        # Non-frequent: OTP 40, SD 0.4 → 16+24 = 40 → C
        assert compute_route_grade(40.0, 0.40, None) == "C"

    def test_grade_d_boundary_at_20(self):
        """Composite exactly 20 → D."""
        # Non-frequent: OTP 20, SD 0.2 → 8+12 = 20 → D
        assert compute_route_grade(20.0, 0.20, None) == "D"

    def test_grade_f_below_20(self):
        """Composite below 20 → F."""
        # Non-frequent: OTP 10, SD 0.10 → 4+6 = 10 → F
        assert compute_route_grade(10.0, 0.10, None) == "F"


class TestGetAllRoutesScorecard:
    """Tests for get_all_routes_scorecard function.

    The scorecard payload is `{window: {start, end, days}, routes: [...]}`
    where each row in `routes` is identity + frequency_class + the windowed
    live overlay (OTP/SD/EWT/bunching pooled over `[end - days + 1, end]`).
    """

    def test_scorecard_with_single_route(self, db_session, sample_route):
        """Single route shows identity + frequency_class even without live data."""
        result = get_all_routes_scorecard(db_session, days=7)

        assert "window" in result
        assert result["window"]["days"] == 7
        # No stop_events seeded → window endpoints are null but the route
        # row still renders with its identity fields.
        assert result["window"]["start"] is None
        assert result["window"]["end"] is None

        routes = result["routes"]
        assert len(routes) == 1
        route_data = routes[0]

        assert route_data["route_id"] == "TEST1"
        assert route_data["route_name"] == "T1"
        assert route_data["route_long_name"] == "Test Route 1"
        assert "frequency_class" in route_data
        # Live overlay keys are present even when their values are None
        assert "otp_all_pct" in route_data
        assert "service_delivered_ratio" in route_data
        assert "ewt_seconds" in route_data
        assert "bunching_rate" in route_data
        # Composite grade (NOTES-18): N/A without live data, since OTP and
        # service_delivered are both required inputs.
        assert route_data["grade"] == "N/A"

    def test_scorecard_with_multiple_routes(self, db_session, sample_routes):
        """Every is_current route appears, sorted with None OTP values last."""
        result = get_all_routes_scorecard(db_session, days=7)
        routes = result["routes"]

        assert len(routes) == 3
        # Without seeded live data, all routes have None for otp_all_pct.
        # The sort key puts None values at the end — but with all None,
        # the order is whatever the upstream query returned (still all 3).
        for route in routes:
            assert route["otp_all_pct"] is None

    def test_scorecard_empty_database(self, db_session):
        """No routes means an empty `routes` list with window metadata still set."""
        result = get_all_routes_scorecard(db_session, days=7)
        assert result["routes"] == []
        assert result["window"]["days"] == 7

    def test_scorecard_days_clamped_to_minimum(self, db_session, sample_route):
        """`days < 1` clamps to 1 — match endpoint validation."""
        result = get_all_routes_scorecard(db_session, days=0)
        assert result["window"]["days"] == 1


class TestLiveMetricsWindowPartialDayExclusion:
    """Partial-day exclusion in the pooled live-metrics window (NOTES-123 review).

    `_compute_live_metrics_for_window_uncached` -- shared by the routes
    scorecard (`get_all_routes_scorecard`) and the contributors ranking
    (`get_route_contributors`) -- pools SD/EWT/bunching over
    `[end_date-days+1, end_date]`. A `data_quality='partial'` date must be
    dropped from that window before any tier (in-memory cache, overlay
    read, live compute) is consulted. Filtering it out only inside the
    overlay reader makes a partial date's absence indistinguishable from
    "not materialized yet," so it falls through to the live-compute
    fallback over raw `stop_events` -- which doesn't exclude partial days
    -- pooling it right back in (review finding 1) and paying the ~35s
    live-compute cost on every cache expiry (finding 3).
    """

    @pytest.fixture(autouse=True)
    def _clear_module_caches(self):
        """Process-global cache isolation.

        Same reasoning as `TestRouteHeaderAnchorsOnRouteOwnLatestDate`'s
        `_clear_live_metrics_cache` fixture: `_live_metrics_cache` and
        `_window_metrics_cache` are module-level and keyed by date, so a
        leftover entry from another test run in the same process could
        leak into these tests' lookups.
        """
        from api.aggregations import _live_metrics_cache, _window_metrics_cache

        _live_metrics_cache.clear()
        _window_metrics_cache.clear()
        yield
        _live_metrics_cache.clear()
        _window_metrics_cache.clear()

    def test_scorecard_pooled_ewt_excludes_partial_day(self, db_session, sample_route, monkeypatch):
        """The routes scorecard's pooled EWT excludes a partial-flagged day.

        Two clean overlay days pool to a modest EWT; a third day flagged
        `partial` carries an extreme observed-headway spread that would
        blow up the pooled EWT if it leaked in. Also asserts the
        live-compute fetch is never invoked at all: with no raw
        `stop_events` seeded for the partial day, a value-only assertion
        would pass even under the bug (live compute finds nothing for
        that date and silently contributes zero) -- the call-count
        assertion is what actually pins the exclusion happening at the
        right layer (review findings 1/2/3).
        """
        import src.ewt as ewt_module
        from api.aggregations import _derive_ewt_metrics, get_all_routes_scorecard
        from src.models import RouteMetricsDailyOverlay, StopEvent, SystemMetricsDaily

        calls = []
        original = ewt_module.fetch_observed_stop_events_for_window

        def spy(db, service_dates, route_ids=None):
            """Record every window of dates handed to the live-compute fetch."""
            calls.append(list(service_dates))
            return original(db, service_dates, route_ids=route_ids)

        monkeypatch.setattr(ewt_module, "fetch_observed_stop_events_for_window", spy)

        # One raw stop_event sets `_latest_service_date_with_stop_events`
        # (the scorecard's window anchor) -- its own content is irrelevant.
        db_session.add(
            StopEvent(
                service_date=eastern_today().isoformat(),
                trip_id="TRIP_ANCHOR",
                route_id="TEST1",
                direction_id=0,
                stop_id="STOP_ANCHOR",
                stop_sequence=1,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )

        clean_stats = [
            {
                "obs_sum_h": 1200.0,
                "obs_sum_h_sq": 1200.0**2 / 2,
                "n_obs": 2,
                "sched_sum_h": 1200.0,
                "sched_sum_h_sq": 1200.0**2 / 2,
                "n_sched": 2,
            }
            for _ in range(2)
        ]
        for i, stats in enumerate(clean_stats):
            d = eastern_today() - timedelta(days=i)
            db_session.add(
                RouteMetricsDailyOverlay(
                    route_id="TEST1",
                    service_date=d.isoformat(),
                    day_type="weekday",
                    data_quality="complete",
                    ewt_obs_sum_h=stats["obs_sum_h"],
                    ewt_obs_sum_h_sq=stats["obs_sum_h_sq"],
                    ewt_n_observed_headways=stats["n_obs"],
                    ewt_sched_sum_h=stats["sched_sum_h"],
                    ewt_sched_sum_h_sq=stats["sched_sum_h_sq"],
                    ewt_n_scheduled_headways=stats["n_sched"],
                )
            )

        partial_date = eastern_today() - timedelta(days=2)
        db_session.add(
            RouteMetricsDailyOverlay(
                route_id="TEST1",
                service_date=partial_date.isoformat(),
                day_type="weekday",
                data_quality="partial",
                ewt_obs_sum_h=36000.0,
                ewt_obs_sum_h_sq=36000.0**2,
                ewt_n_observed_headways=1,
                ewt_sched_sum_h=600.0,
                ewt_sched_sum_h_sq=600.0**2,
                ewt_n_scheduled_headways=1,
            )
        )
        db_session.add(
            SystemMetricsDaily(service_date=partial_date.isoformat(), data_quality="partial")
        )
        db_session.commit()

        expected = _derive_ewt_metrics(
            sum(s["obs_sum_h"] for s in clean_stats),
            sum(s["obs_sum_h_sq"] for s in clean_stats),
            sum(s["sched_sum_h"] for s in clean_stats),
            sum(s["sched_sum_h_sq"] for s in clean_stats),
            sum(s["n_obs"] for s in clean_stats),
            sum(s["n_sched"] for s in clean_stats),
        )["ewt_seconds"]

        result = get_all_routes_scorecard(db_session, days=3)
        route_row = next((r for r in result["routes"] if r["route_id"] == "TEST1"), None)
        assert route_row is not None
        assert route_row["ewt_seconds"] == pytest.approx(expected)
        assert calls == []

    def test_partial_day_never_reaches_live_compute(self, db_session, sample_route, monkeypatch):
        """A partial-flagged date never falls through to the live-compute tier.

        Window is `[partial_date, clean_date]`. `clean_date` resolves from
        the materialized overlay (tier 2); `partial_date` should be
        filtered out of the window before tier 2 is even consulted. If
        instead the exclusion happened inside the overlay reader (the
        pre-fix layer), the missing key would read as "not materialized
        yet" and the raw-`stop_events` live-compute helper
        (`fetch_observed_stop_events_for_window`) would be invoked for it
        -- reproduces review findings 1/2/3.
        """
        import src.ewt as ewt_module
        from api.aggregations import _compute_live_metrics_for_window_uncached
        from src.models import RouteMetricsDailyOverlay, StopEvent, SystemMetricsDaily

        calls = []
        original = ewt_module.fetch_observed_stop_events_for_window

        def spy(db, service_dates, route_ids=None):
            """Record every window of dates handed to the live-compute fetch."""
            calls.append(list(service_dates))
            return original(db, service_dates, route_ids=route_ids)

        monkeypatch.setattr(ewt_module, "fetch_observed_stop_events_for_window", spy)

        clean_date = eastern_today()
        partial_date = eastern_today() - timedelta(days=1)

        db_session.add(
            StopEvent(
                service_date=clean_date.isoformat(),
                trip_id="TRIP_ANCHOR",
                route_id="TEST1",
                direction_id=0,
                stop_id="STOP_ANCHOR",
                stop_sequence=1,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )
        db_session.add(
            RouteMetricsDailyOverlay(
                route_id="TEST1",
                service_date=clean_date.isoformat(),
                day_type="weekday",
                data_quality="complete",
                ewt_obs_sum_h=1200.0,
                ewt_obs_sum_h_sq=1200.0**2 / 2,
                ewt_n_observed_headways=2,
                ewt_sched_sum_h=1200.0,
                ewt_sched_sum_h_sq=1200.0**2 / 2,
                ewt_n_scheduled_headways=2,
            )
        )
        db_session.add(
            RouteMetricsDailyOverlay(
                route_id="TEST1",
                service_date=partial_date.isoformat(),
                day_type="weekday",
                data_quality="partial",
                ewt_obs_sum_h=36000.0,
                ewt_obs_sum_h_sq=36000.0**2,
                ewt_n_observed_headways=1,
                ewt_sched_sum_h=600.0,
                ewt_sched_sum_h_sq=600.0**2,
                ewt_n_scheduled_headways=1,
            )
        )
        db_session.add(
            SystemMetricsDaily(service_date=partial_date.isoformat(), data_quality="partial")
        )
        db_session.commit()

        _compute_live_metrics_for_window_uncached(db_session, clean_date, 2)

        assert calls == []


class TestMakeOtpBlock:
    """Shape contract for the shared `_make_otp_block` factory (NOTES-66).

    Locks the on-the-wire OTP sub-block shape so future drift between the
    live-compute and overlay-hydration paths gets caught here rather than
    at integration time — the original motivation for extracting this
    helper was the PR #115 near-miss bug between exactly those paths.
    """

    @pytest.mark.parametrize(
        "early,on_time,late,total,source,expected",
        [
            # Empty case: total_count == 0 emits the "no data" sentinel
            # carrying source through so consumers can still label it.
            (
                0,
                0,
                0,
                0,
                "proximity",
                {"source": "proximity", "n": 0},
            ),
            # Typical mixed case: 1 early + 8 on-time + 1 late of 10.
            (
                1,
                8,
                1,
                10,
                "proximity",
                {
                    "source": "proximity",
                    "n": 10,
                    "early": 1,
                    "on_time": 8,
                    "late": 1,
                    "early_pct": 10.0,
                    "on_time_pct": 80.0,
                    "late_pct": 10.0,
                },
            ),
            # Source threading: trip_update populated block keeps its
            # source string unmodified.
            (
                0,
                2,
                1,
                3,
                "trip_update",
                {
                    "source": "trip_update",
                    "n": 3,
                    "early": 0,
                    "on_time": 2,
                    "late": 1,
                    "early_pct": 0.0,
                    "on_time_pct": round(200 / 3, 2),
                    "late_pct": round(100 / 3, 2),
                },
            ),
            # Source threading on the empty sentinel — trip_update path
            # must round-trip its source label even with no observations.
            (
                0,
                0,
                0,
                0,
                "trip_update",
                {"source": "trip_update", "n": 0},
            ),
        ],
    )
    def test_shape_contract(self, early, on_time, late, total, source, expected):
        """Helper returns byte-identical dicts to the pre-refactor inline builders."""
        assert _make_otp_block(early, on_time, late, total, source) == expected


class TestAggregateOtpSplitWindow:
    """Sufficient-statistics pooling for the windowed OTP split."""

    def test_returns_none_when_all_days_empty(self):
        """No daily results → None."""
        assert _aggregate_otp_split_window([None, None]) is None

    def test_pools_counts_across_days(self):
        """Day1 (4 on, 1 late, 0 early) + Day2 (2 on, 0 late, 1 early) → 6 on / 1 late / 1 early of 8."""
        day1 = {
            "route_id": "R1",
            "window": {"early_sec": -120, "late_sec": 420},
            "origin": {"source": "proximity", "n": 0},
            "destination": {"source": "trip_update", "n": 0},
            "all_timepoints": {
                "source": "proximity",
                "n": 5,
                "early": 0,
                "on_time": 4,
                "late": 1,
                "early_pct": 0.0,
                "on_time_pct": 80.0,
                "late_pct": 20.0,
            },
        }
        day2 = {
            "route_id": "R1",
            "window": {"early_sec": -120, "late_sec": 420},
            "origin": {"source": "proximity", "n": 0},
            "destination": {"source": "trip_update", "n": 0},
            "all_timepoints": {
                "source": "proximity",
                "n": 3,
                "early": 1,
                "on_time": 2,
                "late": 0,
                "early_pct": 33.33,
                "on_time_pct": 66.67,
                "late_pct": 0.0,
            },
        }
        result = _aggregate_otp_split_window([day1, day2])
        assert result["all_timepoints"]["n"] == 8
        assert result["all_timepoints"]["on_time"] == 6
        assert result["all_timepoints"]["late"] == 1
        assert result["all_timepoints"]["early"] == 1
        assert result["all_timepoints"]["on_time_pct"] == 75.0
        # Empty sub-blocks pool to n=0 — preserves "no data" vs "0% on-time".
        assert result["origin"]["n"] == 0
        assert result["destination"]["n"] == 0


class TestAggregateServiceDeliveredWindow:
    """Service-delivered pooling sums the trip counts and recomputes the ratio."""

    def test_returns_none_when_no_days(self):
        assert _aggregate_service_delivered_window([None]) is None

    def test_sums_scheduled_and_delivered(self):
        """Day1 100/95 + Day2 100/80 → 200/175 = 0.875."""
        day1 = {"route_id": "R1", "scheduled_trips": 100, "delivered_trips": 95, "ratio": 0.95}
        day2 = {"route_id": "R1", "scheduled_trips": 100, "delivered_trips": 80, "ratio": 0.80}
        result = _aggregate_service_delivered_window([day1, day2])
        assert result["scheduled_trips"] == 200
        assert result["delivered_trips"] == 175
        assert result["ratio"] == 0.875

    def test_zero_scheduled_returns_none_ratio(self):
        """No scheduled trips in window → ratio is None (route never runs)."""
        day1 = {"route_id": "R1", "scheduled_trips": 0, "delivered_trips": 0, "ratio": None}
        result = _aggregate_service_delivered_window([day1, day1])
        assert result["ratio"] is None
        assert result["scheduled_trips"] == 0


class TestAggregateEwtWindow:
    """EWT pooling uses sufficient stats so AWT/SWT match a recomputed pool."""

    def test_returns_none_when_empty(self):
        assert _aggregate_ewt_window([]) is None
        assert _aggregate_ewt_window([None]) is None

    def test_pools_sufficient_stats(self):
        """Two days of identical (h=600s) headways pool to AWT=300 (h/2).

        The AWT formula `Σh² / (2·Σh)` over uniform headways reduces to h/2.
        Both days have one observed headway of 600s and one scheduled of 600s,
        so the windowed AWT and SWT must both be 300, and EWT 0.
        """
        day = {
            "route_id": "R1",
            "obs_sum_h": 600.0,
            "obs_sum_h_sq": 600.0 * 600.0,
            "sched_sum_h": 600.0,
            "sched_sum_h_sq": 600.0 * 600.0,
            "n_observed_headways": 1,
            "n_scheduled_headways": 1,
        }
        result = _aggregate_ewt_window([day, day])
        assert result["awt_seconds"] == 300.0
        assert result["swt_seconds"] == 300.0
        assert result["ewt_seconds"] == 0.0
        assert result["n_observed_headways"] == 2
        assert result["n_scheduled_headways"] == 2
        assert result["coverage_ratio"] == 1.0

    def test_ewt_positive_when_observed_bunchier(self):
        """A day with bunched observed headways and even scheduled → AWT > SWT → EWT > 0."""
        day = {
            "route_id": "R1",
            "obs_sum_h": 600.0,
            # Two headways: 100s and 500s → sum=600, sum_h_sq = 10000+250000 = 260000.
            "obs_sum_h_sq": 260000.0,
            "sched_sum_h": 600.0,
            # Two even headways: 300, 300 → sum=600, sum_h_sq=180000.
            "sched_sum_h_sq": 180000.0,
            "n_observed_headways": 2,
            "n_scheduled_headways": 2,
        }
        result = _aggregate_ewt_window([day])
        # AWT = 260000 / (2 * 600) = 216.67; SWT = 180000 / 1200 = 150.
        assert result["awt_seconds"] == 216.67
        assert result["swt_seconds"] == 150.0
        assert result["ewt_seconds"] == 66.67


class TestAggregateBunchingWindow:
    """Bunching pooling sums counts and recomputes the rate."""

    def test_pools_bunched_and_total(self):
        """Day1 5/100 + Day2 10/50 → 15/150 = 0.1."""
        day1 = {"route_id": "R1", "bunching_count": 5, "total_headways": 100, "bunching_rate": 0.05}
        day2 = {"route_id": "R1", "bunching_count": 10, "total_headways": 50, "bunching_rate": 0.20}
        result = _aggregate_bunching_window([day1, day2])
        assert result["bunching_count"] == 15
        assert result["total_headways"] == 150
        assert result["bunching_rate"] == 0.1

    def test_zero_total_returns_none_rate(self):
        """No observed pairs → rate is None (route had no headways at all)."""
        day = {"route_id": "R1", "bunching_count": 0, "total_headways": 0, "bunching_rate": None}
        result = _aggregate_bunching_window([day])
        assert result["bunching_rate"] is None
        assert result["total_headways"] == 0


class TestGetRouteDetailMetrics:
    """Tests for get_route_detail_metrics function (post NOTES-19 cleanup)."""

    def test_route_detail_returns_identity_and_overlay(self, db_session, sample_route):
        """Route detail returns identity + filter echoes + live/excess overlay keys."""
        result = get_route_detail_metrics(db_session, "TEST1", days=7)

        assert result["route_id"] == "TEST1"
        assert result["route_name"] == "T1"
        assert result["route_long_name"] == "Test Route 1"
        assert result["time_period_days"] == 7
        assert result["day_type_filter"] == "all"
        assert result["period_key"] == "all"
        # Live overlay + excess fields are surfaced (values may be None
        # without seeded stop_events / runs).
        assert "frequency_class" in result
        assert "otp_all_pct" in result
        assert "service_delivered_ratio" in result
        assert "ewt_seconds" in result
        assert "bunching_rate" in result
        assert "excess_trip_time_pct" in result
        # Composite grade (NOTES-18): N/A without live data.
        assert result["grade"] == "N/A"

    def test_route_detail_nonexistent_route(self, db_session):
        """Test route detail for non-existent route"""
        result = get_route_detail_metrics(db_session, "NONEXISTENT", days=7)

        assert "error" in result
        assert "not found" in result["error"].lower()

    def test_route_detail_with_days_parameter(self, db_session, sample_route):
        """Test route detail respects days parameter"""
        result = get_route_detail_metrics(db_session, "TEST1", days=14)

        assert result["time_period_days"] == 14


class TestRouteHeaderAnchorsOnRouteOwnLatestDate:
    """NOTES-117: route-detail live KPIs anchor on the route's own latest
    service_date, not the system-wide max across all routes.

    Reproduces the A90-on-a-Saturday bug: a weekday-only route's page
    rendered N/A across every header KPI whenever the system-wide latest
    derived day (`MAX(service_date)` over ALL routes) fell on a day the
    route itself has no stop_events for. `_latest_service_date_for_route`
    and `_resolve_route_anchor_date` are the route-scoped resolution;
    `_latest_service_date_with_stop_events` keeps its system-wide meaning
    for callers that intentionally want it (the scorecard window end).
    """

    @pytest.fixture(autouse=True)
    def _clear_live_metrics_cache(self):
        """Process-global cache isolation.

        Several tests below poke `_live_metrics_cache` directly with
        hard-coded date keys to simulate warm/cold states; without this,
        a leftover entry from one test could leak into another test's
        cache lookup and make results depend on run order.
        """
        from api.aggregations import _live_metrics_cache

        _live_metrics_cache.clear()
        yield
        _live_metrics_cache.clear()

    def _seed_stop_event(self, db_session, route_id, service_date_str, hour=14):
        """One proximity stop_event for `route_id` on `service_date_str`.

        `deviation_sec=0` gives a deterministic 100%-on-time OTP block so
        tests can assert non-null live KPIs without a full GTFS fixture
        (service-delivered's ratio needs GTFS trips/calendar, out of scope
        here — the OTP block alone is enough to prove the anchor flips
        from null to real values). `hour` is naive UTC (matches storage
        convention — see CLAUDE.md); period-key bucketing converts to
        Eastern, currently UTC-4 (EDT), before comparing. Defaults to an
        off-peak-either-way 14:00 UTC (10am EDT); pass e.g. `hour=12`
        (8am EDT) to land inside `period=am_peak`.
        """
        from datetime import datetime as _dt

        from src.models import StopEvent

        d = date.fromisoformat(service_date_str)
        db_session.add(
            StopEvent(
                service_date=service_date_str,
                trip_id=f"TRIP_{route_id}_{service_date_str}_{hour}",
                route_id=route_id,
                direction_id=0,
                stop_id=f"STOP_ANCHOR_{route_id}",
                stop_sequence=1,
                observed_arrival_ts=_dt.combine(d, _dt.min.time()).replace(hour=hour),
                deviation_sec=0,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )
        db_session.commit()

    def _seed_gtfs_scheduled(self, db_session, route_id, service_date_str, trip_count=1):
        """One Calendar service + `trip_count` Trip rows making `route_id`
        GTFS-scheduled on the literal weekday of `service_date_str`.

        Sets only the `Calendar` day-of-week flag matching `date.weekday()`
        for `service_date_str` — matches exactly what `route_scheduled_on_date`
        / `scheduled_service_ids_for_date` resolve, so this seeds "scheduled
        on this exact date," not just "scheduled on this day of week in
        general" (a distinction that matters if a test ever crosses a
        `calendar_dates` exception boundary, though none here do).
        """
        from src.models import Calendar, Trip

        d = date.fromisoformat(service_date_str)
        weekday_fields = (
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        )
        flags = dict.fromkeys(weekday_fields, 0)
        flags[weekday_fields[d.weekday()]] = 1
        service_id = f"SVC_SCHED_{route_id}_{service_date_str}"
        db_session.add(
            Calendar(
                service_id=service_id,
                start_date="20260101",
                end_date="20271231",
                is_current=True,
                **flags,
            )
        )
        for i in range(trip_count):
            db_session.add(
                Trip(
                    trip_id=f"TRIP_SCHED_{route_id}_{service_date_str}_{i}",
                    route_id=route_id,
                    service_id=service_id,
                    direction_id=i % 2,
                    is_current=True,
                )
            )
        db_session.commit()

    def test_latest_service_date_for_route_is_route_scoped(self, db_session, sample_routes):
        """The route-scoped helper ignores other routes' later dates."""
        from api.aggregations import _latest_service_date_for_route

        self._seed_stop_event(db_session, "TEST1", "2026-08-07")  # Friday
        self._seed_stop_event(db_session, "TEST2", "2026-08-08")  # Saturday

        assert _latest_service_date_for_route(db_session, "TEST1") == date(2026, 8, 7)
        assert _latest_service_date_for_route(db_session, "TEST2") == date(2026, 8, 8)
        assert _latest_service_date_for_route(db_session, "NONEXISTENT") is None

    def test_live_metrics_anchor_on_route_own_latest_date_when_system_wide_differs(
        self, db_session, sample_routes
    ):
        """A weekday-only route's latest events are earlier than the
        system-wide max (another route ran later) — the live-metrics
        anchor must follow the route's own latest date, and report it."""
        from api.aggregations import get_live_metrics_for_route_today

        self._seed_stop_event(db_session, "TEST1", "2026-08-07")  # weekday-only route
        self._seed_stop_event(db_session, "TEST2", "2026-08-08")  # system-wide max

        metrics, anchor_date = get_live_metrics_for_route_today(db_session, "TEST1")

        assert anchor_date == date(2026, 8, 7)
        assert metrics is not None
        assert metrics["otp_split"]["all_timepoints"]["on_time_pct"] == 100.0

    def test_live_metrics_anchor_matches_system_wide_when_route_has_data_there(
        self, db_session, sample_routes
    ):
        """No regression for the common case: a route WITH events on the
        system-wide max date still anchors there."""
        from api.aggregations import get_live_metrics_for_route_today

        self._seed_stop_event(db_session, "TEST1", "2026-08-07")
        self._seed_stop_event(db_session, "TEST2", "2026-08-08")
        self._seed_stop_event(db_session, "TEST1", "2026-08-08")  # TEST1 also ran the 8th

        metrics, anchor_date = get_live_metrics_for_route_today(db_session, "TEST1")

        assert anchor_date == date(2026, 8, 8)
        assert metrics is not None

    def test_live_metrics_anchor_survives_warm_cross_route_cache_miss(
        self, db_session, sample_routes
    ):
        """Reproduces the exact bug mechanism: the cross-route
        `_live_metrics_cache` is warm for the system-wide date (e.g. from a
        prior /api/routes scorecard call) but has no entry for this route
        because the route legitimately has no data that day. The
        route-detail call must fall back to the route's own latest date
        instead of treating "missing from a warm cache" as "no data"."""
        import time

        from api.aggregations import _live_metrics_cache, get_live_metrics_for_route_today

        self._seed_stop_event(db_session, "TEST1", "2026-08-07")
        self._seed_stop_event(db_session, "TEST2", "2026-08-08")

        # Simulate a warm scorecard cache for the system-wide date that
        # only has an entry for TEST2 — TEST1 has none, same as production
        # when the cross-route pass finds nothing for it. (`_live_metrics_cache`
        # starts empty and is cleared again after the test by the autouse
        # `_clear_live_metrics_cache` fixture.)
        _live_metrics_cache["2026-08-08"] = (
            time.monotonic(),
            {"TEST2": {"service_delivered": {"ratio": 1.0}}},
        )

        metrics, anchor_date = get_live_metrics_for_route_today(db_session, "TEST1")

        assert anchor_date == date(2026, 8, 7)
        assert metrics is not None

    def test_live_metrics_anchor_uses_route_scoped_anchor_regardless_of_period_key(
        self, db_session, sample_routes
    ):
        """Finding 1 (pr-reviewer, PR #199): the route-scoped anchor must
        apply for every `period_key`, not just `period_key=all`.

        Before the fix, passing any period filter (e.g. `?period=am_peak`)
        alongside the default `day_type=all` fell straight into the
        filtered branch, which called
        `_latest_service_date_for_day_type(db, "all")` — the system-wide
        max — bypassing the NOTES-117 route-scoped fallback entirely. A90
        would populate at "Time: All" and revert to N/A the moment a rider
        picked "AM Peak."
        """
        from api.aggregations import get_live_metrics_for_route_today

        # TEST1 (weekday-only route): AM-peak observation on Friday.
        # hour=12 UTC → 8am Eastern (EDT), inside the am_peak (6-10) window.
        self._seed_stop_event(db_session, "TEST1", "2026-08-07", hour=12)
        # TEST2 pushes the system-wide max to Saturday.
        self._seed_stop_event(db_session, "TEST2", "2026-08-08")

        metrics, anchor_date = get_live_metrics_for_route_today(
            db_session, "TEST1", period_key="am_peak"
        )

        assert anchor_date == date(2026, 8, 7)
        assert metrics is not None
        assert metrics["otp_split"]["all_timepoints"]["on_time_pct"] == 100.0

    def test_live_metrics_anchor_falls_back_when_route_not_scheduled_on_system_date(
        self, db_session, sample_routes
    ):
        """Contrast case for finding 2(b): TEST1 is explicitly NOT
        GTFS-scheduled on the system-wide max date (its calendar only
        covers Friday) — falls back to its own latest date. Same outcome
        as the "no GTFS at all" tests above, but with an explicit calendar
        proving the "not scheduled" branch is what's firing, not just an
        absence of any schedule data."""
        from api.aggregations import get_live_metrics_for_route_today

        self._seed_stop_event(db_session, "TEST1", "2026-08-07")  # Friday
        self._seed_gtfs_scheduled(db_session, "TEST1", "2026-08-07")  # Friday-only service
        self._seed_stop_event(db_session, "TEST2", "2026-08-08")  # system-wide max, Saturday

        metrics, anchor_date = get_live_metrics_for_route_today(db_session, "TEST1")

        assert anchor_date == date(2026, 8, 7)
        assert metrics is not None

    def test_live_metrics_anchor_stays_on_system_date_when_scheduled_but_zero_events(
        self, db_session, sample_routes
    ):
        """Finding 2(a) (pr-reviewer, PR #199): a route SCHEDULED on the
        system-wide latest date must anchor there and show the outage
        (`ratio == 0.0`), not silently jump back to its last day with any
        observations. Discriminating on cache/observation presence instead
        of GTFS scheduled-ness (the pre-fix behavior) hid total-service-
        failure days behind a healthy-looking earlier date."""
        from api.aggregations import get_live_metrics_for_route_today

        # TEST1 ran fine on an earlier Wednesday...
        self._seed_stop_event(db_session, "TEST1", "2026-08-05")
        # ...and is GTFS-scheduled on Saturday 8/8 (the system-wide max
        # below) but delivered nothing that day — no stop_events, no runs.
        self._seed_gtfs_scheduled(db_session, "TEST1", "2026-08-08")
        # TEST2 establishes the system-wide max at 2026-08-08.
        self._seed_stop_event(db_session, "TEST2", "2026-08-08")

        metrics, anchor_date = get_live_metrics_for_route_today(db_session, "TEST1")

        assert anchor_date == date(2026, 8, 8)
        assert metrics is not None
        assert metrics["service_delivered"]["ratio"] == 0.0
        assert metrics["service_delivered"]["scheduled_trips"] == 1
        assert metrics["service_delivered"]["delivered_trips"] == 0
        # No observations that day → the OTP block is the "no data" sentinel.
        assert metrics["otp_split"]["all_timepoints"]["n"] == 0

    def test_live_metrics_anchor_for_scheduled_outage_is_cache_independent(
        self, db_session, sample_routes
    ):
        """Finding 2(c): same scheduled-but-zero-events scenario, but with a
        warm cross-route cache for the system date that has NO entry for
        TEST1 (as if the cross-route pass had somehow skipped it) — proves
        the anchor decision doesn't depend on cache state. Warm (here) and
        cold (the test above) must reach the same anchor and the same
        outage numbers."""
        import time

        from api.aggregations import _live_metrics_cache, get_live_metrics_for_route_today

        self._seed_stop_event(db_session, "TEST1", "2026-08-05")
        self._seed_gtfs_scheduled(db_session, "TEST1", "2026-08-08")
        self._seed_stop_event(db_session, "TEST2", "2026-08-08")

        _live_metrics_cache["2026-08-08"] = (
            time.monotonic(),
            {"TEST2": {"service_delivered": {"ratio": 1.0}}},  # no TEST1 entry
        )

        metrics, anchor_date = get_live_metrics_for_route_today(db_session, "TEST1")

        assert anchor_date == date(2026, 8, 8)
        assert metrics is not None
        assert metrics["service_delivered"]["ratio"] == 0.0

    def test_period_drilldown_anchors_on_same_date_as_the_header(self, db_session, sample_routes):
        """Finding 3 (pr-reviewer, PR #199): `get_route_period_drilldown`'s
        `service_date` must equal `get_route_detail_metrics`'s
        `live_metrics_as_of_date` for the same route. Before the fix, the
        drilldown anchored independently on the system-wide max via
        `_latest_service_date_with_stop_events`, so a weekday-only route
        could show a populated header (anchored Friday) over an empty
        drilldown chart (anchored the following Saturday)."""
        from api.aggregations import get_route_detail_metrics, get_route_period_drilldown

        self._seed_stop_event(db_session, "TEST1", "2026-08-07")  # Friday, weekday-only route
        self._seed_stop_event(db_session, "TEST2", "2026-08-08")  # system-wide max, Saturday

        header = get_route_detail_metrics(db_session, "TEST1", days=7)
        drilldown = get_route_period_drilldown(db_session, "TEST1")

        assert header["live_metrics_as_of_date"] == "2026-08-07"
        assert drilldown["service_date"] == header["live_metrics_as_of_date"]

    def test_route_detail_metrics_reports_anchor_date_and_nonnull_kpis(
        self, db_session, sample_routes
    ):
        """End-to-end: `get_route_detail_metrics` for the weekday-only route
        returns non-null live KPIs and echoes the date it anchored on."""
        self._seed_stop_event(db_session, "TEST1", "2026-08-07")
        self._seed_stop_event(db_session, "TEST2", "2026-08-08")

        result = get_route_detail_metrics(db_session, "TEST1", days=7)

        assert result["live_metrics_as_of_date"] == "2026-08-07"
        assert result["otp_all_pct"] == 100.0


class TestGetRouteTrendData:
    """Tests for get_route_trend_data function"""

    def test_trend_data_otp(self, db_session, sample_route, sample_route_otp_stop_events):
        """Test trend data for OTP metric.

        The fixture inserts five proximity stop_events on `today - 1`
        with a 4-on-time + 1-late deviation distribution → 80%. The
        endpoint emits `days + 1` rows (inclusive endpoints); only that
        one date carries a real value, the rest are null.
        """
        result = get_route_trend_data(db_session, "TEST1", metric="otp", days=30)

        assert result["route_id"] == "TEST1"
        assert result["metric"] == "otp"
        assert result["days"] == 30
        assert "trend_data" in result
        assert len(result["trend_data"]) == 31

        # Verify structure of trend data
        trend_point = result["trend_data"][0]
        assert "date" in trend_point
        assert "otp_percentage" in trend_point

        # Exactly one real value (the fixture date); the rest are null.
        real_values = [row for row in result["trend_data"] if row["otp_percentage"] is not None]
        assert len(real_values) == 1
        assert real_values[0]["otp_percentage"] == 80.0

    def test_trend_data_all_metrics(self, db_session, sample_route):
        """Test trend data shape for every supported metric.

        Post NOTES-19 cleanup, only OTP, service_delivered, and
        excess_trip_time remain. Each carries a different per-row value
        key — assert the right key is present in the returned series.
        """
        metric_fields = {
            "otp": "otp_percentage",
            "service_delivered": "service_delivered_ratio",
            "excess_trip_time": "excess_trip_time_pct",
        }

        for metric, expected_field in metric_fields.items():
            result = get_route_trend_data(db_session, "TEST1", metric=metric, days=30)

            assert result["metric"] == metric
            assert len(result["trend_data"]) > 0
            assert expected_field in result["trend_data"][0]

    def test_trend_data_no_data(self, db_session, sample_route):
        """Test trend data when no daily metrics exist.

        With no rows in `route_metrics_daily`, every service date in the
        window is emitted with `otp_percentage: null` so the frontend can
        distinguish "no data" from a real zero. The series length matches
        the requested window (`days + 1` for the inclusive endpoints).
        """
        result = get_route_trend_data(db_session, "TEST1", metric="otp", days=30)

        assert result["route_id"] == "TEST1"
        assert len(result["trend_data"]) == 31
        assert all(row["otp_percentage"] is None for row in result["trend_data"])

    def test_trend_data_custom_days(self, db_session, sample_route):
        """Test trend data with custom days parameter"""
        result = get_route_trend_data(db_session, "TEST1", metric="otp", days=60)

        assert result["days"] == 60

    def test_trend_data_service_delivered_empty(self, db_session, sample_route):
        """Test service_delivered trend metric returns the expected shape.

        With no runs / GTFS schedule fixtures, every day has scheduled_trips=0
        and ratio=None — but the endpoint still emits one row per service
        date in the window with `service_delivered_ratio: null` so the
        frontend axis stays consistent. The response envelope should be
        well-formed with the right metric label.
        """
        result = get_route_trend_data(db_session, "TEST1", metric="service_delivered", days=30)

        assert result["route_id"] == "TEST1"
        assert result["metric"] == "service_delivered"
        assert result["days"] == 30
        assert len(result["trend_data"]) == 31
        assert all(row["service_delivered_ratio"] is None for row in result["trend_data"])


class TestGetSystemTrendData:
    """Tests for get_system_trend_data — home-page system trend strip."""

    @staticmethod
    def _clear_cache():
        """Drop the module-level system-trend cache between tests.

        The cache is keyed by (metric, days, today_iso); tests share that
        cache via the imported function so an empty-DB result computed in
        one test would mask data inserted in another. Clear before each
        test so each call hits the live DB session.
        """
        from api import aggregations as agg

        agg._system_trend_cache.clear()

    def test_system_trend_otp_no_data(self, db_session):
        """Empty DB returns the right envelope shape with all-null values.

        With no rows in `route_metrics_daily`, every visible service date
        carries `otp_percentage: null` and `prior_window_value` is null
        too — the frontend then suppresses the delta indicator entirely.
        """
        self._clear_cache()
        result = get_system_trend_data(db_session, metric="otp", days=30)

        assert result["metric"] == "otp"
        assert result["days"] == 30
        assert "trend_data" in result
        # Inclusive endpoints: 30 days span = 31 visible days, matching
        # the per-route trend convention.
        assert len(result["trend_data"]) == 31
        assert all(row["otp_percentage"] is None for row in result["trend_data"])
        assert result["prior_window_value"] is None

    def test_system_trend_otp_reads_materialized_history(self, db_session, sample_routes):
        """Historical OTP rows come from the materialized `system_metrics_daily`.

        The hybrid serve path reads every historical date (anything strictly
        before today's Eastern service date) from the materialized table.
        Seeding one row should surface in the visible window with the exact
        value that was written.
        """
        self._clear_cache()
        from src.models import SystemMetricsDaily

        target_date = eastern_today() - timedelta(days=5)
        db_session.add(
            SystemMetricsDaily(
                service_date=target_date.isoformat(),
                otp_percentage=86.4,
                service_delivered_ratio=0.91,
                ewt_seconds=120.0,
                bunching_rate=0.07,
            )
        )
        db_session.commit()

        result = get_system_trend_data(db_session, metric="otp", days=30)

        # Find the row for the target date.
        row = next(r for r in result["trend_data"] if r["date"] == target_date.isoformat())
        assert row["otp_percentage"] == 86.4
        # Other historical days have no row, so they should still be null.
        other_days = [
            r
            for r in result["trend_data"]
            if r["date"] != target_date.isoformat() and r["date"] != eastern_today().isoformat()
        ]
        assert all(r["otp_percentage"] is None for r in other_days)

    def test_system_trend_otp_prior_window(self, db_session, sample_route):
        """Prior-window scalar is the mean of materialized prior-window values.

        Place a single row in the prior window only — its OTP becomes the
        prior_window_value and the visible window is all-null. The delta on
        the frontend would then be (no-data → suppressed) but the API still
        reports the prior value cleanly.
        """
        self._clear_cache()
        from src.models import SystemMetricsDaily

        # Land in prior window: 35 days ago is comfortably before the 30-day
        # current window's start (which is `today - 30`).
        prior_date = eastern_today() - timedelta(days=35)
        db_session.add(
            SystemMetricsDaily(
                service_date=prior_date.isoformat(),
                otp_percentage=72.0,
            )
        )
        db_session.commit()

        result = get_system_trend_data(db_session, metric="otp", days=30)

        # Only one prior-window day has data, so the prior mean is just its value.
        assert result["prior_window_value"] == 72.0
        # Visible historical window remains empty (today is live-computed and
        # has no data either, so it's also null).
        assert all(row["otp_percentage"] is None for row in result["trend_data"])

    def test_system_trend_service_delivered_envelope(self, db_session):
        """Service-delivered system trend returns the right envelope shape.

        With no Run / GTFS fixtures, every day's scheduled is 0 and the
        ratio is null — but the envelope must still emit one row per
        visible service date so the frontend axis stays consistent.
        """
        self._clear_cache()
        result = get_system_trend_data(db_session, metric="service_delivered", days=30)

        assert result["metric"] == "service_delivered"
        assert len(result["trend_data"]) == 31
        assert all(row["service_delivered_ratio"] is None for row in result["trend_data"])
        assert result["prior_window_value"] is None

    def test_system_trend_ewt_envelope(self, db_session):
        """EWT system trend returns the right envelope shape on empty DB.

        With no GTFS schedule and no stop_events, every day's pooled
        observed/scheduled lists are empty so EWT is null. Endpoint must
        still emit a complete 31-day visible window.
        """
        self._clear_cache()
        result = get_system_trend_data(db_session, metric="ewt", days=30)

        assert result["metric"] == "ewt"
        assert len(result["trend_data"]) == 31
        assert all(row["ewt_seconds"] is None for row in result["trend_data"])
        assert result["prior_window_value"] is None

    def test_system_trend_bunching_envelope(self, db_session):
        """Bunching system trend returns the right envelope shape on empty DB."""
        self._clear_cache()
        result = get_system_trend_data(db_session, metric="bunching", days=30)

        assert result["metric"] == "bunching"
        assert len(result["trend_data"]) == 31
        assert all(row["bunching_rate"] is None for row in result["trend_data"])
        assert result["prior_window_value"] is None

    def test_system_trend_invalid_metric_raises(self, db_session):
        """Unknown metric is a programming error and surfaces ValueError.

        The API layer rejects unknown metrics with HTTP 400 before reaching
        the data function — this test pins the underlying contract.
        """
        self._clear_cache()
        import pytest

        with pytest.raises(ValueError):
            get_system_trend_data(db_session, metric="not_a_metric", days=30)

    def test_system_trend_custom_days(self, db_session):
        """`days` parameter shapes the visible window; envelope length follows."""
        self._clear_cache()
        result = get_system_trend_data(db_session, metric="otp", days=14)

        assert result["days"] == 14
        # Inclusive endpoints: 14 days span = 15 visible days.
        assert len(result["trend_data"]) == 15

    def test_system_trend_caching(self, db_session, sample_route):
        """Repeated calls within the TTL return the cached payload.

        Verifies the cache plumbing: a second call for the same
        (metric, days, today) returns an object equal to the first
        without re-querying. Side effect probe: the second call's
        result is the *same dict instance* the first call produced
        (cache stores the reference, not a copy).
        """
        self._clear_cache()
        first = get_system_trend_data(db_session, metric="otp", days=30)
        second = get_system_trend_data(db_session, metric="otp", days=30)
        assert first is second


class TestGetRouteContributors:
    """Tests for get_route_contributors — NOTES-39 biggest-contributors view.

    Covers the contribution_score formula, sign convention for higher- vs
    lower-is-better metrics, baseline sourcing from `system_metrics_daily`,
    and the scheduled-trips volume proxy.
    """

    @staticmethod
    def _clear_cache():
        """Drop the module-level contributors cache between tests."""
        from api import aggregations as agg

        agg._contributors_cache.clear()

    @pytest.fixture(autouse=True)
    def _isolate_route_targets(self, tmp_path, monkeypatch):
        """Point the route_targets loader at a missing file for every test in this class.

        These tests exercise the baseline-fallback path (no per-route
        target configured), so we need the loader to return None for
        every metric. NOTES-47 added a dedicated test class for the
        target-override path; this fixture keeps the baseline tests
        deterministic against future YAML edits.
        """
        from src import route_targets as _rt

        missing = tmp_path / "absent.yaml"
        monkeypatch.setenv("WMATA_ROUTE_TARGETS_PATH", str(missing))
        _rt.reset_cache_for_tests()
        yield
        _rt.reset_cache_for_tests()

    def _seed_system_baseline(self, db_session, otp=80.0, days_back=5):
        """Seed N days of `system_metrics_daily` rows ending `days_back` ago.

        Uniform value per row so the window mean equals the value, making
        the contribution arithmetic in the assertion path obvious.
        """
        from src.models import SystemMetricsDaily

        rows = []
        for i in range(7):
            d = eastern_today() - timedelta(days=days_back + i)
            rows.append(
                SystemMetricsDaily(
                    service_date=d.isoformat(),
                    otp_percentage=otp,
                    service_delivered_ratio=0.9,
                    ewt_seconds=120.0,
                    bunching_rate=0.05,
                )
            )
        db_session.add_all(rows)
        db_session.commit()

    def _seed_route_otp(self, db_session, route_id, otp_value, n_days=5):
        """Seed N days of proximity stop_events so `_route_otp_window_mean` reads `otp_value`%.

        Post NOTES-19 migration: route OTP is computed from stop_events, not
        `route_metrics_daily`. Seeds 100 proximity events per day with the
        on-time/late split that buckets to `otp_value`% under the WMATA
        window — a uniform per-day rate so the window mean equals the seed.
        """
        from datetime import datetime as _dt

        from src.models import StopEvent

        events_per_day = 100
        on_time_count = int(round(otp_value / 100.0 * events_per_day))
        late_count = events_per_day - on_time_count
        rows = []
        for i in range(n_days):
            d = eastern_today() - timedelta(days=i + 1)
            base_ts = _dt.combine(d, _dt.min.time()).replace(hour=14)
            for j in range(on_time_count):
                rows.append(
                    StopEvent(
                        service_date=d.isoformat(),
                        trip_id=f"TRIP_{route_id}_{i}_OT_{j}",
                        route_id=route_id,
                        direction_id=0,
                        stop_id=f"STOP_OTP_{route_id}",
                        stop_sequence=1,
                        observed_arrival_ts=base_ts + timedelta(seconds=j),
                        deviation_sec=0,
                        source="proximity",
                        schedule_relationship="SCHEDULED",
                    )
                )
            for j in range(late_count):
                rows.append(
                    StopEvent(
                        service_date=d.isoformat(),
                        trip_id=f"TRIP_{route_id}_{i}_LATE_{j}",
                        route_id=route_id,
                        direction_id=0,
                        stop_id=f"STOP_OTP_{route_id}",
                        stop_sequence=1,
                        observed_arrival_ts=base_ts + timedelta(seconds=on_time_count + j),
                        deviation_sec=600,
                        source="proximity",
                        schedule_relationship="SCHEDULED",
                    )
                )
        db_session.add_all(rows)
        db_session.commit()

    def _seed_gtfs_trips(self, db_session, route_id, trip_count, day_type="weekday"):
        """Seed GTFS Trip + Calendar rows so the route has scheduled trips.

        Creates one Calendar service active for the requested day_type plus
        `trip_count` Trip rows under that service. The contributors path
        computes scheduled-trips per day_type by `COUNT(DISTINCT trip_id)`,
        so seeding `trip_count` distinct trip_ids gives a known volume.
        """
        from src.models import Calendar, Trip

        service_id = f"SVC_{route_id}"
        cal = Calendar(
            service_id=service_id,
            monday=0,
            tuesday=1 if day_type == "weekday" else 0,
            wednesday=0,
            thursday=0,
            friday=0,
            saturday=1 if day_type == "saturday" else 0,
            sunday=1 if day_type == "sunday" else 0,
            start_date="20260101",
            end_date="20271231",
            is_current=True,
        )
        db_session.add(cal)
        for i in range(trip_count):
            db_session.add(
                Trip(
                    trip_id=f"TRIP_{route_id}_{i}",
                    route_id=route_id,
                    service_id=service_id,
                    direction_id=i % 2,
                    is_current=True,
                )
            )
        db_session.commit()

    def test_contributors_empty_db(self, db_session):
        """Empty DB returns the envelope shape with null baseline.

        With no `system_metrics_daily` rows, baseline is null and the
        contributors list is empty (we can't rank without a comparison
        target).
        """
        self._clear_cache()
        from api.aggregations import get_route_contributors

        result = get_route_contributors(db_session, metric="otp", days=30)

        assert result["metric"] == "otp"
        assert result["days"] == 30
        assert result["baseline_value"] is None
        assert result["higher_is_better"] is True
        assert result["contributors"] == []

    def test_contributors_otp_higher_is_better_sign(self, db_session, sample_routes):
        """OTP: route below baseline → positive contribution_score.

        TEST1: 60% OTP, system baseline 80%, 10 scheduled trips/window.
        contribution_score = (80 - 60) * 10 = 200 (positive — drag).
        """
        self._clear_cache()
        from api.aggregations import get_route_contributors

        self._seed_system_baseline(db_session, otp=80.0)
        self._seed_route_otp(db_session, "TEST1", 60.0)
        self._seed_gtfs_trips(db_session, "TEST1", trip_count=10, day_type="weekday")

        result = get_route_contributors(db_session, metric="otp", days=30)

        assert result["baseline_value"] == 80.0
        assert result["higher_is_better"] is True
        # TEST1 should be the only scoreable contributor.
        ours = next((c for c in result["contributors"] if c["route_id"] == "TEST1"), None)
        assert ours is not None
        assert ours["route_value"] == 60.0
        assert ours["scheduled_trips"] > 0
        # Score = (80 - 60) * scheduled_trips. Positive sign — dragging system.
        assert ours["contribution_score"] > 0
        assert ours["contribution_score"] == (80.0 - 60.0) * ours["scheduled_trips"]

    def test_contributors_otp_route_above_baseline_negative_score(self, db_session, sample_routes):
        """OTP: route above baseline → negative contribution_score.

        TEST1: 95% OTP, system baseline 80%. Score is negative because the
        route is *helping* the system; sort order pushes it to the bottom.
        """
        self._clear_cache()
        from api.aggregations import get_route_contributors

        self._seed_system_baseline(db_session, otp=80.0)
        self._seed_route_otp(db_session, "TEST1", 95.0)
        self._seed_gtfs_trips(db_session, "TEST1", trip_count=10, day_type="weekday")

        result = get_route_contributors(db_session, metric="otp", days=30)
        ours = next((c for c in result["contributors"] if c["route_id"] == "TEST1"), None)
        assert ours is not None
        assert ours["route_value"] == 95.0
        # Score = (80 - 95) * scheduled_trips → negative.
        assert ours["contribution_score"] < 0

    def test_contributors_lower_is_better_sign_flipped(self, db_session, sample_routes):
        """EWT (lower-is-better): route above baseline → positive score.

        Contribution magnitude is sign-flipped for lower-is-better metrics
        so positive always means "dragging the system down." For EWT:
        baseline 120s, route 240s → (baseline - route) is -120, sign-flip
        gives +120. Score = +120 * scheduled_trips (positive).

        EWT/bunching `route_value` comes from the live cache, not
        materialized history — without seeded stop_events the live cache
        returns nothing for the route. We assert the *envelope* and the
        sign convention via the metric's `higher_is_better` flag instead;
        the formula proof for OTP above carries via the same code path.
        """
        self._clear_cache()
        from api.aggregations import get_route_contributors

        self._seed_system_baseline(db_session)
        result = get_route_contributors(db_session, metric="ewt", days=30)

        assert result["metric"] == "ewt"
        assert result["higher_is_better"] is False
        # Baseline window mean = 120.0 from the seed.
        assert result["baseline_value"] == 120.0

    def test_contributors_baseline_window_mean(self, db_session, sample_routes):
        """Baseline is the simple mean of non-null values in the window.

        Three rows: 70, 80, 90 → mean 80.
        """
        self._clear_cache()
        from api.aggregations import get_route_contributors
        from src.models import SystemMetricsDaily

        for i, otp in enumerate([70.0, 80.0, 90.0]):
            d = eastern_today() - timedelta(days=i + 1)
            db_session.add(SystemMetricsDaily(service_date=d.isoformat(), otp_percentage=otp))
        db_session.commit()

        result = get_route_contributors(db_session, metric="otp", days=30)
        assert result["baseline_value"] == 80.0

    def test_contributors_drops_routes_with_no_volume(self, db_session, sample_routes):
        """Route with 0 scheduled trips in window is dropped, not zero-scored.

        Without GTFS Trip rows, `scheduled_trips_in_window` is 0, which
        would give every route a zero score and bury real signal under
        ties. We drop them instead.
        """
        self._clear_cache()
        from api.aggregations import get_route_contributors

        self._seed_system_baseline(db_session, otp=80.0)
        self._seed_route_otp(db_session, "TEST1", 60.0)
        # Note: no _seed_gtfs_trips — route has no schedule.

        result = get_route_contributors(db_session, metric="otp", days=30)
        assert all(c["route_id"] != "TEST1" for c in result["contributors"])

    def test_contributors_drops_routes_with_no_route_value(self, db_session, sample_routes):
        """Route without a window OTP value is dropped (can't score it).

        TEST1 has GTFS schedule but no `route_metrics_daily` rows in the
        window. With baseline available but no route value, contribution
        is undefined — drop rather than fabricate.
        """
        self._clear_cache()
        from api.aggregations import get_route_contributors

        self._seed_system_baseline(db_session, otp=80.0)
        self._seed_gtfs_trips(db_session, "TEST1", trip_count=10, day_type="weekday")

        result = get_route_contributors(db_session, metric="otp", days=30)
        assert all(c["route_id"] != "TEST1" for c in result["contributors"])

    def test_contributors_sort_order(self, db_session, sample_routes):
        """Routes are returned sorted by contribution_score desc.

        TEST1 at 50% (gap=30), TEST2 at 70% (gap=10), both with the same
        volume. TEST1 must rank ahead of TEST2.
        """
        self._clear_cache()
        from api.aggregations import get_route_contributors

        self._seed_system_baseline(db_session, otp=80.0)
        self._seed_route_otp(db_session, "TEST1", 50.0)
        self._seed_route_otp(db_session, "TEST2", 70.0)
        self._seed_gtfs_trips(db_session, "TEST1", trip_count=10, day_type="weekday")
        self._seed_gtfs_trips(db_session, "TEST2", trip_count=10, day_type="weekday")

        result = get_route_contributors(db_session, metric="otp", days=30)
        ours = [c for c in result["contributors"] if c["route_id"] in ("TEST1", "TEST2")]
        assert len(ours) == 2
        assert ours[0]["route_id"] == "TEST1"
        assert ours[0]["contribution_score"] > ours[1]["contribution_score"]

    def test_contributors_invalid_metric_raises(self, db_session):
        """Unsupported metric name surfaces a ValueError from the helper."""
        self._clear_cache()
        import pytest

        from api.aggregations import get_route_contributors

        with pytest.raises(ValueError):
            get_route_contributors(db_session, metric="not_a_metric", days=30)

    def test_contributors_caching(self, db_session, sample_routes):
        """Repeated calls within the TTL return the cached payload."""
        self._clear_cache()
        from api.aggregations import get_route_contributors

        self._seed_system_baseline(db_session, otp=80.0)
        first = get_route_contributors(db_session, metric="otp", days=30)
        second = get_route_contributors(db_session, metric="otp", days=30)
        assert first is second

    def test_contributors_otp_window_mean_excludes_partial_day(self, db_session, sample_routes):
        """A `data_quality='partial'` date is excluded from the OTP window mean (NOTES-123).

        Seeds 5 clean days at 90% OTP plus a 6th day with a 0%-on-time
        StopEvent flagged `partial` in `system_metrics_daily` -- the same
        kind of collection-outage day that squares multi-hour observation
        gaps into inflated EWT elsewhere. Without exclusion the raw window
        mean would blend all 6 days ((90*5 + 0) / 6 = 75); with it, the
        partial day contributes nothing and the mean stays at the clean
        5-day value.
        """
        self._clear_cache()
        from datetime import datetime as _dt

        from api.aggregations import get_route_contributors
        from src.models import StopEvent, SystemMetricsDaily

        # Push the baseline seed window back so it doesn't collide with the
        # partial date seeded below (1-6 days ago).
        self._seed_system_baseline(db_session, otp=80.0, days_back=10)
        self._seed_route_otp(db_session, "TEST1", 90.0, n_days=5)
        self._seed_gtfs_trips(db_session, "TEST1", trip_count=10, day_type="weekday")

        partial_date = eastern_today() - timedelta(days=6)
        base_ts = _dt.combine(partial_date, _dt.min.time()).replace(hour=14)
        db_session.add(
            StopEvent(
                service_date=partial_date.isoformat(),
                trip_id="TRIP_TEST1_PARTIAL",
                route_id="TEST1",
                direction_id=0,
                stop_id="STOP_OTP_TEST1",
                stop_sequence=1,
                observed_arrival_ts=base_ts,
                deviation_sec=600,  # late -> 0% on-time this day
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )
        db_session.add(
            SystemMetricsDaily(service_date=partial_date.isoformat(), data_quality="partial")
        )
        db_session.commit()

        result = get_route_contributors(db_session, metric="otp", days=30)
        ours = next((c for c in result["contributors"] if c["route_id"] == "TEST1"), None)
        assert ours is not None
        assert ours["route_value"] == 90.0

    def test_contributors_days_included_counts_dates_that_fed_the_baseline(
        self, db_session, sample_routes
    ):
        """`days_included` matches `_system_baseline_for_window`'s real denominator (review finding 5).

        Seeds 5 `complete` rows with an `otp_percentage` value plus 2
        `partial` rows with no value, all inside the 30-day window. Most of
        the 30 calendar days have no `system_metrics_daily` row at all —
        `days_included` must read 5 (the count of rows that actually fed
        the baseline mean), not `30 - 2 = 28`, which would silently count
        every date with no row as "included."
        """
        self._clear_cache()
        from api.aggregations import get_route_contributors
        from src.models import SystemMetricsDaily

        for offset in range(1, 6):
            d = eastern_today() - timedelta(days=offset)
            db_session.add(SystemMetricsDaily(service_date=d.isoformat(), otp_percentage=80.0))
        for offset in (10, 11):
            d = eastern_today() - timedelta(days=offset)
            db_session.add(SystemMetricsDaily(service_date=d.isoformat(), data_quality="partial"))
        db_session.commit()

        result = get_route_contributors(db_session, metric="otp", days=30)
        assert result["baseline_value"] == 80.0
        assert result["days_included"] == 5

    def test_contributors_ewt_window_excludes_partial_day(
        self, db_session, sample_routes, monkeypatch
    ):
        """A `data_quality='partial'` date is excluded from the pooled EWT window (review finding 1).

        Two clean `route_metrics_daily_overlay` days pool to a modest EWT;
        a third day flagged `partial` carries an extreme observed-headway
        spread that would blow up the pooled EWT if it leaked in. Also
        asserts the partial date is never among the dates handed to the
        live-compute fetch: with no raw `stop_events` seeded for the
        partial day, a value-only assertion would pass even under the bug
        (live compute finds nothing for that date and silently
        contributes zero) -- this is what actually pins the exclusion
        happening at the right layer. (The 30-day window is otherwise
        sparse -- only the 3 seeded dates have overlay rows -- so the
        other ~27 dates legitimately fall through to live compute; that's
        real "not backfilled yet" behavior, not the bug, hence asserting
        the partial date's absence rather than an empty call list.)
        Proves the fix sits there: with the overlay row present but
        flagged partial, a buggy per-row skip inside the overlay reader
        would make the date look "not materialized yet" and fall through
        to a live recompute over raw `stop_events` that does NOT exclude
        partial days — pooling it right back in (review findings 1/2/3).
        """
        self._clear_cache()
        import src.ewt as ewt_module
        from api.aggregations import _derive_ewt_metrics, get_route_contributors
        from src.models import RouteMetricsDailyOverlay, StopEvent, SystemMetricsDaily

        calls = []
        original = ewt_module.fetch_observed_stop_events_for_window

        def spy(db, service_dates, route_ids=None):
            """Record every window of dates handed to the live-compute fetch."""
            calls.append(list(service_dates))
            return original(db, service_dates, route_ids=route_ids)

        monkeypatch.setattr(ewt_module, "fetch_observed_stop_events_for_window", spy)

        self._seed_system_baseline(db_session, days_back=10)
        self._seed_gtfs_trips(db_session, "TEST1", trip_count=10, day_type="weekday")

        # One raw stop_event just sets `_latest_service_date_with_stop_events`
        # (the pooled window's anchor) -- its own content is irrelevant here.
        db_session.add(
            StopEvent(
                service_date=(eastern_today() - timedelta(days=1)).isoformat(),
                trip_id="TRIP_ANCHOR",
                route_id="TEST1",
                direction_id=0,
                stop_id="STOP_ANCHOR",
                stop_sequence=1,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )

        clean_stats = [
            {
                "obs_sum_h": 1200.0,
                "obs_sum_h_sq": 1200.0**2 / 2,
                "n_obs": 2,
                "sched_sum_h": 1200.0,
                "sched_sum_h_sq": 1200.0**2 / 2,
                "n_sched": 2,
            }
            for _ in range(2)
        ]
        for i, stats in enumerate(clean_stats):
            d = eastern_today() - timedelta(days=i + 1)
            db_session.add(
                RouteMetricsDailyOverlay(
                    route_id="TEST1",
                    service_date=d.isoformat(),
                    day_type="weekday",
                    data_quality="complete",
                    ewt_obs_sum_h=stats["obs_sum_h"],
                    ewt_obs_sum_h_sq=stats["obs_sum_h_sq"],
                    ewt_n_observed_headways=stats["n_obs"],
                    ewt_sched_sum_h=stats["sched_sum_h"],
                    ewt_sched_sum_h_sq=stats["sched_sum_h_sq"],
                    ewt_n_scheduled_headways=stats["n_sched"],
                )
            )

        partial_date = eastern_today() - timedelta(days=3)
        db_session.add(
            RouteMetricsDailyOverlay(
                route_id="TEST1",
                service_date=partial_date.isoformat(),
                day_type="weekday",
                data_quality="partial",
                ewt_obs_sum_h=36000.0,
                ewt_obs_sum_h_sq=36000.0**2,
                ewt_n_observed_headways=1,
                ewt_sched_sum_h=600.0,
                ewt_sched_sum_h_sq=600.0**2,
                ewt_n_scheduled_headways=1,
            )
        )
        db_session.add(
            SystemMetricsDaily(service_date=partial_date.isoformat(), data_quality="partial")
        )
        db_session.commit()

        expected = _derive_ewt_metrics(
            sum(s["obs_sum_h"] for s in clean_stats),
            sum(s["obs_sum_h_sq"] for s in clean_stats),
            sum(s["sched_sum_h"] for s in clean_stats),
            sum(s["sched_sum_h_sq"] for s in clean_stats),
            sum(s["n_obs"] for s in clean_stats),
            sum(s["n_sched"] for s in clean_stats),
        )["ewt_seconds"]

        result = get_route_contributors(db_session, metric="ewt", days=30)
        ours = next((c for c in result["contributors"] if c["route_id"] == "TEST1"), None)
        assert ours is not None
        assert ours["route_value"] == pytest.approx(expected)
        assert all(partial_date not in call_dates for call_dates in calls)

    def test_contributors_baseline_anchors_on_same_window_as_route_value(
        self, db_session, sample_routes
    ):
        """`baseline_value`/`days_included` anchor on the same date `route_value` pools (review finding 4).

        Seeds one raw `stop_event` 3 days ago (so the pooled-metrics
        anchor, `_latest_service_date_with_stop_events`, is `today - 3`)
        plus a `system_metrics_daily` row dated *today* with an extreme
        OTP value. Anchoring the baseline on `eastern_today()` (the
        pre-fix behavior) would pull today's row into the 30-day baseline
        window even though it falls after the anchor the pooled
        SD/EWT/bunching `route_value` actually uses -- the two 30-day
        spans wouldn't even share the same calendar days. Anchoring both
        on the same date excludes it.
        """
        self._clear_cache()
        from api.aggregations import get_route_contributors
        from src.models import StopEvent, SystemMetricsDaily

        self._seed_system_baseline(db_session, otp=80.0, days_back=10)
        db_session.add(
            SystemMetricsDaily(service_date=eastern_today().isoformat(), otp_percentage=999.0)
        )
        db_session.add(
            StopEvent(
                service_date=(eastern_today() - timedelta(days=3)).isoformat(),
                trip_id="TRIP_ANCHOR",
                route_id="TEST1",
                direction_id=0,
                stop_id="STOP_ANCHOR",
                stop_sequence=1,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )
        db_session.commit()

        result = get_route_contributors(db_session, metric="otp", days=30)
        assert result["baseline_value"] == 80.0

    def test_contributors_per_route_target_overrides_baseline(
        self, db_session, sample_routes, tmp_path, monkeypatch
    ):
        """When a route's target is configured (NOTES-47) it overrides the system baseline.

        Seed: system baseline OTP 80 %, TEST1 route OTP 60 %, scheduled
        trips 10 (weekday × 5). With no target, contribution would be
        (80 − 60) × N. With a per-route target of 90 %, contribution
        becomes (90 − 60) × N — bigger gap, bigger score. The row's
        `reference_source` reports "target" and `target_value` carries
        the configured number.
        """
        from src import route_targets as _rt

        # Override the autouse fixture's missing-file pointer with a
        # populated YAML that sets TEST1's OTP target to 90 %.
        yaml_path = tmp_path / "with_target.yaml"
        yaml_path.write_text(
            """
system_default:
  otp: 78
routes:
  "TEST1":
    otp: 90
""",
            encoding="utf-8",
        )
        monkeypatch.setenv("WMATA_ROUTE_TARGETS_PATH", str(yaml_path))
        _rt.reset_cache_for_tests()

        self._clear_cache()
        from api.aggregations import get_route_contributors

        self._seed_system_baseline(db_session, otp=80.0)
        self._seed_route_otp(db_session, "TEST1", 60.0)
        self._seed_gtfs_trips(db_session, "TEST1", trip_count=10, day_type="weekday")

        result = get_route_contributors(db_session, metric="otp", days=30)
        ours = next((c for c in result["contributors"] if c["route_id"] == "TEST1"), None)
        assert ours is not None
        assert ours["reference_source"] == "target"
        assert ours["target_value"] == 90.0
        assert ours["reference_value"] == 90.0
        assert ours["route_value"] == 60.0
        # Score = (target − route_value) × scheduled_trips → larger than
        # the baseline-only counterfactual.
        assert ours["contribution_score"] == (90.0 - 60.0) * ours["scheduled_trips"]
        # The envelope still carries the system_target_value from the YAML.
        assert result["system_target_value"] == 78.0

    def test_as_of_date_anchors_window_instead_of_latest_service_date(
        self, db_session, sample_routes
    ):
        """`as_of_date`, when given, anchors the window on that date instead of
        `_latest_service_date_with_stop_events` (PR #219 review finding 1).

        Without a fix, `get_route_contributors` always anchors on the latest
        stop_events date regardless of what's passed, so the system weekly
        narrative's "biggest OTP-dragging routes" sections silently describe
        the latest week of stop_events rather than the `--as-of` week being
        summarized. TEST1 has a 60%-OTP week ending 60 days ago; a stray
        recent stop_event on a different route makes the *latest* service
        date land elsewhere. The default (no `as_of_date`) call must not see
        TEST1's historical week at all; passing `as_of_date` for that
        historical date must.
        """
        self._clear_cache()
        from datetime import datetime as _dt

        from api.aggregations import get_route_contributors
        from src.models import StopEvent

        historical_anchor = eastern_today() - timedelta(days=60)

        # Baseline window ending at the historical anchor (80% OTP).
        self._seed_system_baseline(db_session, otp=80.0, days_back=60)
        self._seed_gtfs_trips(db_session, "TEST1", trip_count=10, day_type="weekday")

        # TEST1's OTP events, all dated within the 7-day window ending at
        # historical_anchor -- a 60% OTP week.
        events_per_day = 100
        on_time_count = int(round(0.60 * events_per_day))
        late_count = events_per_day - on_time_count
        rows = []
        for i in range(5):
            d = historical_anchor - timedelta(days=i)
            base_ts = _dt.combine(d, _dt.min.time()).replace(hour=14)
            for j in range(on_time_count):
                rows.append(
                    StopEvent(
                        service_date=d.isoformat(),
                        trip_id=f"TRIP_HIST_{i}_OT_{j}",
                        route_id="TEST1",
                        direction_id=0,
                        stop_id="STOP_OTP_TEST1",
                        stop_sequence=1,
                        observed_arrival_ts=base_ts + timedelta(seconds=j),
                        deviation_sec=0,
                        source="proximity",
                        schedule_relationship="SCHEDULED",
                    )
                )
            for j in range(late_count):
                rows.append(
                    StopEvent(
                        service_date=d.isoformat(),
                        trip_id=f"TRIP_HIST_{i}_LATE_{j}",
                        route_id="TEST1",
                        direction_id=0,
                        stop_id="STOP_OTP_TEST1",
                        stop_sequence=1,
                        observed_arrival_ts=base_ts + timedelta(seconds=on_time_count + j),
                        deviation_sec=600,
                        source="proximity",
                        schedule_relationship="SCHEDULED",
                    )
                )
        db_session.add_all(rows)
        # A stray recent stop_event on a different route -- moves
        # `_latest_service_date_with_stop_events` well past the historical
        # window without giving TEST1 any recent data of its own.
        recent_date = eastern_today() - timedelta(days=1)
        db_session.add(
            StopEvent(
                service_date=recent_date.isoformat(),
                trip_id="TRIP_RECENT_STRAY",
                route_id="TEST2",
                direction_id=0,
                stop_id="STOP_OTP_TEST2",
                stop_sequence=1,
                observed_arrival_ts=_dt.combine(recent_date, _dt.min.time()).replace(hour=14),
                deviation_sec=0,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )
        db_session.commit()

        # Default: anchors on the latest stop_events date (recent_date).
        # TEST1 has no data there, so it's absent from the contributors list.
        default_result = get_route_contributors(db_session, metric="otp", days=7)
        default_ours = next(
            (c for c in default_result["contributors"] if c["route_id"] == "TEST1"), None
        )
        assert default_ours is None

        self._clear_cache()

        # Explicit as_of_date: anchors on the historical window instead.
        historical_result = get_route_contributors(
            db_session, metric="otp", days=7, as_of_date=historical_anchor
        )
        historical_ours = next(
            (c for c in historical_result["contributors"] if c["route_id"] == "TEST1"), None
        )
        assert historical_ours is not None
        assert historical_ours["route_value"] == 60.0

    def test_explicit_as_of_date_equal_to_today_does_not_share_cache_with_default(
        self, db_session, sample_routes
    ):
        """An explicit `as_of_date` equal to today must not share a cache
        entry with the default (`None`) anchor (PR #219 review finding 1,
        follow-up).

        The default anchor computes `end_date =
        _latest_service_date_with_stop_events(db)`, not `eastern_today()` —
        they only coincide once stop_events has caught up to today. Before
        the fix, both branches keyed the cache on the bare ISO date, so an
        explicit `as_of_date=eastern_today()` call landing within the TTL
        of a default call would silently return the *default* call's
        result (anchored on yesterday, the latest stop_events date) instead
        of its own (anchored on today, which has no stop_events yet).

        TEST1 has a 60%-OTP single day at yesterday and nothing at all
        today. days=1 makes the two windows disjoint single days, so the
        two calls must disagree if -- and only if -- they're computed
        (not cache-collided) independently.
        """
        self._clear_cache()
        from datetime import datetime as _dt

        from api.aggregations import get_route_contributors
        from src.models import StopEvent, SystemMetricsDaily

        # The seeded calendar (_seed_gtfs_trips day_type="weekday") has no
        # weekend service, so anchor the seeded day on the most recent
        # WEEKDAY before today — a bare `today - 1` lands on Sat/Sun when
        # the suite runs on Sun/Mon and drops TEST1 for lack of scheduled
        # trips. Any past date works for the default anchor, which follows
        # _latest_service_date_with_stop_events, not the calendar date.
        yesterday = eastern_today() - timedelta(days=1)
        while yesterday.weekday() >= 5:
            yesterday -= timedelta(days=1)

        # days=1 windows need only a single system_metrics_daily row each
        # (avoids the service_date PK collision _seed_system_baseline's
        # 7-row seed would hit across two overlapping 7-day calls).
        db_session.add_all(
            [
                SystemMetricsDaily(service_date=eastern_today().isoformat(), otp_percentage=80.0),
                SystemMetricsDaily(service_date=yesterday.isoformat(), otp_percentage=80.0),
            ]
        )
        db_session.commit()

        self._seed_gtfs_trips(db_session, "TEST1", trip_count=10, day_type="weekday")

        events_per_day = 100
        on_time_count = 60
        late_count = events_per_day - on_time_count
        base_ts = _dt.combine(yesterday, _dt.min.time()).replace(hour=14)
        rows = []
        for j in range(on_time_count):
            rows.append(
                StopEvent(
                    service_date=yesterday.isoformat(),
                    trip_id=f"TRIP_YDAY_OT_{j}",
                    route_id="TEST1",
                    direction_id=0,
                    stop_id="STOP_OTP_TEST1",
                    stop_sequence=1,
                    observed_arrival_ts=base_ts + timedelta(seconds=j),
                    deviation_sec=0,
                    source="proximity",
                    schedule_relationship="SCHEDULED",
                )
            )
        for j in range(late_count):
            rows.append(
                StopEvent(
                    service_date=yesterday.isoformat(),
                    trip_id=f"TRIP_YDAY_LATE_{j}",
                    route_id="TEST1",
                    direction_id=0,
                    stop_id="STOP_OTP_TEST1",
                    stop_sequence=1,
                    observed_arrival_ts=base_ts + timedelta(seconds=on_time_count + j),
                    deviation_sec=600,
                    source="proximity",
                    schedule_relationship="SCHEDULED",
                )
            )
        db_session.add_all(rows)
        db_session.commit()

        # Default: anchors on _latest_service_date_with_stop_events
        # (yesterday, the only date with stop_events). TEST1 scores.
        default_result = get_route_contributors(db_session, metric="otp", days=1)
        default_ours = next(
            (c for c in default_result["contributors"] if c["route_id"] == "TEST1"), None
        )
        assert default_ours is not None
        assert default_ours["route_value"] == 60.0

        # Explicit as_of_date=today, landing well within the cache TTL of
        # the call above. Must NOT return the cached default-anchor result
        # -- today has no stop_events, so TEST1 has no route_value and is
        # dropped.
        explicit_result = get_route_contributors(
            db_session, metric="otp", days=1, as_of_date=eastern_today()
        )
        explicit_ours = next(
            (c for c in explicit_result["contributors"] if c["route_id"] == "TEST1"), None
        )
        assert explicit_ours is None


class TestComputeRouteDeltas:
    """Tests for `compute_route_deltas` (NOTES-38).

    Deltas are computed from `route_metrics_daily_overlay` (OTP / SD / EWT /
    bunching) and from `compute_excess_trip_time` live (excess_trip_time_pct).
    The excess live-compute is stubbed so tests stay fast.
    """

    @staticmethod
    def _clear_cache():
        """Drop module-level deltas cache between tests."""
        from api import aggregations as agg

        agg._deltas_cache.clear()

    @staticmethod
    def _stub_excess_trip_time(monkeypatch, pct_by_route_date=None):
        """Replace `compute_excess_trip_time` with a caller-provided lookup.

        `pct_by_route_date` is `{route_id: {iso_date: pct}}` (or None).
        Returns `n_trips=0` for any missing entry so the live-compute path
        emits None for those (route, date) pairs — matching thin-data.
        """
        from api import aggregations as agg

        pct_by_route_date = pct_by_route_date or {}

        def fake(db, route_id, service_date):
            """Stub for compute_excess_trip_time."""
            ds = service_date.isoformat()
            pct = pct_by_route_date.get(route_id, {}).get(ds)
            if pct is not None:
                return {
                    "pct_over_110": pct,
                    "n_trips": 10,
                    "median_actual_sec": 1800,
                    "median_scheduled_sec": 1600,
                }
            return {
                "pct_over_110": None,
                "n_trips": 0,
                "median_actual_sec": None,
                "median_scheduled_sec": None,
            }

        monkeypatch.setattr(agg, "compute_excess_trip_time", fake)

    def _seed_overlay_window(
        self,
        db_session,
        route_id,
        current_otp,
        prior_otp,
        n_each=7,
    ):
        """Seed `n_each` overlay rows in each window with the given OTP levels.

        OTP counts are set so `on_time_pct ≈ current_otp / 100 * all_n`.
        All other sufficient statistics default to zero so the other metrics
        (SD, EWT, bunching) return None for those days.
        """
        from src.models import RouteMetricsDailyOverlay

        rows = []
        for i in range(n_each):
            d = eastern_today() - timedelta(days=i)
            on_time = int(current_otp)
            rows.append(
                RouteMetricsDailyOverlay(
                    route_id=route_id,
                    service_date=d.isoformat(),
                    day_type="weekday",
                    otp_all_early=0,
                    otp_all_on_time=on_time,
                    otp_all_late=100 - on_time,
                    otp_origin_early=0,
                    otp_origin_on_time=on_time,
                    otp_origin_late=100 - on_time,
                    otp_destination_early=0,
                    otp_destination_on_time=on_time,
                    otp_destination_late=100 - on_time,
                    scheduled_trips=10,
                    delivered_trips=10,
                )
            )
        for i in range(n_each):
            d = eastern_today() - timedelta(days=n_each + i)
            on_time = int(prior_otp)
            rows.append(
                RouteMetricsDailyOverlay(
                    route_id=route_id,
                    service_date=d.isoformat(),
                    day_type="weekday",
                    otp_all_early=0,
                    otp_all_on_time=on_time,
                    otp_all_late=100 - on_time,
                    otp_origin_early=0,
                    otp_origin_on_time=on_time,
                    otp_origin_late=100 - on_time,
                    otp_destination_early=0,
                    otp_destination_on_time=on_time,
                    otp_destination_late=100 - on_time,
                    scheduled_trips=10,
                    delivered_trips=10,
                )
            )
        db_session.add_all(rows)
        db_session.commit()

    def test_deltas_returns_all_five_metrics(self, db_session, sample_route, monkeypatch):
        """The deltas dict carries one entry per scorecard metric."""
        self._clear_cache()
        from api.aggregations import compute_route_deltas

        self._stub_excess_trip_time(monkeypatch)
        deltas = compute_route_deltas(db_session, "TEST1")
        assert set(deltas.keys()) == {
            "otp",
            "service_delivered",
            "ewt",
            "bunching",
            "excess_trip_time_pct",
        }
        for metric_block in deltas.values():
            assert set(metric_block.keys()) >= {"value", "valid", "current_n", "prior_n"}

    def test_deltas_otp_sign_convention(self, db_session, sample_route, monkeypatch):
        """`value = current - prior`, no flip for higher-is-better OTP.

        Current 7 days at 80% OTP, prior 7 at 70%. Delta should be ~+10
        (positive: current window's mean is higher than prior).
        """
        self._clear_cache()
        from api.aggregations import compute_route_deltas

        self._stub_excess_trip_time(monkeypatch)
        self._seed_overlay_window(db_session, "TEST1", current_otp=80.0, prior_otp=70.0)

        deltas = compute_route_deltas(db_session, "TEST1")
        otp = deltas["otp"]
        assert otp["valid"] is True
        # 80 on_time out of 100 total = 80.0%; 70 on_time = 70.0%
        assert abs(otp["value"] - 10.0) < 0.01
        assert otp["current_n"] == 7
        assert otp["prior_n"] == 7

    def test_deltas_thin_data_suppresses_when_under_three_valid_days(
        self, db_session, sample_route, monkeypatch
    ):
        """Fewer than 3 valid days in either window → `valid=False`, `value=None`."""
        self._clear_cache()
        from api.aggregations import compute_route_deltas
        from src.models import RouteMetricsDailyOverlay

        # Only 2 days of overlay data in the current window, none in prior.
        rows = []
        for i in range(2):
            d = eastern_today() - timedelta(days=i)
            rows.append(
                RouteMetricsDailyOverlay(
                    route_id="TEST1",
                    service_date=d.isoformat(),
                    day_type="weekday",
                    otp_all_early=5,
                    otp_all_on_time=82,
                    otp_all_late=13,
                    otp_origin_early=0,
                    otp_origin_on_time=82,
                    otp_origin_late=18,
                    otp_destination_early=0,
                    otp_destination_on_time=82,
                    otp_destination_late=18,
                    scheduled_trips=10,
                    delivered_trips=9,
                )
            )
        db_session.add_all(rows)
        db_session.commit()
        self._stub_excess_trip_time(monkeypatch)

        deltas = compute_route_deltas(db_session, "TEST1")
        assert deltas["otp"]["valid"] is False
        assert deltas["otp"]["value"] is None
        assert deltas["otp"]["current_n"] == 2
        assert deltas["otp"]["prior_n"] == 0

    def test_deltas_ewt_coverage_floor_suppresses_low_observed_headways(
        self, db_session, sample_route, monkeypatch
    ):
        """EWT-specific coverage gate: insufficient observed headways suppresses.

        Three valid days of EWT but only a few observed headways per day in
        each window — below the EWT_MIN_OBS_HEADWAYS floor. The delta should
        suppress even though the day-count rule passes.
        """
        self._clear_cache()
        from api.aggregations import (
            DELTA_MIN_VALID_DAYS,
            EWT_MIN_OBS_HEADWAYS,
            compute_route_deltas,
        )
        from src.models import RouteMetricsDailyOverlay

        # Seed enough days (>= DELTA_MIN_VALID_DAYS) but sparse EWT headways.
        rows = []
        # Use a sum_h / sum_h_sq pair that yields a non-null EWT per day
        # but with n_observed far below EWT_MIN_OBS_HEADWAYS per window.
        obs_per_day = max(1, (EWT_MIN_OBS_HEADWAYS // DELTA_MIN_VALID_DAYS) - 1)
        sum_h = 300.0 * obs_per_day  # representative observed headway sum
        sum_h_sq = sum_h**2 / obs_per_day  # AWT = sum_h / (2n) ~ 150s
        sched_sum_h = sum_h * 0.9
        sched_sum_h_sq = sched_sum_h**2 / obs_per_day
        for window_offset in (0, 7):
            for i in range(DELTA_MIN_VALID_DAYS):
                d = eastern_today() - timedelta(days=window_offset + i)
                rows.append(
                    RouteMetricsDailyOverlay(
                        route_id="TEST1",
                        service_date=d.isoformat(),
                        day_type="weekday",
                        otp_all_early=0,
                        otp_all_on_time=80,
                        otp_all_late=20,
                        otp_origin_early=0,
                        otp_origin_on_time=80,
                        otp_origin_late=20,
                        otp_destination_early=0,
                        otp_destination_on_time=80,
                        otp_destination_late=20,
                        scheduled_trips=10,
                        delivered_trips=9,
                        ewt_obs_sum_h=sum_h,
                        ewt_obs_sum_h_sq=sum_h_sq,
                        ewt_n_observed_headways=obs_per_day,
                        ewt_sched_sum_h=sched_sum_h,
                        ewt_sched_sum_h_sq=sched_sum_h_sq,
                        ewt_n_scheduled_headways=obs_per_day,
                    )
                )
        db_session.add_all(rows)
        db_session.commit()
        self._stub_excess_trip_time(monkeypatch)

        deltas = compute_route_deltas(db_session, "TEST1")
        # Pooled observed headways per window < EWT_MIN_OBS_HEADWAYS → suppressed.
        assert deltas["ewt"]["valid"] is False
        assert deltas["ewt"]["value"] is None
        # Day counts are still surfaced for tooltip/debugging.
        assert deltas["ewt"]["current_n"] == DELTA_MIN_VALID_DAYS
        assert deltas["ewt"]["prior_n"] == DELTA_MIN_VALID_DAYS

    def test_deltas_unknown_route_returns_suppressed_block(self, db_session, monkeypatch):
        """A route absent from every source returns a fully-suppressed shape."""
        self._clear_cache()
        from api.aggregations import compute_route_deltas

        self._stub_excess_trip_time(monkeypatch)
        deltas = compute_route_deltas(db_session, "DOES_NOT_EXIST")
        for metric_block in deltas.values():
            assert metric_block["valid"] is False
            assert metric_block["value"] is None
            assert metric_block["current_n"] == 0
            assert metric_block["prior_n"] == 0

    def test_scorecard_payload_includes_deltas_block(self, db_session, sample_route, monkeypatch):
        """`get_all_routes_scorecard` carries a `deltas` block per route."""
        self._clear_cache()
        from api.aggregations import get_all_routes_scorecard

        self._stub_excess_trip_time(monkeypatch)
        result = get_all_routes_scorecard(db_session, days=7)

        assert "routes" in result
        assert len(result["routes"]) == 1
        deltas = result["routes"][0]["deltas"]
        assert set(deltas.keys()) == {
            "otp",
            "service_delivered",
            "ewt",
            "bunching",
            "excess_trip_time_pct",
        }

    def test_route_detail_payload_includes_deltas_block(
        self, db_session, sample_route, monkeypatch
    ):
        """`get_route_detail_metrics` carries a `deltas` block."""
        self._clear_cache()
        from api.aggregations import get_route_detail_metrics

        self._stub_excess_trip_time(monkeypatch)
        result = get_route_detail_metrics(db_session, "TEST1", days=7)

        assert "deltas" in result
        deltas = result["deltas"]
        assert set(deltas.keys()) == {
            "otp",
            "service_delivered",
            "ewt",
            "bunching",
            "excess_trip_time_pct",
        }
        # No overlay data seeded → all metrics suppressed.
        for metric_block in deltas.values():
            assert metric_block["valid"] is False
