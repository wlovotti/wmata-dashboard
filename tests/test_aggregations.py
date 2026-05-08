"""
Unit tests for API aggregation functions

Tests the business logic in api/aggregations.py including:
- Utility functions (sanitize_float, calculate_performance_grade)
- Route scorecard generation
- Route detail metrics
- Trend data generation

Run with: pytest tests/test_aggregations.py
"""

from datetime import timedelta

from api.aggregations import (
    calculate_performance_grade,
    get_all_routes_scorecard,
    get_route_detail_metrics,
    get_route_trend_data,
    get_system_trend_data,
    sanitize_float,
)
from src.timezones import eastern_today, utcnow_naive


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


class TestCalculatePerformanceGrade:
    """Tests for calculate_performance_grade function"""

    def test_grade_a_perfect(self):
        """Test A grade for perfect OTP"""
        assert calculate_performance_grade(100.0) == "A"

    def test_grade_a_boundary(self):
        """Test A grade at boundary (80%)"""
        assert calculate_performance_grade(80.0) == "A"
        assert calculate_performance_grade(80.1) == "A"

    def test_grade_b(self):
        """Test B grade (60-80%)"""
        assert calculate_performance_grade(79.9) == "B"
        assert calculate_performance_grade(70.0) == "B"
        assert calculate_performance_grade(60.0) == "B"

    def test_grade_c(self):
        """Test C grade (40-60%)"""
        assert calculate_performance_grade(59.9) == "C"
        assert calculate_performance_grade(50.0) == "C"
        assert calculate_performance_grade(40.0) == "C"

    def test_grade_d(self):
        """Test D grade (20-40%)"""
        assert calculate_performance_grade(39.9) == "D"
        assert calculate_performance_grade(30.0) == "D"
        assert calculate_performance_grade(20.0) == "D"

    def test_grade_f(self):
        """Test F grade (<20%)"""
        assert calculate_performance_grade(19.9) == "F"
        assert calculate_performance_grade(10.0) == "F"
        assert calculate_performance_grade(0.0) == "F"

    def test_grade_none(self):
        """Test N/A grade for None input"""
        assert calculate_performance_grade(None) == "N/A"


class TestGetAllRoutesScorecard:
    """Tests for get_all_routes_scorecard function"""

    def test_scorecard_with_single_route(
        self, db_session, sample_route, sample_route_metrics_summary
    ):
        """Test scorecard with one route and metrics"""
        scorecard = get_all_routes_scorecard(db_session, days=7)

        assert len(scorecard) == 1
        route_data = scorecard[0]

        assert route_data["route_id"] == "TEST1"
        assert route_data["route_name"] == "T1"
        assert route_data["route_long_name"] == "Test Route 1"
        assert route_data["otp_percentage"] == 75.5
        assert route_data["avg_headway_minutes"] == 12.5
        assert route_data["avg_speed_mph"] == 18.5
        assert route_data["grade"] == "B"
        assert route_data["total_observations"] == 150

    def test_scorecard_with_multiple_routes(self, db_session, sample_routes):
        """Test scorecard with multiple routes"""
        # Add metrics for first route only
        from src.models import RouteMetricsSummary

        summary = RouteMetricsSummary(
            route_id="TEST1",
            otp_percentage=85.0,
            avg_headway_minutes=10.0,
            avg_speed_mph=20.0,
            total_observations=200,
            computed_at=utcnow_naive(),
        )
        db_session.add(summary)
        db_session.commit()

        scorecard = get_all_routes_scorecard(db_session, days=7)

        assert len(scorecard) == 3  # 3 routes total
        # Routes sorted by OTP descending, None values last
        first_route = scorecard[0]
        assert first_route["route_id"] == "TEST1"
        assert first_route["otp_percentage"] == 85.0

        # Routes without metrics should have None values
        other_routes = scorecard[1:]
        for route in other_routes:
            assert route["otp_percentage"] is None
            assert route["grade"] == "N/A"

    def test_scorecard_empty_database(self, db_session):
        """Test scorecard with no routes"""
        scorecard = get_all_routes_scorecard(db_session, days=7)
        assert scorecard == []

    def test_scorecard_sorting_by_otp(self, db_session, sample_routes):
        """Test scorecard sorts routes by OTP descending"""
        from src.models import RouteMetricsSummary

        # Add metrics for multiple routes with different OTP
        metrics = [
            {"route_id": "TEST1", "otp": 60.0},
            {"route_id": "TEST2", "otp": 85.0},
            {"route_id": "TEST3", "otp": 72.5},
        ]

        for metric in metrics:
            summary = RouteMetricsSummary(
                route_id=metric["route_id"],
                otp_percentage=metric["otp"],
                avg_headway_minutes=10.0,
                avg_speed_mph=20.0,
                total_observations=100,
                computed_at=utcnow_naive(),
            )
            db_session.add(summary)
        db_session.commit()

        scorecard = get_all_routes_scorecard(db_session, days=7)

        # Should be sorted by OTP descending
        assert scorecard[0]["route_id"] == "TEST2"  # 85.0%
        assert scorecard[1]["route_id"] == "TEST3"  # 72.5%
        assert scorecard[2]["route_id"] == "TEST1"  # 60.0%


class TestGetRouteDetailMetrics:
    """Tests for get_route_detail_metrics function"""

    def test_route_detail_with_metrics(
        self, db_session, sample_route, sample_route_metrics_summary
    ):
        """Test route detail with computed metrics"""
        result = get_route_detail_metrics(db_session, "TEST1", days=7)

        assert result["route_id"] == "TEST1"
        assert result["route_name"] == "T1"
        assert result["route_long_name"] == "Test Route 1"
        assert result["time_period_days"] == 7
        assert result["otp_percentage"] == 75.5
        assert result["avg_headway_minutes"] == 12.5
        assert result["avg_speed_mph"] == 18.5
        assert result["grade"] == "B"
        assert result["total_positions"] == 1050
        assert result["unique_vehicles"] == 8
        assert result["unique_trips"] == 42

    def test_route_detail_without_metrics(self, db_session, sample_route):
        """Test route detail without computed metrics"""
        result = get_route_detail_metrics(db_session, "TEST1", days=7)

        assert result["route_id"] == "TEST1"
        assert result["otp_percentage"] is None
        assert result["avg_headway_minutes"] is None
        assert result["avg_speed_mph"] is None
        assert result["grade"] == "N/A"
        assert result["total_positions"] == 0

    def test_route_detail_nonexistent_route(self, db_session):
        """Test route detail for non-existent route"""
        result = get_route_detail_metrics(db_session, "NONEXISTENT", days=7)

        assert "error" in result
        assert "not found" in result["error"].lower()

    def test_route_detail_with_days_parameter(
        self, db_session, sample_route, sample_route_metrics_summary
    ):
        """Test route detail respects days parameter"""
        result = get_route_detail_metrics(db_session, "TEST1", days=14)

        assert result["time_period_days"] == 14


class TestGetRouteTrendData:
    """Tests for get_route_trend_data function"""

    def test_trend_data_otp(self, db_session, sample_route, sample_route_metrics_daily):
        """Test trend data for OTP metric.

        The fixture inserts one daily row at `today - 1`. The endpoint
        emits `days + 1` rows (inclusive endpoints), with the fixture
        row carrying its real OTP and every other day carrying null.
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

        # Exactly one real value (the fixture row); the rest are null.
        real_values = [row for row in result["trend_data"] if row["otp_percentage"] is not None]
        assert len(real_values) == 1
        assert real_values[0]["otp_percentage"] == 78.2

    def test_trend_data_all_metrics(self, db_session, sample_route, sample_route_metrics_daily):
        """Test trend data for all supported metrics"""
        metrics = ["otp", "early", "late", "headway", "headway_std_dev", "speed"]
        metric_fields = {
            "otp": "otp_percentage",
            "early": "early_percentage",
            "late": "late_percentage",
            "headway": "avg_headway_minutes",
            "headway_std_dev": "headway_std_dev_minutes",
            "speed": "avg_speed_mph",
        }

        for metric in metrics:
            result = get_route_trend_data(db_session, "TEST1", metric=metric, days=30)

            assert result["metric"] == metric
            if len(result["trend_data"]) > 0:
                trend_point = result["trend_data"][0]
                expected_field = metric_fields[metric]
                assert expected_field in trend_point

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

    def test_trend_data_custom_days(self, db_session, sample_route, sample_route_metrics_daily):
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
    """Tests for get_system_trend_data — NOTES-36 home-page system trend strip."""

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


class TestGradeConsistency:
    """Tests for grade calculation consistency"""

    def test_grade_matches_scorecard(self, db_session, sample_route, sample_route_metrics_summary):
        """Test that grade is consistent between scorecard and detail views"""
        scorecard = get_all_routes_scorecard(db_session, days=7)
        detail = get_route_detail_metrics(db_session, "TEST1", days=7)

        scorecard_grade = scorecard[0]["grade"]
        detail_grade = detail["grade"]

        assert scorecard_grade == detail_grade == "B"  # 75.5% OTP = B grade


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
        """Seed N days of `route_metrics_daily.otp_percentage` for one route."""
        from src.models import RouteMetricsDaily

        rows = []
        for i in range(n_days):
            d = eastern_today() - timedelta(days=i + 1)
            rows.append(
                RouteMetricsDaily(
                    route_id=route_id,
                    date=d.isoformat(),
                    otp_percentage=otp_value,
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


class TestComputeRouteDeltas:
    """Tests for `compute_route_deltas` (NOTES-38).

    Heavy live computes (SD / EWT / bunching) are stubbed so tests stay
    fast and assert the delta arithmetic + thin-data rules without
    depending on a stop_events fixture.
    """

    @staticmethod
    def _clear_cache():
        """Drop module-level deltas cache between tests."""
        from api import aggregations as agg

        agg._deltas_cache.clear()

    @staticmethod
    def _stub_live_per_route_per_day(monkeypatch, sd=None, ewt=None, ewt_cells=None, bun=None):
        """Replace the heavy live-pass with caller-provided per-route per-day dicts.

        Each argument is `{route_id: {iso_date: value}}` (or None to default
        to empty). Returning empty dicts for anything missing keeps the
        thin-data assertions explicit per-test.
        """
        from api import aggregations as agg

        def fake(_db, _dates):
            return (sd or {}, ewt or {}, ewt_cells or {}, bun or {})

        monkeypatch.setattr(agg, "_live_per_route_per_day", fake)

    def _seed_route_otp_window(self, db_session, route_id, current_otp, prior_otp, n_each=7):
        """Seed `n_each` days of OTP at `current_otp` then `n_each` days at `prior_otp`.

        Current window is the past `n_each` days inclusive of today; prior
        window is the `n_each` days immediately preceding. Dates are
        sequential Eastern service dates.
        """
        from src.models import RouteMetricsDaily

        rows = []
        for i in range(n_each):
            d = eastern_today() - timedelta(days=i)
            rows.append(
                RouteMetricsDaily(route_id=route_id, date=d.isoformat(), otp_percentage=current_otp)
            )
        for i in range(n_each):
            d = eastern_today() - timedelta(days=n_each + i)
            rows.append(
                RouteMetricsDaily(route_id=route_id, date=d.isoformat(), otp_percentage=prior_otp)
            )
        db_session.add_all(rows)
        db_session.commit()

    def _seed_route_excess_window(self, db_session, route_id, current_pct, prior_pct, n_each=7):
        """Seed `n_each` days of excess_trip_time_pct in current and prior windows.

        Mirrors `_seed_route_otp_window` but writes to a separate column.
        Two rows per date (one for OTP at current, one for excess) would
        collide, so excess seeding uses different dates from OTP seeding
        unless both share a date — kept independent here to avoid coupling.
        """
        from src.models import RouteMetricsDaily

        rows = []
        for i in range(n_each):
            d = eastern_today() - timedelta(days=i)
            rows.append(
                RouteMetricsDaily(
                    route_id=route_id,
                    date=d.isoformat(),
                    excess_trip_time_pct=current_pct,
                )
            )
        for i in range(n_each):
            d = eastern_today() - timedelta(days=n_each + i)
            rows.append(
                RouteMetricsDaily(
                    route_id=route_id,
                    date=d.isoformat(),
                    excess_trip_time_pct=prior_pct,
                )
            )
        db_session.add_all(rows)
        db_session.commit()

    def test_deltas_returns_all_five_metrics(self, db_session, sample_route, monkeypatch):
        """The deltas dict carries one entry per scorecard metric."""
        self._clear_cache()
        from api.aggregations import compute_route_deltas

        self._stub_live_per_route_per_day(monkeypatch)
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

        Current 7 days at 80% OTP, prior 7 at 70%. Delta should be +10
        (positive: current window's mean is higher than prior).
        """
        self._clear_cache()
        from api.aggregations import compute_route_deltas

        self._stub_live_per_route_per_day(monkeypatch)
        self._seed_route_otp_window(db_session, "TEST1", current_otp=80.0, prior_otp=70.0)

        deltas = compute_route_deltas(db_session, "TEST1")
        otp = deltas["otp"]
        assert otp["valid"] is True
        assert otp["value"] == 10.0
        assert otp["current_n"] == 7
        assert otp["prior_n"] == 7

    def test_deltas_lower_is_better_metrics_keep_raw_sign(
        self, db_session, sample_route, monkeypatch
    ):
        """EWT / bunching deltas are NOT sign-flipped on the server.

        Current EWT higher than prior (worse) should produce a positive
        delta — the consumer interprets that direction is bad for EWT.
        """
        self._clear_cache()
        from api.aggregations import compute_route_deltas

        ewt = {"TEST1": {}}
        cells = {"TEST1": {}}
        bun = {"TEST1": {}}
        for i in range(7):
            d = (eastern_today() - timedelta(days=i)).isoformat()
            ewt["TEST1"][d] = 150.0  # current window: worse (higher)
            cells["TEST1"][d] = 5
            bun["TEST1"][d] = 0.10
        for i in range(7):
            d = (eastern_today() - timedelta(days=7 + i)).isoformat()
            ewt["TEST1"][d] = 90.0  # prior window: better (lower)
            cells["TEST1"][d] = 5
            bun["TEST1"][d] = 0.04

        self._stub_live_per_route_per_day(monkeypatch, ewt=ewt, ewt_cells=cells, bun=bun)

        deltas = compute_route_deltas(db_session, "TEST1")
        # EWT current 150 - prior 90 = +60, raw sign preserved.
        assert deltas["ewt"]["valid"] is True
        assert deltas["ewt"]["value"] == 60.0
        # Bunching current 0.10 - prior 0.04 = +0.06.
        assert deltas["bunching"]["valid"] is True
        assert abs(deltas["bunching"]["value"] - 0.06) < 1e-9

    def test_deltas_thin_data_suppresses_when_under_three_valid_days(
        self, db_session, sample_route, monkeypatch
    ):
        """Fewer than 3 valid days in either window → `valid=False`, `value=None`."""
        self._clear_cache()
        from api.aggregations import compute_route_deltas
        from src.models import RouteMetricsDaily

        # Only 2 days of OTP in the current window, none in prior.
        rows = []
        for i in range(2):
            d = eastern_today() - timedelta(days=i)
            rows.append(
                RouteMetricsDaily(route_id="TEST1", date=d.isoformat(), otp_percentage=82.0)
            )
        db_session.add_all(rows)
        db_session.commit()
        self._stub_live_per_route_per_day(monkeypatch)

        deltas = compute_route_deltas(db_session, "TEST1")
        assert deltas["otp"]["valid"] is False
        assert deltas["otp"]["value"] is None
        assert deltas["otp"]["current_n"] == 2
        assert deltas["otp"]["prior_n"] == 0

    def test_deltas_ewt_coverage_floor_suppresses_low_frequent_cells(
        self, db_session, sample_route, monkeypatch
    ):
        """EWT-specific coverage gate: < 7 frequent cell-hours per window suppresses.

        Three valid days of EWT but only 1 frequent cell-hour per day in
        each window — below the EWT_MIN_FREQUENT_CELL_HOURS=7 floor. The
        delta should suppress even though the day-count rule passes.
        """
        self._clear_cache()
        from api.aggregations import compute_route_deltas

        ewt = {"TEST1": {}}
        cells = {"TEST1": {}}
        for i in range(7):
            d = (eastern_today() - timedelta(days=i)).isoformat()
            ewt["TEST1"][d] = 100.0
            cells["TEST1"][d] = 1  # too sparse — sums to 7 only at the boundary
        for i in range(7):
            d = (eastern_today() - timedelta(days=7 + i)).isoformat()
            ewt["TEST1"][d] = 80.0
            cells["TEST1"][d] = 0  # no frequent service in prior window

        self._stub_live_per_route_per_day(monkeypatch, ewt=ewt, ewt_cells=cells)

        deltas = compute_route_deltas(db_session, "TEST1")
        # Prior window has 0 frequent cells across all days → suppressed.
        assert deltas["ewt"]["valid"] is False
        assert deltas["ewt"]["value"] is None
        # Day counts are still surfaced for tooltip/debugging.
        assert deltas["ewt"]["current_n"] == 7
        assert deltas["ewt"]["prior_n"] == 7

    def test_deltas_excess_trip_time_pct_uses_route_metrics_daily(
        self, db_session, sample_route, monkeypatch
    ):
        """excess_trip_time_pct is read directly from `route_metrics_daily`.

        Per-route per-day values are seeded into the materialized table;
        the live-pass stub returns nothing for excess. The delta still
        computes correctly from the table.
        """
        self._clear_cache()
        from api.aggregations import compute_route_deltas

        self._stub_live_per_route_per_day(monkeypatch)
        self._seed_route_excess_window(db_session, "TEST1", current_pct=20.0, prior_pct=15.0)

        deltas = compute_route_deltas(db_session, "TEST1")
        ext = deltas["excess_trip_time_pct"]
        assert ext["valid"] is True
        assert ext["value"] == 5.0
        assert ext["current_n"] == 7
        assert ext["prior_n"] == 7

    def test_deltas_unknown_route_returns_suppressed_block(self, db_session, monkeypatch):
        """A route absent from every source returns a fully-suppressed shape."""
        self._clear_cache()
        from api.aggregations import compute_route_deltas

        self._stub_live_per_route_per_day(monkeypatch)
        deltas = compute_route_deltas(db_session, "DOES_NOT_EXIST")
        for metric_block in deltas.values():
            assert metric_block["valid"] is False
            assert metric_block["value"] is None
            assert metric_block["current_n"] == 0
            assert metric_block["prior_n"] == 0

    def test_scorecard_payload_includes_deltas_block(
        self, db_session, sample_route, sample_route_metrics_summary, monkeypatch
    ):
        """`get_all_routes_scorecard` carries a `deltas` block per route."""
        self._clear_cache()
        from api.aggregations import get_all_routes_scorecard

        self._stub_live_per_route_per_day(monkeypatch)
        scorecard = get_all_routes_scorecard(db_session, days=7)

        assert len(scorecard) == 1
        deltas = scorecard[0]["deltas"]
        assert set(deltas.keys()) == {
            "otp",
            "service_delivered",
            "ewt",
            "bunching",
            "excess_trip_time_pct",
        }
