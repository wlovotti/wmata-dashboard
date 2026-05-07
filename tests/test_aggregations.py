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
