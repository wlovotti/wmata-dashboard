"""
Unit tests for API aggregation functions

Tests the business logic in api/aggregations.py including:
- Utility functions (sanitize_float, calculate_performance_grade)
- Route scorecard generation
- Route detail metrics
- Trend data generation

Run with: pytest tests/test_aggregations.py
"""

import math
from datetime import datetime, timedelta

import pytest

from api.aggregations import (
    calculate_performance_grade,
    get_all_routes_scorecard,
    get_route_detail_metrics,
    get_route_trend_data,
    sanitize_float,
)


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
            computed_at=datetime.utcnow(),
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
                computed_at=datetime.utcnow(),
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
        assert result["total_arrivals_analyzed"] == 45
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
        assert result["total_arrivals_analyzed"] == 0
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
        """Test trend data for OTP metric"""
        result = get_route_trend_data(db_session, "TEST1", metric="otp", days=30)

        assert result["route_id"] == "TEST1"
        assert result["metric"] == "otp"
        assert result["days"] == 30
        assert "trend_data" in result
        assert len(result["trend_data"]) > 0

        # Verify structure of trend data
        trend_point = result["trend_data"][0]
        assert "date" in trend_point
        assert "otp_percentage" in trend_point

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
        """Test trend data when no daily metrics exist"""
        result = get_route_trend_data(db_session, "TEST1", metric="otp", days=30)

        assert result["route_id"] == "TEST1"
        assert result["trend_data"] == []

    def test_trend_data_custom_days(self, db_session, sample_route, sample_route_metrics_daily):
        """Test trend data with custom days parameter"""
        result = get_route_trend_data(db_session, "TEST1", metric="otp", days=60)

        assert result["days"] == 60


class TestGradeConsistency:
    """Tests for grade calculation consistency"""

    def test_grade_matches_scorecard(
        self, db_session, sample_route, sample_route_metrics_summary
    ):
        """Test that grade is consistent between scorecard and detail views"""
        scorecard = get_all_routes_scorecard(db_session, days=7)
        detail = get_route_detail_metrics(db_session, "TEST1", days=7)

        scorecard_grade = scorecard[0]["grade"]
        detail_grade = detail["grade"]

        assert scorecard_grade == detail_grade == "B"  # 75.5% OTP = B grade
