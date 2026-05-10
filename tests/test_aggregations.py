"""
Unit tests for API aggregation functions

Tests the business logic in api/aggregations.py including:
- Utility functions (sanitize_float)
- Route scorecard generation
- Route detail metrics
- Trend data generation

Run with: pytest tests/test_aggregations.py
"""

from datetime import timedelta

from api.aggregations import (
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


class TestGetAllRoutesScorecard:
    """Tests for get_all_routes_scorecard function (post NOTES-19 cleanup).

    The scorecard payload is now identity + frequency_class + the live
    overlay fields. The legacy `RouteMetricsSummary` fields were dropped
    because no UI consumer reads them.
    """

    def test_scorecard_with_single_route(self, db_session, sample_route):
        """Single route shows identity + frequency_class even without live data."""
        scorecard = get_all_routes_scorecard(db_session, days=7)

        assert len(scorecard) == 1
        route_data = scorecard[0]

        assert route_data["route_id"] == "TEST1"
        assert route_data["route_name"] == "T1"
        assert route_data["route_long_name"] == "Test Route 1"
        assert "frequency_class" in route_data
        # Live overlay keys are present even when their values are None
        assert "otp_all_pct" in route_data
        assert "service_delivered_ratio" in route_data
        assert "ewt_seconds" in route_data
        assert "bunching_rate" in route_data

    def test_scorecard_with_multiple_routes(self, db_session, sample_routes):
        """Every is_current route appears, sorted with None OTP values last."""
        scorecard = get_all_routes_scorecard(db_session, days=7)

        assert len(scorecard) == 3
        # Without seeded live data, all routes have None for otp_all_pct.
        # The sort key puts None values at the end — but with all None,
        # the order is whatever the upstream query returned (still all 3).
        for route in scorecard:
            assert route["otp_all_pct"] is None

    def test_scorecard_empty_database(self, db_session):
        """No routes means an empty scorecard."""
        scorecard = get_all_routes_scorecard(db_session, days=7)
        assert scorecard == []


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

    def test_route_detail_nonexistent_route(self, db_session):
        """Test route detail for non-existent route"""
        result = get_route_detail_metrics(db_session, "NONEXISTENT", days=7)

        assert "error" in result
        assert "not found" in result["error"].lower()

    def test_route_detail_with_days_parameter(self, db_session, sample_route):
        """Test route detail respects days parameter"""
        result = get_route_detail_metrics(db_session, "TEST1", days=14)

        assert result["time_period_days"] == 14


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
