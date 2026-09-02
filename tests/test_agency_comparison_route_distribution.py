"""
Tests for the route-level OTP / service_delivered distribution block
(NOTES-141) added to `get_agency_comparison_data`'s payload.

The headline agency-comparison figure is one window-mean per metric per
agency, which hides the spread -- two agencies with identical mean OTP can
have very different shares of bad routes. This item adds a per-agency,
per-metric `route_distribution` block (median/IQR/histogram/threshold-share)
computed from each agency's own route-level window means, over the SAME
matched window the headline already uses.

Following `tests/test_agency_comparison.py`'s pattern: `get_agency_comparison_data`
takes a dict of already-open sessions (one physical DB per agency), so the
full-envelope tests below build independent in-memory SQLite sessions rather
than using the shared `db_session` fixture. The pure-function and
single-agency tests use `db_session` (SQLite, from conftest) since they only
need one session.
"""

from datetime import timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api.aggregations import (
    AGENCY_COMPARISON_WINDOW_START,
    ROUTE_DISTRIBUTION_METRICS,
    _empty_route_distribution,
    _percentile,
    _route_distribution_bucket_index,
    _route_distribution_for_agency,
    _route_distribution_to_pct,
    _summarize_route_distribution,
    get_agency_comparison_data,
)
from src.models import Base, Route, StopEvent, SystemMetricsDaily
from src.timezones import eastern_today


def _make_session():
    """Build a fresh in-memory SQLite session, mirroring test_agency_comparison.py."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def _seed_route(session, route_id, route_type=3):
    """Insert one current Route row."""
    session.add(
        Route(
            route_id=route_id,
            route_short_name=route_id,
            route_long_name=f"Test {route_id}",
            route_type=route_type,
            is_current=True,
        )
    )


def _seed_route_otp(session, route_id, otp_value, service_date):
    """Seed one day of proximity stop_events so `_route_otp_window_mean` reads `otp_value`%.

    Mirrors `TestGetRouteContributors._seed_route_otp` in test_aggregations.py:
    a uniform on-time/late split over 100 events on one date so the window
    mean equals the seed value exactly.
    """
    from datetime import datetime as _dt

    events_per_day = 100
    on_time_count = int(round(otp_value / 100.0 * events_per_day))
    late_count = events_per_day - on_time_count
    base_ts = _dt.combine(service_date, _dt.min.time()).replace(hour=14)
    rows = []
    for j in range(on_time_count):
        rows.append(
            StopEvent(
                service_date=service_date.isoformat(),
                trip_id=f"TRIP_{route_id}_OT_{j}",
                route_id=route_id,
                direction_id=0,
                stop_id=f"STOP_{route_id}",
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
                service_date=service_date.isoformat(),
                trip_id=f"TRIP_{route_id}_LATE_{j}",
                route_id=route_id,
                direction_id=0,
                stop_id=f"STOP_{route_id}",
                stop_sequence=1,
                observed_arrival_ts=base_ts + timedelta(seconds=on_time_count + j),
                deviation_sec=600,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )
    session.add_all(rows)


class TestPercentile:
    """Pure-function tests for `_percentile`'s linear interpolation."""

    def test_single_value_returns_itself_regardless_of_pct(self):
        assert _percentile([42.0], 25) == 42.0
        assert _percentile([42.0], 75) == 42.0

    def test_median_of_four_values(self):
        """[50,60,70,80] -> p50 interpolates between index 1 and 2: 65.0."""
        assert _percentile([50.0, 60.0, 70.0, 80.0], 50) == pytest.approx(65.0)

    def test_p25_and_p75_of_four_values(self):
        values = [50.0, 60.0, 70.0, 80.0]
        assert _percentile(values, 25) == pytest.approx(57.5)
        assert _percentile(values, 75) == pytest.approx(72.5)

    def test_odd_length_median_lands_exactly_on_middle_value(self):
        assert _percentile([10.0, 20.0, 30.0], 50) == pytest.approx(20.0)


class TestRouteDistributionBucketing:
    """Pure-function tests for the shared percentage-scale histogram buckets."""

    def test_bucket_edges_are_right_open_except_last(self):
        assert _route_distribution_bucket_index(59.9) == 0
        assert _route_distribution_bucket_index(60.0) == 1
        assert _route_distribution_bucket_index(69.9) == 1
        assert _route_distribution_bucket_index(70.0) == 2
        assert _route_distribution_bucket_index(89.9) == 3
        assert _route_distribution_bucket_index(90.0) == 4
        assert _route_distribution_bucket_index(100.0) == 4

    def test_over_100_clamps_into_top_bucket(self):
        """A bad upstream read above 100 doesn't raise -- it lands in the top bucket."""
        assert _route_distribution_bucket_index(150.0) == 4

    def test_service_delivered_ratio_rescales_to_percentage_axis(self):
        """service_delivered's 0-1 ratio maps onto the same 0-100 axis as OTP."""
        assert _route_distribution_to_pct("service_delivered", 0.8) == pytest.approx(80.0)
        assert _route_distribution_to_pct("otp", 80.0) == pytest.approx(80.0)


class TestSummarizeRouteDistribution:
    """`_summarize_route_distribution` -- the median/IQR/histogram/share summary."""

    def test_empty_route_values_returns_zeroed_envelope(self):
        result = _summarize_route_distribution({}, "otp", 75.0)
        assert result["route_count"] == 0
        assert result["median"] is None
        assert result["p25"] is None
        assert result["p75"] is None
        assert result["share_at_or_above_threshold"] is None
        assert result["threshold"] == 75.0
        assert sum(bucket["count"] for bucket in result["histogram"]) == 0

    def test_none_values_excluded_from_every_statistic(self):
        """Routes with no qualifying data (None) don't count toward route_count."""
        result = _summarize_route_distribution({"A": 80.0, "B": None, "C": 60.0}, "otp", 75.0)
        assert result["route_count"] == 2
        assert result["median"] == pytest.approx(70.0)

    def test_four_route_otp_distribution_matches_hand_computed_stats(self):
        """[50,60,70,80] OTP -> median 65, p25 57.5, p75 72.5, histogram
        one route per bucket except the top, and only 80 clears a 75 threshold.
        """
        route_values = {"A": 50.0, "B": 60.0, "C": 70.0, "D": 80.0}
        result = _summarize_route_distribution(route_values, "otp", 75.0)

        assert result["route_count"] == 4
        assert result["median"] == pytest.approx(65.0)
        assert result["p25"] == pytest.approx(57.5)
        assert result["p75"] == pytest.approx(72.5)
        assert result["threshold"] == 75.0
        assert result["share_at_or_above_threshold"] == pytest.approx(0.25)

        counts_by_label = {b["label"]: b["count"] for b in result["histogram"]}
        assert counts_by_label == {
            "<60": 1,
            "60-70": 1,
            "70-80": 1,
            "80-90": 1,
            "90+": 0,
        }

    def test_service_delivered_histogram_uses_rescaled_axis(self):
        """A 0.5/0.65/0.95 service_delivered spread buckets on the 0-100 axis."""
        result = _summarize_route_distribution(
            {"A": 0.50, "B": 0.65, "C": 0.95}, "service_delivered", 0.95
        )
        counts_by_label = {b["label"]: b["count"] for b in result["histogram"]}
        assert counts_by_label["<60"] == 1
        assert counts_by_label["60-70"] == 1
        assert counts_by_label["90+"] == 1
        assert result["share_at_or_above_threshold"] == pytest.approx(1 / 3)

    def test_threshold_none_yields_null_share(self):
        """No configured target -> share_at_or_above_threshold stays None,
        not a divide-by-zero or a bogus 0/1.
        """
        result = _summarize_route_distribution({"A": 80.0}, "otp", None)
        assert result["threshold"] is None
        assert result["share_at_or_above_threshold"] is None


class TestRouteDistributionForAgency:
    """`_route_distribution_for_agency` -- the per-agency query/aggregation wiring."""

    def test_otp_distribution_from_seeded_stop_events(self, db_session, monkeypatch):
        """Four routes seeded with known OTP window means reproduce the
        hand-computed stats from TestSummarizeRouteDistribution, proving the
        DB-backed path (`_route_otp_window_mean`) feeds the same summarizer.

        service_delivered is monkeypatched to a no-op window-metrics reader
        so this test isolates the OTP path without needing a full GTFS +
        Runs fixture (out of scope here -- covered by the smaller directly
        monkeypatched test below).
        """
        d = eastern_today() - timedelta(days=1)
        for route_id, otp_value in [("A", 50.0), ("B", 60.0), ("C", 70.0), ("D", 80.0)]:
            _seed_route(db_session, route_id)
            _seed_route_otp(db_session, route_id, otp_value, d)
        db_session.commit()

        monkeypatch.setattr(
            "api.aggregations.get_live_metrics_for_window", lambda db, end_date, days: {}
        )

        result = _route_distribution_for_agency(db_session, d, days=1)

        assert result["otp"]["route_count"] == 4
        assert result["otp"]["median"] == pytest.approx(65.0)
        assert result["otp"]["p25"] == pytest.approx(57.5)
        assert result["otp"]["p75"] == pytest.approx(72.5)
        # service_delivered got nothing from the monkeypatched empty window.
        assert result["service_delivered"]["route_count"] == 0

    def test_service_delivered_distribution_from_live_metrics_window(self, db_session, monkeypatch):
        """service_delivered reads through `get_live_metrics_for_window` +
        `_live_metric_fields`; monkeypatch that call directly to prove the
        wiring without seeding a full GTFS/Runs fixture.
        """
        _seed_route(db_session, "A")
        _seed_route(db_session, "B")
        db_session.commit()

        monkeypatch.setattr(
            "api.aggregations.get_live_metrics_for_window",
            lambda db, end_date, days: {
                "A": {"service_delivered": {"ratio": 0.90}},
                "B": {"service_delivered": {"ratio": 0.80}},
            },
        )

        result = _route_distribution_for_agency(db_session, eastern_today(), days=7)

        assert result["service_delivered"]["route_count"] == 2
        assert result["service_delivered"]["median"] == pytest.approx(0.85)

    def test_thresholds_come_from_configured_system_targets(
        self, db_session, monkeypatch, tmp_path
    ):
        """The threshold is `config/route_targets.yaml`'s system_default,
        read via `src.route_targets.get_system_targets` -- not hard-coded
        in this module. Point the loader at an isolated YAML with known
        values so the assertion doesn't depend on the checked-in file.
        """
        from src import route_targets as rt

        cfg = tmp_path / "targets.yaml"
        cfg.write_text(
            "system_default:\n"
            "  otp: 82.5\n"
            "  service_delivered: 0.91\n"
            "  ewt_minutes: 3.0\n"
            "  bunching_pct: 0.04\n"
            "routes: {}\n"
        )
        monkeypatch.setenv("WMATA_ROUTE_TARGETS_PATH", str(cfg))
        rt.reset_cache_for_tests()
        monkeypatch.setattr(
            "api.aggregations.get_live_metrics_for_window", lambda db, end_date, days: {}
        )

        result = _route_distribution_for_agency(db_session, eastern_today(), days=1)

        assert result["otp"]["threshold"] == pytest.approx(82.5)
        assert result["service_delivered"]["threshold"] == pytest.approx(0.91)
        rt.reset_cache_for_tests()


class TestEmptyRouteDistribution:
    """`_empty_route_distribution` -- the no-shared-anchor degrade path."""

    def test_carries_real_thresholds_with_zero_routes(self):
        result = _empty_route_distribution()
        assert set(result) == set(ROUTE_DISTRIBUTION_METRICS)
        for metric in ROUTE_DISTRIBUTION_METRICS:
            assert result[metric]["route_count"] == 0
            assert result[metric]["median"] is None
            assert result[metric]["threshold"] is not None


class TestAgencyComparisonRouteDistributionEnvelope:
    """`get_agency_comparison_data`'s `route_distribution` key -- full envelope wiring."""

    def test_route_distribution_present_and_matches_seeded_route(self):
        """One agency, one system_metrics_daily row (sets the anchor), one
        route with a known OTP window mean -> route_distribution reflects it.
        """
        wmata_db = _make_session()
        wmata_db.add(
            SystemMetricsDaily(
                service_date=AGENCY_COMPARISON_WINDOW_START.isoformat(),
                otp_percentage=70.0,
                data_quality="complete",
            )
        )
        _seed_route(wmata_db, "TEST1")
        _seed_route_otp(wmata_db, "TEST1", 80.0, AGENCY_COMPARISON_WINDOW_START)
        wmata_db.commit()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db})
        finally:
            wmata_db.close()

        agency = result["agencies"][0]
        assert "route_distribution" in agency
        otp_dist = agency["route_distribution"]["otp"]
        assert otp_dist["route_count"] == 1
        assert otp_dist["median"] == pytest.approx(80.0)
        assert otp_dist["p25"] == pytest.approx(80.0)
        assert otp_dist["p75"] == pytest.approx(80.0)
        # 80% clears the checked-in system_default OTP target (75%).
        assert otp_dist["share_at_or_above_threshold"] == pytest.approx(1.0)

        sd_dist = agency["route_distribution"]["service_delivered"]
        assert sd_dist["route_count"] == 0  # no service_delivered data seeded
        assert sd_dist["threshold"] is not None

    def test_no_shared_anchor_degrades_to_empty_distribution(self):
        """No agency has any data in the window -> route_distribution is the
        null block (real thresholds, zero routes), not a crash.
        """
        wmata_db = _make_session()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db})
        finally:
            wmata_db.close()

        assert result["window_end"] is None
        dist = result["agencies"][0]["route_distribution"]
        for metric in ROUTE_DISTRIBUTION_METRICS:
            assert dist[metric]["route_count"] == 0

    def test_payload_is_additive_existing_fields_unchanged(self):
        """CompareStrip/Overview read `window_start`, `window_end`,
        `agencies[].agency`, `.display_name`, `.metrics.<metric>.window_mean`
        /`.wow_delta`, and `.service_level` -- none of that shape changes,
        `route_distribution` only adds a new sibling key. Reproduces the
        pre-existing `test_window_mean_skips_nulls_and_excludes_before_window_start`
        scenario from test_agency_comparison.py to prove the old fields are
        untouched by this change.
        """
        wmata_db = _make_session()
        wmata_db.add(
            SystemMetricsDaily(
                service_date=(AGENCY_COMPARISON_WINDOW_START - timedelta(days=1)).isoformat(),
                otp_percentage=10.0,
                data_quality="complete",
            )
        )
        wmata_db.add(
            SystemMetricsDaily(
                service_date=AGENCY_COMPARISON_WINDOW_START.isoformat(),
                otp_percentage=60.0,
                data_quality="complete",
            )
        )
        wmata_db.add(
            SystemMetricsDaily(
                service_date=(AGENCY_COMPARISON_WINDOW_START + timedelta(days=1)).isoformat(),
                otp_percentage=None,
                data_quality="complete",
            )
        )
        wmata_db.commit()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db})
        finally:
            wmata_db.close()

        assert result["window_start"] == AGENCY_COMPARISON_WINDOW_START.isoformat()
        agency = result["agencies"][0]
        assert agency["agency"] == "wmata"
        assert agency["display_name"] == "WMATA Metrobus"
        otp = agency["metrics"]["otp"]
        assert otp["window_mean"] == 60.0
        assert otp["days_included"] == 1
        assert set(agency["service_level"]) == {
            "median_headway_seconds",
            "pct_at_most_15min",
            "n_headways",
        }
        # The new key is present alongside, not instead of, the old ones.
        assert "route_distribution" in agency
        assert set(agency) == {
            "agency",
            "display_name",
            "metrics",
            "service_level",
            "route_distribution",
        }
