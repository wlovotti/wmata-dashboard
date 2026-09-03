"""
Tests for the route-level OTP / service_delivered distribution block
(NOTES-141) added to `get_agency_comparison_data`'s payload.

The headline agency-comparison figure is one window-mean per metric per
agency, which hides the spread -- two agencies with identical mean OTP can
have very different shares of bad routes. This item adds a per-agency,
per-metric `route_distribution` block (median/IQR/histogram/threshold-share)
computed from each agency's own route-level window means, over the SAME
matched window AND the SAME day-set the headline already uses -- including
`data_quality='partial'` days (review finding 1: SFMTA's entire matched
window is partial-flagged by design, NOTES-104, so excluding those days
here would zero SFMTA out completely).

Following `tests/test_agency_comparison.py`'s pattern: `get_agency_comparison_data`
takes a dict of already-open sessions (one physical DB per agency), so the
full-envelope tests below build independent in-memory SQLite sessions rather
than using the shared `db_session` fixture. The pure-function and
single-agency tests use `db_session` (SQLite, from conftest) since they only
need one session.
"""

from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from api.aggregations import (
    AGENCY_COMPARISON_METRICS,
    AGENCY_COMPARISON_WINDOW_START,
    ROUTE_DISTRIBUTION_METRICS,
    _bulk_route_otp_daily_percentages,
    _bulk_route_service_delivered_window,
    _compute_otp_per_day_with_filters,
    _empty_route_distribution,
    _get_route_distribution_cached,
    _mean_skip_null,
    _route_distribution_bucket_index,
    _route_distribution_for_agency,
    _route_distribution_to_pct,
    _summarize_route_distribution,
    get_agency_comparison_data,
)
from src.models import (
    Base,
    Calendar,
    Route,
    RouteMetricsDailyOverlay,
    Run,
    StopEvent,
    SystemMetricsDaily,
    Trip,
)
from src.time_periods import ALL_HOURS

_WEEKDAY_FIELDS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


@pytest.fixture(autouse=True)
def _reset_route_distribution_cache():
    """Reset the module-level `_route_distribution_cache` before/after every
    test in this file.

    Same bleed risk `tests/conftest.py`'s `_reset_ewt_schedule_caches`
    documents for other `_db_identity`-keyed caches: every SQLite
    `:memory:` test session renders the identical `_db_identity` string
    (verified directly -- see `_get_route_distribution_cached`'s
    docstring), and this file's tests often reuse
    `AGENCY_COMPARISON_WINDOW_START` and the same agency names, so two
    tests could otherwise collide on the exact same cache key.
    """
    from api import aggregations as agg

    agg._route_distribution_cache.clear()
    yield
    agg._route_distribution_cache.clear()


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
    """Seed one day of proximity stop_events so the route's OTP for that
    date reads `otp_value`%.

    Mirrors `TestGetRouteContributors._seed_route_otp` in
    test_aggregations.py: a uniform on-time/not-on-time split over 100
    events on one date so the daily percentage equals the seed value
    exactly. Of the not-on-time events, one is seeded EARLY
    (deviation_sec=-300, below `OTP_EARLY_SEC`) rather than late whenever
    there's at least one to spare -- exercising the window's lower bound,
    not just its upper one (`+600`) -- without changing `on_time_count`
    or the total, so `otp_value` still comes out exact.
    """
    from datetime import datetime as _dt

    events_per_day = 100
    on_time_count = int(round(otp_value / 100.0 * events_per_day))
    not_on_time_count = events_per_day - on_time_count
    early_count = 1 if not_on_time_count >= 1 else 0
    late_count = not_on_time_count - early_count
    base_ts = _dt.combine(service_date, _dt.min.time()).replace(hour=14)
    rows = []
    for j in range(on_time_count):
        rows.append(
            StopEvent(
                service_date=service_date.isoformat(),
                trip_id=f"TRIP_{route_id}_{service_date.isoformat()}_OT_{j}",
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
                trip_id=f"TRIP_{route_id}_{service_date.isoformat()}_LATE_{j}",
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
    for j in range(early_count):
        rows.append(
            StopEvent(
                service_date=service_date.isoformat(),
                trip_id=f"TRIP_{route_id}_{service_date.isoformat()}_EARLY_{j}",
                route_id=route_id,
                direction_id=0,
                stop_id=f"STOP_{route_id}",
                stop_sequence=1,
                observed_arrival_ts=base_ts + timedelta(seconds=on_time_count + late_count + j),
                deviation_sec=-300,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )
    session.add_all(rows)


def _seed_service_delivered(
    session, route_id, service_date, scheduled_trip_count, delivered_trip_count
):
    """Seed Calendar + Trip + Run rows so `compute_service_delivered_for_routes`
    (and therefore `_bulk_route_service_delivered_window`) reads
    `delivered_trip_count / scheduled_trip_count` for `route_id` on
    `service_date`.

    Calendar's day-of-week flag is set to match `service_date`'s actual
    weekday -- service-delivered resolves per exact date, not a
    representative weekday (see src/service_delivered.py). Run rows use
    `stops_observable=5` (mid-size-route branch of the trip-length-aware
    existence threshold in `compute_service_delivered`, floor 2) and
    `stops_observed=5`, comfortably clearing that floor so every seeded
    Run row counts as delivered.
    """
    day_field = _WEEKDAY_FIELDS[service_date.weekday()]
    service_id = f"SVC_{route_id}_{service_date.isoformat()}"
    cal_kwargs = dict.fromkeys(_WEEKDAY_FIELDS, 0)
    cal_kwargs[day_field] = 1
    session.add(
        Calendar(
            service_id=service_id,
            start_date="20260101",
            end_date="20271231",
            is_current=True,
            **cal_kwargs,
        )
    )
    for i in range(scheduled_trip_count):
        session.add(
            Trip(
                trip_id=f"TRIP_SD_{route_id}_{service_date.isoformat()}_{i}",
                route_id=route_id,
                service_id=service_id,
                direction_id=i % 2,
                is_current=True,
            )
        )
    for i in range(delivered_trip_count):
        session.add(
            Run(
                service_date=service_date.isoformat(),
                trip_id=f"TRIP_SD_{route_id}_{service_date.isoformat()}_{i}",
                route_id=route_id,
                direction_id=i % 2,
                source="trip_update",
                stops_observed=5,
                stops_observable=5,
            )
        )


def _seed_overlay_row(
    session,
    route_id,
    service_date,
    otp_all_early=0,
    otp_all_on_time=0,
    otp_all_late=0,
    scheduled_trips=0,
    delivered_trips=0,
):
    """Seed one `route_metrics_daily_overlay` row -- the materialized fast
    path both bulk distribution helpers read first (NOTES-141 delta
    review). Only the OTP-all-timepoints and service-delivered fields
    these tests care about are parameterized; every other sufficient-
    statistic column defaults to 0 on the model.
    """
    session.add(
        RouteMetricsDailyOverlay(
            route_id=route_id,
            service_date=service_date.isoformat(),
            day_type="weekday",
            otp_all_early=otp_all_early,
            otp_all_on_time=otp_all_on_time,
            otp_all_late=otp_all_late,
            scheduled_trips=scheduled_trips,
            delivered_trips=delivered_trips,
        )
    )


class TestRouteDistributionBucketing:
    """Pure-function tests for the per-metric percentage-scale histogram buckets."""

    def test_otp_bucket_edges_are_right_open_except_last(self):
        assert _route_distribution_bucket_index("otp", 59.9) == 0
        assert _route_distribution_bucket_index("otp", 60.0) == 1
        assert _route_distribution_bucket_index("otp", 69.9) == 1
        assert _route_distribution_bucket_index("otp", 70.0) == 2
        assert _route_distribution_bucket_index("otp", 89.9) == 3
        assert _route_distribution_bucket_index("otp", 90.0) == 4
        assert _route_distribution_bucket_index("otp", 100.0) == 4

    def test_otp_over_100_clamps_into_top_bucket(self):
        """A bad upstream read above 100 doesn't raise -- it lands in the top bucket."""
        assert _route_distribution_bucket_index("otp", 150.0) == 4

    def test_service_delivered_has_its_own_tighter_edges(self):
        """Review finding 4: service_delivered does NOT share OTP's edges.

        A value that would sit in OTP's single top bucket (>=90) spans
        THREE different service_delivered buckets, because the metric's
        real observed range clusters tightly near 90-100% -- reusing OTP's
        edges dumped ~97% of routes into one bar.
        """
        assert _route_distribution_bucket_index("service_delivered", 84.9) == 0
        assert _route_distribution_bucket_index("service_delivered", 85.0) == 1
        assert _route_distribution_bucket_index("service_delivered", 91.0) == 2
        assert _route_distribution_bucket_index("service_delivered", 92.5) == 3
        assert _route_distribution_bucket_index("service_delivered", 95.0) == 4
        assert _route_distribution_bucket_index("service_delivered", 100.0) == 4

    def test_service_delivered_ratio_rescales_to_percentage_axis(self):
        """service_delivered's 0-1 ratio maps onto the shared 0-100 axis."""
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

    def test_service_delivered_histogram_uses_its_own_edges(self):
        """0.80 / 0.87 / 0.94 / 0.97 land in four different
        service_delivered-specific buckets (<85, 85-90, 92.5-95, 95+); the
        90-92.5 bucket is empty and only 0.97 clears a 0.95 threshold.
        """
        result = _summarize_route_distribution(
            {"A": 0.80, "B": 0.87, "C": 0.94, "D": 0.97}, "service_delivered", 0.95
        )
        counts_by_label = {b["label"]: b["count"] for b in result["histogram"]}
        assert counts_by_label == {
            "<85": 1,
            "85-90": 1,
            "90-92.5": 0,
            "92.5-95": 1,
            "95+": 1,
        }
        assert result["share_at_or_above_threshold"] == pytest.approx(0.25)

    def test_threshold_none_yields_null_share(self):
        """No configured target -> share_at_or_above_threshold stays None,
        not a divide-by-zero or a bogus 0/1.
        """
        result = _summarize_route_distribution({"A": 80.0}, "otp", None)
        assert result["threshold"] is None
        assert result["share_at_or_above_threshold"] is None


class TestBulkRouteOtpMatchesPerRouteHelper:
    """NOTES-141 review finding 3a: the bulk grouped-query OTP helper that
    replaced the ~128-queries-per-request per-route fan-out must use the
    EXACT same on-time definition as `_compute_otp_per_day_with_filters`
    (the per-route helper it replaces) under `ALL_HOURS`.
    """

    def test_matches_per_route_helper_across_routes_and_dates(self, db_session):
        d1 = date(2026, 8, 3)
        d2 = date(2026, 8, 4)
        _seed_route(db_session, "A")
        _seed_route(db_session, "B")
        _seed_route_otp(db_session, "A", 70.0, d1)
        _seed_route_otp(db_session, "A", 90.0, d2)
        _seed_route_otp(db_session, "B", 40.0, d1)
        db_session.commit()

        bulk = _bulk_route_otp_daily_percentages(db_session, d1, d2)

        for route_id in ("A", "B"):
            per_route_by_date = _compute_otp_per_day_with_filters(
                db_session, route_id, d1, d2, ALL_HOURS
            )
            expected = _mean_skip_null(list(per_route_by_date.values()))
            if expected is None:
                assert bulk.get(route_id) is None
            else:
                assert bulk.get(route_id) == pytest.approx(expected)

        # Concrete numbers: route A's window mean is (70+90)/2 = 80; route
        # B only has a value on d1, so its window mean is just 40.
        assert bulk["A"] == pytest.approx(80.0)
        assert bulk["B"] == pytest.approx(40.0)

    def test_route_with_no_stop_events_is_absent_from_result(self, db_session):
        _seed_route(db_session, "GHOST")
        db_session.commit()
        assert (
            _bulk_route_otp_daily_percentages(db_session, date(2026, 8, 3), date(2026, 8, 3)) == {}
        )

    def test_null_observed_arrival_ts_excluded_by_both_helpers(self, db_session):
        """A stop_event with `deviation_sec` set but no `observed_arrival_ts`
        is skipped by the per-route helper (it guards `ts is None`) and
        must be skipped identically by the bulk helper's explicit
        `observed_arrival_ts.isnot(None)` filter -- without that filter
        the two would silently diverge on this edge case.
        """
        d = date(2026, 8, 5)
        _seed_route(db_session, "A")
        db_session.add(
            StopEvent(
                service_date=d.isoformat(),
                trip_id="TRIP_GHOST_TS",
                route_id="A",
                direction_id=0,
                stop_id="STOP_A",
                stop_sequence=1,
                observed_arrival_ts=None,
                deviation_sec=0,
                source="proximity",
                schedule_relationship="SCHEDULED",
            )
        )
        db_session.commit()

        bulk = _bulk_route_otp_daily_percentages(db_session, d, d)
        per_route = _compute_otp_per_day_with_filters(db_session, "A", d, d, ALL_HOURS)

        assert bulk.get("A") is None
        assert per_route[d.isoformat()] is None


class TestBulkRouteOtpOverlayPath:
    """NOTES-141 delta review finding 1: the overlay is the PRIMARY read
    path, not just a fallback -- `_bulk_route_otp_daily_percentages` must
    read `otp_all_early/on_time/late` from `route_metrics_daily_overlay`
    for materialized dates without touching `stop_events` at all. An
    earlier version scanned `stop_events` unconditionally for the whole
    window and was the dominant cold cost once the sibling
    service_delivered fix already used the overlay (33.1s WMATA + 7.8s
    SFMTA; see `_bulk_route_otp_daily_percentages`'s docstring).
    """

    def test_overlay_row_used_without_any_stop_events(self, db_session):
        """No `stop_events` seeded at all -- if the overlay path weren't
        used, this would return nothing. 8 on-time / 2 late / 0 early on
        the overlay row -> 80%.
        """
        d = date(2026, 8, 3)
        _seed_overlay_row(db_session, "A", d, otp_all_early=0, otp_all_on_time=8, otp_all_late=2)
        db_session.commit()

        result = _bulk_route_otp_daily_percentages(db_session, d, d)

        assert result["A"] == pytest.approx(80.0)

    def test_zero_n_overlay_row_contributes_nothing(self, db_session):
        """An overlay row with no OTP observations at all (n=0, the
        model's all-zero default) must not contribute a bogus 0% or
        crash -- same 'no data' semantics as the live scan.
        """
        d = date(2026, 8, 3)
        _seed_overlay_row(db_session, "A", d)
        db_session.commit()

        result = _bulk_route_otp_daily_percentages(db_session, d, d)

        assert result.get("A") is None

    def test_missing_date_falls_back_to_live_scan(self, db_session):
        """Day 1 is materialized (overlay row only, no stop_events for
        that date); day 2 has NO overlay row but DOES have stop_events --
        the window mean must combine both sources, proving the fallback
        tier actually engages per-date rather than all-or-nothing.
        """
        d1 = date(2026, 8, 3)
        d2 = date(2026, 8, 4)
        _seed_overlay_row(db_session, "A", d1, otp_all_early=0, otp_all_on_time=9, otp_all_late=1)
        _seed_route(db_session, "A")
        _seed_route_otp(db_session, "A", 70.0, d2)  # live-scan path: no overlay row for d2
        db_session.commit()

        result = _bulk_route_otp_daily_percentages(db_session, d1, d2)

        # (90 + 70) / 2 = 80 -- mean of the overlay day and the scanned day.
        assert result["A"] == pytest.approx(80.0)


class TestBulkRouteServiceDeliveredWindow:
    """`_bulk_route_service_delivered_window` -- pools service_delivered
    across the window from real GTFS + Runs rows, with NO partial-day
    exclusion (review finding 1).
    """

    def test_pools_across_dates(self, db_session):
        """Unequal daily denominators (10/10 then 3/5) so the pooled
        sum-then-recompute ratio is distinguishable from a naive mean of
        daily ratios -- mean-of-ratios would be (1.0 + 0.6) / 2 = 0.8;
        the pooled ratio is 13/15 ≈ 0.8667.
        """
        d1 = date(2026, 8, 3)
        d2 = date(2026, 8, 4)
        _seed_service_delivered(
            db_session, "A", d1, scheduled_trip_count=10, delivered_trip_count=10
        )
        _seed_service_delivered(db_session, "A", d2, scheduled_trip_count=5, delivered_trip_count=3)
        db_session.commit()

        result = _bulk_route_service_delivered_window(db_session, d1, d2)

        # `_reduce_service_delivered` rounds to 4dp: 13/15 -> 0.8667.
        assert result["A"] == pytest.approx(0.8667)
        assert result["A"] != pytest.approx(0.8)

    def test_route_with_no_schedule_is_absent(self, db_session):
        assert (
            _bulk_route_service_delivered_window(db_session, date(2026, 8, 3), date(2026, 8, 3))
            == {}
        )

    def test_overlay_row_used_without_any_runs_or_trips(self, db_session):
        """No Calendar/Trip/Run rows seeded at all -- if the overlay path
        weren't used, this would return nothing.
        """
        d = date(2026, 8, 3)
        _seed_overlay_row(db_session, "A", d, scheduled_trips=10, delivered_trips=7)
        db_session.commit()

        result = _bulk_route_service_delivered_window(db_session, d, d)

        assert result["A"] == pytest.approx(0.7)


class TestRouteDistributionForAgency:
    """`_route_distribution_for_agency` -- the per-agency query/aggregation wiring."""

    def test_otp_distribution_from_seeded_stop_events(self, db_session):
        """Four routes seeded with known OTP window means reproduce the
        hand-computed stats from TestSummarizeRouteDistribution, proving
        the bulk DB-backed path feeds the same summarizer.
        """
        d = date(2026, 8, 3)
        for route_id, otp_value in [("A", 50.0), ("B", 60.0), ("C", 70.0), ("D", 80.0)]:
            _seed_route(db_session, route_id)
            _seed_route_otp(db_session, route_id, otp_value, d)
        db_session.commit()

        result = _route_distribution_for_agency(db_session, d, days=1)

        assert result["otp"]["route_count"] == 4
        assert result["otp"]["median"] == pytest.approx(65.0)
        assert result["otp"]["p25"] == pytest.approx(57.5)
        assert result["otp"]["p75"] == pytest.approx(72.5)
        # No Calendar/Trip/Run rows seeded -> no service_delivered data.
        assert result["service_delivered"]["route_count"] == 0

    def test_service_delivered_distribution_from_seeded_runs(self, db_session):
        """service_delivered pools real scheduled/delivered trip counts via
        `_bulk_route_service_delivered_window`, not the shared
        `get_live_metrics_for_window` path (which excludes partial days).
        """
        d = date(2026, 8, 3)
        _seed_route(db_session, "A")
        _seed_route(db_session, "B")
        _seed_service_delivered(db_session, "A", d, scheduled_trip_count=10, delivered_trip_count=9)
        _seed_service_delivered(db_session, "B", d, scheduled_trip_count=10, delivered_trip_count=8)
        db_session.commit()

        result = _route_distribution_for_agency(db_session, d, days=1)

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

        result = _route_distribution_for_agency(db_session, date(2026, 8, 3), days=1)

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


class TestRouteDistributionCaching:
    """NOTES-141 review finding 3b: `_get_route_distribution_cached` reuses
    a prior computation within the TTL, keyed by (agency_name, db
    identity, window_start, window_end).
    """

    def test_second_call_within_ttl_reuses_cached_result(self, db_session, monkeypatch):
        _seed_route(db_session, "A")
        _seed_route_otp(db_session, "A", 80.0, date(2026, 8, 3))
        db_session.commit()

        calls = []
        real = _route_distribution_for_agency

        def _counting(*args, **kwargs):
            calls.append(1)
            return real(*args, **kwargs)

        monkeypatch.setattr("api.aggregations._route_distribution_for_agency", _counting)

        first = _get_route_distribution_cached(
            db_session, "wmata", "2026-08-03", date(2026, 8, 3), 1
        )
        second = _get_route_distribution_cached(
            db_session, "wmata", "2026-08-03", date(2026, 8, 3), 1
        )

        assert first == second
        assert len(calls) == 1

    def test_different_window_end_is_a_cache_miss(self, db_session):
        _seed_route(db_session, "A")
        _seed_route_otp(db_session, "A", 80.0, date(2026, 8, 3))
        _seed_route_otp(db_session, "A", 40.0, date(2026, 8, 4))
        db_session.commit()

        first = _get_route_distribution_cached(
            db_session, "wmata", "2026-08-03", date(2026, 8, 3), 1
        )
        second = _get_route_distribution_cached(
            db_session, "wmata", "2026-08-03", date(2026, 8, 4), 1
        )

        assert first["otp"]["median"] == pytest.approx(80.0)
        # Different window_end -> a real cache miss, not the stale 80.0.
        assert second["otp"]["median"] == pytest.approx(40.0)


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
        `agencies[].agency`, `.display_name`, `.metrics.<metric>` (every
        field of it, for every headline metric), and `.service_level` --
        none of that shape changes; `route_distribution` only adds a new
        sibling key. Reproduces the pre-existing
        `test_window_mean_skips_nulls_and_excludes_before_window_start`
        scenario from test_agency_comparison.py to prove the old fields
        are untouched by this change.
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

        # Every headline metric keeps exactly its pre-PR field set.
        assert set(agency["metrics"]) == set(AGENCY_COMPARISON_METRICS)
        for metric in AGENCY_COMPARISON_METRICS:
            assert set(agency["metrics"][metric]) == {
                "window_mean",
                "wow_delta",
                "days_included",
                "partial_days",
            }
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


class TestAgencyComparisonRouteDistributionIncludesPartialDays:
    """NOTES-141 review finding 1 -- the concrete repro: SFMTA's entire
    matched window is `data_quality='partial'` by design (NOTES-104: the
    laptop-side vehicle_positions-only completeness ceiling). Before this
    fix, every date being partial meant `_partial_service_dates_in_window`
    excluded the whole window from the OTP sub-metric, leaving
    `route_count == 0` for every route -- verified live: 39 of 41 SFMTA
    dates partial, 0 of 68 routes scored. The distribution must use the
    same day-set as the headline `window_mean` above it, which does NOT
    exclude partial days.
    """

    def test_all_partial_days_window_still_populates_both_submetrics(self):
        db = _make_session()
        d1 = AGENCY_COMPARISON_WINDOW_START
        d2 = AGENCY_COMPARISON_WINDOW_START + timedelta(days=1)
        for d, otp_pct in ((d1, 65.0), (d2, 67.0)):
            db.add(
                SystemMetricsDaily(
                    service_date=d.isoformat(),
                    otp_percentage=otp_pct,
                    data_quality="partial",
                    coverage_pct=0.33,
                )
            )
        _seed_route(db, "SFMTA1")
        _seed_route_otp(db, "SFMTA1", 65.0, d1)
        _seed_route_otp(db, "SFMTA1", 67.0, d2)
        _seed_service_delivered(db, "SFMTA1", d1, scheduled_trip_count=10, delivered_trip_count=9)
        _seed_service_delivered(db, "SFMTA1", d2, scheduled_trip_count=10, delivered_trip_count=8)
        db.commit()
        try:
            result = get_agency_comparison_data({"sfmta": db})
        finally:
            db.close()

        agency = result["agencies"][0]
        # The pre-existing headline behavior: partial days still count.
        assert agency["metrics"]["otp"]["partial_days"] == 2
        assert agency["metrics"]["otp"]["window_mean"] == pytest.approx(66.0)

        # The fix under test: the distribution is NOT zeroed out.
        otp_dist = agency["route_distribution"]["otp"]
        sd_dist = agency["route_distribution"]["service_delivered"]
        assert otp_dist["route_count"] == 1
        assert otp_dist["median"] == pytest.approx(66.0)
        assert sd_dist["route_count"] == 1
        assert sd_dist["median"] == pytest.approx(17 / 20)


class TestCrossAgencyIsolation:
    """Two different agencies' sessions must never share a cached
    `route_distribution`, even though every SQLite `:memory:` test
    session renders the identical `_db_identity` string -- guarded by
    including `agency_name` in `_route_distribution_cache`'s key (see
    `_get_route_distribution_cached`'s docstring).
    """

    def _seeded_pair(self):
        wmata_db = _make_session()
        sfmta_db = _make_session()
        d = AGENCY_COMPARISON_WINDOW_START
        for db in (wmata_db, sfmta_db):
            db.add(
                SystemMetricsDaily(
                    service_date=d.isoformat(), otp_percentage=70.0, data_quality="complete"
                )
            )
        _seed_route(wmata_db, "WMATA_ROUTE")
        _seed_route_otp(wmata_db, "WMATA_ROUTE", 95.0, d)
        _seed_route(sfmta_db, "SFMTA_ROUTE")
        _seed_route_otp(sfmta_db, "SFMTA_ROUTE", 20.0, d)
        wmata_db.commit()
        sfmta_db.commit()
        return wmata_db, sfmta_db

    def test_two_agencies_with_different_routes_get_independent_distributions(self):
        wmata_db, sfmta_db = self._seeded_pair()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db, "sfmta": sfmta_db})
        finally:
            wmata_db.close()
            sfmta_db.close()

        by_agency = {a["agency"]: a for a in result["agencies"]}
        assert by_agency["wmata"]["route_distribution"]["otp"]["median"] == pytest.approx(95.0)
        assert by_agency["sfmta"]["route_distribution"]["otp"]["median"] == pytest.approx(20.0)

    def test_second_call_after_first_is_still_isolated(self):
        """A prior lookup that only warmed WMATA's cache slot (e.g. an
        earlier page load, or CompareStrip firing before the full
        /compare page) must not leak into SFMTA's slot on a later call
        that includes both agencies, or vice versa.
        """
        wmata_db, sfmta_db = self._seeded_pair()
        try:
            get_agency_comparison_data({"wmata": wmata_db})
            result = get_agency_comparison_data({"wmata": wmata_db, "sfmta": sfmta_db})
        finally:
            wmata_db.close()
            sfmta_db.close()

        by_agency = {a["agency"]: a for a in result["agencies"]}
        assert by_agency["wmata"]["route_distribution"]["otp"]["median"] == pytest.approx(95.0)
        assert by_agency["sfmta"]["route_distribution"]["otp"]["median"] == pytest.approx(20.0)


class TestRouteDistributionFailureIsolation:
    """NOTES-141 delta review finding 5: a `route_distribution` compute
    failure must not 500 the whole `/api/agency-comparison` response --
    `CompareStrip.jsx` on the Overview page also calls this endpoint, so
    a bug in this feature shouldn't be able to take that down too. Same
    failure-isolation contract `service_level` already has.
    """

    def test_compute_failure_degrades_to_none_not_a_crash(self, monkeypatch):
        wmata_db = _make_session()
        wmata_db.add(
            SystemMetricsDaily(
                service_date=AGENCY_COMPARISON_WINDOW_START.isoformat(),
                otp_percentage=70.0,
                data_quality="complete",
            )
        )
        wmata_db.commit()

        def _boom(*args, **kwargs):
            raise RuntimeError("route_metrics_daily_overlay schema mismatch")

        monkeypatch.setattr("api.aggregations._get_route_distribution_cached", _boom)

        try:
            result = get_agency_comparison_data({"wmata": wmata_db})
        finally:
            wmata_db.close()

        agency = result["agencies"][0]
        assert agency["route_distribution"] is None
        # Everything else on the envelope is unaffected by the failure.
        assert agency["metrics"]["otp"]["window_mean"] == pytest.approx(70.0)
        assert "service_level" in agency
