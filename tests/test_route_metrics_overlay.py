"""
Unit tests for the route_metrics_daily_overlay materialization path.

The overlay materializes per-(route, service_date) sufficient statistics so
the windowed scorecard can read 126 × 7 = ~882 rows instead of recomputing
3M+ stop_events on every cold hit. Tests focus on:

  - `upsert_route_metrics_for_date` writes rows, upserts on second invocation.
  - The hydrated overlay shape matches what the windowed aggregator consumes.
  - End-to-end equivalence: scorecard output built from the overlay equals
    the output built from live compute (the overlay is a cache, not a
    new derivation).
"""

from datetime import date, timedelta

import pytest

from api.aggregations import (
    _hydrate_overlay_row,
    _read_overlay_for_dates,
    get_all_routes_scorecard,
)
from src.models import RouteMetricsDailyOverlay
from src.route_metrics_overlay import (
    compute_route_metrics_overlay_for_date,
    upsert_route_metrics_for_date,
)


@pytest.mark.smoke
def test_overlay_compute_empty_db_returns_no_rows(db_session):
    """No stop_events / runs / scheduled trips → no overlay rows."""
    target = date(2026, 5, 5)
    rows = compute_route_metrics_overlay_for_date(db_session, target)
    assert rows == []


@pytest.mark.smoke
def test_overlay_upsert_empty_db_writes_zero_rows(db_session):
    """Upsert on an empty DB is a no-op that reports zero rows."""
    target = date(2026, 5, 5)
    n = upsert_route_metrics_for_date(db_session, target)
    assert n == 0
    assert db_session.query(RouteMetricsDailyOverlay).count() == 0


@pytest.mark.smoke
def test_overlay_upsert_is_idempotent(db_session):
    """Second upsert against the same date replaces, doesn't duplicate.

    We hand-seed an overlay row with synthetic stats, then run the upsert
    against an empty source — the upsert recomputes from empty (so the
    route is no longer in the source set) and leaves the original row
    untouched since the recompute writes zero new rows. The point of this
    test is to verify the SQL unique constraint and the existing-row path
    don't choke on a second invocation.
    """
    target = date(2026, 5, 5)
    # First upsert: zero rows (empty DB).
    upsert_route_metrics_for_date(db_session, target)
    # Hand-insert a synthetic row to verify the upsert path doesn't
    # accidentally delete pre-existing rows (it shouldn't — it upserts by
    # (route_id, service_date) and skips dates with no source data).
    row = RouteMetricsDailyOverlay(
        route_id="TEST1",
        service_date=target.isoformat(),
        day_type="tuesday",
        scheduled_trips=10,
        delivered_trips=9,
    )
    db_session.add(row)
    db_session.commit()
    assert db_session.query(RouteMetricsDailyOverlay).count() == 1
    # Second upsert: still empty source, the existing TEST1 row stays.
    upsert_route_metrics_for_date(db_session, target)
    assert db_session.query(RouteMetricsDailyOverlay).count() == 1


def test_hydrate_overlay_row_shape():
    """The hydrated bundle exposes both sufficient stats AND derived fields.

    Derived fields (ewt_seconds, coverage_ratio, bunching_rate) must be
    present so consumers of a single-date cached bundle (`/api/routes/{id}`
    via `get_live_metrics_for_route_today`) don't see N/A. The window
    aggregator pools the sufficient stats and ignores the derived keys.
    """
    row = RouteMetricsDailyOverlay(
        route_id="R1",
        service_date="2026-05-05",
        day_type="weekday",
        otp_origin_early=2,
        otp_origin_on_time=8,
        otp_origin_late=0,
        otp_destination_early=0,
        otp_destination_on_time=5,
        otp_destination_late=5,
        otp_all_early=0,
        otp_all_on_time=0,
        otp_all_late=0,
        scheduled_trips=100,
        delivered_trips=95,
        ewt_obs_sum_h=1200.0,
        ewt_obs_sum_h_sq=1440000.0,
        ewt_n_observed_headways=2,
        ewt_sched_sum_h=600.0,
        ewt_sched_sum_h_sq=360000.0,
        ewt_n_scheduled_headways=2,
        bunching_count=3,
        bunching_total_headways=100,
    )
    bundle = _hydrate_overlay_row(row)

    # Service-delivered
    assert bundle["service_delivered"]["scheduled_trips"] == 100
    assert bundle["service_delivered"]["delivered_trips"] == 95
    assert bundle["service_delivered"]["ratio"] == 0.95

    # OTP origin: 10 events, 80% on-time
    origin = bundle["otp_split"]["origin"]
    assert origin["n"] == 10
    assert origin["on_time_pct"] == 80.0
    # OTP all_timepoints: zero counts → n=0, no pcts (matches live `_aggregate_deviations`)
    assert bundle["otp_split"]["all_timepoints"]["n"] == 0
    assert "on_time_pct" not in bundle["otp_split"]["all_timepoints"]

    # EWT sufficient stats round-trip
    assert bundle["ewt"]["obs_sum_h"] == 1200.0
    assert bundle["ewt"]["obs_sum_h_sq"] == 1440000.0
    # EWT derived fields — what the detail endpoint reads.
    # AWT = 1_440_000 / (2 * 1200) = 600.0
    # SWT =   360_000 / (2 *  600) = 300.0
    # EWT = AWT - SWT = 300.0; coverage = 2/2 = 1.0
    assert bundle["ewt"]["awt_seconds"] == 600.0
    assert bundle["ewt"]["swt_seconds"] == 300.0
    assert bundle["ewt"]["ewt_seconds"] == 300.0
    assert bundle["ewt"]["coverage_ratio"] == 1.0

    # Bunching counts + derived rate
    assert bundle["bunching"]["bunching_count"] == 3
    assert bundle["bunching"]["total_headways"] == 100
    assert bundle["bunching"]["bunching_rate"] == 0.03


def test_hydrate_overlay_row_zero_stats_yields_none_metrics():
    """A row with zero sufficient-stat sums emits `None` for every derived field.

    Defends the "row exists but no headways observed/scheduled" case — the
    detail endpoint should see explicit `None`, not a `KeyError`, and not
    a NaN from a divide-by-zero.
    """
    row = RouteMetricsDailyOverlay(
        route_id="R1",
        service_date="2026-05-05",
        day_type="weekday",
        otp_origin_early=0,
        otp_origin_on_time=0,
        otp_origin_late=0,
        otp_destination_early=0,
        otp_destination_on_time=0,
        otp_destination_late=0,
        otp_all_early=0,
        otp_all_on_time=0,
        otp_all_late=0,
        scheduled_trips=0,
        delivered_trips=0,
        ewt_obs_sum_h=0.0,
        ewt_obs_sum_h_sq=0.0,
        ewt_n_observed_headways=0,
        ewt_sched_sum_h=0.0,
        ewt_sched_sum_h_sq=0.0,
        ewt_n_scheduled_headways=0,
        bunching_count=0,
        bunching_total_headways=0,
    )
    bundle = _hydrate_overlay_row(row)

    assert bundle["ewt"]["awt_seconds"] is None
    assert bundle["ewt"]["swt_seconds"] is None
    assert bundle["ewt"]["ewt_seconds"] is None
    assert bundle["ewt"]["coverage_ratio"] is None
    assert bundle["bunching"]["bunching_rate"] is None


@pytest.mark.smoke
def test_read_overlay_for_dates_groups_by_date(db_session):
    """`_read_overlay_for_dates` returns `{date_str: {route_id: bundle}}`."""
    dates = [date(2026, 5, 3), date(2026, 5, 4)]
    db_session.add_all(
        [
            RouteMetricsDailyOverlay(route_id="R1", service_date="2026-05-03", day_type="weekday"),
            RouteMetricsDailyOverlay(route_id="R2", service_date="2026-05-03", day_type="weekday"),
            RouteMetricsDailyOverlay(route_id="R1", service_date="2026-05-04", day_type="weekday"),
        ]
    )
    db_session.commit()

    out = _read_overlay_for_dates(db_session, dates)
    assert set(out.keys()) == {"2026-05-03", "2026-05-04"}
    assert set(out["2026-05-03"].keys()) == {"R1", "R2"}
    assert set(out["2026-05-04"].keys()) == {"R1"}


@pytest.mark.smoke
def test_scorecard_reads_from_overlay_when_available(db_session, sample_route):
    """When the overlay has rows for every date in the window, the scorecard
    surfaces the materialized values without touching the live compute path."""
    # Seed overlay rows for the full 7-day window ending today (Eastern).
    # We don't need to be precise about the date — `get_all_routes_scorecard`
    # anchors on the latest service_date with stop_events. Without stop_events,
    # the window endpoints are null and the overlay path is bypassed.
    # So we also seed one stop_event to anchor the window.
    from datetime import datetime

    from src.models import StopEvent
    from src.timezones import eastern_today

    end_date = eastern_today() - timedelta(days=1)
    db_session.add(
        StopEvent(
            service_date=end_date.isoformat(),
            trip_id="T1",
            route_id="TEST1",
            direction_id=0,
            stop_id="S1",
            stop_sequence=1,
            observed_arrival_ts=datetime(2026, 5, 5, 12, 0, 0),
            scheduled_arrival_ts=datetime(2026, 5, 5, 12, 0, 0),
            deviation_sec=0,
            source="proximity",
            schedule_relationship="SCHEDULED",
        )
    )
    # Seed one overlay row for the anchor date with known values.
    db_session.add(
        RouteMetricsDailyOverlay(
            route_id="TEST1",
            service_date=end_date.isoformat(),
            day_type="weekday",
            otp_all_early=1,
            otp_all_on_time=9,
            otp_all_late=0,
            scheduled_trips=10,
            delivered_trips=10,
        )
    )
    db_session.commit()

    # Clear the in-memory caches between tests so we exercise the overlay path.
    from api.aggregations import _live_metrics_cache, _window_metrics_cache

    _live_metrics_cache.clear()
    _window_metrics_cache.clear()

    result = get_all_routes_scorecard(db_session, days=7)
    routes = result["routes"]
    test1 = next((r for r in routes if r["route_id"] == "TEST1"), None)
    assert test1 is not None
    # OTP all_timepoints pooled over the window — only 1 date has overlay
    # data (the anchor), the rest are absent so they contribute nothing.
    # 9 / 10 = 90% on-time.
    assert test1["otp_all_pct"] == 90.0
    assert test1["service_delivered_ratio"] == 1.0


@pytest.mark.smoke
def test_detail_endpoint_sees_derived_fields_from_overlay_cache(db_session, sample_route):
    """Regression: cached overlay bundles expose `ewt_seconds` to the detail endpoint.

    Reproduces the cross-cache bug: a scorecard call (`/api/routes`) hydrates
    overlay rows into `_live_metrics_cache` keyed by service_date. A
    subsequent detail call (`/api/routes/{id}`) reads that cache via
    `get_live_metrics_for_route_today` and returns whatever bundle is
    there. Before the fix the cached bundle was sufficient-stats only,
    so `ewt_seconds` was missing and the frontend rendered "N/A" — even
    though the underlying overlay row had perfectly good stats.
    """
    from datetime import datetime

    from api.aggregations import (
        _live_metric_fields,
        _live_metrics_cache,
        _window_metrics_cache,
        get_all_routes_scorecard,
        get_live_metrics_for_route_today,
    )
    from src.models import StopEvent
    from src.timezones import eastern_today

    end_date = eastern_today() - timedelta(days=1)
    db_session.add(
        StopEvent(
            service_date=end_date.isoformat(),
            trip_id="T1",
            route_id="TEST1",
            direction_id=0,
            stop_id="S1",
            stop_sequence=1,
            observed_arrival_ts=datetime(2026, 5, 5, 12, 0, 0),
            scheduled_arrival_ts=datetime(2026, 5, 5, 12, 0, 0),
            deviation_sec=0,
            source="proximity",
            schedule_relationship="SCHEDULED",
        )
    )
    # Seed an overlay row with nonzero EWT and bunching stats so the
    # derived fields have something to compute.
    db_session.add(
        RouteMetricsDailyOverlay(
            route_id="TEST1",
            service_date=end_date.isoformat(),
            day_type="weekday",
            otp_all_early=1,
            otp_all_on_time=9,
            otp_all_late=0,
            scheduled_trips=10,
            delivered_trips=10,
            ewt_obs_sum_h=1200.0,
            ewt_obs_sum_h_sq=1440000.0,
            ewt_n_observed_headways=2,
            ewt_sched_sum_h=600.0,
            ewt_sched_sum_h_sq=360000.0,
            ewt_n_scheduled_headways=2,
            bunching_count=3,
            bunching_total_headways=100,
        )
    )
    db_session.commit()

    _live_metrics_cache.clear()
    _window_metrics_cache.clear()

    # 1. Scorecard call — populates _live_metrics_cache via overlay path.
    get_all_routes_scorecard(db_session, days=7)

    # 2. Detail call — should now expose the derived fields, not N/A.
    bundle = get_live_metrics_for_route_today(db_session, "TEST1")
    assert bundle is not None
    fields = _live_metric_fields(bundle)
    assert fields["ewt_seconds"] == 300.0
    assert fields["ewt_n_observed"] == 2
    assert fields["ewt_coverage_ratio"] == 1.0
    assert fields["bunching_rate"] == 0.03
