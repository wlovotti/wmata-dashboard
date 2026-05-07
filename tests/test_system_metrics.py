"""
Unit tests for the system_metrics_daily materialization path (NOTES-48).

Covers:
  - `src/system_metrics.py:compute_system_metrics_for_date` returns the
    expected dict shape and reads from `route_metrics_daily` for OTP.
  - `pipelines/compute_daily_metrics.py:upsert_system_metrics_for_date`
    writes a row to `system_metrics_daily` for the requested date and
    overwrites on second invocation (upsert semantics).
"""

from datetime import timedelta

import pytest

from src.models import RouteMetricsDaily, SystemMetricsDaily
from src.system_metrics import compute_system_metrics_for_date
from src.timezones import eastern_today


@pytest.mark.smoke
def test_compute_system_metrics_for_date_empty_db(db_session):
    """Empty DB returns the right dict shape with all-null values.

    With no `route_metrics_daily` rows and no `stop_events`, every
    headline is null but the dict shape must match the
    `system_metrics_daily` row schema (minus computed_at / service_date).
    """
    target_date = eastern_today() - timedelta(days=2)
    result = compute_system_metrics_for_date(db_session, target_date)

    assert set(result.keys()) == {
        "otp_percentage",
        "service_delivered_ratio",
        "ewt_seconds",
        "bunching_rate",
    }
    assert result["otp_percentage"] is None
    # service_delivered may be 0.0 if there are scheduled runs in fixtures,
    # but with an empty DB it should be None (no scheduled trips).
    assert result["service_delivered_ratio"] is None
    assert result["ewt_seconds"] is None
    assert result["bunching_rate"] is None


@pytest.mark.smoke
def test_compute_system_metrics_for_date_otp_weighted(db_session, sample_routes):
    """OTP rollup is weighted by per-route `total_arrivals`.

    Two routes with different sample volumes — the high-volume route
    dominates the system value. Verifies the helper drives through the
    weighted-mean logic in `_system_otp_series`.
    """
    target_date = eastern_today() - timedelta(days=3)
    # TEST1: 90% OTP, 1000 arrivals. TEST2: 50% OTP, 100 arrivals.
    # Rider-weighted = (0.9*1000 + 0.5*100) / 1100 ≈ 86.36%.
    db_session.add(
        RouteMetricsDaily(
            route_id="TEST1",
            date=target_date.isoformat(),
            otp_percentage=90.0,
            total_arrivals=1000,
        )
    )
    db_session.add(
        RouteMetricsDaily(
            route_id="TEST2",
            date=target_date.isoformat(),
            otp_percentage=50.0,
            total_arrivals=100,
        )
    )
    db_session.commit()

    result = compute_system_metrics_for_date(db_session, target_date)

    assert result["otp_percentage"] is not None
    assert 86.0 < result["otp_percentage"] < 87.0


def test_pipeline_upserts_system_metrics_row(db_session, sample_routes):
    """Daily pipeline writes one row per service_date to `system_metrics_daily`.

    Calling `upsert_system_metrics_for_date` should produce exactly one
    row with the computed values; calling it again replaces in place
    (primary key on service_date).
    """
    from pipelines.compute_daily_metrics import upsert_system_metrics_for_date

    target_date = eastern_today() - timedelta(days=4)

    # Seed one route's daily metric so OTP comes out non-null.
    db_session.add(
        RouteMetricsDaily(
            route_id="TEST1",
            date=target_date.isoformat(),
            otp_percentage=88.0,
            total_arrivals=500,
        )
    )
    db_session.commit()

    # First write
    written = upsert_system_metrics_for_date(db_session, target_date)
    assert written is not None
    assert written["otp_percentage"] == 88.0

    rows = (
        db_session.query(SystemMetricsDaily)
        .filter(SystemMetricsDaily.service_date == target_date.isoformat())
        .all()
    )
    assert len(rows) == 1
    assert rows[0].otp_percentage == 88.0
    assert rows[0].computed_at is not None

    # Mutate the underlying row and re-run — upsert should overwrite.
    daily = (
        db_session.query(RouteMetricsDaily)
        .filter(
            RouteMetricsDaily.route_id == "TEST1",
            RouteMetricsDaily.date == target_date.isoformat(),
        )
        .first()
    )
    daily.otp_percentage = 75.0
    db_session.commit()

    written_again = upsert_system_metrics_for_date(db_session, target_date)
    assert written_again["otp_percentage"] == 75.0

    rows = (
        db_session.query(SystemMetricsDaily)
        .filter(SystemMetricsDaily.service_date == target_date.isoformat())
        .all()
    )
    assert len(rows) == 1  # still exactly one row
    assert rows[0].otp_percentage == 75.0
