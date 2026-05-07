"""
Unit tests for the system_metrics_daily materialization path (NOTES-48).

Covers:
  - `src/system_metrics.py:compute_system_metrics_for_date` returns the
    expected dict shape and reads OTP from `stop_events` (post-NOTES-19
    pivot off the legacy `route_metrics_daily` path).
  - `pipelines/compute_daily_metrics.py:upsert_system_metrics_for_date`
    writes a row to `system_metrics_daily` for the requested date and
    overwrites on second invocation (upsert semantics).
"""

from datetime import datetime, timedelta

import pytest

from src.models import RouteMetricsDaily, StopEvent, SystemMetricsDaily
from src.system_metrics import compute_system_metrics_for_date
from src.timezones import eastern_today


def _make_stop_event(
    *,
    service_date,
    route_id,
    trip_id,
    stop_sequence,
    deviation_sec,
    source="proximity",
):
    """Build a minimal proximity stop_event for OTP rollup tests.

    Only fields the OTP path reads are populated meaningfully; the rest
    use plausible defaults so the row passes NOT NULL constraints.
    """
    base_ts = datetime(2026, 1, 1, 12, 0, 0)
    return StopEvent(
        service_date=service_date,
        trip_id=trip_id,
        route_id=route_id,
        direction_id=0,
        stop_id=f"STOP_{trip_id}_{stop_sequence}",
        stop_sequence=stop_sequence,
        observed_arrival_ts=base_ts + timedelta(seconds=deviation_sec),
        scheduled_arrival_ts=base_ts,
        deviation_sec=deviation_sec,
        source=source,
        schedule_relationship="SCHEDULED",
    )


@pytest.mark.smoke
def test_compute_system_metrics_for_date_empty_db(db_session):
    """Empty DB returns the right dict shape with all-null values.

    With no `stop_events` and no `runs`, every headline is null but the
    dict shape must match the `system_metrics_daily` row schema (minus
    computed_at / service_date).
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
    # No runs on the date → service_delivered is null per the Run-existence
    # discriminator (matches the per-route rule from PR #77).
    assert result["service_delivered_ratio"] is None
    assert result["ewt_seconds"] is None
    assert result["bunching_rate"] is None


@pytest.mark.smoke
def test_compute_system_metrics_for_date_otp_pooled(db_session, sample_routes):
    """OTP rollup pools every proximity stop_event across routes.

    Two routes with different sample volumes — pooling counts (rather
    than per-route weighting) is mathematically equivalent to rider-
    weighted averaging.

    TEST1: 9 of 10 events on-time (90%). TEST2: 1 of 2 events on-time
    (50%). Pooled: 10 / 12 = 83.33%.
    """
    target_date = eastern_today() - timedelta(days=3)
    target_iso = target_date.isoformat()

    events = []
    # TEST1: 9 on-time + 1 late = 10 events, 90% on-time.
    for i in range(9):
        events.append(
            _make_stop_event(
                service_date=target_iso,
                route_id="TEST1",
                trip_id="TRIP_T1",
                stop_sequence=i + 1,
                deviation_sec=0,
            )
        )
    events.append(
        _make_stop_event(
            service_date=target_iso,
            route_id="TEST1",
            trip_id="TRIP_T1",
            stop_sequence=10,
            deviation_sec=600,  # +10 min, late
        )
    )
    # TEST2: 1 on-time + 1 early = 2 events, 50% on-time.
    events.append(
        _make_stop_event(
            service_date=target_iso,
            route_id="TEST2",
            trip_id="TRIP_T2",
            stop_sequence=1,
            deviation_sec=60,
        )
    )
    events.append(
        _make_stop_event(
            service_date=target_iso,
            route_id="TEST2",
            trip_id="TRIP_T2",
            stop_sequence=2,
            deviation_sec=-300,  # -5 min, early
        )
    )
    db_session.add_all(events)
    db_session.commit()

    result = compute_system_metrics_for_date(db_session, target_date)

    assert result["otp_percentage"] is not None
    # 10 on-time / 12 total = 83.33%
    assert 83.0 < result["otp_percentage"] < 84.0


@pytest.mark.smoke
def test_compute_system_metrics_for_date_ignores_route_metrics_daily_for_otp(
    db_session, sample_routes
):
    """Pivot off `route_metrics_daily` for OTP: legacy rows must not surface.

    Seed `route_metrics_daily` rows but no `stop_events` — OTP must be
    null. Proves the system rollup no longer depends on the legacy
    daily-batch pipeline for OTP (NOTES-19, partial).
    """
    target_date = eastern_today() - timedelta(days=6)
    db_session.add(
        RouteMetricsDaily(
            route_id="TEST1",
            date=target_date.isoformat(),
            otp_percentage=88.0,
            total_arrivals=500,
        )
    )
    db_session.commit()

    result = compute_system_metrics_for_date(db_session, target_date)
    assert result["otp_percentage"] is None


@pytest.mark.smoke
def test_compute_system_metrics_for_date_service_delivered_null_when_no_runs(
    db_session, sample_routes
):
    """Service-delivered: zero `runs` rows → None, not 0.0.

    Mirrors the per-route discriminator from PR #77 — without Run data
    we can't observe delivery, and `0.0` would falsely advertise
    "complete failure" on dates the collector wasn't recording.
    """
    target_date = eastern_today() - timedelta(days=7)
    # No Run rows seeded.
    result = compute_system_metrics_for_date(db_session, target_date)
    assert result["service_delivered_ratio"] is None


@pytest.mark.smoke
def test_compute_system_metrics_for_date_ignores_trip_update_for_otp(db_session, sample_routes):
    """OTP rollup filters to `source='proximity'` events only.

    A trip_update-only row contributes to neither numerator nor
    denominator; OTP for the date stays null.
    """
    target_date = eastern_today() - timedelta(days=4)
    db_session.add(
        _make_stop_event(
            service_date=target_date.isoformat(),
            route_id="TEST1",
            trip_id="TRIP_T1",
            stop_sequence=1,
            deviation_sec=0,
            source="trip_update",
        )
    )
    db_session.commit()

    result = compute_system_metrics_for_date(db_session, target_date)
    assert result["otp_percentage"] is None


def test_pipeline_upserts_system_metrics_row(db_session, sample_routes):
    """Daily pipeline writes one row per service_date to `system_metrics_daily`.

    Calling `upsert_system_metrics_for_date` should produce exactly one
    row with the computed values; calling it again replaces in place
    (primary key on service_date).
    """
    from pipelines.compute_daily_metrics import upsert_system_metrics_for_date

    target_date = eastern_today() - timedelta(days=4)
    target_iso = target_date.isoformat()

    # Seed two on-time + zero off-time events → 100% pooled OTP.
    db_session.add_all(
        [
            _make_stop_event(
                service_date=target_iso,
                route_id="TEST1",
                trip_id="TRIP_T1",
                stop_sequence=1,
                deviation_sec=30,
            ),
            _make_stop_event(
                service_date=target_iso,
                route_id="TEST1",
                trip_id="TRIP_T1",
                stop_sequence=2,
                deviation_sec=120,
            ),
        ]
    )
    db_session.commit()

    # First write
    written = upsert_system_metrics_for_date(db_session, target_date)
    assert written is not None
    assert written["otp_percentage"] == 100.0

    rows = (
        db_session.query(SystemMetricsDaily)
        .filter(SystemMetricsDaily.service_date == target_iso)
        .all()
    )
    assert len(rows) == 1
    assert rows[0].otp_percentage == 100.0
    assert rows[0].computed_at is not None

    # Add a late event so the rollup changes; re-run upsert.
    db_session.add(
        _make_stop_event(
            service_date=target_iso,
            route_id="TEST1",
            trip_id="TRIP_T1",
            stop_sequence=3,
            deviation_sec=900,  # +15 min, late
        )
    )
    db_session.commit()

    written_again = upsert_system_metrics_for_date(db_session, target_date)
    # 2 on-time / 3 total = 66.67%
    assert written_again["otp_percentage"] is not None
    assert 66.0 < written_again["otp_percentage"] < 67.0

    rows = (
        db_session.query(SystemMetricsDaily)
        .filter(SystemMetricsDaily.service_date == target_iso)
        .all()
    )
    assert len(rows) == 1  # still exactly one row
    assert 66.0 < rows[0].otp_percentage < 67.0
