"""
Unit tests for the system_metrics_daily materialization path (cloud-migration Phase 1).

Covers:
  - `src/system_metrics.py:compute_system_metrics_for_date` returns the
    expected dict shape and reads OTP from `stop_events`.
  - `src/system_metrics.py:upsert_system_metrics_for_date` writes a row
    to `system_metrics_daily` for the requested date and overwrites on
    second invocation (upsert semantics).
"""

from datetime import datetime, timedelta

import pytest

from src.models import StopEvent, SystemMetricsDaily
from src.system_metrics import compute_system_metrics_for_date, upsert_system_metrics_for_date
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
        "swt_seconds",
        "bunching_rate",
    }
    assert result["otp_percentage"] is None
    # No runs on the date → service_delivered is null per the Run-existence
    # discriminator (matches the per-route rule from PR #77).
    assert result["service_delivered_ratio"] is None
    assert result["ewt_seconds"] is None
    assert result["swt_seconds"] is None
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


def test_pipeline_upserts_system_metrics_row(db_session, sample_routes, monkeypatch):
    """Daily pipeline writes one row per service_date to `system_metrics_daily`.

    Calling `upsert_system_metrics_for_date` should produce exactly one
    row with the computed values; calling it again replaces in place
    (primary key on service_date).

    Bypasses the ingest-coverage guard (``src.data_completeness``) because
    this test seeds ``stop_events`` directly rather than the upstream
    ingest tables — a state that can't occur in production but is the
    minimum useful setup for testing the upsert path.
    """
    monkeypatch.setattr(
        "src.data_completeness.is_date_sufficiently_complete",
        lambda *args, **kwargs: True,
    )
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


@pytest.mark.smoke
def test_upsert_system_metrics_forwards_tz_name_to_completeness_guard(
    db_session, sample_routes, monkeypatch
):
    """NOTES-100: ``tz_name`` reaches the completeness guard, not just the
    default parameter list -- a non-Eastern agency (e.g. sfmta,
    America/Los_Angeles) must have its coverage window checked against
    its own local day, not silently against Eastern.

    This test only pins down the guard's plumbing (via the monkeypatched
    completeness functions below); it doesn't assert on the metric
    computation's own EWT/bunching hour-of-day bucketing, which is
    exercised separately (see the agency-local hour bucketing fix,
    PR #190).
    """
    seen_tz_names = []

    def _fake_coverage_pct(db, service_date, tz_name="America/New_York"):
        seen_tz_names.append(tz_name)
        return 1.0

    def _fake_is_complete(db, service_date, threshold=0.80, tz_name="America/New_York"):
        seen_tz_names.append(tz_name)
        return True

    monkeypatch.setattr("src.data_completeness.coverage_pct_for_date", _fake_coverage_pct)
    monkeypatch.setattr("src.data_completeness.is_date_sufficiently_complete", _fake_is_complete)

    target_date = eastern_today() - timedelta(days=5)
    upsert_system_metrics_for_date(db_session, target_date, tz_name="America/Los_Angeles")

    assert seen_tz_names == ["America/Los_Angeles", "America/Los_Angeles"]


@pytest.mark.smoke
def test_upsert_system_metrics_forwards_completeness_threshold(
    db_session, sample_routes, monkeypatch
):
    """NOTES-100: ``completeness_threshold`` reaches the completeness guard.
    WMATA's flat 80% is unreachable for an agency whose collector cadence
    doesn't poll every feed on every tick (SFMTA) -- callers must be able
    to override it, and the override must actually flow through, not just
    be accepted and dropped.
    """
    seen_thresholds = []

    def _fake_is_complete(db, service_date, threshold=0.80, tz_name="America/New_York"):
        seen_thresholds.append(threshold)
        return True

    monkeypatch.setattr(
        "src.data_completeness.coverage_pct_for_date", lambda db, service_date, tz_name=None: 1.0
    )
    monkeypatch.setattr("src.data_completeness.is_date_sufficiently_complete", _fake_is_complete)

    target_date = eastern_today() - timedelta(days=6)
    upsert_system_metrics_for_date(db_session, target_date, completeness_threshold=0.5333)

    assert seen_thresholds == [0.5333]


@pytest.mark.smoke
def test_upsert_system_metrics_default_threshold_is_the_flat_constant(
    db_session, sample_routes, monkeypatch
):
    """Omitting completeness_threshold keeps today's WMATA behavior unchanged."""
    from src.data_completeness import MIN_COVERAGE_FOR_MATERIALIZATION

    seen_thresholds = []

    def _fake_is_complete(db, service_date, threshold=0.80, tz_name="America/New_York"):
        seen_thresholds.append(threshold)
        return True

    monkeypatch.setattr(
        "src.data_completeness.coverage_pct_for_date", lambda db, service_date, tz_name=None: 1.0
    )
    monkeypatch.setattr("src.data_completeness.is_date_sufficiently_complete", _fake_is_complete)

    target_date = eastern_today() - timedelta(days=7)
    upsert_system_metrics_for_date(db_session, target_date)

    assert seen_thresholds == [MIN_COVERAGE_FOR_MATERIALIZATION]


def test_compute_system_metrics_includes_swt_key_empty_db(db_session):
    """Empty DB: the metrics dict carries swt_seconds=None, never a missing key."""
    metrics = compute_system_metrics_for_date(db_session, datetime(2026, 8, 10).date())
    assert "swt_seconds" in metrics
    assert metrics["swt_seconds"] is None


def test_system_swt_computed_from_schedule_pool_alone(db_session, monkeypatch):
    """SWT is schedule-side only: with zero observed stop_events it is still
    computed from the frequent scheduled pool (EWT stays None)."""
    import api.aggregations as agg

    def _fake_sched(db, day_type, route_ids=None, gtfs_snapshot_id=None):
        # One frequent cell: two 600s headways -> SWT = (600²+600²)/(2·1200) = 300.
        return {"64": {(0, "S1", 8): [600.0, 600.0]}}

    monkeypatch.setattr(agg, "fetch_scheduled_cell_hours_for_routes", _fake_sched)
    monkeypatch.setattr(agg, "get_cell_hour_gate_sec", lambda route_id: 900)

    ewt, swt, bunching = agg._system_ewt_and_bunching_for_date(
        db_session, datetime(2026, 8, 10).date(), {}
    )
    assert ewt is None
    assert swt == pytest.approx(300.0)
    assert bunching is None


def test_upsert_persists_swt_seconds(db_session, monkeypatch):
    """The upsert writes swt_seconds on both the insert and update paths."""
    from src import system_metrics as sm

    def _fake_compute(db, service_date, gtfs_snapshot_id=None, tz_name="America/New_York"):
        return {
            "otp_percentage": 70.0,
            "service_delivered_ratio": 0.9,
            "ewt_seconds": 150.0,
            "swt_seconds": 300.0,
            "bunching_rate": 0.03,
        }

    monkeypatch.setattr(sm, "compute_system_metrics_for_date", _fake_compute)
    monkeypatch.setattr(
        "src.data_completeness.coverage_pct_for_date",
        lambda db, service_date, tz_name="America/New_York": 1.0,
    )
    monkeypatch.setattr(
        "src.data_completeness.is_date_sufficiently_complete",
        lambda db, service_date, threshold=0.80, tz_name="America/New_York": True,
    )

    upsert_system_metrics_for_date(db_session, datetime(2026, 8, 10).date())
    row = db_session.query(SystemMetricsDaily).filter_by(service_date="2026-08-10").one()
    assert row.swt_seconds == 300.0

    # Update path: change the value, re-run, confirm overwrite.
    def _fake_compute_2(db, service_date, gtfs_snapshot_id=None, tz_name="America/New_York"):
        out = _fake_compute(db, service_date)
        out["swt_seconds"] = 280.0
        return out

    monkeypatch.setattr(sm, "compute_system_metrics_for_date", _fake_compute_2)
    upsert_system_metrics_for_date(db_session, datetime(2026, 8, 10).date())
    row = db_session.query(SystemMetricsDaily).filter_by(service_date="2026-08-10").one()
    assert row.swt_seconds == 280.0
