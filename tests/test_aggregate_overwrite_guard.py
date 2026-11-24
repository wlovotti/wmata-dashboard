"""Kept daily aggregates are never overwritten from pruned (absent) stop-level rows.

Retention (src/retention.py) keeps `system_metrics_daily`,
`route_metrics_daily_overlay` and `route_headway_metrics` forever while
pruning the `runs`/`stop_events` they were computed from; re-running a
per-date rollup for such a date must refuse instead of writing empty values.
"""

from datetime import date

import pytest

from src.models import (
    RouteHeadwayMetrics,
    RouteMetricsDailyOverlay,
    Run,
    StopEvent,
    SystemMetricsDaily,
)
from src.system_metrics import AggregateOverwriteRefused

D = date(2026, 6, 1)


def _complete(monkeypatch):
    """Bypass the ingest-coverage guard (these tests seed derived tables directly)."""
    monkeypatch.setattr(
        "src.data_completeness.resolve_data_quality",
        lambda *args, **kwargs: ("complete", 1.0, False),
    )


def _seed_stop_level(session, d=D, sources=("proximity", "trip_update")):
    """One run + one stop_event per source in ``sources`` for ``d``."""
    session.add(
        Run(
            service_date=d.isoformat(),
            trip_id="t",
            route_id="R1",
            direction_id=0,
            source="proximity",
        )
    )
    for source in sources:
        session.add(
            StopEvent(
                service_date=d.isoformat(),
                trip_id="t",
                route_id="R1",
                direction_id=0,
                stop_id="s",
                stop_sequence=1,
                source=source,
                deviation_sec=0,
            )
        )
    session.commit()


@pytest.mark.smoke
def test_system_metrics_refuses_without_stop_level_rows(db_session, monkeypatch):
    """Existing observed row + no runs/stop_events → refusal, row untouched."""
    from src.system_metrics import upsert_system_metrics_for_date

    _complete(monkeypatch)
    db_session.add(
        SystemMetricsDaily(service_date=D.isoformat(), otp_percentage=81.0, data_quality="complete")
    )
    db_session.commit()
    with pytest.raises(AggregateOverwriteRefused, match="pull-and-derive.sh --date 2026-06-01"):
        upsert_system_metrics_for_date(db_session, D)
    assert db_session.get(SystemMetricsDaily, D.isoformat()).otp_percentage == 81.0


@pytest.mark.smoke
def test_system_metrics_force_overwrites(db_session, monkeypatch):
    """``force=True`` is the explicit escape hatch."""
    from src.system_metrics import upsert_system_metrics_for_date

    _complete(monkeypatch)
    db_session.add(
        SystemMetricsDaily(service_date=D.isoformat(), otp_percentage=81.0, data_quality="complete")
    )
    db_session.commit()
    upsert_system_metrics_for_date(db_session, D, force=True)
    assert db_session.get(SystemMetricsDaily, D.isoformat()).otp_percentage is None


@pytest.mark.smoke
def test_system_metrics_normal_rederive_path_still_works(db_session, monkeypatch):
    """With stop-level rows present (refresh reloaded them) the upsert proceeds."""
    from src.system_metrics import upsert_system_metrics_for_date

    _complete(monkeypatch)
    db_session.add(
        SystemMetricsDaily(service_date=D.isoformat(), otp_percentage=50.0, data_quality="complete")
    )
    db_session.commit()
    _seed_stop_level(db_session)
    written = upsert_system_metrics_for_date(db_session, D)
    assert written["otp_percentage"] == 100.0


@pytest.mark.smoke
def test_system_metrics_no_data_day_with_schedule_only_row_is_not_refused(db_session, monkeypatch):
    """A genuine no-data date (only schedule-side SWT, SD 0) recomputes freely."""
    from src.system_metrics import upsert_system_metrics_for_date

    _complete(monkeypatch)
    db_session.add(
        SystemMetricsDaily(
            service_date=D.isoformat(),
            swt_seconds=300.0,
            service_delivered_ratio=0.0,
            data_quality="complete",
        )
    )
    db_session.commit()
    assert upsert_system_metrics_for_date(db_session, D) is not None


@pytest.mark.smoke
def test_overlay_refuses_without_stop_level_rows(db_session, monkeypatch):
    """Existing overlay rows with observed counts + no stop-level rows → refusal."""
    from src.route_metrics_overlay import upsert_route_metrics_for_date

    _complete(monkeypatch)
    db_session.add(
        RouteMetricsDailyOverlay(
            route_id="R1",
            service_date=D.isoformat(),
            day_type="weekday",
            scheduled_trips=10,
            delivered_trips=9,
        )
    )
    db_session.commit()
    with pytest.raises(AggregateOverwriteRefused):
        upsert_route_metrics_for_date(db_session, D)
    assert upsert_route_metrics_for_date(db_session, D, force=True) == 0


@pytest.mark.smoke
def test_overlay_schedule_only_rows_are_not_refused(db_session, monkeypatch):
    """Rows with only schedule-side numbers (nothing observed) don't block a recompute."""
    from src.route_metrics_overlay import upsert_route_metrics_for_date

    _complete(monkeypatch)
    db_session.add(
        RouteMetricsDailyOverlay(
            route_id="R1", service_date=D.isoformat(), day_type="weekday", scheduled_trips=10
        )
    )
    db_session.commit()
    assert upsert_route_metrics_for_date(db_session, D) == 0


@pytest.mark.smoke
def test_compute_bunching_refused_dates(db_session):
    """route_headway_metrics with observed headways and no stop-level rows is refused."""
    from pipelines.compute_bunching import refused_dates

    other = date(2026, 6, 2)
    for d in (D, other):
        db_session.add(
            RouteHeadwayMetrics(
                route_id="R1",
                date=d.isoformat(),
                time_period="am_peak",
                day_type="weekday",
                total_headways=4,
            )
        )
    db_session.commit()
    _seed_stop_level(db_session, other)
    third = date(2026, 6, 3)
    db_session.add(
        RouteHeadwayMetrics(
            route_id="R1",
            date=third.isoformat(),
            time_period="am_peak",
            day_type="weekday",
            total_headways=4,
        )
    )
    db_session.commit()
    _seed_stop_level(db_session, third, sources=("proximity",))  # bunching needs trip_update
    assert refused_dates(db_session, [D, other, third]) == [D, third]


@pytest.mark.smoke
def test_wrapper_cli_exits_1_on_refusal(db_session, monkeypatch, capsys):
    """The pipeline wrapper turns the refusal into exit 1 with a clear message."""
    import pipelines.upsert_system_metrics_daily as mod

    _complete(monkeypatch)
    db_session.add(
        SystemMetricsDaily(service_date=D.isoformat(), otp_percentage=81.0, data_quality="complete")
    )
    db_session.commit()

    class _NoClose:
        """Proxy that keeps the test session open when the CLI closes it."""

        def __init__(self, s):
            self._s = s

        def __getattr__(self, name):
            return getattr(self._s, name)

        def close(self):
            """No-op."""

    monkeypatch.setattr(mod, "get_session", lambda db_url=None: _NoClose(db_session))
    monkeypatch.setattr("sys.argv", ["x", "--date", D.isoformat()])
    assert mod.main() == 1
    assert "REFUSED" in capsys.readouterr().err
    assert db_session.get(SystemMetricsDaily, D.isoformat()).otp_percentage == 81.0


@pytest.mark.smoke
def test_tu_only_rederive_cannot_null_proximity_otp(db_session, monkeypatch):
    """Review S1: after VM-era VP is gone, a TU-only re-derive recreates runs + trip_update
    rows; the kept (proximity-based) OTP must not be replaced with NULL."""
    from src.route_metrics_overlay import upsert_route_metrics_for_date
    from src.system_metrics import upsert_system_metrics_for_date

    _complete(monkeypatch)
    db_session.add(
        SystemMetricsDaily(service_date=D.isoformat(), otp_percentage=81.0, data_quality="complete")
    )
    db_session.add(
        RouteMetricsDailyOverlay(
            route_id="R1",
            service_date=D.isoformat(),
            day_type="weekday",
            otp_all_on_time=5,
        )
    )
    db_session.commit()
    _seed_stop_level(db_session, sources=("trip_update",))
    with pytest.raises(AggregateOverwriteRefused, match="proximity"):
        upsert_system_metrics_for_date(db_session, D)
    with pytest.raises(AggregateOverwriteRefused, match="proximity"):
        upsert_route_metrics_for_date(db_session, D)
    assert db_session.get(SystemMetricsDaily, D.isoformat()).otp_percentage == 81.0


@pytest.mark.smoke
def test_ewt_requires_trip_update_rows(db_session, monkeypatch):
    """A kept EWT/bunching value needs trip_update rows, not just proximity ones."""
    from src.system_metrics import upsert_system_metrics_for_date

    _complete(monkeypatch)
    db_session.add(
        SystemMetricsDaily(service_date=D.isoformat(), ewt_seconds=120.0, data_quality="complete")
    )
    db_session.commit()
    _seed_stop_level(db_session, sources=("proximity",))
    with pytest.raises(AggregateOverwriteRefused, match="trip_update"):
        upsert_system_metrics_for_date(db_session, D)
