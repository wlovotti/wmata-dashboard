"""API honesty for dates whose stop-level rows were pruned (issue #265).

Seeds runs/stop_events for an old kept "island" (ISLAND, e.g. a partial
date retention refused to prune) and for recent dates (FLOOR..yesterday),
plus observed daily aggregates for a pruned date in between (PRUNED: a
`system_metrics_daily` row and a `route_metrics_daily_overlay` row, no
runs). Endpoints reading stop-level rows must say so instead of returning
silent nulls/zeros — and the kept island must not mask the gap.
"""

from datetime import timedelta

import pytest

from src.models import RouteMetricsDailyOverlay, Run, StopEvent, SystemMetricsDaily
from src.timezones import eastern_today

pytestmark = [pytest.mark.smoke, pytest.mark.api]

TODAY = eastern_today()
FLOOR = TODAY - timedelta(days=5)
PRUNED = TODAY - timedelta(days=10)
ISLAND = TODAY - timedelta(days=12)


def _stop_level(session, d):
    """One run + one stop_event for ``d``."""
    session.add(
        Run(
            service_date=d.isoformat(),
            trip_id="t",
            route_id="R1",
            direction_id=0,
            source="proximity",
        )
    )
    session.add(
        StopEvent(
            service_date=d.isoformat(),
            trip_id="t",
            route_id="R1",
            direction_id=0,
            stop_id="s",
            stop_sequence=1,
            source="proximity",
        )
    )


@pytest.fixture
def seeded(db_session):
    """See the module docstring."""
    d = FLOOR
    while d < TODAY:
        _stop_level(db_session, d)
        d += timedelta(days=1)
    _stop_level(db_session, ISLAND)
    for d in (ISLAND, PRUNED):
        db_session.add(
            SystemMetricsDaily(
                service_date=d.isoformat(), otp_percentage=80.0, data_quality="complete"
            )
        )
    # An outage date: aggregates exist but nothing was observed — not "pruned".
    db_session.add(
        SystemMetricsDaily(
            service_date=(PRUNED - timedelta(days=1)).isoformat(),
            swt_seconds=300.0,
            service_delivered_ratio=0.0,
            data_quality="complete",
        )
    )
    db_session.add(
        RouteMetricsDailyOverlay(
            route_id="R1",
            service_date=PRUNED.isoformat(),
            day_type="weekday",
            otp_all_early=1,
            otp_all_on_time=8,
            otp_all_late=1,
            scheduled_trips=10,
            delivered_trips=9,
        )
    )
    db_session.commit()
    return db_session


def test_retention_endpoint(client, seeded, monkeypatch):
    """/api/retention reports the configured windows and the contiguous detail floor."""
    # CI has no SFMTA_DATABASE_URL; the client fixture routes every agency to the test DB.
    monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///unused_by_client_fixture")
    body = client.get("/api/retention").json()
    assert body["stop_level_days"] == 100 and body["raw_days"] == 7
    assert body["detail_retained_from"] == (PRUNED + timedelta(days=1)).isoformat()
    assert body["earliest_detail_date"] == ISLAND.isoformat()
    assert body["pruned_dates"] == 1
    sf = client.get("/api/retention?agency=sfmta").json()
    assert sf["stop_level_days"] == 35


def test_route_trend_falls_back_to_daily_aggregate(client, seeded):
    """Pruned dates use the overlay (official OTP / service-delivered) and say so."""
    body = client.get("/api/routes/R1/trend?metric=otp&days=30").json()
    rows = {r["date"]: r for r in body["trend_data"]}
    old = rows[PRUNED.isoformat()]
    assert old["otp_percentage"] == pytest.approx(80.0)
    assert old["detail_retained"] is False and old["source"] == "daily_aggregate"
    # The outage date and the kept island are computed live, unmarked.
    assert "detail_retained" not in rows[(PRUNED - timedelta(days=1)).isoformat()]
    assert "detail_retained" not in rows[ISLAND.isoformat()]
    assert "detail_retained" not in rows[FLOOR.isoformat()]
    assert body["retention"]["window_truncated"] is True

    sd = client.get("/api/routes/R1/trend?metric=service_delivered&days=30").json()
    old = {r["date"]: r for r in sd["trend_data"]}[PRUNED.isoformat()]
    assert old["service_delivered_ratio"] == pytest.approx(0.9)

    rider = client.get("/api/routes/R1/trend?metric=otp&days=30&otp_window=rider").json()
    old = {r["date"]: r for r in rider["trend_data"]}[PRUNED.isoformat()]
    assert old["otp_percentage"] is None and old["detail_retained"] is False

    excess = client.get("/api/routes/R1/trend?metric=excess_trip_time&days=30").json()
    old = {r["date"]: r for r in excess["trend_data"]}[PRUNED.isoformat()]
    assert old["excess_trip_time_pct"] is None and old["detail_retained"] is False


def test_route_trend_days_is_bounded(client, seeded):
    """The per-day loop no longer accepts an unbounded window."""
    body = client.get("/api/routes/R1/trend?metric=otp&days=100000").json()
    assert body["days"] == 400


def test_pooled_windows_report_truncation(client, seeded):
    """/stops and /bunching-causes carry a retention block; a window after the gap is whole."""
    body = client.get("/api/routes/R1/stops?days=30").json()
    assert body["retention"]["window_truncated"] is True
    assert body["retention"]["pruned_dates_in_window"] == 1
    body = client.get("/api/routes/R1/bunching-causes?days=3").json()
    assert body["retention"]["window_truncated"] is False


def test_block_views_return_410_for_pruned_dates(client, seeded):
    """Single-date block views refuse pruned dates explicitly; kept dates still work."""
    r = client.get(f"/api/blocks/active?service_date={PRUNED.isoformat()}")
    assert r.status_code == 410 and "not retained" in r.json()["detail"]
    r = client.get(f"/api/blocks/B1?service_date={PRUNED.isoformat()}")
    assert r.status_code == 410
    r = client.get(f"/api/routes/R1/blocks?service_date={PRUNED.isoformat()}")
    assert r.status_code == 410
    assert client.get(f"/api/blocks/active?service_date={ISLAND.isoformat()}").status_code == 200
    assert client.get(f"/api/blocks/active?service_date={FLOOR.isoformat()}").status_code == 200


def test_missing_run_mentions_pruning(client, seeded):
    """A missing run id 404s with the retention note once anything was pruned."""
    r = client.get("/api/runs/999999/deviations")
    assert r.status_code == 404 and "pruned" in r.json()["detail"]


def test_nothing_pruned_means_no_signal(client, db_session):
    """Without pruned dates nothing is truncated or marked."""
    body = client.get("/api/routes/R1/trend?metric=otp&days=7").json()
    assert body["retention"]["window_truncated"] is False
    assert all("detail_retained" not in r for r in body["trend_data"])
    assert client.get("/api/runs/1/deviations").json()["detail"] == "Run 1 not found"


def test_scorecard_and_route_detail_carry_retention(client, seeded):
    """Review S3: pooled scorecard / route-detail windows report pruned dates too."""
    body = client.get("/api/routes?days=30").json()
    assert body["retention"]["window_truncated"] is True
    assert body["retention"]["pruned_dates_in_window"] == 1
    body = client.get("/api/routes?days=7").json()
    assert body["retention"]["window_truncated"] is False


def test_retention_block_failure_never_breaks_the_endpoint(client, seeded, monkeypatch):
    """Review nit: a failing retention note is logged and the block omitted, not a 500."""
    import api.main

    def _boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(api.main, "retention_note", _boom)
    r = client.get("/api/routes?days=30")
    assert r.status_code == 200 and "retention" not in r.json()


def test_routes_window_uses_the_agency_service_day():
    """SFMTA's /api/routes window starts from Pacific "today", WMATA's from Eastern."""
    from api.main import _agency_today
    from src.timezones import local_today

    assert _agency_today("sfmta") == local_today("America/Los_Angeles")
    assert _agency_today("wmata") == local_today("America/New_York")
