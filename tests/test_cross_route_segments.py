"""
Tests for the cross-route segment diagnostic (NOTES-59).

Covers:
  - ``get_cross_route_segments`` aggregation function (empty table case,
    populated case with ≥2-route filter, period filter pass-through)
  - ``refresh_cross_route_segments`` pipeline (build_rollup logic against
    synthetic ``route_diagnostic_segment`` rows, ≥2-route gate)
  - ``/api/segments`` HTTP endpoint (smoke — valid period returns 200,
    invalid period returns 400)

The test DB is SQLite in-memory (db_session fixture from conftest.py);
no Postgres-specific SQL is used in the new code paths under test.
"""

import json

import pytest

from api.aggregations import get_cross_route_segments
from src.models import (
    CrossRouteSegmentRollup,
    Route,
    RouteDiagnosticSegment,
    Stop,
)
from src.timezones import utcnow_naive

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_stop(db, stop_id, stop_name, is_current=True):
    """Insert a minimal Stop row and flush."""
    s = Stop(
        stop_id=stop_id,
        stop_name=stop_name,
        is_current=is_current,
        stop_lat=38.9,
        stop_lon=-77.0,
    )
    db.add(s)
    db.flush()
    return s


def _make_route(db, route_id, short_name="R1", is_current=True):
    """Insert a minimal Route row and flush."""
    from src.models import Agency

    # Ensure a parent agency exists so the FK is satisfied (SQLite enforces
    # FK if the engine has PRAGMA foreign_keys = ON; even without enforcement
    # we need the agency row for the route_name_map query).
    agency = db.query(Agency).filter_by(agency_id="WMATA").first()
    if agency is None:
        agency = Agency(agency_id="WMATA", agency_name="WMATA")
        db.add(agency)
        db.flush()

    r = Route(
        route_id=route_id,
        route_short_name=short_name,
        route_long_name=f"Route {route_id}",
        is_current=is_current,
        agency_id="WMATA",
    )
    db.add(r)
    db.flush()
    return r


def _make_diag_segment(
    db,
    route_id,
    direction_id=0,
    period="all",
    from_stop_id="S1",
    to_stop_id="S2",
    from_seq=1,
    to_seq=2,
    mean_slip_sec=60.0,
    n_observations=100,
):
    """Insert a minimal RouteDiagnosticSegment row and flush."""
    s = RouteDiagnosticSegment(
        route_id=route_id,
        direction_id=direction_id,
        period=period,
        from_seq=from_seq,
        from_stop_id=from_stop_id,
        to_seq=to_seq,
        to_stop_id=to_stop_id,
        mean_slip_sec=mean_slip_sec,
        cum_slip_sec=mean_slip_sec,
        n_observations=n_observations,
        is_timepoint=False,
        computed_at=utcnow_naive(),
    )
    db.add(s)
    db.flush()
    return s


def _make_rollup(
    db,
    from_stop_id="S1",
    to_stop_id="S2",
    period="all",
    total_weighted_slip_sec=6000.0,
    n_routes=2,
    n_route_directions=2,
    n_total_observations=100,
    contributing_routes_json=None,
    peak_period=None,
):
    """Insert a CrossRouteSegmentRollup row and flush."""
    if contributing_routes_json is None:
        contributing_routes_json = json.dumps(
            [
                {
                    "route_id": "R1",
                    "route_short_name": "R1",
                    "direction_id": 0,
                    "mean_slip_sec": 60.0,
                    "n_observations": 100,
                },
            ]
        )
    row = CrossRouteSegmentRollup(
        from_stop_id=from_stop_id,
        to_stop_id=to_stop_id,
        period=period,
        total_weighted_slip_sec=total_weighted_slip_sec,
        n_routes=n_routes,
        n_route_directions=n_route_directions,
        n_total_observations=n_total_observations,
        contributing_routes_json=contributing_routes_json,
        peak_period=peak_period,
        computed_at=utcnow_naive(),
    )
    db.add(row)
    db.flush()
    return row


# ---------------------------------------------------------------------------
# get_cross_route_segments — aggregation function
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_get_cross_route_segments_empty_returns_zero_rows(db_session):
    """Empty rollup table → n_rows=0 with the right envelope."""
    result = get_cross_route_segments(db_session, period="all")
    assert result["n_rows"] == 0
    assert result["segments"] == []
    assert result["period"] == "all"
    assert result["lookback_days"] == 30


@pytest.mark.smoke
def test_get_cross_route_segments_returns_rows_with_stop_names(db_session):
    """Populated rollup + stop rows → stop_name joined correctly."""
    _make_stop(db_session, "S1", "First & Main St NW")
    _make_stop(db_session, "S2", "Second & Oak Ave NW")
    contributing = [
        {
            "route_id": "R1",
            "route_short_name": "R1",
            "direction_id": 0,
            "mean_slip_sec": 70.0,
            "n_observations": 80,
        },
        {
            "route_id": "R2",
            "route_short_name": "R2",
            "direction_id": 0,
            "mean_slip_sec": 50.0,
            "n_observations": 60,
        },
    ]
    _make_rollup(
        db_session,
        from_stop_id="S1",
        to_stop_id="S2",
        period="all",
        total_weighted_slip_sec=70.0 * 80 + 50.0 * 60,
        n_routes=2,
        n_route_directions=2,
        n_total_observations=140,
        contributing_routes_json=json.dumps(contributing),
        peak_period="pm_peak",
    )

    result = get_cross_route_segments(db_session, period="all")
    assert result["n_rows"] == 1
    seg = result["segments"][0]
    assert seg["from_stop_id"] == "S1"
    assert seg["from_stop_name"] == "First & Main St NW"
    assert seg["to_stop_id"] == "S2"
    assert seg["to_stop_name"] == "Second & Oak Ave NW"
    assert seg["n_routes"] == 2
    assert seg["n_total_observations"] == 140
    assert seg["peak_period"] == "pm_peak"
    # slip_min_per_trip = total / n_obs / 60
    expected_slip = (70.0 * 80 + 50.0 * 60) / 140 / 60
    assert abs(seg["slip_min_per_trip"] - expected_slip) < 0.001
    assert len(seg["contributing_routes"]) == 2


@pytest.mark.smoke
def test_get_cross_route_segments_period_filter(db_session):
    """Only rows matching the requested period are returned."""
    _make_rollup(db_session, from_stop_id="SA", to_stop_id="SB", period="all")
    _make_rollup(
        db_session,
        from_stop_id="SA",
        to_stop_id="SB",
        period="pm_peak",
        total_weighted_slip_sec=9999.0,
    )

    result_all = get_cross_route_segments(db_session, period="all")
    result_pm = get_cross_route_segments(db_session, period="pm_peak")
    assert result_all["n_rows"] == 1
    assert result_pm["n_rows"] == 1
    assert result_pm["segments"][0]["total_weighted_slip_sec"] == 9999.0


@pytest.mark.smoke
def test_get_cross_route_segments_limit_cap(db_session):
    """Limit parameter caps the result set."""
    for i in range(5):
        _make_rollup(
            db_session,
            from_stop_id=f"X{i}",
            to_stop_id=f"Y{i}",
            period="all",
            total_weighted_slip_sec=float(i * 1000),
        )
    result = get_cross_route_segments(db_session, period="all", limit=3)
    assert len(result["segments"]) == 3


# ---------------------------------------------------------------------------
# Pipeline logic — build_rollup (unit-level, no DB)
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_build_rollup_filters_single_route_pairs(db_session):
    """Stop-pairs with only 1 distinct route are excluded from the rollup."""
    from pipelines.refresh_cross_route_segments import _build_rollup

    _make_route(db_session, "R1", short_name="R1")
    _make_diag_segment(
        db_session,
        route_id="R1",
        from_stop_id="A",
        to_stop_id="B",
        period="all",
        mean_slip_sec=90.0,
        n_observations=100,
    )

    rows = _build_rollup(db_session, "all")
    assert all(r["n_routes"] >= 2 for r in rows), "Single-route pair should have been filtered out"


@pytest.mark.smoke
def test_build_rollup_aggregates_two_route_pair(db_session):
    """Two routes sharing the same stop-pair produce one output row."""
    from pipelines.refresh_cross_route_segments import MIN_ROUTES_PER_PAIR, _build_rollup

    _make_route(db_session, "R3", short_name="R3")
    _make_route(db_session, "R4", short_name="R4")
    _make_diag_segment(
        db_session,
        route_id="R3",
        from_stop_id="C",
        to_stop_id="D",
        period="all",
        mean_slip_sec=60.0,
        n_observations=200,
    )
    _make_diag_segment(
        db_session,
        route_id="R4",
        from_stop_id="C",
        to_stop_id="D",
        period="all",
        mean_slip_sec=30.0,
        n_observations=100,
    )

    rows = _build_rollup(db_session, "all")
    # Filter to the (C, D) pair only
    cd_rows = [r for r in rows if r["from_stop_id"] == "C" and r["to_stop_id"] == "D"]
    assert len(cd_rows) == 1, "Expected exactly one aggregated row for (C, D)"
    row = cd_rows[0]
    assert row["n_routes"] >= MIN_ROUTES_PER_PAIR
    expected_weighted = 60.0 * 200 + 30.0 * 100
    assert abs(row["total_weighted_slip_sec"] - expected_weighted) < 0.01
    assert row["n_total_observations"] == 300


# ---------------------------------------------------------------------------
# /api/segments endpoint — smoke via TestClient
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_api_segments_empty_returns_200(client):
    """GET /api/segments returns 200 even when the rollup table is empty."""
    resp = client.get("/api/segments?period=all")
    assert resp.status_code == 200
    body = resp.json()
    assert "segments" in body
    assert "n_rows" in body
    assert "period" in body


@pytest.mark.smoke
def test_api_segments_invalid_period_returns_400(client):
    """GET /api/segments with an invalid period returns 400."""
    resp = client.get("/api/segments?period=not_a_period")
    assert resp.status_code == 400


@pytest.mark.smoke
def test_api_segments_valid_periods(client):
    """All valid period values return 200."""
    for period in ("all", "am_peak", "midday", "pm_peak", "evening", "late"):
        resp = client.get(f"/api/segments?period={period}")
        assert resp.status_code == 200, f"Expected 200 for period={period}, got {resp.status_code}"
