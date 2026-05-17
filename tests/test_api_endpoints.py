"""
API endpoint tests for WMATA Dashboard

Tests all REST API endpoints with various scenarios:
- Success cases (200 OK)
- Not found cases (404)
- Bad request cases (400)
- Edge cases (empty data, query parameters)

Run with: pytest -m api
"""

import pytest

from src.models import Route, RouteDiagnosticSegment, Shape, Stop, Trip


@pytest.mark.api
def test_root_endpoint(client):
    """Test API root endpoint returns health check"""
    response = client.get("/")
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "ok"
    assert data["name"] == "WMATA Performance API"
    assert "version" in data


@pytest.mark.api
def test_get_routes_success(client, sample_route):
    """Test GET /api/routes returns `{window, routes}` scorecard shape."""
    response = client.get("/api/routes")
    assert response.status_code == 200

    data = response.json()
    assert "window" in data
    assert data["window"]["days"] == 7
    assert isinstance(data["routes"], list)
    assert len(data["routes"]) == 1

    route = data["routes"][0]
    assert route["route_id"] == "TEST1"
    assert route["route_name"] == "T1"
    assert "frequency_class" in route
    # Live overlay keys are surfaced (values may be None without seeded data).
    assert "otp_all_pct" in route
    assert "service_delivered_ratio" in route
    assert "ewt_seconds" in route
    assert "bunching_rate" in route


@pytest.mark.api
def test_get_routes_empty_database(client):
    """Test GET /api/routes with no data returns empty `routes` list with window metadata."""
    response = client.get("/api/routes")
    assert response.status_code == 200
    body = response.json()
    assert body["routes"] == []
    assert body["window"]["days"] == 7


@pytest.mark.api
def test_get_routes_with_days_parameter(client, sample_route):
    """Test GET /api/routes with days query parameter sets window length."""
    response = client.get("/api/routes?days=14")
    assert response.status_code == 200

    data = response.json()
    assert data["window"]["days"] == 14
    assert isinstance(data["routes"], list)


@pytest.mark.api
def test_get_route_success(client, sample_route):
    """Test GET /api/routes/{route_id} returns identity + filter echoes + overlay."""
    response = client.get("/api/routes/TEST1")
    assert response.status_code == 200

    data = response.json()
    assert data["route_id"] == "TEST1"
    assert data["route_name"] == "T1"
    assert data["route_long_name"] == "Test Route 1"
    assert data["day_type_filter"] == "all"
    assert data["period_key"] == "all"
    assert "otp_all_pct" in data
    assert "excess_trip_time_pct" in data


@pytest.mark.api
def test_get_route_not_found(client):
    """Test GET /api/routes/{route_id} with non-existent route returns 404"""
    response = client.get("/api/routes/NONEXISTENT")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.api
def test_get_route_with_days_parameter(client, sample_route):
    """Test GET /api/routes/{route_id} with days query parameter"""
    response = client.get("/api/routes/TEST1?days=14")
    assert response.status_code == 200
    assert response.json()["route_id"] == "TEST1"


@pytest.mark.api
def test_get_route_trend_success(client, sample_route):
    """Test GET /api/routes/{route_id}/trend returns time-series data"""
    response = client.get("/api/routes/TEST1/trend?metric=otp")
    assert response.status_code == 200

    data = response.json()
    assert data["route_id"] == "TEST1"
    assert data["metric"] == "otp"
    assert "trend_data" in data
    assert isinstance(data["trend_data"], list)

    if len(data["trend_data"]) > 0:
        trend_point = data["trend_data"][0]
        assert "date" in trend_point
        assert "otp_percentage" in trend_point


@pytest.mark.api
def test_get_route_trend_all_metrics(client, sample_route):
    """Test GET /api/routes/{route_id}/trend with all surviving valid metrics"""
    valid_metrics = ["otp", "service_delivered", "excess_trip_time"]

    for metric in valid_metrics:
        response = client.get(f"/api/routes/TEST1/trend?metric={metric}")
        assert response.status_code == 200

        data = response.json()
        assert data["metric"] == metric


@pytest.mark.api
def test_get_route_trend_invalid_metric(client, sample_route):
    """Test GET /api/routes/{route_id}/trend with invalid metric returns 400"""
    response = client.get("/api/routes/TEST1/trend?metric=invalid_metric")
    assert response.status_code == 400
    assert "Invalid metric" in response.json()["detail"]


@pytest.mark.api
def test_get_route_trend_legacy_metric_rejected(client, sample_route):
    """Legacy metrics dropped in NOTES-19 cleanup return 400."""
    for legacy in ("early", "late", "headway", "headway_std_dev", "speed"):
        response = client.get(f"/api/routes/TEST1/trend?metric={legacy}")
        assert response.status_code == 400


@pytest.mark.api
def test_get_route_trend_with_days_parameter(client, sample_route):
    """Test GET /api/routes/{route_id}/trend with days parameter"""
    response = client.get("/api/routes/TEST1/trend?metric=otp&days=60")
    assert response.status_code == 200
    assert response.json()["days"] == 60


@pytest.mark.api
def test_get_route_time_periods_success(client, sample_route):
    """Test GET /api/routes/{route_id}/time-periods returns performance by time of day"""
    response = client.get("/api/routes/TEST1/time-periods")
    assert response.status_code == 200

    data = response.json()
    assert data["route_id"] == "TEST1"
    assert "periods" in data
    assert isinstance(data["periods"], dict)


@pytest.mark.api
def test_get_route_time_periods_with_days(client, sample_route):
    """Test GET /api/routes/{route_id}/time-periods with days parameter"""
    response = client.get("/api/routes/TEST1/time-periods?days=14")
    assert response.status_code == 200
    assert response.json()["days"] == 14


@pytest.mark.api
def test_get_route_period_drilldown_success(client, sample_route):
    """Test GET /api/routes/{route_id}/period-drilldown returns the expected envelope.

    With no stop_events derived in the test DB, the endpoint anchors on no
    service_date and returns empty period lists — but the response shape
    should still be the documented one.
    """
    response = client.get("/api/routes/TEST1/period-drilldown")
    assert response.status_code == 200

    data = response.json()
    assert data["route_id"] == "TEST1"
    assert data["service_date"] is None
    assert data["day_type"] is None
    assert data["ewt"] == []
    assert data["bunching"] == []


@pytest.mark.api
def test_get_route_period_drilldown_not_found(client):
    """Test GET /api/routes/{route_id}/period-drilldown 404s for unknown routes."""
    response = client.get("/api/routes/NONEXISTENT/period-drilldown")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.api
def test_get_route_bunching_causes_empty(client, sample_route):
    """GET /api/routes/{route_id}/bunching-causes returns the breakdown envelope.

    With no stop_events seeded the result is a zero-pair payload — but the
    five categories should all be present in the breakdown dict so the
    frontend can render zero-height bars without conditional shape checks.
    """
    response = client.get("/api/routes/TEST1/bunching-causes")
    assert response.status_code == 200

    data = response.json()
    assert data["route_id"] == "TEST1"
    assert data["days"] == 30
    assert data["day_type"] == "all"
    assert data["period"] == "all"
    assert data["n_bunched_pairs"] == 0
    breakdown = data["breakdown"]
    assert set(breakdown.keys()) == {
        "leader_late_only",
        "trailer_early_only",
        "both_off",
        "neither_off",
        "unknown",
    }
    for cat in breakdown.values():
        assert cat["count"] == 0
        assert cat["pct"] == 0.0


@pytest.mark.api
def test_get_route_bunching_causes_invalid_day_type(client, sample_route):
    """Invalid day_type returns 400 with the validation error detail."""
    response = client.get("/api/routes/TEST1/bunching-causes?day_type=funkyday")
    assert response.status_code == 400
    assert "day_type" in response.json()["detail"].lower()


@pytest.mark.api
def test_get_route_bunching_causes_invalid_period(client, sample_route):
    """Invalid period returns 400 with the validation error detail."""
    response = client.get("/api/routes/TEST1/bunching-causes?period=earlybird")
    assert response.status_code == 400
    assert "period" in response.json()["detail"].lower()


@pytest.mark.api
def test_get_route_shapes_success(client, db_session, sample_route, sample_trip):
    """Test GET /api/routes/{route_id}/shapes returns GTFS shapes"""
    # Create shape data
    shapes = []
    for i in range(5):
        shape = Shape(
            shape_id="SHAPE_TEST1",
            shape_pt_lat=38.9072 + (i * 0.001),
            shape_pt_lon=-77.0369 + (i * 0.001),
            shape_pt_sequence=i,
        )
        shapes.append(shape)

    db_session.add_all(shapes)
    # Link trip to shape
    sample_trip.shape_id = "SHAPE_TEST1"
    db_session.commit()

    response = client.get("/api/routes/TEST1/shapes")
    assert response.status_code == 200

    data = response.json()
    assert data["route_id"] == "TEST1"
    assert "shapes" in data
    assert isinstance(data["shapes"], list)
    assert len(data["shapes"]) == 1

    shape_data = data["shapes"][0]
    assert shape_data["shape_id"] == "SHAPE_TEST1"
    assert "points" in shape_data
    assert len(shape_data["points"]) == 5

    # Verify point structure
    point = shape_data["points"][0]
    assert "lat" in point
    assert "lon" in point


@pytest.mark.api
def test_get_route_shapes_no_shapes(client, sample_route):
    """Test GET /api/routes/{route_id}/shapes with no shape data returns 404"""
    response = client.get("/api/routes/TEST1/shapes")
    assert response.status_code == 404
    assert "No shapes found" in response.json()["detail"]


@pytest.mark.api
def test_get_route_shapes_multiple_shapes(client, db_session, sample_route):
    """Test GET /api/routes/{route_id}/shapes with multiple shape variants"""
    # Create two different shape variants for the route
    for shape_num in [1, 2]:
        trip = Trip(
            trip_id=f"TRIP_TEST_{shape_num}",
            route_id=sample_route.route_id,
            service_id="WEEKDAY",
            direction_id=shape_num - 1,
            trip_headsign=f"Direction {shape_num}",
            shape_id=f"SHAPE_TEST{shape_num}",
        )
        db_session.add(trip)

        # Add shape points
        for i in range(3):
            shape = Shape(
                shape_id=f"SHAPE_TEST{shape_num}",
                shape_pt_lat=38.9072 + (i * 0.001 * shape_num),
                shape_pt_lon=-77.0369 + (i * 0.001 * shape_num),
                shape_pt_sequence=i,
            )
            db_session.add(shape)

    db_session.commit()

    response = client.get("/api/routes/TEST1/shapes")
    assert response.status_code == 200

    data = response.json()
    assert len(data["shapes"]) == 2
    assert data["shapes"][0]["shape_id"] in ["SHAPE_TEST1", "SHAPE_TEST2"]


# ---------------------------------------------------------------------------
# /api/schedule-audit (NOTES-60)
# ---------------------------------------------------------------------------


def _seed_schedule_audit_fixture(db_session):
    """Seed two routes + four segments spanning under-/over-padded slip.

    Layout:
      ROUTE_A (direction 0), period=all:
        - SEG1 (stop1 -> stop2): mean_slip_sec=+120, n=300  (under-padded, high vol)
        - SEG2 (stop2 -> stop3): mean_slip_sec=-30,  n=300  (over-padded, low magnitude)
      ROUTE_B (direction 1), period=all:
        - SEG3 (stop4 -> stop5): mean_slip_sec=+60,  n=600  (under-padded, biggest leverage)
        - SEG4 (stop5 -> stop6): mean_slip_sec=-90,  n=200  (over-padded)
      ROUTE_A (direction 0), period=am_peak:
        - SEG5 (stop1 -> stop2): mean_slip_sec=+200, n=100  (peak-only row)
    """
    # Routes
    db_session.add_all(
        [
            Route(
                route_id="ROUTE_A",
                route_short_name="A1",
                route_long_name="Test Route A",
                route_type=3,
                is_current=True,
            ),
            Route(
                route_id="ROUTE_B",
                route_short_name="B1",
                route_long_name="Test Route B",
                route_type=3,
                is_current=True,
            ),
        ]
    )
    # Stops
    for i in range(1, 7):
        db_session.add(
            Stop(
                stop_id=f"stop{i}",
                stop_name=f"Stop {i}",
                stop_lat=38.9 + 0.001 * i,
                stop_lon=-77.0 - 0.001 * i,
                is_current=True,
            )
        )
    # Diagnostic segment rows
    db_session.add_all(
        [
            # ROUTE_A direction 0 — all-day
            RouteDiagnosticSegment(
                route_id="ROUTE_A",
                direction_id=0,
                period="all",
                from_seq=1,
                from_stop_id="stop1",
                to_seq=2,
                to_stop_id="stop2",
                mean_slip_sec=120.0,
                cum_slip_sec=120.0,
                n_observations=300,
                is_timepoint=False,
            ),
            RouteDiagnosticSegment(
                route_id="ROUTE_A",
                direction_id=0,
                period="all",
                from_seq=2,
                from_stop_id="stop2",
                to_seq=3,
                to_stop_id="stop3",
                mean_slip_sec=-30.0,
                cum_slip_sec=90.0,
                n_observations=300,
                is_timepoint=False,
            ),
            # ROUTE_B direction 1 — all-day
            RouteDiagnosticSegment(
                route_id="ROUTE_B",
                direction_id=1,
                period="all",
                from_seq=1,
                from_stop_id="stop4",
                to_seq=2,
                to_stop_id="stop5",
                mean_slip_sec=60.0,
                cum_slip_sec=60.0,
                n_observations=600,
                is_timepoint=False,
            ),
            RouteDiagnosticSegment(
                route_id="ROUTE_B",
                direction_id=1,
                period="all",
                from_seq=2,
                from_stop_id="stop5",
                to_seq=3,
                to_stop_id="stop6",
                mean_slip_sec=-90.0,
                cum_slip_sec=-30.0,
                n_observations=200,
                is_timepoint=False,
            ),
            # ROUTE_A direction 0 — am_peak only
            RouteDiagnosticSegment(
                route_id="ROUTE_A",
                direction_id=0,
                period="am_peak",
                from_seq=1,
                from_stop_id="stop1",
                to_seq=2,
                to_stop_id="stop2",
                mean_slip_sec=200.0,
                cum_slip_sec=200.0,
                n_observations=100,
                is_timepoint=False,
            ),
        ]
    )
    db_session.commit()


@pytest.mark.api
def test_schedule_audit_empty_database(client):
    """Empty DB returns the documented shape with zero segments."""
    response = client.get("/api/schedule-audit")
    assert response.status_code == 200
    data = response.json()
    assert data["period"] == "all"
    assert data["sign"] == "all"
    assert data["lookback_days"] == 30
    assert data["n_rows"] == 0
    assert data["segments"] == []


@pytest.mark.api
def test_schedule_audit_default_sort_by_abs_minutes_per_day(client, db_session):
    """Default sort is absolute slip × daily trips. Biggest leverage first.

    ROUTE_B SEG3: 60s × 600/30 / 60 = 20.0 min/day  (largest |minutes_per_day|)
    ROUTE_A SEG1: 120s × 300/30 / 60 = 20.0 min/day (tie — second by route_id)
    ROUTE_B SEG4: 90s × 200/30 / 60 = 10.0 min/day
    ROUTE_A SEG2: 30s × 300/30 / 60 = 5.0 min/day
    """
    _seed_schedule_audit_fixture(db_session)
    response = client.get("/api/schedule-audit")
    assert response.status_code == 200
    data = response.json()
    assert data["n_rows"] == 4
    segments = data["segments"]
    assert len(segments) == 4
    # First two tied at |20.0|; tie-break is route_id ascending, so ROUTE_A first
    assert abs(segments[0]["minutes_per_day"]) == pytest.approx(20.0)
    assert abs(segments[1]["minutes_per_day"]) == pytest.approx(20.0)
    assert segments[0]["route_id"] == "ROUTE_A"
    assert segments[1]["route_id"] == "ROUTE_B"
    assert abs(segments[2]["minutes_per_day"]) == pytest.approx(10.0)
    assert abs(segments[3]["minutes_per_day"]) == pytest.approx(5.0)
    # Joined names propagate
    assert segments[0]["from_stop_name"] == "Stop 1"
    assert segments[0]["to_stop_name"] == "Stop 2"
    assert segments[0]["route_short_name"] == "A1"


@pytest.mark.api
def test_schedule_audit_sign_filter_under(client, db_session):
    """`sign=under` returns only positive-slip rows (under-padded)."""
    _seed_schedule_audit_fixture(db_session)
    response = client.get("/api/schedule-audit?sign=under")
    assert response.status_code == 200
    segments = response.json()["segments"]
    assert len(segments) == 2  # ROUTE_A SEG1 and ROUTE_B SEG3
    for s in segments:
        assert s["mean_slip_sec"] > 0
        assert s["minutes_per_day"] > 0


@pytest.mark.api
def test_schedule_audit_sign_filter_over(client, db_session):
    """`sign=over` returns only negative-slip rows (over-padded)."""
    _seed_schedule_audit_fixture(db_session)
    response = client.get("/api/schedule-audit?sign=over")
    assert response.status_code == 200
    segments = response.json()["segments"]
    assert len(segments) == 2  # ROUTE_A SEG2 and ROUTE_B SEG4
    for s in segments:
        assert s["mean_slip_sec"] < 0
        assert s["minutes_per_day"] < 0


@pytest.mark.api
def test_schedule_audit_route_filter(client, db_session):
    """`route_id` filter restricts to one route."""
    _seed_schedule_audit_fixture(db_session)
    response = client.get("/api/schedule-audit?route_id=ROUTE_A")
    assert response.status_code == 200
    segments = response.json()["segments"]
    assert len(segments) == 2  # SEG1 + SEG2 (am_peak SEG5 excluded by default period=all)
    assert all(s["route_id"] == "ROUTE_A" for s in segments)


@pytest.mark.api
def test_schedule_audit_period_filter(client, db_session):
    """`period=am_peak` returns only the peak-tagged row."""
    _seed_schedule_audit_fixture(db_session)
    response = client.get("/api/schedule-audit?period=am_peak")
    assert response.status_code == 200
    data = response.json()
    assert data["period"] == "am_peak"
    segments = data["segments"]
    assert len(segments) == 1
    assert segments[0]["route_id"] == "ROUTE_A"
    assert segments[0]["mean_slip_sec"] == 200.0
    assert segments[0]["period"] == "am_peak"


@pytest.mark.api
def test_schedule_audit_direction_filter(client, db_session):
    """`direction_id=1` returns only direction 1 rows."""
    _seed_schedule_audit_fixture(db_session)
    response = client.get("/api/schedule-audit?direction_id=1")
    assert response.status_code == 200
    segments = response.json()["segments"]
    assert all(s["direction_id"] == 1 for s in segments)
    assert {s["route_id"] for s in segments} == {"ROUTE_B"}


@pytest.mark.api
def test_schedule_audit_limit(client, db_session):
    """`limit` caps the segments list."""
    _seed_schedule_audit_fixture(db_session)
    response = client.get("/api/schedule-audit?limit=2")
    assert response.status_code == 200
    data = response.json()
    assert data["n_rows"] == 4  # totals reflect pre-limit count
    assert len(data["segments"]) == 2


@pytest.mark.api
def test_schedule_audit_invalid_period(client):
    """Invalid period returns 400."""
    response = client.get("/api/schedule-audit?period=funkytime")
    assert response.status_code == 400
    assert "period" in response.json()["detail"].lower()


@pytest.mark.api
def test_schedule_audit_invalid_sign(client):
    """Invalid sign returns 400."""
    response = client.get("/api/schedule-audit?sign=sideways")
    assert response.status_code == 400
    assert "sign" in response.json()["detail"].lower()


@pytest.mark.api
def test_schedule_audit_invalid_direction(client):
    """Invalid direction_id returns 400."""
    response = client.get("/api/schedule-audit?direction_id=2")
    assert response.status_code == 400
    assert "direction" in response.json()["detail"].lower()
