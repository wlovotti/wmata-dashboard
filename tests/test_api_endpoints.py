"""
API endpoint tests for WMATA Dashboard

Tests all REST API endpoints with various scenarios:
- Success cases (200 OK)
- Not found cases (404)
- Bad request cases (400)
- Edge cases (empty data, query parameters)

Run with: pytest -m api
"""

from datetime import datetime

import pytest

from src.models import RouteMetricsDaily, Shape, Trip


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
def test_get_routes_success(client, sample_route, sample_route_metrics_summary):
    """Test GET /api/routes returns scorecard with data"""
    response = client.get("/api/routes")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1  # Only one route with metrics

    route = data[0]
    assert route["route_id"] == "TEST1"
    assert route["route_name"] == "T1"
    assert route["otp_percentage"] == 75.5
    assert route["avg_headway_minutes"] == 12.5
    assert route["avg_speed_mph"] == 18.5
    assert route["grade"] == "B"  # 75.5% OTP = B grade


@pytest.mark.api
def test_get_routes_empty_database(client):
    """Test GET /api/routes with no data returns empty list"""
    response = client.get("/api/routes")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.api
def test_get_routes_with_days_parameter(client, sample_route, sample_route_metrics_summary):
    """Test GET /api/routes with days query parameter"""
    response = client.get("/api/routes?days=14")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    # Parameter is accepted (though ignored in current implementation using pre-computed data)


@pytest.mark.api
def test_get_route_success(client, sample_route, sample_route_metrics_summary):
    """Test GET /api/routes/{route_id} returns detailed metrics"""
    response = client.get("/api/routes/TEST1")
    assert response.status_code == 200

    data = response.json()
    assert data["route_id"] == "TEST1"
    assert data["route_name"] == "T1"
    assert data["route_long_name"] == "Test Route 1"
    assert data["otp_percentage"] == 75.5
    assert data["avg_headway_minutes"] == 12.5
    assert data["avg_speed_mph"] == 18.5
    assert data["grade"] == "B"
    assert data["total_arrivals_analyzed"] == 45
    assert data["total_positions"] == 1050
    assert data["unique_vehicles"] == 8
    assert data["unique_trips"] == 42


@pytest.mark.api
def test_get_route_not_found(client):
    """Test GET /api/routes/{route_id} with non-existent route returns 404"""
    response = client.get("/api/routes/NONEXISTENT")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.api
def test_get_route_no_metrics(client, sample_route):
    """Test GET /api/routes/{route_id} for route without computed metrics"""
    response = client.get("/api/routes/TEST1")
    assert response.status_code == 200

    data = response.json()
    assert data["route_id"] == "TEST1"
    assert data["otp_percentage"] is None
    assert data["avg_headway_minutes"] is None
    assert data["grade"] == "N/A"


@pytest.mark.api
def test_get_route_with_days_parameter(client, sample_route, sample_route_metrics_summary):
    """Test GET /api/routes/{route_id} with days query parameter"""
    response = client.get("/api/routes/TEST1?days=14")
    assert response.status_code == 200
    assert response.json()["route_id"] == "TEST1"


@pytest.mark.api
def test_get_route_trend_success(client, sample_route, sample_route_metrics_daily):
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
def test_get_route_trend_all_metrics(client, sample_route, sample_route_metrics_daily):
    """Test GET /api/routes/{route_id}/trend with all valid metrics"""
    valid_metrics = ["otp", "early", "late", "headway", "headway_std_dev", "speed"]

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
def test_get_route_trend_with_days_parameter(client, sample_route, sample_route_metrics_daily):
    """Test GET /api/routes/{route_id}/trend with days parameter"""
    response = client.get("/api/routes/TEST1/trend?metric=otp&days=60")
    assert response.status_code == 200
    assert response.json()["days"] == 60


@pytest.mark.api
def test_get_route_segments_success(client, db_session, sample_route, sample_trip):
    """Test GET /api/routes/{route_id}/segments returns speed segments"""
    # Create shape data for the route
    shapes = []
    for i in range(10):
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

    response = client.get("/api/routes/TEST1/segments")
    assert response.status_code == 200

    data = response.json()
    assert data["route_id"] == "TEST1"
    assert "segments" in data
    assert isinstance(data["segments"], list)


@pytest.mark.api
def test_get_route_segments_no_shape_data(client, sample_route):
    """Test GET /api/routes/{route_id}/segments with no shape data"""
    response = client.get("/api/routes/TEST1/segments")
    assert response.status_code == 200

    data = response.json()
    assert data["route_id"] == "TEST1"
    assert data["segments"] == []


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
