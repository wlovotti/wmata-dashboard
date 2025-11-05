"""
Smoke tests for WMATA Dashboard

Quick tests that verify critical paths are working.
These should run fast (<10s) and fail fast if something is fundamentally broken.

Run with: pytest -m smoke
"""

import pytest

from src.models import Route


@pytest.mark.smoke
def test_database_connection(db_session):
    """Test that database connection works"""
    # Should be able to execute a simple query
    result = db_session.execute("SELECT 1").scalar()
    assert result == 1


@pytest.mark.smoke
def test_database_can_create_and_query_route(db_session):
    """Test basic database insert and query"""
    # Create a route
    route = Route(
        route_id="SMOKE1",
        route_short_name="SM1",
        route_long_name="Smoke Test Route",
        route_type=3,
        is_current=True,
    )
    db_session.add(route)
    db_session.commit()

    # Query it back
    queried_route = db_session.query(Route).filter_by(route_id="SMOKE1").first()
    assert queried_route is not None
    assert queried_route.route_short_name == "SM1"


@pytest.mark.smoke
def test_api_server_responds(client):
    """Test that API server starts and responds"""
    # Just check that we get any response
    response = client.get("/api/routes")
    # Should get a response (might be 200 with empty list or other valid response)
    assert response.status_code in [200, 404, 500]  # Any response means server is running


@pytest.mark.smoke
def test_api_routes_endpoint_structure(client, sample_route, sample_route_metrics_summary):
    """Test that /api/routes returns expected JSON structure"""
    response = client.get("/api/routes")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)

    if len(data) > 0:
        # Check first route has expected keys
        route = data[0]
        expected_keys = [
            "route_id",
            "route_name",
            "otp_percentage",
            "avg_headway_minutes",
            "avg_speed_mph",
            "grade",
        ]
        for key in expected_keys:
            assert key in route, f"Missing key: {key}"


@pytest.mark.smoke
def test_critical_modules_import():
    """Test that critical modules can be imported"""
    # Database
    from src.database import get_session

    assert get_session is not None

    # Analytics
    from src.analytics import calculate_headways

    assert calculate_headways is not None

    # API
    from api.aggregations import get_all_routes_scorecard

    assert get_all_routes_scorecard is not None

    # Models
    from src.models import Route, Stop, Trip

    assert Route is not None
    assert Stop is not None
    assert Trip is not None
