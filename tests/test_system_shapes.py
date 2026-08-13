"""Tests for the bulk system-shapes aggregation (NOTES-84 system map)."""

import pytest

import api.aggregations as agg
from src.models import Route, Shape, Trip


@pytest.fixture(autouse=True)
def _clear_shapes_cache():
    """The system-shapes cache is keyed by a constant, so it MUST be cleared
    between tests — otherwise one test's seeded payload leaks into the next
    test's empty-DB expectation."""
    agg._shapes_cache.clear()
    yield
    agg._shapes_cache.clear()


def _seed_route_with_shapes(db):
    """One current route with two shape variants: shape A serves 2 current
    trips, shape B serves 1 — A is representative. A retired (is_current=False)
    trip points at shape C, which must be ignored entirely."""
    db.add(Route(route_id="R1", route_short_name="R1", route_type=3, is_current=True))
    db.add_all(
        [
            Trip(trip_id="t1", route_id="R1", shape_id="A", is_current=True),
            Trip(trip_id="t2", route_id="R1", shape_id="A", is_current=True),
            Trip(trip_id="t3", route_id="R1", shape_id="B", is_current=True),
            Trip(trip_id="t4", route_id="R1", shape_id="C", is_current=False),
        ]
    )
    # Shape A: 3 collinear points → simplifier collapses to 2.
    db.add_all(
        [
            Shape(shape_id="A", shape_pt_lat=38.90, shape_pt_lon=-77.00, shape_pt_sequence=1),
            Shape(shape_id="A", shape_pt_lat=38.91, shape_pt_lon=-77.00, shape_pt_sequence=2),
            Shape(shape_id="A", shape_pt_lat=38.92, shape_pt_lon=-77.00, shape_pt_sequence=3),
            Shape(shape_id="B", shape_pt_lat=38.80, shape_pt_lon=-77.10, shape_pt_sequence=1),
            Shape(shape_id="B", shape_pt_lat=38.81, shape_pt_lon=-77.11, shape_pt_sequence=2),
            Shape(shape_id="C", shape_pt_lat=38.70, shape_pt_lon=-77.20, shape_pt_sequence=1),
            Shape(shape_id="C", shape_pt_lat=38.71, shape_pt_lon=-77.21, shape_pt_sequence=2),
        ]
    )
    db.commit()


@pytest.mark.smoke
def test_empty_database_returns_empty_routes(db_session):
    assert agg.get_system_shapes(db_session) == {"routes": []}


@pytest.mark.smoke
def test_most_trips_shape_wins_and_is_simplified(db_session):
    _seed_route_with_shapes(db_session)
    result = agg.get_system_shapes(db_session)
    assert len(result["routes"]) == 1
    entry = result["routes"][0]
    assert entry["route_id"] == "R1"
    # Shape A won (2 trips > 1), and its 3 collinear points simplified to 2.
    assert entry["points"] == [[38.90, -77.00], [38.92, -77.00]]


def test_retired_trips_do_not_elect_a_shape(db_session):
    """A route whose only trips are is_current=False contributes nothing."""
    db_session.add(Route(route_id="R2", route_short_name="R2", route_type=3, is_current=True))
    db_session.add(Trip(trip_id="t9", route_id="R2", shape_id="C", is_current=False))
    db_session.add_all(
        [
            Shape(shape_id="C", shape_pt_lat=38.70, shape_pt_lon=-77.20, shape_pt_sequence=1),
            Shape(shape_id="C", shape_pt_lat=38.71, shape_pt_lon=-77.21, shape_pt_sequence=2),
        ]
    )
    db_session.commit()
    assert agg.get_system_shapes(db_session) == {"routes": []}


def test_result_is_cached(db_session):
    _seed_route_with_shapes(db_session)
    first = agg.get_system_shapes(db_session)
    # Second call within TTL returns the identical cached object.
    assert agg.get_system_shapes(db_session) is first
