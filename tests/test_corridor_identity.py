"""Unit tests for corridor identity helpers (NOTES-62)."""

import pytest

from src.corridor_identity import (
    bearing_circular_distance,
    bearing_degrees,
    haversine_meters,
)

# Synthetic reference points at D.C. latitude. Exact-bearing tests use
# pure deltas (same lat or same lon) so the geometry is unambiguous.
LINCOLN_MEMORIAL = (38.8893, -77.0502)
WASHINGTON_MONUMENT = (38.8895, -77.0353)  # ~1.3 km east

# Exact synthetic points: same longitude => due N/S; same latitude => due E/W.
DC_BASE = (38.89, -77.05)
DC_NORTH = (38.90, -77.05)  # 0.01 deg north of DC_BASE, same lon
DC_EAST = (38.89, -77.04)  # 0.01 deg east of DC_BASE, same lat


def test_haversine_zero_distance():
    """Same point has zero distance."""
    assert haversine_meters(*LINCOLN_MEMORIAL, *LINCOLN_MEMORIAL) == pytest.approx(0.0, abs=0.01)


def test_haversine_known_distance():
    """Lincoln Memorial to Washington Monument is ~1.3 km."""
    dist = haversine_meters(*LINCOLN_MEMORIAL, *WASHINGTON_MONUMENT)
    assert dist == pytest.approx(1290, abs=30)


def test_bearing_due_east():
    """Bearing across pure east longitudinal delta is exactly 90 degrees."""
    b = bearing_degrees(*DC_BASE, *DC_EAST)
    assert b == pytest.approx(90.0, abs=0.1)


def test_bearing_due_north():
    """Bearing across pure north latitudinal delta is exactly 0 degrees."""
    b = bearing_degrees(*DC_BASE, *DC_NORTH)
    assert b == pytest.approx(0.0, abs=0.1)


def test_bearing_due_south():
    """Bearing from north point back to base is exactly 180 degrees."""
    b = bearing_degrees(*DC_NORTH, *DC_BASE)
    assert b == pytest.approx(180.0, abs=0.1)


def test_bearing_due_west():
    """Bearing from east point back to base is exactly 270 degrees."""
    b = bearing_degrees(*DC_EAST, *DC_BASE)
    assert b == pytest.approx(270.0, abs=0.1)


def test_bearing_circular_distance_simple():
    """Bearings 10 and 350 are 20 degrees apart, not 340."""
    assert bearing_circular_distance(10, 350) == pytest.approx(20.0)


def test_bearing_circular_distance_opposite():
    """Bearings 0 and 180 are 180 degrees apart."""
    assert bearing_circular_distance(0, 180) == pytest.approx(180.0)


def test_bearing_circular_distance_same():
    """Same bearing has zero distance."""
    assert bearing_circular_distance(45, 45) == 0.0


def test_bearing_circular_distance_wraps():
    """Bearings 359 and 1 are 2 degrees apart."""
    assert bearing_circular_distance(359, 1) == pytest.approx(2.0)
