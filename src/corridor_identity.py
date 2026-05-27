"""
Corridor identity algorithm — NOTES-62.

Pure-Python helpers for the shape-matching pipeline. Bearing computation,
haversine distance, and proximity-plus-bearing match logic. No DB calls
here; the orchestrating pipeline pulls shape data, calls these helpers,
and writes results.

Spec: docs/superpowers/specs/2026-05-25-cross-route-corridor-design.md
"""

from __future__ import annotations

import math

# Calibration knobs (Section 5 of the spec).
SHAPE_PROXIMITY_THRESHOLD_M = 15.0
BEARING_AGREEMENT_THRESHOLD_DEG = 30.0
MIN_CORRIDOR_LENGTH_M = 500.0
MIN_RUN_POINTS = 5
ENDPOINT_STOP_SNAP_M = 100.0

EARTH_RADIUS_M = 6_371_000.0


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters between two (lat, lon) points."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def bearing_degrees(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing in degrees (0-360) from point 1 to point 2."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    y = math.sin(dlambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    theta = math.atan2(y, x)
    return (math.degrees(theta) + 360.0) % 360.0


def bearing_circular_distance(b1: float, b2: float) -> float:
    """Smallest angular distance between two bearings in degrees (0-180)."""
    diff = abs(b1 - b2) % 360.0
    return min(diff, 360.0 - diff)
