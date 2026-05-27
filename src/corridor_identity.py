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


def augment_shape_with_bearings(
    points: list[tuple[float, float, int]],
) -> list[tuple[float, float, int, float]]:
    """
    Compute the local bearing at every shape point.

    Input rows: (lat, lon, shape_pt_sequence) sorted by sequence.
    Output rows: (lat, lon, shape_pt_sequence, bearing_deg).

    Bearing at point i is the compass angle from i to i+1. At the last
    point, look back: bearing from i-1 to i. Single-point shapes are
    invalid input and will raise.
    """
    if len(points) < 2:
        raise ValueError("Shape must have at least 2 points to compute bearings")

    result: list[tuple[float, float, int, float]] = []
    for i, (lat, lon, seq) in enumerate(points):
        if i < len(points) - 1:
            next_lat, next_lon, _ = points[i + 1]
            bearing = bearing_degrees(lat, lon, next_lat, next_lon)
        else:
            prev_lat, prev_lon, _ = points[i - 1]
            bearing = bearing_degrees(prev_lat, prev_lon, lat, lon)
        result.append((lat, lon, seq, bearing))
    return result


# Type aliases for clarity.
ShapeKey = tuple[str, int]  # (route_id, direction_id)
PointKey = tuple[str, int, int]  # (route_id, direction_id, shape_pt_sequence)


def pick_canonical_shapes(
    trip_shape_counts: list[tuple[str, int, str, int]],
) -> dict[tuple[str, int], str]:
    """
    For each (route_id, direction_id), pick the shape_id with the
    highest trip count as the canonical representative.

    Input rows: (route_id, direction_id, shape_id, n_trips).
    Output: mapping (route_id, direction_id) -> canonical shape_id.

    Ties broken by lexicographic shape_id (deterministic).
    """
    best: dict[tuple[str, int], tuple[int, str]] = {}
    for route_id, direction_id, shape_id, n_trips in trip_shape_counts:
        key = (route_id, direction_id)
        if key not in best:
            best[key] = (n_trips, shape_id)
        else:
            current_trips, current_shape = best[key]
            if n_trips > current_trips or (n_trips == current_trips and shape_id < current_shape):
                best[key] = (n_trips, shape_id)
    return {key: shape_id for key, (_, shape_id) in best.items()}


def compute_colocated_route_sets(
    canonical_shapes: dict[ShapeKey, tuple[str, list[tuple[float, float, int, float]]]],
) -> dict[PointKey, set[ShapeKey]]:
    """
    For each canonical shape point, return the set of OTHER (route_id,
    direction_id) shapes that pass within SHAPE_PROXIMITY_THRESHOLD_M
    AND within BEARING_AGREEMENT_THRESHOLD_DEG of bearing.

    Input: mapping (route_id, direction_id) -> (canonical_shape_id, bearing-augmented points).
    Output: mapping (route_id, direction_id, shape_pt_sequence) -> set of (route_id, direction_id) keys.

    Algorithm: bucket each point into a ~30m x 30m grid cell; for each
    point, scan the 9 neighbor cells (self + 8 surrounding) for
    candidate matches; apply exact haversine + bearing test.

    O(N) expected where N = total point count, given uniform spatial
    distribution across the grid. For ~500k WMATA shape points and ~30m
    bucket size, per-cell load is small.
    """
    # ~30m grid: at D.C.'s latitude, 1 degree latitude ~= 111 km, so
    # 30m / 111000 m/deg ~= 0.00027 deg. Same scale for longitude at
    # this latitude (cos(38.9 deg) ~= 0.78), so cells are roughly
    # 30m N-S by 38m E-W — close enough for a coarse spatial index.
    grid_size_deg = 0.00027

    grid: dict[tuple[int, int], list[tuple[str, int, int, float, float, float]]] = {}
    for (route_id, direction_id), (_shape_id, points) in canonical_shapes.items():
        for lat, lon, seq, bearing in points:
            cell = (int(lat / grid_size_deg), int(lon / grid_size_deg))
            grid.setdefault(cell, []).append((route_id, direction_id, seq, lat, lon, bearing))

    result: dict[PointKey, set[ShapeKey]] = {}

    for (route_id, direction_id), (_shape_id, points) in canonical_shapes.items():
        for lat, lon, seq, bearing in points:
            point_key: PointKey = (route_id, direction_id, seq)
            colocated: set[ShapeKey] = set()

            cell = (int(lat / grid_size_deg), int(lon / grid_size_deg))
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    neighbors = grid.get((cell[0] + dx, cell[1] + dy), [])
                    for (
                        other_route_id,
                        other_direction_id,
                        _other_seq,
                        other_lat,
                        other_lon,
                        other_bearing,
                    ) in neighbors:
                        if (other_route_id, other_direction_id) == (
                            route_id,
                            direction_id,
                        ):
                            continue
                        if (
                            haversine_meters(lat, lon, other_lat, other_lon)
                            >= SHAPE_PROXIMITY_THRESHOLD_M
                        ):
                            continue
                        if (
                            bearing_circular_distance(bearing, other_bearing)
                            >= BEARING_AGREEMENT_THRESHOLD_DEG
                        ):
                            continue
                        colocated.add((other_route_id, other_direction_id))

            result[point_key] = colocated

    return result
