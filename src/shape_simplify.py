"""Polyline simplification for map payloads (the NOTES-84 system map).

Douglas-Peucker over raw (lat, lon) degrees. Degree-space distance is
anisotropic (~1.28x lat vs lon at DC latitudes) but the tolerance here is a
display-level knob, not a geodesic measurement — 1e-4 deg is ~11 m N-S,
invisible at system-map zoom, and typically cuts GTFS shape point counts
5-10x.
"""

DEFAULT_TOLERANCE_DEG = 1e-4


def _perpendicular_distance(point, start, end):
    """Distance in degrees from `point` to the segment start→end.

    Falls back to point-to-point distance when start == end. Clamps the
    projection to the segment so hairpin shapes don't measure against the
    infinite line.
    """
    (px, py), (sx, sy), (ex, ey) = point, start, end
    dx, dy = ex - sx, ey - sy
    if dx == 0 and dy == 0:
        return ((px - sx) ** 2 + (py - sy) ** 2) ** 0.5
    t = ((px - sx) * dx + (py - sy) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    cx, cy = sx + t * dx, sy + t * dy
    return ((px - cx) ** 2 + (py - cy) ** 2) ** 0.5


def simplify_polyline(points, tolerance_deg=DEFAULT_TOLERANCE_DEG):
    """Simplify a polyline with iterative Douglas-Peucker.

    Args:
        points: ordered list of (lat, lon) tuples.
        tolerance_deg: max perpendicular deviation (in degrees) a dropped
            point may have from the simplified line. Points deviating MORE
            than this are kept.

    Returns:
        A new list of (lat, lon) tuples — always includes the first and last
        input points, never longer than the input. Inputs of length <= 2 are
        returned as a shallow copy.

    Iterative (explicit stack) rather than recursive: GTFS rail/limited
    shapes can run thousands of points and recursion depth is input-shaped.
    """
    if len(points) <= 2:
        return list(points)
    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        start, end = stack.pop()
        max_dist = 0.0
        max_idx = None
        for i in range(start + 1, end):
            d = _perpendicular_distance(points[i], points[start], points[end])
            if d > max_dist:
                max_dist, max_idx = d, i
        if max_idx is not None and max_dist > tolerance_deg:
            keep[max_idx] = True
            stack.append((start, max_idx))
            stack.append((max_idx, end))
    return [p for p, k in zip(points, keep) if k]
