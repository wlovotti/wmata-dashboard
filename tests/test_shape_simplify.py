"""Tests for the Douglas-Peucker polyline simplifier (NOTES-84 system map)."""

import pytest

from src.shape_simplify import DEFAULT_TOLERANCE_DEG, simplify_polyline


@pytest.mark.smoke
def test_short_inputs_returned_unchanged():
    """Polylines with 0-2 points cannot be simplified — returned as-is (copy)."""
    assert simplify_polyline([]) == []
    assert simplify_polyline([(38.9, -77.0)]) == [(38.9, -77.0)]
    two = [(38.9, -77.0), (38.91, -77.01)]
    out = simplify_polyline(two)
    assert out == two
    assert out is not two  # a copy, not the caller's list


@pytest.mark.smoke
def test_collinear_points_collapse_to_endpoints():
    """Interior points on a straight line are within any positive tolerance."""
    line = [(38.90, -77.00), (38.91, -77.00), (38.92, -77.00), (38.93, -77.00)]
    assert simplify_polyline(line) == [(38.90, -77.00), (38.93, -77.00)]


@pytest.mark.smoke
def test_significant_deviation_is_preserved():
    """A point far off the chord (>> tolerance) must survive simplification."""
    dogleg = [(38.90, -77.00), (38.905, -76.99), (38.91, -77.00)]  # ~0.01 deg spike
    assert simplify_polyline(dogleg) == dogleg


@pytest.mark.smoke
def test_endpoints_always_preserved_and_count_never_grows():
    """First/last points survive; output is never longer than input."""
    wiggle = [(38.90 + i * 0.001, -77.00 + (0.00005 if i % 2 else 0.0)) for i in range(50)]
    out = simplify_polyline(wiggle)
    assert out[0] == wiggle[0]
    assert out[-1] == wiggle[-1]
    assert len(out) <= len(wiggle)
    # The 0.00005-deg wiggle is inside the 1e-4 default tolerance → big reduction.
    assert len(out) < len(wiggle) / 2


def test_tolerance_zero_keeps_everything_noncollinear():
    """tolerance 0 keeps every point that deviates at all."""
    dogleg = [(0.0, 0.0), (0.5, 0.0001), (1.0, 0.0)]
    assert simplify_polyline(dogleg, tolerance_deg=0.0) == dogleg


assert DEFAULT_TOLERANCE_DEG == pytest.approx(1e-4)
