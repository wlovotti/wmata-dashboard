"""Unit tests for the schedule-derived daytime service-level stats (NOTES-115).

Pure-function tests only — no database. The wrapper
`service_level_for_agency` is exercised through the comparison-endpoint
tests (tests/test_agency_comparison.py) with the schedule fetch
monkeypatched, because `fetch_scheduled_cell_hours_for_routes` caches by
db identity and in-memory SQLite sessions share one identity.
"""

import pytest

from src.service_level import compute_service_level_stats


def test_empty_schedule_returns_nulls():
    """No routes at all -> null stats, zero samples."""
    out = compute_service_level_stats({})
    assert out == {
        "median_headway_seconds": None,
        "pct_at_most_15min": None,
        "n_headways": 0,
    }


def test_daytime_hour_filter_is_half_open():
    """Hours 7..18 are in; hour 6 and hour 19 are out."""
    sched = {
        "A": {
            (0, "S1", 6): [100.0],  # before window
            (0, "S1", 7): [600.0],  # in
            (0, "S1", 18): [1200.0],  # in
            (0, "S1", 19): [100.0],  # after window
        }
    }
    out = compute_service_level_stats(sched)
    assert out["n_headways"] == 2
    assert out["median_headway_seconds"] == pytest.approx(900.0)


def test_reference_stop_is_max_samples_per_route_direction():
    """Per (route, direction) only the stop with the most daytime samples
    contributes — stop-dense routes must not be overweighted."""
    sched = {
        "A": {
            (0, "S1", 8): [600.0, 600.0, 600.0],  # reference (3 samples)
            (0, "S2", 8): [60.0, 60.0],  # decoy stop, ignored
        }
    }
    out = compute_service_level_stats(sched)
    assert out["n_headways"] == 3
    assert out["median_headway_seconds"] == pytest.approx(600.0)


def test_directions_pool_independently():
    """Each direction picks its own reference stop; both directions pool."""
    sched = {
        "A": {
            (0, "S1", 8): [600.0, 600.0],
            (1, "S9", 8): [1200.0, 1200.0, 1200.0],
        }
    }
    out = compute_service_level_stats(sched)
    assert out["n_headways"] == 5
    assert out["median_headway_seconds"] == pytest.approx(1200.0)


def test_median_and_share_trip_weighted_across_routes():
    """Routes pool sample-by-sample (trip-weighted), not route-equal."""
    sched = {
        "FREQ": {(0, "S1", 9): [600.0, 600.0, 600.0, 600.0]},  # 4 samples ≤ 15 min
        "RARE": {(0, "S2", 9): [1800.0, 1800.0]},  # 2 samples > 15 min
    }
    out = compute_service_level_stats(sched)
    assert out["n_headways"] == 6
    assert out["median_headway_seconds"] == pytest.approx(600.0)
    assert out["pct_at_most_15min"] == pytest.approx(4 / 6, abs=1e-4)


def test_share_boundary_is_inclusive_at_900s():
    """A headway of exactly 15 min counts toward the ≤ 15 min share."""
    sched = {"A": {(0, "S1", 10): [900.0, 901.0]}}
    out = compute_service_level_stats(sched)
    assert out["pct_at_most_15min"] == pytest.approx(0.5)
