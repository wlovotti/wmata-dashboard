"""Unit tests for the schedule-derived daytime service-level stats (NOTES-115).

Pure-function tests only — no database, except for
`test_service_level_for_agency_filters_to_bus_routes` below, which
exercises the `service_level_for_agency` wrapper with the schedule fetch
monkeypatched (because `fetch_scheduled_cell_hours_for_routes` caches by
db identity and in-memory SQLite sessions share one identity) but real
`Route` rows so the bus-only comparison filtering (PR #201) has
something to query.
"""

import pytest

from src.service_level import compute_service_level_stats, service_level_for_agency


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


def test_reference_stop_tie_broken_by_higher_stop_id():
    """Two stops tied on sample count -> the higher stop_id's samples win,
    deterministically (not first-seen, since the upstream SQL has no
    ORDER BY and dict iteration order isn't a stable tie-break)."""
    sched = {
        "A": {
            (0, "S1", 8): [600.0, 600.0],  # tied count, lower stop_id
            (0, "S9", 8): [1200.0, 1200.0],  # tied count, higher stop_id
        }
    }
    out = compute_service_level_stats(sched)
    assert out["n_headways"] == 2
    assert out["median_headway_seconds"] == pytest.approx(1200.0)


def test_service_level_for_agency_filters_to_bus_routes(db_session, monkeypatch):
    """Bus-only comparison filtering (PR #201): rail/cable headways must
    not skew the bus-vs-bus comparison. A mixed-mode feed (SFMTA's Muni
    Metro light rail + cable car alongside its bus routes) should only
    contribute its route_type=3 route's samples to the service-level
    pool."""
    import src.ewt as ewt_module
    from src.models import Route

    db_session.add_all(
        [
            Route(route_id="BUS1", route_short_name="B1", route_type=3, is_current=True),
            Route(route_id="RAIL1", route_short_name="R1", route_type=0, is_current=True),
        ]
    )
    db_session.commit()

    def _fake_sched(db, day_type, route_ids=None, gtfs_snapshot_id=None):
        return {
            "BUS1": {(0, "S1", 9): [600.0, 600.0]},
            "RAIL1": {(0, "S2", 9): [120.0, 120.0]},
        }

    monkeypatch.setattr(ewt_module, "fetch_scheduled_cell_hours_for_routes", _fake_sched)

    out = service_level_for_agency(db_session)

    assert out["n_headways"] == 2
    assert out["median_headway_seconds"] == pytest.approx(600.0)
