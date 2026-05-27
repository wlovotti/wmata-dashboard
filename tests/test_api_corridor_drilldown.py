"""Tests for /api/corridors/{corridor_id}/segments drill-down (NOTES-62 Task 18).

Reuses the same Corridor + CorridorRouteMembership seeding pattern as
``test_api_corridors.py`` and additionally seeds RouteDiagnosticSegment
rows in and out of the corridor's per-route stop_sequence range so the
membership filter is exercised.
"""

from __future__ import annotations

import pytest

from src.models import (
    Corridor,
    CorridorRouteMembership,
    RouteDiagnosticSegment,
    Stop,
)


@pytest.fixture
def seeded_corridor_with_segments(db_session):
    """Insert a corridor + memberships + diagnostic segments (in-range + out-of-range)."""
    db_session.add_all(
        [
            Stop(
                stop_id="S2",
                stop_name="14th & T NW",
                stop_lat=38.916,
                stop_lon=-77.032,
                is_current=True,
            ),
            Stop(
                stop_id="S3",
                stop_name="14th & S NW",
                stop_lat=38.914,
                stop_lon=-77.032,
                is_current=True,
            ),
        ]
    )

    corridor = Corridor(
        direction_bearing_deg=180.0,
        direction_cardinal="S",
        start_stop_id="S1",
        end_stop_id="S5",
        length_m=1670.0,
        n_routes=2,
        route_set="D50,D5X",
        display_name="SB: 14th & U NW -> 14th & K NW",
        geometry_wkt="LINESTRING(-77.032 38.917, -77.032 38.902)",
        gtfs_snapshot_id=1,
    )
    db_session.add(corridor)
    db_session.flush()

    db_session.add_all(
        [
            CorridorRouteMembership(
                corridor_id=corridor.corridor_id,
                route_id="D50",
                direction_id=0,
                canonical_shape_id="D50_S",
                start_stop_sequence=3,
                end_stop_sequence=10,
            ),
            CorridorRouteMembership(
                corridor_id=corridor.corridor_id,
                route_id="D5X",
                direction_id=0,
                canonical_shape_id="D5X_S",
                start_stop_sequence=1,
                end_stop_sequence=6,
            ),
        ]
    )

    db_session.add_all(
        [
            # D50: in-range seg 5->6
            RouteDiagnosticSegment(
                route_id="D50",
                direction_id=0,
                period="all",
                from_seq=5,
                from_stop_id="S2",
                to_seq=6,
                to_stop_id="S3",
                mean_slip_sec=45.0,
                cum_slip_sec=225.0,
                n_observations=30,
                is_timepoint=False,
            ),
            # D50: out-of-range (before start_stop_sequence=3)
            RouteDiagnosticSegment(
                route_id="D50",
                direction_id=0,
                period="all",
                from_seq=1,
                from_stop_id="S0",
                to_seq=2,
                to_stop_id="S1",
                mean_slip_sec=10.0,
                cum_slip_sec=10.0,
                n_observations=20,
                is_timepoint=False,
            ),
            # D50: out-of-range (after end_stop_sequence=10)
            RouteDiagnosticSegment(
                route_id="D50",
                direction_id=0,
                period="all",
                from_seq=11,
                from_stop_id="S10",
                to_seq=12,
                to_stop_id="S11",
                mean_slip_sec=15.0,
                cum_slip_sec=180.0,
                n_observations=20,
                is_timepoint=False,
            ),
            # D5X: in-range seg 2->3
            RouteDiagnosticSegment(
                route_id="D5X",
                direction_id=0,
                period="all",
                from_seq=2,
                from_stop_id="S2",
                to_seq=3,
                to_stop_id="S3",
                mean_slip_sec=70.0,
                cum_slip_sec=140.0,
                n_observations=15,
                is_timepoint=False,
            ),
            # Different period — should not appear under period=all
            RouteDiagnosticSegment(
                route_id="D50",
                direction_id=0,
                period="am_peak",
                from_seq=5,
                from_stop_id="S2",
                to_seq=6,
                to_stop_id="S3",
                mean_slip_sec=120.0,
                cum_slip_sec=120.0,
                n_observations=8,
                is_timepoint=False,
            ),
        ]
    )
    db_session.commit()
    return corridor.corridor_id


def test_drilldown_returns_only_in_range_segments(client, seeded_corridor_with_segments):
    """Filter by each membership's (route, direction, seq range) and period."""
    resp = client.get(f"/api/corridors/{seeded_corridor_with_segments}/segments")
    assert resp.status_code == 200
    body = resp.json()
    assert body["corridor_id"] == seeded_corridor_with_segments
    assert body["period"] == "all"
    segs = body["segments"]
    # Exactly 2 in-range, period=all segments — D50 (5->6) and D5X (2->3).
    assert len(segs) == 2
    identifiers = {(s["route_id"], s["from_seq"], s["to_seq"]) for s in segs}
    assert identifiers == {("D50", 5, 6), ("D5X", 2, 3)}
    # Ordered by mean_slip_sec descending.
    assert segs[0]["route_id"] == "D5X"
    assert segs[0]["mean_slip_sec"] == 70.0
    # Stop names are joined for the frontend.
    assert segs[0]["from_stop_name"] == "14th & T NW"
    assert segs[0]["to_stop_name"] == "14th & S NW"


def test_drilldown_period_filter(client, seeded_corridor_with_segments):
    """period=am_peak returns only the am_peak-tagged in-range row."""
    resp = client.get(f"/api/corridors/{seeded_corridor_with_segments}/segments?period=am_peak")
    assert resp.status_code == 200
    body = resp.json()
    assert body["period"] == "am_peak"
    assert len(body["segments"]) == 1
    assert body["segments"][0]["mean_slip_sec"] == 120.0


def test_drilldown_unknown_corridor_returns_404(client):
    """A nonexistent corridor_id returns 404 (not an empty 200)."""
    resp = client.get("/api/corridors/999999/segments")
    assert resp.status_code == 404


def test_drilldown_invalid_period_returns_400(client, seeded_corridor_with_segments):
    """Bad period returns 400."""
    resp = client.get(f"/api/corridors/{seeded_corridor_with_segments}/segments?period=bogus")
    assert resp.status_code == 400
