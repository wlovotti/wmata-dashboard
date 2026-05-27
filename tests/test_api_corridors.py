"""Tests for /api/segments?level=corridor.

Uses the in-memory SQLite ``client`` fixture (see ``conftest.py``); seeds
a minimal Corridor + CorridorRouteMembership + CorridorSlipRollup
fixture so the assertions fire deterministically rather than relying on
the populated dev DB.

The drill-down ``/api/corridors/{id}/segments`` endpoint is exercised in
``test_api_corridor_drilldown.py`` (Task 18).

NOTES-62 Phase 4 API surface.
"""

from __future__ import annotations

import pytest

from src.models import (
    Corridor,
    CorridorRouteMembership,
    CorridorSlipRollup,
    Stop,
)


@pytest.fixture
def seeded_corridor(db_session):
    """Insert one corridor + 2 contributing routes + 'all' and 'am_peak' slip rollups."""
    stops = [
        Stop(
            stop_id="S1",
            stop_name="14th & U NW",
            stop_lat=38.917,
            stop_lon=-77.032,
            is_current=True,
        ),
        Stop(
            stop_id="S5",
            stop_name="14th & K NW",
            stop_lat=38.902,
            stop_lon=-77.032,
            is_current=True,
        ),
    ]
    db_session.add_all(stops)

    corridor = Corridor(
        direction_bearing_deg=180.0,
        direction_cardinal="S",
        start_stop_id="S1",
        end_stop_id="S5",
        length_m=1670.0,
        n_routes=2,
        route_set="D50,D5X",
        display_name="SB: 14th & U NW -> 14th & K NW",
        geometry_wkt="LINESTRING(-77.032 38.917, -77.032 38.910, -77.032 38.902)",
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
            CorridorSlipRollup(
                corridor_id=corridor.corridor_id,
                period="all",
                n_route_directions=2,
                n_observed_segments=14,
                n_total_observations=420,
                total_weighted_slip_sec=8400.0,
                mean_slip_per_segment_sec=600.0,
                mean_slip_per_observation_sec=20.0,
                peak_period="am_peak",
            ),
            CorridorSlipRollup(
                corridor_id=corridor.corridor_id,
                period="am_peak",
                n_route_directions=2,
                n_observed_segments=14,
                n_total_observations=120,
                total_weighted_slip_sec=4800.0,
                mean_slip_per_segment_sec=342.86,
                mean_slip_per_observation_sec=40.0,
                peak_period=None,
            ),
        ]
    )
    db_session.commit()
    return corridor.corridor_id


def test_get_segments_level_corridor_returns_expected_shape(client, seeded_corridor):
    """level=corridor returns the corridor-mode response with geometry_wkt for Leaflet."""
    response = client.get("/api/segments?level=corridor")
    assert response.status_code == 200
    body = response.json()
    assert body["level"] == "corridor"
    assert body["period"] == "all"
    assert body["n_rows"] >= 1
    assert "corridors" in body

    first = next(c for c in body["corridors"] if c["corridor_id"] == seeded_corridor)
    assert first["display_name"] == "SB: 14th & U NW -> 14th & K NW"
    assert first["direction_cardinal"] == "S"
    assert first["start_stop_id"] == "S1"
    assert first["start_stop_name"] == "14th & U NW"
    assert first["end_stop_id"] == "S5"
    assert first["end_stop_name"] == "14th & K NW"
    assert first["length_m"] == 1670.0
    assert first["n_routes"] == 2
    assert first["route_set"] == "D50,D5X"
    assert first["total_weighted_slip_sec"] == 8400.0
    assert first["mean_slip_per_observation_sec"] == 20.0
    assert first["peak_period"] == "am_peak"
    # Leaflet pivot: frontend renders geometry client-side, so the API
    # ships the WKT LineString rather than a pre-rendered preview URL.
    assert first["geometry_wkt"].startswith("LINESTRING(")
    assert {c["route_id"] for c in first["contributing_routes"]} == {"D50", "D5X"}


def test_get_segments_level_corridor_filters_by_period(client, seeded_corridor):
    """period=am_peak returns the am_peak rollup row, not the 'all' row."""
    response = client.get("/api/segments?level=corridor&period=am_peak")
    assert response.status_code == 200
    body = response.json()
    assert body["period"] == "am_peak"
    first = next(c for c in body["corridors"] if c["corridor_id"] == seeded_corridor)
    assert first["total_weighted_slip_sec"] == 4800.0


def test_get_segments_level_segment_back_compat(client):
    """Default and level=segment behave identically; PR #140 contract unchanged."""
    default = client.get("/api/segments")
    explicit = client.get("/api/segments?level=segment")
    assert default.status_code == 200
    assert explicit.status_code == 200
    assert "segments" in default.json()
    assert "segments" in explicit.json()


def test_get_segments_invalid_level(client):
    """level must be 'segment' or 'corridor'."""
    response = client.get("/api/segments?level=corridors")  # typo
    assert response.status_code == 400
