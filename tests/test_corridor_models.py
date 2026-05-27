"""Smoke tests for Corridor + CorridorRouteMembership + CorridorSlipRollup ORM models (NOTES-62)."""

from src.models import Corridor, CorridorRouteMembership, CorridorSlipRollup


def test_corridor_tablename():
    """Corridor maps to the corridors table."""
    assert Corridor.__tablename__ == "corridors"


def test_corridor_route_membership_tablename():
    """CorridorRouteMembership maps to the corridor_route_membership table."""
    assert CorridorRouteMembership.__tablename__ == "corridor_route_membership"


def test_corridor_required_columns():
    """Corridor declares every column from the spec."""
    columns = {c.name for c in Corridor.__table__.columns}
    expected = {
        "corridor_id",
        "direction_bearing_deg",
        "direction_cardinal",
        "start_stop_id",
        "end_stop_id",
        "length_m",
        "n_routes",
        "route_set",
        "display_name",
        "geometry_wkt",
        "gtfs_snapshot_id",
        "created_at",
    }
    assert expected.issubset(columns), f"Missing: {expected - columns}"


def test_corridor_route_membership_required_columns():
    """CorridorRouteMembership declares every column from the spec."""
    columns = {c.name for c in CorridorRouteMembership.__table__.columns}
    expected = {
        "corridor_id",
        "route_id",
        "direction_id",
        "canonical_shape_id",
        "start_stop_sequence",
        "end_stop_sequence",
    }
    assert expected.issubset(columns), f"Missing: {expected - columns}"


def test_corridor_slip_rollup_tablename():
    """CorridorSlipRollup maps to the corridor_slip_rollup table."""
    assert CorridorSlipRollup.__tablename__ == "corridor_slip_rollup"


def test_corridor_slip_rollup_required_columns():
    """CorridorSlipRollup declares every column from the spec."""
    columns = {c.name for c in CorridorSlipRollup.__table__.columns}
    expected = {
        "corridor_id",
        "period",
        "n_route_directions",
        "n_observed_segments",
        "n_total_observations",
        "total_weighted_slip_sec",
        "mean_slip_per_segment_sec",
        "mean_slip_per_observation_sec",
        "peak_period",
        "computed_at",
    }
    assert expected.issubset(columns), f"Missing: {expected - columns}"
