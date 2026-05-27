"""Integration test for the refresh_corridors pipeline (NOTES-62).

Seeds a synthetic 3-route GTFS scenario into a Postgres session and
verifies the pipeline writes the expected ``corridors`` +
``corridor_route_membership`` rows.

Marked ``pg`` because the pipeline's SQL uses Postgres-specific
constructs (``ANY(array)`` parameter binding) that SQLite cannot
execute.
"""

from __future__ import annotations

import pytest

from pipelines.refresh_corridors import refresh_corridors
from src.models import Corridor, CorridorRouteMembership

pytestmark = pytest.mark.pg


def test_refresh_corridors_two_routes_same_corridor(pg_session, populate_fixture_gtfs):
    """Two parallel routes along a synthetic east-west street yield two corridors.

    Fixture details:
      - FX1, FX2 share 10 stops along synthetic East St (both directions)
      - FX3 is a 3-stop perpendicular north-south route with no overlap

    Expected (filtering for route_set='FX1,FX2'):
      - 2 corridors (East St eastbound, East St westbound)
      - 2 distinct opposing cardinals
      - 4 corridor_route_membership rows (2 corridors x 2 routes)
      - FX3 produces no corridor (no colocation with other routes)

    Note: ``pg_session`` inherits the dev DB's prior GTFS content (only
    writes roll back via SAVEPOINT), so the fixture's synthetic routes
    are seeded alongside real WMATA shapes. We filter every assertion
    to the ``FX*`` namespace, which doesn't collide with real route_ids.
    """
    populate_fixture_gtfs(pg_session, scenario="two_routes_one_corridor")

    refresh_corridors(session=pg_session, gtfs_snapshot_id=1)

    fixture_corridors = pg_session.query(Corridor).filter(Corridor.route_set == "FX1,FX2").all()
    assert len(fixture_corridors) == 2, [c.display_name for c in fixture_corridors]

    cardinals = {c.direction_cardinal for c in fixture_corridors}
    assert len(cardinals) == 2, cardinals

    fixture_corridor_ids = {c.corridor_id for c in fixture_corridors}
    fixture_memberships = (
        pg_session.query(CorridorRouteMembership)
        .filter(CorridorRouteMembership.corridor_id.in_(fixture_corridor_ids))
        .all()
    )
    assert len(fixture_memberships) == 4  # 2 corridors x 2 routes
    route_ids = {m.route_id for m in fixture_memberships}
    assert route_ids == {"FX1", "FX2"}

    # FX3 (perpendicular) must not appear in any corridor membership.
    fx3_memberships = (
        pg_session.query(CorridorRouteMembership)
        .filter(CorridorRouteMembership.route_id == "FX3")
        .count()
    )
    assert fx3_memberships == 0
