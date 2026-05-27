"""Integration test for the refresh_corridor_slip pipeline (NOTES-62).

Seeds the same synthetic 3-route fixture used in
``test_refresh_corridors.py``, runs ``refresh_corridors`` to materialize
the corridor + membership tables, then inserts a known set of
``route_diagnostic_segment`` rows and verifies the slip aggregation
math on the eastbound FX corridor.

Marked ``pg`` because the pipeline's INSERT...SELECT uses
``ARRAY_AGG`` for the peak_period backfill (Postgres-only).
"""

from __future__ import annotations

import pytest

from pipelines.refresh_corridor_slip import refresh_corridor_slip
from pipelines.refresh_corridors import refresh_corridors
from src.models import Corridor, CorridorSlipRollup, RouteDiagnosticSegment

pytestmark = pytest.mark.pg


def test_refresh_corridor_slip_aggregates_from_per_route_segments(
    pg_session, populate_fixture_gtfs
):
    """Slip aggregation sums (mean_slip_sec * n_observations) across contributing routes.

    Setup:
      - 2 routes (FX1, FX2) on East St; one EB corridor + one WB corridor.
      - 9 dir-0 segments per route (seq 1->2, ..., 9->10), each with
        mean_slip_sec=60.0 and n_observations=10. Only dir-0 segments
        are seeded, so only the eastbound (route_set='FX1,FX2') corridor
        accumulates slip; the westbound has none.

    Expected eastbound total_weighted_slip_sec:
      9 segments * 60.0 sec * 10 obs * 2 routes = 10800.0
    """
    populate_fixture_gtfs(pg_session, scenario="two_routes_one_corridor")
    refresh_corridors(session=pg_session, gtfs_snapshot_id=1)

    # Look up the FX corridors so we can scope assertions to them.
    fx_corridors = pg_session.query(Corridor).filter(Corridor.route_set == "FX1,FX2").all()
    fx_corridor_ids = {c.corridor_id for c in fx_corridors}
    assert len(fx_corridors) == 2, "fixture should produce both EB + WB FX corridors"

    # Seed direction-0 segments only — 9 per route.
    for route_id in ("FX1", "FX2"):
        for from_seq in range(1, 10):
            pg_session.add(
                RouteDiagnosticSegment(
                    route_id=route_id,
                    direction_id=0,
                    period="all",
                    from_seq=from_seq,
                    from_stop_id=f"east_{from_seq - 1}",
                    to_seq=from_seq + 1,
                    to_stop_id=f"east_{from_seq}",
                    mean_slip_sec=60.0,
                    cum_slip_sec=60.0 * from_seq,
                    n_observations=10,
                    is_timepoint=False,
                )
            )
    pg_session.flush()

    refresh_corridor_slip(session=pg_session)

    fx_rollups = (
        pg_session.query(CorridorSlipRollup)
        .filter(CorridorSlipRollup.corridor_id.in_(fx_corridor_ids))
        .filter(CorridorSlipRollup.period == "all")
        .all()
    )
    assert len(fx_rollups) == 1, [r.corridor_id for r in fx_rollups]

    eb = fx_rollups[0]
    assert eb.n_route_directions == 2  # FX1 dir 0 + FX2 dir 0
    assert eb.n_observed_segments == 18  # 9 segments x 2 routes
    assert eb.n_total_observations == 180  # 18 segments x 10 obs
    assert abs(eb.total_weighted_slip_sec - 10800.0) < 1e-6, eb.total_weighted_slip_sec
    assert abs(eb.mean_slip_per_segment_sec - 600.0) < 1e-6
    assert abs(eb.mean_slip_per_observation_sec - 60.0) < 1e-6
