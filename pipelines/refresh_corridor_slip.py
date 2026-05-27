"""Pipeline: rebuild ``corridor_slip_rollup`` from per-route slip data.

Joins ``corridor_route_membership`` against the existing
``route_diagnostic_segment`` table and aggregates per (corridor_id,
period). Designed to run nightly from ``pipelines/run_daily_batch.py``
after the per-route diagnostics have been refreshed.

Two passes:
  1. INSERT ... SELECT that does the primary aggregation.
  2. UPDATE that backfills ``peak_period`` on each corridor's
     ``period='all'`` row (the named period with the highest
     ``total_weighted_slip_sec``).

Source window: ``route_diagnostic_segment`` is a 30-day rolling
aggregate (see ``RouteDiagnosticSegment`` docstring), so the rollup
inherits that window automatically — no per-day batching here.

Row-count guard: if the source has data but the rollup ends up empty,
the pipeline raises. The most likely cause is an empty
``corridor_route_membership`` (corridors not yet refreshed), which
would silently zero out the rollup and break the UI without the guard.

Usage:
    uv run python pipelines/refresh_corridor_slip.py
"""

from __future__ import annotations

from sqlalchemy import delete, text
from sqlalchemy.orm import Session

from src.models import CorridorSlipRollup


def refresh_corridor_slip(session: Session) -> dict[str, int]:
    """Rebuild the ``corridor_slip_rollup`` table.

    Truncates the table then re-aggregates from
    ``route_diagnostic_segment`` through ``corridor_route_membership``.
    The caller is responsible for committing the surrounding transaction.

    Args:
        session: SQLAlchemy session bound to the target database.

    Returns:
        Dict with ``input_segments`` (source row count) and
        ``rollups_inserted`` (output row count).

    Raises:
        RuntimeError: if ``route_diagnostic_segment`` has rows but the
        aggregation produces none — a signal that
        ``corridor_route_membership`` is empty or otherwise mis-joined.
    """
    input_count = session.execute(
        text("SELECT COUNT(*) FROM route_diagnostic_segment")
    ).scalar_one()

    session.execute(delete(CorridorSlipRollup))

    session.execute(
        text(
            """
            INSERT INTO corridor_slip_rollup (
                corridor_id,
                period,
                n_route_directions,
                n_observed_segments,
                n_total_observations,
                total_weighted_slip_sec,
                mean_slip_per_segment_sec,
                mean_slip_per_observation_sec,
                computed_at
            )
            SELECT
                crm.corridor_id,
                rds.period,
                COUNT(DISTINCT (rds.route_id, rds.direction_id))
                    AS n_route_directions,
                COUNT(*) AS n_observed_segments,
                SUM(rds.n_observations) AS n_total_observations,
                SUM(rds.mean_slip_sec * rds.n_observations)
                    AS total_weighted_slip_sec,
                CASE
                    WHEN COUNT(*) > 0
                    THEN SUM(rds.mean_slip_sec * rds.n_observations)
                         / COUNT(*)
                    ELSE NULL
                END AS mean_slip_per_segment_sec,
                CASE
                    WHEN SUM(rds.n_observations) > 0
                    THEN SUM(rds.mean_slip_sec * rds.n_observations)
                         / SUM(rds.n_observations)
                    ELSE NULL
                END AS mean_slip_per_observation_sec,
                NOW() AS computed_at
            FROM corridor_route_membership crm
            JOIN route_diagnostic_segment rds
              ON rds.route_id = crm.route_id
             AND rds.direction_id = crm.direction_id
             AND rds.from_seq >= crm.start_stop_sequence
             AND rds.to_seq   <= crm.end_stop_sequence
            GROUP BY crm.corridor_id, rds.period
            """
        )
    )

    inserted = session.execute(text("SELECT COUNT(*) FROM corridor_slip_rollup")).scalar_one()

    if input_count > 0 and inserted == 0:
        raise RuntimeError(
            f"refresh_corridor_slip produced 0 rows from {input_count} input "
            "segments. Check that corridor_route_membership is populated and "
            "that route_diagnostic_segment has matching (route_id, direction_id) "
            "rows whose (from_seq, to_seq) falls within a corridor's stop range."
        )

    # Backfill peak_period on each corridor's period='all' row with the
    # named period (am_peak/midday/pm_peak/evening/late) carrying the
    # highest total_weighted_slip_sec. ARRAY_AGG with ORDER BY is the
    # idiomatic "pick the top by another column" pattern in Postgres.
    session.execute(
        text(
            """
            UPDATE corridor_slip_rollup csr_all
            SET peak_period = sub.peak_period
            FROM (
                SELECT corridor_id,
                       (ARRAY_AGG(period ORDER BY total_weighted_slip_sec DESC))[1]
                           AS peak_period
                FROM corridor_slip_rollup
                WHERE period != 'all'
                GROUP BY corridor_id
            ) sub
            WHERE csr_all.corridor_id = sub.corridor_id
              AND csr_all.period = 'all'
            """
        )
    )

    session.flush()
    return {"input_segments": input_count, "rollups_inserted": inserted}


def main() -> None:
    """CLI entrypoint: refresh corridor slip rollup against the configured DB."""
    from dotenv import load_dotenv

    from src.database import get_session

    load_dotenv()
    session = get_session()
    try:
        counts = refresh_corridor_slip(session=session)
        session.commit()
        print(f"[refresh_corridor_slip] {counts}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
