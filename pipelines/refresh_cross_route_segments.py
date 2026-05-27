"""
Refresh `cross_route_segment_rollup` — aggregate per-segment slip across all
routes that share the same (from_stop_id, to_stop_id) stop-pair. NOTES-59.

Reads `route_diagnostic_segment` rows that were already materialized by
`pipelines/refresh_route_diagnostic_profile.py` (PR #107) and aggregates
them by stop-pair × period. Only stop-pairs traversed by ≥2 distinct
route_ids are retained; single-route stop-pairs carry no cross-route signal.

V1 uses stop-pair identity matching only — same ``(from_stop_id, to_stop_id)``
across routes counts as the same segment.  Shape-aware corridor rollup
(NOTES-62) is the complementary view — see
``pipelines/refresh_corridors.py`` + ``pipelines/refresh_corridor_slip.py``
and ``GET /api/segments?level=corridor``.

Design: full replace (DELETE then INSERT) on every run.  The rollup table
is bounded — one row per unique (from_stop_id, to_stop_id, period) pair —
and the source `route_diagnostic_segment` is itself a bounded materialized
table.  Full-replace is simpler and more predictable than an incremental
diff.

Usage:
  uv run python -m pipelines.refresh_cross_route_segments
  uv run python -m pipelines.refresh_cross_route_segments --period pm_peak
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime

from dotenv import load_dotenv

from src.database import get_session
from src.models import CrossRouteSegmentRollup, Route, RouteDiagnosticSegment
from src.route_diagnostics import ALL_PERIODS
from src.timezones import utcnow_naive

# Minimum number of distinct route_ids per stop-pair to be included.
MIN_ROUTES_PER_PAIR = 2

# Named periods to consider when computing peak_period on the 'all' row.
_NAMED_PERIODS = ("am_peak", "midday", "pm_peak", "evening", "late")


def _build_rollup(db, period: str) -> list[dict]:
    """Aggregate route_diagnostic_segment rows by stop-pair for one period.

    Returns a list of dicts shaped for upsert into `cross_route_segment_rollup`.
    Only pairs with at least ``MIN_ROUTES_PER_PAIR`` distinct route_ids are
    returned; pairs with a single route are silently dropped.

    For ``period == 'all'``, also populates ``peak_period`` — the named period
    with the highest ``total_weighted_slip_sec`` for the pair — by querying the
    named-period rows already materialized in the same table.  When no
    named-period data is available for a pair, ``peak_period`` is None.

    Args:
        db: SQLAlchemy session.
        period: One of the values in ``ALL_PERIODS``.

    Returns:
        List of rollup row dicts.
    """
    seg_rows = (
        db.query(RouteDiagnosticSegment).filter(RouteDiagnosticSegment.period == period).all()
    )

    # Bulk-load route short names once to avoid N+1 queries.  Stop names are
    # not needed here — they are joined in the API read layer from the current
    # GTFS snapshot each time the endpoint is called.
    route_ids = {r.route_id for r in seg_rows}

    route_name_map: dict[str, str | None] = dict(
        db.query(Route.route_id, Route.route_short_name)
        .filter(Route.route_id.in_(route_ids), Route.is_current)
        .all()
    )

    # Accumulate per-stop-pair → list of (route_id, direction_id,
    # mean_slip_sec, n_observations) rows.
    pair_rows: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in seg_rows:
        pair = (r.from_stop_id, r.to_stop_id)
        pair_rows[pair].append(
            {
                "route_id": r.route_id,
                "route_short_name": route_name_map.get(r.route_id),
                "direction_id": r.direction_id,
                "mean_slip_sec": r.mean_slip_sec,
                "n_observations": r.n_observations,
            }
        )

    # For 'all' period, also build a per-pair peak_period lookup from the
    # named-period rows already materialized in this pipeline run.
    peak_period_map: dict[tuple[str, str], str | None] = {}
    if period == "all":
        named_rows = (
            db.query(RouteDiagnosticSegment)
            .filter(RouteDiagnosticSegment.period.in_(list(_NAMED_PERIODS)))
            .all()
        )
        # per-pair-per-named-period: sum(mean_slip_sec * n_observations)
        pair_period_slip: dict[tuple[str, str], dict[str, float]] = defaultdict(
            lambda: defaultdict(float)
        )
        for r in named_rows:
            pair = (r.from_stop_id, r.to_stop_id)
            pair_period_slip[pair][r.period] += r.mean_slip_sec * r.n_observations
        for pair, per_period in pair_period_slip.items():
            if per_period:
                peak_period_map[pair] = max(per_period, key=lambda p: per_period[p])

    now = utcnow_naive()
    out: list[dict] = []
    for pair, rows in pair_rows.items():
        distinct_routes = {r["route_id"] for r in rows}
        if len(distinct_routes) < MIN_ROUTES_PER_PAIR:
            continue

        from_stop_id, to_stop_id = pair
        total_weighted = sum(r["mean_slip_sec"] * r["n_observations"] for r in rows)
        n_total_obs = sum(r["n_observations"] for r in rows)

        # Sort contributing routes by n_observations descending for the
        # drilldown list (most-observed route first).
        contributing = sorted(rows, key=lambda r: -r["n_observations"])
        contributing_json = json.dumps(contributing)

        out.append(
            {
                "from_stop_id": from_stop_id,
                "to_stop_id": to_stop_id,
                "period": period,
                "total_weighted_slip_sec": total_weighted,
                "n_routes": len(distinct_routes),
                "n_route_directions": len(rows),
                "n_total_observations": n_total_obs,
                "contributing_routes_json": contributing_json,
                "peak_period": peak_period_map.get(pair) if period == "all" else None,
                "computed_at": now,
            }
        )

    return out


def refresh_cross_route_segments(db, period: str | None = None) -> dict[str, int]:
    """Rebuild cross_route_segment_rollup for the given period(s).

    If ``period`` is None, rebuilds all periods.  Always performs a full
    replace (DELETE + INSERT) within the requested period(s) so stale
    stop-pair rows that fell below the ≥2-route threshold are pruned.

    Args:
        db: SQLAlchemy session.
        period: If set, process only this period; otherwise all periods.

    Returns:
        Dict mapping period name → number of rows inserted.
    """
    periods = [period] if period else list(ALL_PERIODS)
    counts: dict[str, int] = {}
    for p in periods:
        # Full replace for this period.
        db.query(CrossRouteSegmentRollup).filter(CrossRouteSegmentRollup.period == p).delete(
            synchronize_session=False
        )
        rows = _build_rollup(db, p)
        if rows:
            db.add_all(
                [
                    CrossRouteSegmentRollup(
                        from_stop_id=r["from_stop_id"],
                        to_stop_id=r["to_stop_id"],
                        period=r["period"],
                        total_weighted_slip_sec=r["total_weighted_slip_sec"],
                        n_routes=r["n_routes"],
                        n_route_directions=r["n_route_directions"],
                        n_total_observations=r["n_total_observations"],
                        contributing_routes_json=r["contributing_routes_json"],
                        peak_period=r["peak_period"],
                        computed_at=r["computed_at"],
                    )
                    for r in rows
                ]
            )
        db.flush()
        counts[p] = len(rows)
    return counts


def main() -> int:
    """CLI entry point — refresh cross-route segment rollup."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--period",
        type=str,
        default=None,
        choices=list(ALL_PERIODS),
        help="If set, refresh only this period (default: all periods).",
    )
    # Accepted but ignored — keeps parity with run_daily_batch.py dispatcher
    # which passes --all-routes and --date to every pipeline uniformly.
    parser.add_argument("--all-routes", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--date", type=str, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    load_dotenv()
    db = get_session()
    try:
        t0 = datetime.now()
        counts = refresh_cross_route_segments(db, period=args.period)
        db.commit()
        elapsed = (datetime.now() - t0).total_seconds()
        total = sum(counts.values())
        print(
            f"refresh_cross_route_segments: {total} rows across "
            f"{len(counts)} period(s) in {elapsed:.1f}s"
        )
        for p, n in sorted(counts.items()):
            print(f"  {p}: {n} stop-pairs (≥{MIN_ROUTES_PER_PAIR} routes)")
        return 0
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
