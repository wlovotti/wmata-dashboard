"""
Refresh `route_diagnostic_segment` / `_timepoint` / `_direction` for every
current route. NOTES-57.

Reads the last `--lookback-days` (default 30) of `stop_events`, runs
`src/route_diagnostics.py:compute_route_diagnostics` per route × period,
and upserts the result into the three diagnostic tables. Idempotent:
re-running replaces every (route, period, ...) cell touched.

Designed to be called once per daily-batch invocation, after the
per-date derivation pipelines have committed their stop_events / runs
rows. The diagnostic surfaces are inherently windowed (30-day pooled
sample), so this is a non-date-scoped housekeeping step rather than a
per-date one — it sweeps the entire current window in one pass.

Usage:
  uv run python -m pipelines.refresh_route_diagnostic_profile
  uv run python -m pipelines.refresh_route_diagnostic_profile --lookback-days 60
  uv run python -m pipelines.refresh_route_diagnostic_profile --route D80   # one route only
"""

from __future__ import annotations

import argparse
import sys
from datetime import date as date_type
from datetime import datetime
from typing import Any

from dotenv import load_dotenv

from src.database import get_session
from src.models import (
    Route,
    RouteDiagnosticDirection,
    RouteDiagnosticSegment,
    RouteDiagnosticTimepoint,
)
from src.route_diagnostics import (
    ALL_PERIODS,
    compute_route_diagnostics,
    default_service_date_range,
    fetch_route_timepoint_stops,
)
from src.timezones import eastern_today


def _list_active_route_ids(db) -> list[str]:
    """Return sorted route_ids for every current GTFS route."""
    return [
        r[0]
        for r in db.query(Route.route_id).filter(Route.is_current).order_by(Route.route_id).all()
    ]


def _replace_segments(
    db,
    route_id: str,
    rows: list[dict[str, Any]],
    timepoint_stop_ids: set[str],
) -> int:
    """Delete + insert every `route_diagnostic_segment` row for `route_id`.

    Delete-then-insert is the simplest correct upsert across SQLite (tests)
    and Postgres (prod) without dialect-specific SQL. The table is bounded
    in size (one row per (route, direction, period, segment) ≈ a few
    hundred rows per route), so the cost is trivial.
    """
    db.query(RouteDiagnosticSegment).filter(RouteDiagnosticSegment.route_id == route_id).delete(
        synchronize_session=False
    )
    objs = [
        RouteDiagnosticSegment(
            route_id=r["route_id"],
            direction_id=r["direction_id"],
            period=r["period"],
            from_seq=r["from_seq"],
            from_stop_id=r["from_stop_id"],
            to_seq=r["to_seq"],
            to_stop_id=r["to_stop_id"],
            mean_slip_sec=r["mean_slip_sec"],
            cum_slip_sec=r["cum_slip_sec"],
            n_observations=r["n_observations"],
            is_timepoint=r["to_stop_id"] in timepoint_stop_ids,
            computed_at=r["computed_at"],
        )
        for r in rows
    ]
    db.add_all(objs)
    return len(objs)


def _replace_timepoints(db, route_id: str, rows: list[dict[str, Any]]) -> int:
    """Delete + insert every `route_diagnostic_timepoint` row for `route_id`."""
    db.query(RouteDiagnosticTimepoint).filter(RouteDiagnosticTimepoint.route_id == route_id).delete(
        synchronize_session=False
    )
    objs = [
        RouteDiagnosticTimepoint(
            route_id=r["route_id"],
            direction_id=r["direction_id"],
            period=r["period"],
            timepoint_stop_id=r["timepoint_stop_id"],
            classification=r["classification"],
            median_dev_entering=r["median_dev_entering"],
            median_dev_leaving=r["median_dev_leaving"],
            p10_dev_entering=r["p10_dev_entering"],
            p10_dev_leaving=r["p10_dev_leaving"],
            n_observations=r["n_observations"],
            computed_at=r["computed_at"],
        )
        for r in rows
    ]
    db.add_all(objs)
    return len(objs)


def _replace_directions(db, route_id: str, rows: list[dict[str, Any]]) -> int:
    """Delete + insert every `route_diagnostic_direction` row for `route_id`."""
    db.query(RouteDiagnosticDirection).filter(RouteDiagnosticDirection.route_id == route_id).delete(
        synchronize_session=False
    )
    objs = [
        RouteDiagnosticDirection(
            route_id=r["route_id"],
            direction_id=r["direction_id"],
            period=r["period"],
            early_pct=r["early_pct"],
            late_pct=r["late_pct"],
            signature=r["signature"],
            n_observations=r["n_observations"],
            computed_at=r["computed_at"],
        )
        for r in rows
    ]
    db.add_all(objs)
    return len(objs)


def refresh_for_route(
    db,
    route_id: str,
    service_date_range: tuple[date_type, date_type],
) -> dict[str, int]:
    """Refresh all three diagnostic tables for one `route_id`.

    Returns row counts per table, primarily for logging. A route with no
    eligible stop_events in the window will produce all zeros without
    raising — this is the right behavior for routes that drop out of the
    GTFS active set or have purely scheduled-only days.
    """
    result = compute_route_diagnostics(db, route_id, service_date_range, periods=ALL_PERIODS)
    timepoint_stop_ids = fetch_route_timepoint_stops(db, route_id)

    n_seg = _replace_segments(db, route_id, result["segments"], timepoint_stop_ids)
    n_tp = _replace_timepoints(db, route_id, result["timepoints"])
    n_dir = _replace_directions(db, route_id, result["directions"])
    return {"segments": n_seg, "timepoints": n_tp, "directions": n_dir}


def main() -> int:
    """CLI entry point — refresh every current route, return 0 on success."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Service-date window depth in days (default: 30).",
    )
    parser.add_argument(
        "--route",
        type=str,
        default=None,
        help="If set, refresh only this route_id (otherwise every current route).",
    )
    # Accepted but ignored — the diagnostic refresh is non-date-scoped (it
    # always sweeps the lookback window). The flag exists so
    # `pipelines/run_daily_batch.py` can dispatch every per-date pipeline
    # with the same args.
    parser.add_argument("--all-routes", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--date", type=str, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    load_dotenv()
    today = eastern_today()
    service_date_range = default_service_date_range(today, days=args.lookback_days)
    print(
        f"refresh_route_diagnostic_profile: window "
        f"{service_date_range[0].isoformat()}..{service_date_range[1].isoformat()}"
    )

    db = get_session()
    try:
        if args.route:
            route_ids = [args.route]
        else:
            route_ids = _list_active_route_ids(db)

        total = {"segments": 0, "timepoints": 0, "directions": 0}
        failures = 0
        started = datetime.now()
        for i, rid in enumerate(route_ids, 1):
            try:
                counts = refresh_for_route(db, rid, service_date_range)
                db.commit()
                for k in total:
                    total[k] += counts[k]
                print(
                    f"  [{i}/{len(route_ids)}] {rid}: "
                    f"seg={counts['segments']} tp={counts['timepoints']} "
                    f"dir={counts['directions']}"
                )
            except Exception as exc:
                failures += 1
                db.rollback()
                print(f"  [{i}/{len(route_ids)}] {rid}: FAIL — {exc}")

        elapsed = (datetime.now() - started).total_seconds()
        print(
            f"refresh_route_diagnostic_profile: done in {elapsed:.1f}s. "
            f"segments={total['segments']} timepoints={total['timepoints']} "
            f"directions={total['directions']} failures={failures}"
        )
        return 0 if failures == 0 else 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
