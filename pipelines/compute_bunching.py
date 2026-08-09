"""
Materialize bunching rates into `route_headway_metrics` (PR #53).

Computes per-(route, service_date, time_period) bunching counts via
`src/bunching.py` and upserts five rows per route per service_date — one
per time_period. Idempotent: re-running the same (route, date) replaces
the prior rows via the unique constraint.

Multi-agency (NOTES-100): pass ``--agency`` (matching a
``config/agencies/<agency>.yaml``) to compute against a non-WMATA
database with the correct service-date timezone for the ``--date``-less
default AND (as of the agency-local hour bucketing fix, PR #190) for the
bunching computation's own observed-side hour-of-day bucketing —
``cfg.timezone`` is threaded into ``materialize_for_routes`` /
``materialize_bunching_for_route_date`` / ``compute_bunching_for_route_date``.
Omitting ``--agency`` keeps the WMATA/Eastern default. Note: ``--days``
without an explicit ``--date`` still resolves via
``iter_recent_eastern_dates`` (Eastern-hardcoded) — always pass
``--date`` explicitly for a non-WMATA backfill.

Usage:
  uv run python pipelines/compute_bunching.py --route C51 --date 2026-05-03
  uv run python pipelines/compute_bunching.py --all-routes --date 2026-05-03
  uv run python pipelines/compute_bunching.py --all-routes  # defaults to today (agency-local)
  uv run python pipelines/compute_bunching.py --all-routes --date 2026-07-23 --agency sfmta
"""

import argparse
import time
from datetime import date as date_type
from datetime import datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy.orm import Session

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.batch_iterator import run_route_date_grid
from src.bunching import compute_bunching_for_route_date
from src.database import get_session
from src.date_ranges import iter_eastern_dates, iter_recent_eastern_dates
from src.gtfs_versioning import gtfs_version_filter
from src.models import Route, RouteHeadwayMetrics
from src.timezones import local_today, utcnow_naive
from src.upsert_helpers import upsert_rows


def materialize_bunching_for_route_date(
    db: Session,
    route_id: str,
    service_date: date_type,
    verbose: bool = False,
    tz_name: str = "America/New_York",
) -> dict:
    """Compute and upsert bunching rows for one (route, service_date).

    Returns counters describing the run. Always writes five rows (one per
    time_period) even when the route has no eligible observations — empty
    rows let consumers distinguish "evaluated" from "not evaluated."

    `tz_name` (NOTES-103 multi-agency; default Eastern) is forwarded to
    `compute_bunching_for_route_date` so the observed side buckets by the
    agency's own local hour, matching the scheduled side (always
    agency-local, GTFS clock time). Without this, a non-Eastern agency's
    cell-hour join is degenerate — see the agency-local hour bucketing
    fix, PR #190.
    """
    start_ts = time.time()
    rows = compute_bunching_for_route_date(db, route_id, service_date, tz_name=tz_name)

    computed_at = utcnow_naive()
    insert_rows = [
        {
            "route_id": r["route_id"],
            "date": r["service_date"],
            "time_period": r["time_period"],
            "day_type": r["day_type"],
            "bunching_count": r["bunching_count"],
            "total_headways": r["total_headways"],
            "bunching_rate": r["bunching_rate"],
            "computed_at": computed_at,
        }
        for r in rows
    ]

    rows_written = 0
    if insert_rows:
        upsert_rows(
            db,
            RouteHeadwayMetrics,
            insert_rows,
            constraint_name="uq_route_headway_metrics_key",
            update_cols=[
                "day_type",
                "bunching_count",
                "total_headways",
                "bunching_rate",
                "computed_at",
            ],
        )
        rows_written = len(insert_rows)

    nonempty_periods = sum(1 for r in rows if r["total_headways"] > 0)
    bunched_total = sum(r["bunching_count"] for r in rows)
    obs_total = sum(r["total_headways"] for r in rows)

    result = {
        "route_id": route_id,
        "service_date": service_date.isoformat(),
        "rows_written": rows_written,
        "nonempty_periods": nonempty_periods,
        "bunched_total": bunched_total,
        "obs_total": obs_total,
        "elapsed_sec": round(time.time() - start_ts, 2),
    }
    if verbose:
        rate = (bunched_total / obs_total) if obs_total else None
        rate_str = f"{rate:.1%}" if rate is not None else "n/a"
        print(
            f"  {route_id} {service_date.isoformat()}: "
            f"{bunched_total}/{obs_total} bunched ({rate_str}), "
            f"{nonempty_periods}/5 periods populated ({result['elapsed_sec']}s)"
        )
    return result


def materialize_for_routes(
    db: Session,
    route_ids: list[str],
    service_dates: list[date_type],
    tz_name: str = "America/New_York",
) -> list[dict]:
    """Drive `materialize_bunching_for_route_date` over a (routes × dates) grid.

    `tz_name` (NOTES-103 multi-agency; default Eastern) is forwarded to
    every per-(route, date) call.
    """
    return run_route_date_grid(
        materialize_bunching_for_route_date,
        db,
        route_ids,
        service_dates,
        verbose=True,
        tz_name=tz_name,
    )


def main():
    """CLI entry point — parse args, materialize bunching, print summary."""
    parser = argparse.ArgumentParser(
        description="Materialize bunching rates into route_headway_metrics."
    )
    parser.add_argument("--route", help="Single route_id (e.g., C51)")
    parser.add_argument(
        "--all-routes",
        action="store_true",
        help="Process every current route. Mutually exclusive with --route.",
    )
    parser.add_argument(
        "--date",
        help="Service date in YYYY-MM-DD form (Eastern). Defaults to today (Eastern).",
    )
    parser.add_argument(
        "--days",
        type=int,
        help="Backfill the last N days ending on --date (or today). Mutually exclusive with --start-date.",
    )
    parser.add_argument("--start-date", help="Start of backfill range (inclusive), YYYY-MM-DD.")
    parser.add_argument("--end-date", help="End of backfill range (inclusive), YYYY-MM-DD.")
    parser.add_argument(
        "--gtfs-snapshot-id",
        type=int,
        default=None,
        help=(
            "List routes from this historical GTFS snapshot instead of the "
            "current one (bunching itself reads runs, not the schedule) — "
            "for backfilling dates whose schedule has been superseded."
        ),
    )
    parser.add_argument(
        "--agency",
        default="wmata",
        help=(
            "Agency name matching config/agencies/<agency>.yaml (default: "
            "'wmata'). Selects the service-date timezone (for the "
            "--date-less default only — see module docstring) and target "
            "database — see pipelines/run_daily_batch.py."
        ),
    )
    args = parser.parse_args()

    if not args.route and not args.all_routes:
        parser.error("must pass --route ROUTE_ID or --all-routes")
    if args.route and args.all_routes:
        parser.error("--route and --all-routes are mutually exclusive")
    if args.days is not None and (args.start_date or args.end_date):
        parser.error("--days and --start-date/--end-date are mutually exclusive")
    if (args.start_date is None) != (args.end_date is None):
        parser.error("--start-date and --end-date must be used together")
    if args.days is not None and args.date is None and args.agency != "wmata":
        # --days without --date resolves via iter_recent_eastern_dates,
        # which is hardcoded to eastern_today() (see module docstring) --
        # silently using Eastern "today" for a non-WMATA agency's backfill
        # window would compute the wrong date range. Always pass --date
        # explicitly alongside --days for a non-default agency.
        parser.error(
            "--days without --date is Eastern-hardcoded and unsafe for "
            f"--agency {args.agency!r} — pass --date explicitly alongside --days"
        )

    load_dotenv()
    cfg = load_agency_config(args.agency)

    if args.start_date and args.end_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        if start > end:
            parser.error("--start-date must be <= --end-date")
        service_dates: list[date_type] = list(iter_eastern_dates(start, end))
    elif args.days is not None:
        if args.date:
            end = datetime.strptime(args.date, "%Y-%m-%d").date()
            start = end - timedelta(days=args.days - 1)
            service_dates = list(iter_eastern_dates(start, end))
        else:
            service_dates = list(iter_recent_eastern_dates(args.days))
    elif args.date:
        service_dates = [datetime.strptime(args.date, "%Y-%m-%d").date()]
    else:
        service_dates = [local_today(cfg.timezone)]

    db = get_session(db_url=resolve_agency_db_url(cfg))
    try:
        if args.route:
            route_ids = [args.route]
        else:
            route_ids = [
                r.route_id
                for r in db.query(Route)
                .filter(gtfs_version_filter(Route, args.gtfs_snapshot_id))
                .all()
            ]
            print(f"Processing {len(route_ids)} routes × {len(service_dates)} dates...")

        results = materialize_for_routes(db, route_ids, service_dates, tz_name=cfg.timezone)

        rows_written = sum(r["rows_written"] for r in results)
        bunched_total = sum(r["bunched_total"] for r in results)
        obs_total = sum(r["obs_total"] for r in results)
        elapsed = sum(r["elapsed_sec"] for r in results)
        rate = (bunched_total / obs_total) if obs_total else None
        rate_str = f"{rate:.1%}" if rate is not None else "n/a"
        print()
        print(
            f"Total: {bunched_total}/{obs_total} bunched ({rate_str}) — "
            f"{rows_written} rows upserted across {len(service_dates)} date(s)"
        )
        print(f"Elapsed: {elapsed:.1f}s")
    finally:
        db.close()


if __name__ == "__main__":
    main()
