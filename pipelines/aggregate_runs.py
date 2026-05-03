"""
Aggregate `stop_events` into `runs` — one row per (service_date, trip_id, source).

This is the C-phase entry point on the stop_events foundation (PRs #42, #43, #44):
runs is the natural unit for trip-level metrics — end-to-end excess time
(NOTES.md NOTES-12), per-run deviation charts (NOTES-5), the delivered-runs
numerator for service-delivered ratio (NOTES-11), and the OTP origin/destination
split (NOTES-10) all build directly on it.

The aggregation is intentionally trivial: load a (route_id, service_date) slice
of stop_events, group by (trip_id, source), compute first/last sequence,
first/last observed timestamp, max gap between consecutive observed arrivals,
and the deviation distribution. The schedule-anchor problem the original
analysis (`analysis/run_quality.py`) had to solve is already solved upstream
in stop_events — `service_date` and `scheduled_arrival_ts` are set authoritatively
by the derivation pipelines from `trip_start_date`, so this pipeline does not
need to re-anchor.

No materialized `is_complete` flag — each downstream metric applies its own
filter at query time. See `Run` in `src/models.py` for the canonical filters.

Idempotent: re-running the same (route, service_date) upserts via the
`uq_runs_service_trip_source` constraint, replacing prior aggregations.

Usage:
  uv run python pipelines/aggregate_runs.py --route C51 --date 2026-05-03
  uv run python pipelines/aggregate_runs.py --all-routes --date 2026-05-03
  uv run python pipelines/aggregate_runs.py --all-routes  # defaults to today (Eastern)
"""

import argparse
import time
from collections import defaultdict
from datetime import date as date_type
from datetime import datetime

import numpy as np
from dotenv import load_dotenv
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.database import get_session
from src.models import Route, Run, StopEvent, StopTime
from src.timezones import eastern_today


def aggregate_run_rows(
    events: list[StopEvent],
    sched_counts: dict[str, int],
    service_date_str: str,
    derived_at: datetime,
) -> list[dict]:
    """Group stop_events by (trip_id, source) and produce one run row per group.

    Pure function — takes the materialized stop_events plus a per-trip schedule
    count and returns insertable row dicts. No DB access, so it's the natural
    test seam for run-level aggregation logic.
    """
    groups: dict[tuple[str, str], list[StopEvent]] = defaultdict(list)
    for e in events:
        groups[(e.trip_id, e.source)].append(e)

    rows: list[dict] = []
    for (trip_id, source), group in groups.items():
        # vehicle_id: latest non-null wins. Vehicle reassignment mid-trip is
        # rare but real; promote it to its own column when it shows up as a
        # metric driver.
        vehicle_id = None
        for e in group:
            if e.vehicle_id:
                vehicle_id = e.vehicle_id

        # direction_id is determined by trip_id, so any row's value will do.
        direction_id = group[0].direction_id
        route_id = group[0].route_id

        observed = [e for e in group if e.observed_arrival_ts is not None]
        skipped_count = sum(1 for e in group if e.schedule_relationship == "SKIPPED")

        observed_by_seq = sorted(observed, key=lambda e: e.stop_sequence)
        observed_by_ts = sorted(observed, key=lambda e: e.observed_arrival_ts)

        first_obs_seq = observed_by_seq[0].stop_sequence if observed_by_seq else None
        last_obs_seq = observed_by_seq[-1].stop_sequence if observed_by_seq else None
        first_obs_ts = observed_by_ts[0].observed_arrival_ts if observed_by_ts else None
        last_obs_ts = observed_by_ts[-1].observed_arrival_ts if observed_by_ts else None

        # max_gap_sec is in observed-arrival ordering, not stop_sequence
        # ordering — a missing middle stop produces a single large gap rather
        # than two small ones, which is what completeness filters care about.
        max_gap_sec: int | None = None
        if len(observed_by_ts) >= 2:
            gaps = [
                (
                    observed_by_ts[i].observed_arrival_ts
                    - observed_by_ts[i - 1].observed_arrival_ts
                ).total_seconds()
                for i in range(1, len(observed_by_ts))
            ]
            max_gap_sec = int(max(gaps))

        deviations = [e.deviation_sec for e in group if e.deviation_sec is not None]
        dev_p50_sec = int(np.percentile(deviations, 50)) if deviations else None
        dev_p95_sec = int(np.percentile(deviations, 95)) if deviations else None

        sched_arrivals = [
            e.scheduled_arrival_ts for e in group if e.scheduled_arrival_ts is not None
        ]
        sched_first = min(sched_arrivals) if sched_arrivals else None
        sched_last = max(sched_arrivals) if sched_arrivals else None

        rows.append(
            {
                "service_date": service_date_str,
                "trip_id": trip_id,
                "route_id": route_id,
                "direction_id": direction_id,
                "source": source,
                "vehicle_id": vehicle_id,
                "stops_scheduled": sched_counts.get(trip_id),
                "sched_first_arrival_ts": sched_first,
                "sched_last_arrival_ts": sched_last,
                "stops_observed": len(observed),
                "stops_skipped": skipped_count,
                "first_obs_seq": first_obs_seq,
                "last_obs_seq": last_obs_seq,
                "first_obs_ts": first_obs_ts,
                "last_obs_ts": last_obs_ts,
                "max_gap_sec": max_gap_sec,
                "dev_p50_sec": dev_p50_sec,
                "dev_p95_sec": dev_p95_sec,
                "derived_at": derived_at,
            }
        )

    return rows


def aggregate_runs_for_route_date(
    db: Session,
    route_id: str,
    service_date: date_type,
    verbose: bool = False,
) -> dict:
    """Materialize runs for one (route_id, service_date) from stop_events.

    Loads every stop_event for the slice, joins per-trip schedule counts from
    GTFS stop_times, groups by (trip_id, source) via `aggregate_run_rows`, and
    upserts. Returns counters describing the run.
    """
    start_ts = time.time()
    service_date_str = service_date.isoformat()

    events = (
        db.query(StopEvent)
        .filter(
            StopEvent.route_id == route_id,
            StopEvent.service_date == service_date_str,
        )
        .all()
    )
    if not events:
        return {
            "route_id": route_id,
            "service_date": service_date_str,
            "stop_events": 0,
            "rows_written": 0,
            "elapsed_sec": round(time.time() - start_ts, 2),
            "note": "No stop_events for this (route, service_date)",
        }

    trip_ids = {e.trip_id for e in events}
    sched_counts: dict[str, int] = dict(
        db.query(StopTime.trip_id, func.count(StopTime.id))
        .filter(StopTime.trip_id.in_(trip_ids), StopTime.is_current)
        .group_by(StopTime.trip_id)
        .all()
    )

    derived_at = datetime.utcnow()
    rows = aggregate_run_rows(events, sched_counts, service_date_str, derived_at)

    rows_written = 0
    if rows:
        stmt = pg_insert(Run).values(rows)
        update_cols = {
            c: stmt.excluded[c]
            for c in (
                "route_id",
                "direction_id",
                "vehicle_id",
                "stops_scheduled",
                "sched_first_arrival_ts",
                "sched_last_arrival_ts",
                "stops_observed",
                "stops_skipped",
                "first_obs_seq",
                "last_obs_seq",
                "first_obs_ts",
                "last_obs_ts",
                "max_gap_sec",
                "dev_p50_sec",
                "dev_p95_sec",
                "derived_at",
            )
        }
        stmt = stmt.on_conflict_do_update(
            constraint="uq_runs_service_trip_source",
            set_=update_cols,
        )
        db.execute(stmt)
        db.commit()
        rows_written = len(rows)

    result = {
        "route_id": route_id,
        "service_date": service_date_str,
        "stop_events": len(events),
        "rows_written": rows_written,
        "elapsed_sec": round(time.time() - start_ts, 2),
    }
    if verbose:
        print(
            f"  {route_id} {service_date_str}: "
            f"{result['stop_events']} stop_events → "
            f"{result['rows_written']} runs ({result['elapsed_sec']}s)"
        )
    return result


def aggregate_for_routes(
    db: Session,
    route_ids: list[str],
    service_date: date_type,
) -> list[dict]:
    """Drive `aggregate_runs_for_route_date` over a list of routes, one date."""
    return [aggregate_runs_for_route_date(db, r, service_date, verbose=True) for r in route_ids]


def main():
    """CLI entry point — parses args, runs aggregation, prints a summary."""
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate stop_events into runs for one (route, date) or all routes on a date."
        )
    )
    parser.add_argument("--route", help="Single route_id to process (e.g., C51)")
    parser.add_argument(
        "--all-routes",
        action="store_true",
        help="Process every current route. Mutually exclusive with --route.",
    )
    parser.add_argument(
        "--date",
        help="Service date in YYYY-MM-DD form (Eastern). Defaults to today (Eastern).",
    )
    args = parser.parse_args()

    if not args.route and not args.all_routes:
        parser.error("must pass --route ROUTE_ID or --all-routes")
    if args.route and args.all_routes:
        parser.error("--route and --all-routes are mutually exclusive")

    load_dotenv()
    if args.date:
        service_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        service_date = eastern_today()

    db = get_session()
    try:
        if args.route:
            route_ids = [args.route]
        else:
            route_ids = [r.route_id for r in db.query(Route).filter(Route.is_current).all()]
            print(f"Processing {len(route_ids)} current routes for {service_date.isoformat()}...")

        results = aggregate_for_routes(db, route_ids, service_date)

        total_events = sum(r["stop_events"] for r in results)
        total_written = sum(r["rows_written"] for r in results)
        total_elapsed = sum(r["elapsed_sec"] for r in results)
        print()
        print(f"Total: {total_events} stop_events → {total_written} runs")
        print(f"Elapsed: {total_elapsed:.1f}s")
    finally:
        db.close()


if __name__ == "__main__":
    main()
