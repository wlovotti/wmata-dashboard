"""
Derive `stop_events` rows from VehiclePosition data — the `source='proximity'`
half of the stop_events foundation (PRs #42, #43, #44).

The proximity derivation matches each vehicle position to the nearest scheduled
stop within 50 m, then keeps the FIRST detection per (trip_id, stop_sequence)
as the observed arrival time. This is the standard pattern from TIDES /
GTFS-Performance for arrival-side metrics — first-within-threshold is more
faithful to "the bus arrived" than the existing OTP path's last-within-threshold
(which models passenger-boarding fairness, a different question).

The pipeline runs against one (route_id, service_date) at a time. Idempotent:
re-running the same window upserts via ON CONFLICT, replacing prior derivations.

Service-date semantics: positions are filtered by GTFS-RT `trip_start_date`,
not by Eastern calendar window, so a trip starting at 23:50 on service-date N
and running past midnight is still attributed to service-date N. The collector
only began populating `trip_start_date` on 2026-05-03; positions before that
have it null and will not be picked up by this pipeline. That is an explicit
forward-only design choice — historical backfill is not in scope.

Usage:
  uv run python pipelines/derive_stop_events.py --route C51 --date 2026-05-03
  uv run python pipelines/derive_stop_events.py --all-routes --date 2026-05-03
  uv run python pipelines/derive_stop_events.py --all-routes  # defaults to today (Eastern)
"""

import argparse
import time
from datetime import date as date_type
from datetime import datetime

import numpy as np
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from pipelines.stop_events_common import (
    build_stop_time_index,
    parse_trip_start_date,
    resolve_stop_time,
)
from src.batch_iterator import run_route_date_grid
from src.database import get_session
from src.models import Route, Stop, StopEvent, StopTime, Trip, VehiclePosition
from src.timezones import eastern_today, utcnow_naive

PROXIMITY_THRESHOLD_M = 50.0
EARTH_RADIUS_M = 6_371_000


def derive_proximity_stop_events(
    db: Session,
    route_id: str,
    service_date: date_type,
    proximity_m: float = PROXIMITY_THRESHOLD_M,
    verbose: bool = False,
) -> dict:
    """Materialize stop_events for one (route_id, service_date) with source='proximity'.

    Loads vehicle positions whose GTFS-RT `trip_start_date` matches `service_date`
    (so trips that ran past midnight stay attributed to their start date), matches
    each to the nearest scheduled stop within `proximity_m`, keeps the FIRST
    detection per (trip_id, stop_sequence), and upserts a stop_event row per
    surviving observation. Returns counters describing the run.
    """
    start_ts = time.time()
    trip_start_date_str = service_date.strftime("%Y%m%d")

    # Filter positions by trip_start_date — the GTFS-RT-reported service date —
    # rather than by Eastern calendar window. Trips that start at 23:50 on
    # service-date N and run past midnight produce positions on calendar day
    # N+1, but they belong to service date N. trip_start_date is the canonical
    # disambiguator, sourced from the RT TripDescriptor.
    positions = (
        db.query(VehiclePosition)
        .filter(
            VehiclePosition.route_id == route_id,
            VehiclePosition.trip_start_date == trip_start_date_str,
        )
        .order_by(VehiclePosition.timestamp)
        .all()
    )
    if not positions:
        return {
            "route_id": route_id,
            "service_date": service_date.isoformat(),
            "positions": 0,
            "matched_to_stop": 0,
            "rows_written": 0,
            "elapsed_sec": round(time.time() - start_ts, 2),
        }

    # Trips for this route (current GTFS only) — direction lookup + filter unknowns
    trips = db.query(Trip).filter(Trip.route_id == route_id, Trip.is_current).all()
    trip_direction = {t.trip_id: t.direction_id for t in trips}
    if not trip_direction:
        return {
            "route_id": route_id,
            "service_date": service_date.isoformat(),
            "positions": len(positions),
            "matched_to_stop": 0,
            "rows_written": 0,
            "elapsed_sec": round(time.time() - start_ts, 2),
            "note": "No current trips for this route in GTFS",
        }

    # Stops served by this route's current trips
    trip_ids = [t.trip_id for t in trips]
    stop_times = (
        db.query(StopTime).filter(StopTime.trip_id.in_(trip_ids), StopTime.is_current).all()
    )
    stop_ids_for_route = {st.stop_id for st in stop_times}
    if not stop_ids_for_route:
        return {
            "route_id": route_id,
            "service_date": service_date.isoformat(),
            "positions": len(positions),
            "matched_to_stop": 0,
            "rows_written": 0,
            "elapsed_sec": round(time.time() - start_ts, 2),
            "note": "No stop_times for this route's trips",
        }

    stops = db.query(Stop).filter(Stop.stop_id.in_(stop_ids_for_route), Stop.is_current).all()
    if not stops:
        return {
            "route_id": route_id,
            "service_date": service_date.isoformat(),
            "positions": len(positions),
            "matched_to_stop": 0,
            "rows_written": 0,
            "elapsed_sec": round(time.time() - start_ts, 2),
            "note": "No current Stop rows for stop_ids referenced by stop_times",
        }

    # Numpy arrays for vectorized haversine
    stop_id_arr = np.array([s.stop_id for s in stops])
    stop_lat_rad = np.radians([s.stop_lat for s in stops])
    stop_lon_rad = np.radians([s.stop_lon for s in stops])

    stop_time_index = build_stop_time_index(stop_times)

    # For each (trip_id, stop_sequence), keep the FIRST in-proximity observation
    # plus its match distance. Subsequent observations within proximity are ignored
    # — they correspond to dwell time at the stop, not the arrival event.
    earliest: dict[tuple[str, int], dict] = {}
    matched_to_stop = 0

    for pos in positions:
        # Trip filter: drop positions whose trip_id isn't in current GTFS for this route.
        if not pos.trip_id or pos.trip_id not in trip_direction:
            continue

        lat1 = np.radians(pos.latitude)
        lon1 = np.radians(pos.longitude)
        dlat = stop_lat_rad - lat1
        dlon = stop_lon_rad - lon1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(stop_lat_rad) * np.sin(dlon / 2) ** 2
        c = 2 * np.arcsin(np.sqrt(a))
        distances = EARTH_RADIUS_M * c

        min_idx = int(np.argmin(distances))
        min_distance = float(distances[min_idx])
        if min_distance > proximity_m:
            continue

        nearest_stop_id = str(stop_id_arr[min_idx])
        matched_to_stop += 1

        candidates = stop_time_index.get((pos.trip_id, nearest_stop_id))
        if not candidates:
            # Position is near a stop the trip doesn't actually serve in GTFS.
            continue

        # Per-position service date — anchored on the trip's GTFS-RT start_date,
        # not the pipeline-wide service_date arg, in case a position's
        # trip_start_date drifts (e.g., legacy rows that lack the field fall
        # back to the requested service_date).
        position_service_date = parse_trip_start_date(pos.trip_start_date) or service_date
        chosen = resolve_stop_time(candidates, pos.timestamp, position_service_date)
        if chosen is None:
            continue

        key = (pos.trip_id, chosen["stop_sequence"])
        existing = earliest.get(key)
        if existing is None or pos.timestamp < existing["observed_arrival_ts"]:
            earliest[key] = {
                "trip_id": pos.trip_id,
                "stop_id": nearest_stop_id,
                "stop_sequence": chosen["stop_sequence"],
                "scheduled_arrival_ts": chosen["scheduled_arrival_ts"],
                "scheduled_departure_ts": chosen["scheduled_departure_ts"],
                "observed_arrival_ts": pos.timestamp,
                "vehicle_id": pos.vehicle_id,
                "match_distance_m": min_distance,
            }

    rows = []
    derived_at = utcnow_naive()
    service_date_str = service_date.isoformat()
    for entry in earliest.values():
        deviation_sec = None
        if entry["scheduled_arrival_ts"] is not None:
            deviation_sec = int(
                (entry["observed_arrival_ts"] - entry["scheduled_arrival_ts"]).total_seconds()
            )
        rows.append(
            {
                "service_date": service_date_str,
                "trip_id": entry["trip_id"],
                "route_id": route_id,
                "direction_id": trip_direction[entry["trip_id"]],
                "vehicle_id": entry["vehicle_id"],
                "stop_id": entry["stop_id"],
                "stop_sequence": entry["stop_sequence"],
                "scheduled_arrival_ts": entry["scheduled_arrival_ts"],
                "scheduled_departure_ts": entry["scheduled_departure_ts"],
                "observed_arrival_ts": entry["observed_arrival_ts"],
                "deviation_sec": deviation_sec,
                "source": "proximity",
                "schedule_relationship": "SCHEDULED",
                "match_distance_m": entry["match_distance_m"],
                "derived_at": derived_at,
            }
        )

    rows_written = 0
    if rows:
        # Postgres upsert keyed on the stop_events natural key.
        stmt = pg_insert(StopEvent).values(rows)
        update_cols = {
            c: stmt.excluded[c]
            for c in (
                "route_id",
                "direction_id",
                "vehicle_id",
                "stop_id",
                "scheduled_arrival_ts",
                "scheduled_departure_ts",
                "observed_arrival_ts",
                "deviation_sec",
                "schedule_relationship",
                "match_distance_m",
                "derived_at",
            )
        }
        stmt = stmt.on_conflict_do_update(
            constraint="uq_stop_events_run_stop_source",
            set_=update_cols,
        )
        db.execute(stmt)
        db.commit()
        rows_written = len(rows)

    result = {
        "route_id": route_id,
        "service_date": service_date_str,
        "positions": len(positions),
        "matched_to_stop": matched_to_stop,
        "rows_written": rows_written,
        "elapsed_sec": round(time.time() - start_ts, 2),
    }
    if verbose:
        print(
            f"  {route_id} {service_date_str}: "
            f"{result['positions']} positions → {result['matched_to_stop']} matches → "
            f"{result['rows_written']} stop_events ({result['elapsed_sec']}s)"
        )
    return result


def derive_for_routes(
    db: Session,
    route_ids: list[str],
    service_date: date_type,
    proximity_m: float = PROXIMITY_THRESHOLD_M,
) -> list[dict]:
    """Drive `derive_proximity_stop_events` over a list of routes, one date."""
    return run_route_date_grid(
        derive_proximity_stop_events,
        db,
        route_ids,
        [service_date],
        proximity_m=proximity_m,
        verbose=True,
    )


def main():
    """CLI entry point — parses args, runs derivation, prints a summary."""
    parser = argparse.ArgumentParser(
        description="Derive stop_events (source='proximity') for one (route, date) or all routes on a date."
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
    parser.add_argument(
        "--proximity-meters",
        type=float,
        default=PROXIMITY_THRESHOLD_M,
        help=f"Match radius around each stop (default: {PROXIMITY_THRESHOLD_M} m).",
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

        results = derive_for_routes(db, route_ids, service_date, proximity_m=args.proximity_meters)

        total_positions = sum(r["positions"] for r in results)
        total_matched = sum(r["matched_to_stop"] for r in results)
        total_written = sum(r["rows_written"] for r in results)
        total_elapsed = sum(r["elapsed_sec"] for r in results)
        print()
        print(
            f"Total: {total_positions} positions → {total_matched} matched → {total_written} stop_events"
        )
        print(f"Elapsed: {total_elapsed:.1f}s")
    finally:
        db.close()


if __name__ == "__main__":
    main()
