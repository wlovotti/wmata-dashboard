"""
Derive `stop_events` rows from TripUpdate snapshots — the `source='trip_update'`
half of the stop_events foundation (PRs #42, #43, #44).

The trip_update derivation works backwards from a different signal than the
proximity path: GTFS-RT TripUpdates publishes a refining `predicted_arrival_ts`
for each upcoming (trip, stop) pair every ~15-30 s. As the bus approaches a
stop, predictions converge; once the bus passes, the (trip, stop) row drops
out of the feed entirely. The LAST `predicted_arrival_ts` observed for a
(trip, stop) pair before it disappears is the system's final estimate of the
actual arrival time — and is what we record as `observed_arrival_ts`.

SKIPPED stops are first-class: WMATA marks ~9% of stops with
`schedule_relationship = SKIPPED` (typically pre-announced cancellations or
detours). The pipeline emits one row per skipped stop with `observed_arrival_ts`
NULL and `schedule_relationship = SKIPPED` so downstream metrics can compute
skip rates (NOTES-14) without re-parsing the raw feed.

Service-date attribution: trip_update_snapshots itself does not record
`trip_start_date` (the GTFS-RT TripDescriptor field), so we cross-reference
vehicle_positions for the same trip_id on the target service_date. Trips that
appear in TU but not VP for the day (~1.2k of ~6k on a representative day) are
skipped here. The comparison harness (`compare_stop_event_sources.py`,
PR #44) shows the resulting TU coverage of proximity is 93% on the events
both sources see — the missing 1.2k is a known fallback gap, not silent loss.

Idempotent: re-running the same (route, service_date) upserts via the
`uq_stop_events_run_stop_source` constraint, replacing prior derivations.

Usage:
  uv run python pipelines/derive_stop_events_trip_updates.py --route C51 --date 2026-05-03
  uv run python pipelines/derive_stop_events_trip_updates.py --all-routes --date 2026-05-03
  uv run python pipelines/derive_stop_events_trip_updates.py --all-routes  # defaults to today (Eastern)
"""

import argparse
import time
from datetime import date as date_type
from datetime import datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from pipelines.stop_events_common import (
    EASTERN,
    UTC,
    build_stop_time_seq_index,
    parse_gtfs_time_to_dt,
)
from src.database import get_session
from src.models import Route, StopEvent, StopTime, Trip, TripUpdateSnapshot, VehiclePosition
from src.timezones import eastern_today

# Window around the target service_date to scan for snapshots, in Eastern hours.
# Service hours run roughly 04:00 → 02:00 next day; one extra hour each side
# absorbs late-day stragglers and protects against clock skew at the boundary.
WINDOW_START_HOUR = 3  # 03:00 Eastern of service_date
WINDOW_END_HOUR_NEXT_DAY = 4  # 04:00 Eastern of service_date+1


def _eastern_window_utc(service_date: date_type) -> tuple[datetime, datetime]:
    """Return (start_utc, end_utc) naive datetimes bracketing `service_date` in Eastern.

    The window is wide on purpose — it catches trips that started shortly before
    midnight and trailed past, plus any TU snapshots that arrived slightly after
    the bus passed its last stop.
    """
    start_eastern = datetime.combine(service_date, datetime.min.time()).replace(
        hour=WINDOW_START_HOUR, tzinfo=EASTERN
    )
    end_eastern = datetime.combine(service_date + timedelta(days=1), datetime.min.time()).replace(
        hour=WINDOW_END_HOUR_NEXT_DAY, tzinfo=EASTERN
    )
    return (
        start_eastern.astimezone(UTC).replace(tzinfo=None),
        end_eastern.astimezone(UTC).replace(tzinfo=None),
    )


def _last_snapshots_per_stop(
    snapshots: list[TripUpdateSnapshot],
) -> dict[tuple[str, int], dict]:
    """Reduce raw snapshots to one final-state record per (trip_id, stop_sequence).

    Two facts are extracted per key:
      * the LAST snapshot overall — its `schedule_relationship` is the final
        state and decides whether to emit SKIPPED vs SCHEDULED.
      * the LAST snapshot whose `predicted_arrival_ts` is non-null — its
        prediction is the inferred observed arrival. WMATA sometimes nullifies
        the prediction after the bus passes, so the absolute-last row may be
        useless; the last-with-prediction row is what we want as the time.
    """
    by_key: dict[tuple[str, int], dict] = {}
    for s in snapshots:
        if s.stop_sequence is None:
            continue
        key = (s.trip_id, s.stop_sequence)
        entry = by_key.get(key)
        if entry is None:
            entry = {
                "stop_id": s.stop_id,
                "vehicle_id": s.vehicle_id,
                "final_snapshot_ts": s.snapshot_ts,
                "final_schedule_relationship": s.schedule_relationship,
                "last_pred_snapshot_ts": None,
                "last_predicted_arrival_ts": None,
                "last_predicted_departure_ts": None,
            }
            by_key[key] = entry

        if s.snapshot_ts > entry["final_snapshot_ts"]:
            entry["final_snapshot_ts"] = s.snapshot_ts
            entry["final_schedule_relationship"] = s.schedule_relationship
            # vehicle_id can come and go across snapshots; prefer the latest non-null.
            if s.vehicle_id:
                entry["vehicle_id"] = s.vehicle_id

        if s.predicted_arrival_ts is not None and (
            entry["last_pred_snapshot_ts"] is None or s.snapshot_ts > entry["last_pred_snapshot_ts"]
        ):
            entry["last_pred_snapshot_ts"] = s.snapshot_ts
            entry["last_predicted_arrival_ts"] = s.predicted_arrival_ts
            entry["last_predicted_departure_ts"] = s.predicted_departure_ts

    return by_key


def derive_trip_update_stop_events(
    db: Session,
    route_id: str,
    service_date: date_type,
    verbose: bool = False,
) -> dict:
    """Materialize stop_events for one (route_id, service_date) with source='trip_update'.

    Authoritative trip set comes from vehicle_positions (where `trip_start_date`
    confirms the trip ran on this service_date). For each such trip, scan its
    TripUpdate snapshots in a wide Eastern-day window, reduce to the final state
    per (trip_id, stop_sequence), join the schedule, and upsert one stop_event
    row per stop. Returns counters describing the run.
    """
    start_ts = time.time()
    service_date_str = service_date.isoformat()
    trip_start_date_str = service_date.strftime("%Y%m%d")

    trips = db.query(Trip).filter(Trip.route_id == route_id, Trip.is_current).all()
    trip_direction = {t.trip_id: t.direction_id for t in trips}
    if not trip_direction:
        return _empty_result(route_id, service_date_str, start_ts, "No current trips for route")

    # Authoritative service-date attribution: the trip ran today iff a vehicle
    # position with matching trip_start_date exists. TripUpdate snapshots
    # themselves don't carry trip_start_date, so without VP we can't safely
    # decide whether a TU snapshot belongs to this service_date or another.
    vp_trip_ids = {
        row[0]
        for row in db.query(VehiclePosition.trip_id)
        .filter(
            VehiclePosition.route_id == route_id,
            VehiclePosition.trip_start_date == trip_start_date_str,
        )
        .distinct()
        .all()
    }
    active_trip_ids = vp_trip_ids & set(trip_direction.keys())
    if not active_trip_ids:
        return _empty_result(
            route_id,
            service_date_str,
            start_ts,
            "No vehicle_positions for any current trip on this service_date",
        )

    stop_times = (
        db.query(StopTime).filter(StopTime.trip_id.in_(active_trip_ids), StopTime.is_current).all()
    )
    schedule_index = build_stop_time_seq_index(stop_times)
    if not schedule_index:
        return _empty_result(route_id, service_date_str, start_ts, "No stop_times for active trips")

    window_start, window_end = _eastern_window_utc(service_date)
    snapshots = (
        db.query(TripUpdateSnapshot)
        .filter(
            TripUpdateSnapshot.trip_id.in_(active_trip_ids),
            TripUpdateSnapshot.snapshot_ts.between(window_start, window_end),
        )
        .all()
    )
    if not snapshots:
        return _empty_result(
            route_id, service_date_str, start_ts, "No trip_update_snapshots in window"
        )

    final_state = _last_snapshots_per_stop(snapshots)

    rows = []
    derived_at = datetime.utcnow()
    skipped_count = 0
    no_prediction_count = 0
    for (trip_id, stop_sequence), entry in final_state.items():
        sched = schedule_index.get((trip_id, stop_sequence))
        if sched is None:
            # TU references a (trip, stop_sequence) not in current GTFS — could
            # be an ADDED trip or a stale GTFS snapshot. Skip; ADDED-trip
            # support is a follow-on once we have schema for trip-instance
            # overrides.
            continue

        scheduled_arrival_ts = (
            parse_gtfs_time_to_dt(sched["arrival_time"], service_date)
            if sched["arrival_time"]
            else None
        )
        scheduled_departure_ts = (
            parse_gtfs_time_to_dt(sched["departure_time"], service_date)
            if sched["departure_time"]
            else None
        )

        is_skipped = entry["final_schedule_relationship"] == "SKIPPED"
        if is_skipped:
            schedule_relationship = "SKIPPED"
            observed_arrival_ts = None
            skipped_count += 1
        else:
            observed_arrival_ts = entry["last_predicted_arrival_ts"]
            if observed_arrival_ts is None:
                # No prediction was ever recorded for this stop — can't infer
                # arrival. Drop. Common when a trip ends abruptly.
                no_prediction_count += 1
                continue
            schedule_relationship = "SCHEDULED"

        deviation_sec = None
        if observed_arrival_ts is not None and scheduled_arrival_ts is not None:
            deviation_sec = int((observed_arrival_ts - scheduled_arrival_ts).total_seconds())

        rows.append(
            {
                "service_date": service_date_str,
                "trip_id": trip_id,
                "route_id": route_id,
                "direction_id": trip_direction[trip_id],
                "vehicle_id": entry["vehicle_id"],
                "stop_id": sched["stop_id"],
                "stop_sequence": stop_sequence,
                "scheduled_arrival_ts": scheduled_arrival_ts,
                "scheduled_departure_ts": scheduled_departure_ts,
                "observed_arrival_ts": observed_arrival_ts,
                "deviation_sec": deviation_sec,
                "source": "trip_update",
                "schedule_relationship": schedule_relationship,
                "match_distance_m": None,
                "derived_at": derived_at,
            }
        )

    rows_written = 0
    if rows:
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
        "active_trips": len(active_trip_ids),
        "snapshots_scanned": len(snapshots),
        "stop_keys": len(final_state),
        "skipped_emitted": skipped_count,
        "dropped_no_prediction": no_prediction_count,
        "rows_written": rows_written,
        "elapsed_sec": round(time.time() - start_ts, 2),
    }
    if verbose:
        print(
            f"  {route_id} {service_date_str}: "
            f"{result['active_trips']} trips, {result['snapshots_scanned']:,} snapshots → "
            f"{result['rows_written']} stop_events "
            f"({result['skipped_emitted']} skipped, "
            f"{result['dropped_no_prediction']} dropped) "
            f"({result['elapsed_sec']}s)"
        )
    return result


def _empty_result(route_id: str, service_date_str: str, start_ts: float, note: str) -> dict:
    """Return a uniform zero-row result with a `note` field explaining why."""
    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "active_trips": 0,
        "snapshots_scanned": 0,
        "stop_keys": 0,
        "skipped_emitted": 0,
        "dropped_no_prediction": 0,
        "rows_written": 0,
        "elapsed_sec": round(time.time() - start_ts, 2),
        "note": note,
    }


def derive_for_routes(
    db: Session,
    route_ids: list[str],
    service_date: date_type,
) -> list[dict]:
    """Drive `derive_trip_update_stop_events` over a list of routes, one date."""
    results = []
    for route_id in route_ids:
        results.append(derive_trip_update_stop_events(db, route_id, service_date, verbose=True))
    return results


def main():
    """CLI entry point — parses args, runs derivation, prints a summary."""
    parser = argparse.ArgumentParser(
        description=(
            "Derive stop_events (source='trip_update') for one (route, date) "
            "or all routes on a date."
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

        results = derive_for_routes(db, route_ids, service_date)

        total_trips = sum(r["active_trips"] for r in results)
        total_snaps = sum(r["snapshots_scanned"] for r in results)
        total_written = sum(r["rows_written"] for r in results)
        total_skipped = sum(r["skipped_emitted"] for r in results)
        total_elapsed = sum(r["elapsed_sec"] for r in results)
        print()
        print(
            f"Total: {total_trips} trips, {total_snaps:,} snapshots → "
            f"{total_written} stop_events ({total_skipped} skipped)"
        )
        print(f"Elapsed: {total_elapsed:.1f}s")
    finally:
        db.close()


if __name__ == "__main__":
    main()
