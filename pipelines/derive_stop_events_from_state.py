"""Derive stop_events from trip_update_state (the refactored pipeline).

Replaces the old ``derive_stop_events_trip_updates.py``. Reads
``trip_update_state`` directly — one row per (trip, stop_sequence)
already in final-state — eliminating the ~21M-row/day snapshot scan.

The output schema and semantics MUST be byte-for-byte identical to the
old pipeline (validated during Phase D against ``stop_events_v2``); see
the design doc for the parity criteria.

Service-date attribution: as before, vehicle_positions for the same
trip_id on the target service_date is the authoritative anchor — trip
updates themselves don't record trip_start_date.

Usage:
    uv run python pipelines/derive_stop_events_from_state.py --route C51 --date 2026-05-03
    uv run python pipelines/derive_stop_events_from_state.py --all-routes --date 2026-05-03
"""

import argparse
import sys
import time
from datetime import date as date_type
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import tuple_, update
from sqlalchemy.orm import Session

from pipelines.stop_events_common import parse_gtfs_time_to_dt
from src.batch_iterator import run_route_date_grid
from src.database import get_session
from src.models import Route, StopEvent, StopTime, Trip, TripUpdateState, VehiclePosition
from src.timezones import eastern_today, utcnow_naive
from src.upsert_helpers import upsert_rows


def derive_for_route_date(
    db: Session,
    route_id: str,
    service_date: date_type,
    target_table_name: str = "stop_events",
    verbose: bool = False,
) -> dict:
    """Materialize stop_events for one (route, service_date) from trip_update_state.

    ``target_table_name`` is "stop_events" for production and
    "stop_events_v2" during Phase D side-by-side validation. The target
    table must already exist with the StopEvent schema.

    Returns a counters dict identical in shape to the old pipeline's
    output, for parity comparison.
    """
    start_ts = time.time()
    service_date_str = service_date.isoformat()
    trip_start_date_str = service_date.strftime("%Y%m%d")

    if verbose:
        print(f"  → {route_id} {service_date_str} (from trip_update_state)")

    trips = db.query(Trip).filter(Trip.route_id == route_id, Trip.is_current).all()
    trip_direction = {t.trip_id: t.direction_id for t in trips}
    if not trip_direction:
        return _empty(route_id, service_date_str, start_ts, "No current trips for route")

    # Service-date attribution: a trip ran today iff a vehicle_position
    # with matching trip_start_date exists.
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
        return _empty(
            route_id,
            service_date_str,
            start_ts,
            "No vehicle_positions for any current trip on this service_date",
        )

    # Schedule lookup (trip_id, stop_sequence) -> stop_id + scheduled times.
    schedule_index: dict[tuple[str, int], dict] = {}
    for st in (
        db.query(StopTime).filter(StopTime.trip_id.in_(active_trip_ids), StopTime.is_current).all()
    ):
        schedule_index[(st.trip_id, st.stop_sequence)] = {
            "stop_id": st.stop_id,
            "arrival_time": st.arrival_time,
            "departure_time": st.departure_time,
        }
    if not schedule_index:
        return _empty(route_id, service_date_str, start_ts, "No stop_times for active trips")

    # Read state directly — one row per (trip, stop_sequence). No scan.
    state_rows = (
        db.query(TripUpdateState).filter(TripUpdateState.trip_id.in_(active_trip_ids)).all()
    )

    rows = []
    derived_at = utcnow_naive()
    skipped_count = 0
    no_prediction_count = 0
    derived_keys: list[tuple[str, int]] = []

    target_model = (
        StopEvent if target_table_name == "stop_events" else _resolve_side_table(target_table_name)
    )

    for state in state_rows:
        sched = schedule_index.get((state.trip_id, state.stop_sequence))
        if sched is None:
            continue  # ADDED trip or stale GTFS; skip.

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

        is_skipped = state.final_schedule_relationship == "SKIPPED"
        if is_skipped:
            schedule_relationship = "SKIPPED"
            observed_arrival_ts = None
            skipped_count += 1
        else:
            observed_arrival_ts = state.last_predicted_arrival_ts
            if observed_arrival_ts is None:
                no_prediction_count += 1
                continue
            schedule_relationship = "SCHEDULED"

        deviation_sec = None
        if observed_arrival_ts is not None and scheduled_arrival_ts is not None:
            deviation_sec = int((observed_arrival_ts - scheduled_arrival_ts).total_seconds())

        rows.append(
            {
                "service_date": service_date_str,
                "trip_id": state.trip_id,
                "route_id": route_id,
                "direction_id": trip_direction[state.trip_id],
                "vehicle_id": state.vehicle_id,
                "stop_id": sched["stop_id"],
                "stop_sequence": state.stop_sequence,
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
        derived_keys.append((state.trip_id, state.stop_sequence))

    # UPSERT and the derived_at UPDATE share the caller's transaction.
    # If the UPDATE fails (e.g., serialization conflict), the UPSERT is
    # rolled back too — preventing stop_events without derived_at marks
    # on their source state rows.
    rows_written = 0
    if rows:
        constraint_name = (
            "uq_stop_events_run_stop_source"
            if target_table_name == "stop_events"
            else f"uq_{target_table_name}_run_stop_source"
        )
        upsert_rows(
            db,
            target_model,
            rows,
            constraint_name=constraint_name,
            update_cols=[
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
            ],
        )
        rows_written = len(rows)

        # Mark source state rows as derived. tuple_().in_() is
        # Postgres-specific (most other dialects don't support tuple-IN
        # filters); we are Postgres-only by construction.
        # Using tuple-IN matches exact (trip_id, stop_sequence) pairs —
        # the Cartesian form (trip_id.in_(set) AND stop_sequence.in_(set))
        # would mark cross-product rows as derived.
        db.execute(
            update(TripUpdateState)
            .where(tuple_(TripUpdateState.trip_id, TripUpdateState.stop_sequence).in_(derived_keys))
            .values(derived_at=derived_at)
        )

    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "active_trips": len(active_trip_ids),
        "state_rows_scanned": len(state_rows),
        # Aliases for parity with the old pipeline's counter shape.
        # The new pipeline doesn't "scan" snapshots — it reads final-state
        # rows. snapshots_scanned == state_rows_scanned for downstream
        # comparison purposes.
        "snapshots_scanned": len(state_rows),
        "stop_keys": len(rows),
        "skipped_emitted": skipped_count,
        "dropped_no_prediction": no_prediction_count,
        "rows_written": rows_written,
        "elapsed_sec": round(time.time() - start_ts, 2),
    }


# Module-level cache for side-table model classes. Used by _resolve_side_table
# to avoid recreating the class on every call. Keyed by table name.
_side_table_registry: dict[str, type] = {}


def _resolve_side_table(name: str):
    """Return a model bound to the side table (same schema as StopEvent).

    Used for Phase D validation where we write to ``stop_events_v2``.
    The side table must already exist with identical schema (see
    scripts/migrate_create_stop_events_v2.py).

    The cloned table lives on an isolated MetaData() — NOT Base.metadata —
    so it never participates in ``Base.metadata.create_all()``. That's
    important because the SQLite test fixture creates all tables in
    Base.metadata at session setup; without isolation, the side table
    would be added to the SQLite schema (and worse, the migration's
    Postgres-only DDL would never run there).
    """
    if name != "stop_events_v2":
        raise ValueError(f"Unknown target table: {name}")

    cached = _side_table_registry.get(name)
    if cached is not None:
        return cached

    from sqlalchemy import MetaData

    from src.models import StopEvent

    isolated_meta = MetaData()
    side_table = StopEvent.__table__.to_metadata(isolated_meta, name=name)

    class _SideStopEvent:
        """Lightweight model wrapper pointing at the stop_events_v2 side table.

        Not a Base-derived ORM class — just enough surface (``__table__``,
        ``__tablename__``) to satisfy ``upsert_rows`` which only reads
        ``model.__table__``.
        """

        __table__ = side_table
        __tablename__ = name

    _side_table_registry[name] = _SideStopEvent
    return _SideStopEvent


def _empty(route_id: str, service_date_str: str, start_ts: float, note: str) -> dict:
    """Return an empty result dict when there's nothing to derive."""
    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "active_trips": 0,
        "state_rows_scanned": 0,
        "snapshots_scanned": 0,
        "stop_keys": 0,
        "skipped_emitted": 0,
        "dropped_no_prediction": 0,
        "rows_written": 0,
        "elapsed_sec": round(time.time() - start_ts, 2),
        "note": note,
    }


def main() -> int:
    """CLI entry point for deriving stop_events from trip_update_state."""
    parser = argparse.ArgumentParser(description="Derive stop_events from trip_update_state.")
    parser.add_argument("--route", help="Single route_id")
    parser.add_argument("--all-routes", action="store_true")
    parser.add_argument("--date", help="YYYY-MM-DD; defaults to today (Eastern)")
    parser.add_argument(
        "--target-table",
        default="stop_events",
        choices=["stop_events", "stop_events_v2"],
        help=(
            "Output table. Default: stop_events (production). "
            "Use stop_events_v2 for Phase D side-by-side validation."
        ),
    )
    args = parser.parse_args()

    if args.route and args.all_routes:
        parser.error("pass --route OR --all-routes, not both")
    if not args.route and not args.all_routes:
        parser.error("pass --route or --all-routes")

    load_dotenv()
    service_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else eastern_today()
    db = get_session()
    try:
        if args.route:
            route_ids = [args.route]
        else:
            route_ids = [r.route_id for r in db.query(Route).filter(Route.is_current).all()]
        results = run_route_date_grid(
            derive_for_route_date,
            db,
            route_ids,
            [service_date],
            verbose=True,
            target_table_name=args.target_table,
        )
        for r in results:
            print(r)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
