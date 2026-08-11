"""Derive stop_events from trip_update_state (the refactored pipeline).

Replaces the retired ``derive_stop_events_trip_updates.py``. Reads
``trip_update_state`` directly — one row per (trip, stop_sequence)
already in final-state — eliminating the ~21M-row/day snapshot scan.

Output schema and semantics are identical to the old pipeline (parity
was validated during Phase D against ``stop_events_v2``).

Service-date attribution: as before, vehicle_positions for the same
trip_id on the target service_date is the authoritative anchor — trip
updates themselves don't record trip_start_date.

Multi-agency (NOTES-100): pass ``--agency`` (matching a
``config/agencies/<agency>.yaml``) to derive against a non-WMATA
database with the correct service-date timezone. Omitting it keeps the
WMATA/Eastern default.

Usage:
    uv run python pipelines/derive_stop_events_from_state.py --route C51 --date 2026-05-03
    uv run python pipelines/derive_stop_events_from_state.py --all-routes --date 2026-05-03
    uv run python pipelines/derive_stop_events_from_state.py --all-routes --date 2026-07-23 --agency sfmta
"""

import argparse
import sys
import time
from collections.abc import Iterable, Iterator
from datetime import date as date_type
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import delete, tuple_, update
from sqlalchemy.orm import Session

from pipelines.stop_events_common import resolve_scheduled_instants
from src.agency_config import load_agency_config, resolve_agency_db_url
from src.batch_iterator import run_route_date_grid
from src.database import get_session
from src.gtfs_versioning import gtfs_version_filter
from src.models import Route, StopEvent, StopTime, Trip, TripUpdateState, VehiclePosition
from src.timezones import local_service_date_position_window_utc, local_today, utcnow_naive
from src.upsert_helpers import upsert_rows

# Chunk size for the per-state-row derived_at UPDATE. The UPDATE filters on
# ``tuple_(trip_id, stop_sequence).in_(<list>)``, which Postgres parses as a
# deeply nested row-constructor OR expression. At ~thousands of pairs the
# parser exhausts ``max_stack_depth`` (default 2MB) and the query fails with
# ``StatementTooComplex: stack depth limit exceeded``. 1000 pairs per UPDATE
# stays comfortably under that limit on production-shaped batches.
_STATE_UPDATE_CHUNK_SIZE = 1000


def _iter_chunks[T](items: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield successive lists of length ``size`` from ``items`` (last may be short)."""
    if size <= 0:
        raise ValueError(f"chunk size must be positive, got {size}")
    chunk: list[T] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def derive_for_route_date(
    db: Session,
    route_id: str,
    service_date: date_type,
    target_table_name: str = "stop_events",
    verbose: bool = False,
    gtfs_snapshot_id: int | None = None,
    tz_name: str = "America/New_York",
) -> dict:
    """Materialize stop_events for one (route, service_date) from trip_update_state.

    Rows are usually filed under ``service_date``, but NOTES-110's resolver
    anchor correction can file a row under ``service_date - 1`` instead
    (owl-route schedules whose GTFS-RT-reported service date is one day
    later than the trip's true GTFS service day) — see
    ``resolve_scheduled_instants``. Callers that need every row this call
    wrote, not just the ones under ``service_date``, should use the
    returned ``service_dates_written`` list rather than assuming the
    output lands entirely under the requested date.

    ``target_table_name`` must be "stop_events" (the production table).
    The parameter is retained for call-site compatibility; passing any
    other value raises a ``ValueError``.

    ``tz_name`` (NOTES-100 multi-agency; default Eastern) bounds the
    vehicle_positions scan used for active-trip attribution by the
    agency's own local midnight — see
    ``local_service_date_position_window_utc``.

    Returns a counters dict.
    """
    if target_table_name != "stop_events":
        raise ValueError(
            f"target_table_name must be 'stop_events', got {target_table_name!r}. "
            "The stop_events_v2 side table was used during Phase D validation only "
            "and is no longer supported."
        )
    start_ts = time.time()
    service_date_str = service_date.isoformat()
    trip_start_date_str = service_date.strftime("%Y%m%d")

    if verbose:
        print(f"  → {route_id} {service_date_str} (from trip_update_state)")

    trips = (
        db.query(Trip)
        .filter(Trip.route_id == route_id, gtfs_version_filter(Trip, gtfs_snapshot_id))
        .all()
    )
    trip_direction = {t.trip_id: t.direction_id for t in trips}
    if not trip_direction:
        return _empty(route_id, service_date_str, start_ts, "No current trips for route")

    # Service-date attribution: a trip ran today iff a vehicle_position
    # with matching trip_start_date exists. The timestamp bounds are
    # load-bearing: trip_start_date has no index, so without them this is a
    # full-table seq scan per route (see derive_stop_events.py for the full
    # story — same fix, same ~48h window).
    window_start, window_end = local_service_date_position_window_utc(service_date, tz_name)
    vp_trip_ids = {
        row[0]
        for row in db.query(VehiclePosition.trip_id)
        .filter(
            VehiclePosition.route_id == route_id,
            VehiclePosition.trip_start_date == trip_start_date_str,
            VehiclePosition.timestamp >= window_start,
            VehiclePosition.timestamp < window_end,
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
        db.query(StopTime)
        .filter(
            StopTime.trip_id.in_(active_trip_ids), gtfs_version_filter(StopTime, gtfs_snapshot_id)
        )
        .all()
    ):
        schedule_index[(st.trip_id, st.stop_sequence)] = {
            "stop_id": st.stop_id,
            "arrival_time": st.arrival_time,
            "departure_time": st.departure_time,
        }
    if not schedule_index:
        return _empty(route_id, service_date_str, start_ts, "No stop_times for active trips")

    # Read state directly — one row per (trip, stop_sequence, service_date).
    # The service_date filter prevents bleed-through from prior or subsequent
    # days' runs of the same trip_id (WMATA's scheduled trip_ids repeat
    # day-over-day, so without this filter the derivation would read
    # whichever day's snapshot landed in state last).
    state_rows = (
        db.query(TripUpdateState)
        .filter(TripUpdateState.trip_id.in_(active_trip_ids))
        .filter(TripUpdateState.service_date == service_date)
        .all()
    )

    rows = []
    derived_at = utcnow_naive()
    skipped_count = 0
    no_prediction_count = 0
    derived_keys: list[tuple[str, int]] = []
    # NOTES-111: (trip_id, stop_sequence) pairs whose resolved anchor day
    # differs from the outer-loop service_date -- the misattributed row a
    # prior derivation run (or the pre-NOTES-110 code) may have written
    # under service_date still exists and must be superseded, since
    # service_date is part of uq_stop_events_run_stop_source and a plain
    # upsert can't reach a row keyed on a different service_date.
    stale_keys: list[tuple[str, int]] = []

    target_model = StopEvent

    for state in state_rows:
        sched = schedule_index.get((state.trip_id, state.stop_sequence))
        if sched is None:
            continue  # ADDED trip or stale GTFS; skip.

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

        if observed_arrival_ts is not None:
            # Anchor against whichever of service_date / service_date-1
            # lands the scheduled arrival closest to the observation
            # (NOTES-105) -- trip_update_state.service_date is computed by
            # src.wmata_collector._service_date_for_row the same way
            # vehicle_positions.trip_start_date is, and inherits the same
            # owl-route misattribution on SFMTA.
            scheduled_arrival_ts, scheduled_departure_ts, resolved_service_date = (
                resolve_scheduled_instants(
                    sched["arrival_time"],
                    sched["departure_time"],
                    observed_arrival_ts,
                    service_date,
                    tz_name,
                )
            )
        else:
            # SKIPPED rows have no arrival prediction to disambiguate
            # against, but final_snapshot_ts -- the last snapshot that
            # observed the (trip, stop) before it dropped out of the feed,
            # always populated on TripUpdateState -- is a serviceable
            # proxy observation (NOTES-110 "related latent gap"). Reusing
            # the same resolver keeps the SKIPPED branch's service_date
            # attribution consistent with the SCHEDULED branch instead of
            # always trusting the raw, possibly-misattributed service_date
            # anchor. deviation_sec still comes out None for these rows
            # regardless (observed_arrival_ts stays None below).
            #
            # proxy_guard=True (NOTES-111): final_snapshot_ts is a
            # wall-clock proxy, not a genuine arrival observation -- a trip
            # that drops out of the feed early (vehicle offline, early
            # cancellation, collector gap) can leave it far from the true
            # scheduled time for reasons unrelated to an owl-route
            # misattribution, on any agency including WMATA. The guard
            # only accepts the day-before anchor when the same-day anchor
            # would land the scheduled instant >12h after the proxy (see
            # resolve_scheduled_instants' proxy_guard docstring) --
            # otherwise it keeps the plain service_date anchor exactly
            # like the pre-NOTES-110 code did.
            scheduled_arrival_ts, scheduled_departure_ts, resolved_service_date = (
                resolve_scheduled_instants(
                    sched["arrival_time"],
                    sched["departure_time"],
                    state.final_snapshot_ts,
                    service_date,
                    tz_name,
                    proxy_guard=True,
                )
            )

        deviation_sec = None
        if observed_arrival_ts is not None and scheduled_arrival_ts is not None:
            deviation_sec = int((observed_arrival_ts - scheduled_arrival_ts).total_seconds())

        if resolved_service_date != service_date:
            stale_keys.append((state.trip_id, state.stop_sequence))

        rows.append(
            {
                # NOTES-110: persist the anchor day the resolver actually
                # picked (possibly service_date - 1 for SFMTA owl routes),
                # not the raw outer-loop service_date -- the latter is
                # exactly the misattributed value the resolver corrects for.
                "service_date": resolved_service_date.isoformat(),
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

    rows_written = 0
    if rows:
        constraint_name = "uq_stop_events_run_stop_source"

        if stale_keys:
            # NOTES-111: delete the superseded (service_date, trip_id,
            # stop_sequence, source) rows before the upsert below, in the
            # same transaction (upsert_rows commits both). Scoped to
            # exactly the identities being rewritten this run — never a
            # broad date-range delete.
            db.execute(
                delete(StopEvent).where(
                    StopEvent.service_date == service_date_str,
                    StopEvent.source == "trip_update",
                    tuple_(StopEvent.trip_id, StopEvent.stop_sequence).in_(stale_keys),
                )
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
        # filters); we are Postgres-only by construction. Using tuple-IN
        # matches exact (trip_id, stop_sequence) pairs — the Cartesian
        # form (trip_id.in_(set) AND stop_sequence.in_(set)) would mark
        # cross-product rows as derived.
        #
        # The service_date predicate binds the full natural key. GTFS
        # trip_ids recur every service day, so without it this UPDATE
        # phantom-stamps derived_at on OTHER dates' rows for the same
        # trips — and takes row locks on today's live rows, deadlocking
        # against the collector's concurrent upsert (three dates failed
        # this way during the 2026-07 backfill).
        #
        # Chunked into _STATE_UPDATE_CHUNK_SIZE batches: a single UPDATE
        # over the whole list blows past Postgres's max_stack_depth on
        # large routes (see the constant's docstring).
        #
        # Transaction semantics: upsert_rows() above already committed
        # the stop_events writes. The chunked UPDATEs run in a new
        # transaction that we must commit explicitly, otherwise the
        # session's auto-begin transaction is rolled back at db.close()
        # in main() and every derived_at mark is silently lost — leaving
        # cleanup pass 1 (derived-older-than-2-days) with nothing to delete.
        for chunk in _iter_chunks(derived_keys, _STATE_UPDATE_CHUNK_SIZE):
            db.execute(
                update(TripUpdateState)
                .where(
                    TripUpdateState.service_date == service_date,
                    tuple_(TripUpdateState.trip_id, TripUpdateState.stop_sequence).in_(chunk),
                )
                .values(derived_at=derived_at)
            )
        db.commit()

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
        # NOTES-111: distinct service_date values actually written to
        # stop_events this call, cheap to compute since `rows` already
        # holds every row's resolved service_date. Almost always just
        # [service_date_str], but the resolver's owl-route anchor
        # correction (NOTES-110) can additionally write service_date - 1
        # -- callers (run_daily_batch) must aggregate that date too, not
        # just the one this pipeline was invoked for.
        "service_dates_written": sorted({r["service_date"] for r in rows}),
        "elapsed_sec": round(time.time() - start_ts, 2),
    }


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
        choices=["stop_events"],
        help="Output table (default: stop_events). Only stop_events is supported.",
    )
    parser.add_argument(
        "--gtfs-snapshot-id",
        type=int,
        default=None,
        help=(
            "Derive against this historical GTFS snapshot instead of the "
            "current one — for backfilling service dates whose schedule has "
            "since been superseded by a reload (see gtfs_snapshots table)."
        ),
    )
    parser.add_argument(
        "--agency",
        default="wmata",
        help=(
            "Agency name matching config/agencies/<agency>.yaml (default: "
            "'wmata'). Selects the service-date timezone and target "
            "database — see pipelines/run_daily_batch.py."
        ),
    )
    args = parser.parse_args()

    if args.route and args.all_routes:
        parser.error("pass --route OR --all-routes, not both")
    if not args.route and not args.all_routes:
        parser.error("pass --route or --all-routes")

    load_dotenv()
    cfg = load_agency_config(args.agency)
    service_date = (
        datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else local_today(cfg.timezone)
    )
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
        results = run_route_date_grid(
            derive_for_route_date,
            db,
            route_ids,
            [service_date],
            verbose=True,
            target_table_name=args.target_table,
            gtfs_snapshot_id=args.gtfs_snapshot_id,
            tz_name=cfg.timezone,
        )
        for r in results:
            print(r)

        # NOTES-111: report every distinct service_date actually written
        # (may include service_date - 1 via the resolver's owl-route
        # anchor correction) so run_daily_batch's subprocess wrapper can
        # aggregate day-shifted rows instead of orphaning them in an
        # already-aggregated date.
        all_written = set()
        for r in results:
            all_written.update(r.get("service_dates_written", []))
        print(f"SERVICE_DATES_WRITTEN:{','.join(sorted(all_written))}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
