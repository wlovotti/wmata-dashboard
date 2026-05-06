"""
Stop-skip rate over the runs / stop_events foundation.

Per (route, service_date): `stops_skipped / stops_observable` over TU runs
that actually existed (the RUN_EXISTED filter, `stops_observed >= 3`).
Direct from `stop_events.schedule_relationship = 'SKIPPED'` rows
materialized by the trip_update derivation pipeline (PR #43) — not
derivable from positions at all, the unique value-add of the TripUpdates
feed.

--- Source restriction (load-bearing) ---
SKIPPED is a TripUpdates-only signal. Proximity-derived runs always carry
`stops_skipped = 0` because the proximity pipeline never emits SKIPPED
stop_events — the bus is either close enough to a stop to register an
arrival or it isn't. Summing across both sources would double the
denominator (proximity and TU rows for the same trip share
`stops_scheduled`) and bias the rate down by ~50%. Filter to
`source = 'trip_update'` for both numerator and denominator.

--- Denominator restriction (load-bearing) ---
Skip rate is `SKIPPED / scheduled_on_runs_that_actually_ran`. Using all
GTFS-scheduled stops as the denominator conflates skipped stops with stops
on cancelled runs that never reached them — those should fall out via the
service-delivered ratio (PR #47), not inflate skip rate. Restrict to TU
runs with `stops_observed >= 3` (the same RUN_EXISTED filter PR #47 uses);
without it, a fully-cancelled trip with `stops_skipped = 0` and
`stops_scheduled = 60` adds 60 zeros to the denominator and dilutes the
rate.

--- `stops_observable`, not `stops_scheduled`, as the denominator ---
The TripUpdates feed structurally cannot publish the origin's
StopTimeUpdate (the GTFS-RT TU feed only carries upcoming stops; by the
first snapshot we receive, the trip is already past the origin — see
NOTES-31 closure PR #67 and the `Run.stops_observable` column doc). So
the origin can never appear with `schedule_relationship = 'SKIPPED'` in
the TU feed either — it's mathematically guaranteed to be a non-skipped
contribution to the denominator. Summing `stops_scheduled` inflates the
denominator by exactly 1 per qualifying TU run and pulls the rate down
by a fixed factor; summing `stops_observable` (= `stops_scheduled - 1`
for TU rows) gives ratio-honest accounting against the stops the source
could actually have observed-or-skipped.

--- Per-stop breakdown ---
The per-route rollup uses `runs` directly. Per-stop ranking ("worst-skipped
stops on RouteDetail") needs the stop_id dimension that `runs` aggregates
away, so `compute_per_stop_skip_rate` falls back to `stop_events` joined
to `runs` for the qualifying-run filter. Per the CLAUDE.md `stop_id` /
direction gotcha, that aggregation groups by `(route_id, direction_id,
stop_id)` — termini and shared bays serve both directions under one
`stop_id` and silently double-count without the direction grouping.

Pure computation module mirroring `src/otp_metrics.py`,
`src/service_delivered.py`, and `src/excess_trip_time.py`. API/pipeline
integration is rolled up under NOTES-17.
"""

from __future__ import annotations

from datetime import date as date_type

from sqlalchemy import case, func
from sqlalchemy.orm import Session

from src.models import Run, StopEvent

RUN_EXISTED_MIN_STOPS = 3  # the RUN_EXISTED filter from the Run model docstring


def compute_stop_skip_rate(
    db: Session,
    route_id: str,
    service_date: date_type,
) -> dict:
    """Compute stop-skip rate for one (route, service_date).

    Returns `{route_id, service_date, n_runs, stops_skipped, stops_observable,
    skip_rate}`. `skip_rate` is `None` when `stops_observable == 0` (no TU
    runs with at least RUN_EXISTED_MIN_STOPS observed stops, or only runs
    whose trips have no GTFS schedule match) so callers can distinguish
    "no data" from a real zero. See module docstring for why the denominator
    sums `stops_observable` rather than `stops_scheduled`.
    """
    service_date_str = service_date.isoformat()

    n_runs, skipped, observable = (
        db.query(
            func.count(Run.id),
            func.coalesce(func.sum(Run.stops_skipped), 0),
            func.coalesce(func.sum(Run.stops_observable), 0),
        )
        .filter(
            Run.route_id == route_id,
            Run.service_date == service_date_str,
            Run.source == "trip_update",
            Run.stops_observed >= RUN_EXISTED_MIN_STOPS,
        )
        .one()
    )

    skip_rate = round(skipped / observable, 4) if observable else None

    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "n_runs": int(n_runs),
        "stops_skipped": int(skipped),
        "stops_observable": int(observable),
        "skip_rate": skip_rate,
    }


def compute_stop_skip_rate_for_routes(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
) -> list[dict]:
    """Compute stop-skip rate for every route with TU runs on `service_date`.

    Pass `route_ids` to restrict; default scans all routes that have any
    `source = 'trip_update'` runs on the day. Returns one dict per route,
    sorted by route_id.
    """
    service_date_str = service_date.isoformat()
    if route_ids is None:
        route_ids = sorted(
            r
            for (r,) in db.query(Run.route_id)
            .filter(
                Run.service_date == service_date_str,
                Run.source == "trip_update",
            )
            .distinct()
            .all()
        )
    return [compute_stop_skip_rate(db, r, service_date) for r in route_ids]


def compute_per_stop_skip_rate(
    db: Session,
    route_id: str,
    service_date: date_type,
    min_observations: int = 10,
) -> list[dict]:
    """Compute per-stop skip rate for one (route, service_date).

    Returns one dict per `(direction_id, stop_id)` with at least
    `min_observations` TU stop_events on qualifying runs, sorted by
    skip_rate descending. Each dict carries `{route_id, service_date,
    direction_id, stop_id, stops_skipped, stops_total, skip_rate}`.

    Reads `stop_events` directly because `runs` aggregates the stop_id
    dimension away. The qualifying-run filter (`stops_observed >=
    RUN_EXISTED_MIN_STOPS` on the parent TU run) is applied via a join on
    `(service_date, trip_id, source='trip_update')` so a stop on a fully-
    cancelled run doesn't inflate either numerator or denominator.

    Grouping is `(route_id, direction_id, stop_id)` per the CLAUDE.md
    stop_id/direction gotcha — termini and shared bays serve both
    directions under one `stop_id` and double-count without it.

    `min_observations` filters out single-stop denominators that produce
    spurious 100% rates from one skipped event; default 10 keeps the
    ranking meaningful at the per-day grain.
    """
    service_date_str = service_date.isoformat()

    qualifying_trip_ids = db.query(Run.trip_id).filter(
        Run.route_id == route_id,
        Run.service_date == service_date_str,
        Run.source == "trip_update",
        Run.stops_observed >= RUN_EXISTED_MIN_STOPS,
    )

    skipped_indicator = case((StopEvent.schedule_relationship == "SKIPPED", 1), else_=0)

    rows = (
        db.query(
            StopEvent.direction_id,
            StopEvent.stop_id,
            func.count(StopEvent.id).label("stops_total"),
            func.coalesce(func.sum(skipped_indicator), 0).label("stops_skipped"),
        )
        .filter(
            StopEvent.route_id == route_id,
            StopEvent.service_date == service_date_str,
            StopEvent.source == "trip_update",
            StopEvent.trip_id.in_(qualifying_trip_ids),
        )
        .group_by(StopEvent.direction_id, StopEvent.stop_id)
        .having(func.count(StopEvent.id) >= min_observations)
        .all()
    )

    results = [
        {
            "route_id": route_id,
            "service_date": service_date_str,
            "direction_id": direction_id,
            "stop_id": stop_id,
            "stops_skipped": int(stops_skipped),
            "stops_total": int(stops_total),
            "skip_rate": round(stops_skipped / stops_total, 4),
        }
        for direction_id, stop_id, stops_total, stops_skipped in rows
    ]
    results.sort(key=lambda r: (-r["skip_rate"], r["direction_id"], r["stop_id"]))
    return results
