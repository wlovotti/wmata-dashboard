"""
End-to-end excess trip time over the runs foundation.

Per (route, service_date): median actual trip duration, p95, and the share of
trips where actual > 110% of scheduled. Captures dwell + in-vehicle delay over
the whole trip, not just at endpoints — the metric MBTA OPMI is rolling out
for buses.

--- Per-trip dedup (load-bearing) ---
`runs` has one row per (service_date, trip_id, source), so each trip appears
twice. The endpoint-asymmetry rule from PR #46 applies to trip duration too:
proximity has the better origin observation (78-93% literal `sched_first_seq`
coverage), trip_update has the better destination (87-97% literal
`sched_last_seq`). So per trip:

    actual_duration = TU_row.last_obs_ts - proximity_row.first_obs_ts

with `proximity.origin_dev_sec IS NOT NULL` and `TU.destination_dev_sec IS
NOT NULL` enforced before counting the trip — without those checks the
endpoints fall on the wrong stops and excess-time is biased low (rare, but
the bias is one-sided so it matters at the tail).

When only one source has a row for the trip, fall back to that source's own
bounds (still requiring both `origin_dev_sec` and `destination_dev_sec` not
null on that row, so it's a literal end-to-end measurement). The fallback
contributes few trips in practice because the literal-coverage gap is wide,
but it's correct when it does fire.

`scheduled_duration = sched_last_arrival_ts - sched_first_arrival_ts` is
identical across the trip's source rows, so we read it from whichever row is
present.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date as date_type

import numpy as np
from sqlalchemy.orm import Session

from src.models import Run

EXCESS_RATIO_THRESHOLD = 1.10  # actual > 110% of scheduled = "excess"


def _trip_actual_duration_sec(prox: Run | None, tu: Run | None) -> float | None:
    """Pick the source(s) for one trip's actual duration; return seconds or None.

    Joined case (both rows present) takes proximity's origin and TU's
    destination — see module docstring. Single-source fallback requires the
    one available row to have observed both literal endpoints itself.
    """
    prox_origin_ok = (
        prox is not None and prox.origin_dev_sec is not None and prox.first_obs_ts is not None
    )
    tu_dest_ok = (
        tu is not None and tu.destination_dev_sec is not None and tu.last_obs_ts is not None
    )

    if prox_origin_ok and tu_dest_ok:
        delta = (tu.last_obs_ts - prox.first_obs_ts).total_seconds()
        return delta if delta > 0 else None

    # Single-source fallback: only one source has the trip at all, AND that
    # row spans both literal endpoints itself.
    if prox is not None and tu is None:
        if (
            prox.origin_dev_sec is not None
            and prox.destination_dev_sec is not None
            and prox.first_obs_ts is not None
            and prox.last_obs_ts is not None
        ):
            delta = (prox.last_obs_ts - prox.first_obs_ts).total_seconds()
            return delta if delta > 0 else None
    if tu is not None and prox is None:
        if (
            tu.origin_dev_sec is not None
            and tu.destination_dev_sec is not None
            and tu.first_obs_ts is not None
            and tu.last_obs_ts is not None
        ):
            delta = (tu.last_obs_ts - tu.first_obs_ts).total_seconds()
            return delta if delta > 0 else None
    return None


def _trip_scheduled_duration_sec(prox: Run | None, tu: Run | None) -> float | None:
    """Read scheduled duration from whichever source row has it; same value either way."""
    for run in (prox, tu):
        if (
            run is not None
            and run.sched_first_arrival_ts is not None
            and run.sched_last_arrival_ts is not None
        ):
            delta = (run.sched_last_arrival_ts - run.sched_first_arrival_ts).total_seconds()
            if delta > 0:
                return delta
    return None


def compute_excess_trip_time(
    db: Session,
    route_id: str,
    service_date: date_type,
) -> dict:
    """Compute end-to-end excess trip time stats for one (route, service_date).

    Returns `{route_id, service_date, n_trips, median_actual_sec,
    p95_actual_sec, median_scheduled_sec, pct_over_110}`. All metric fields
    are `None` when `n_trips == 0` so callers can distinguish "no qualifying
    trips" from a real zero.
    """
    service_date_str = service_date.isoformat()

    runs_by_trip: dict[str, dict[str, Run]] = defaultdict(dict)
    for run in (
        db.query(Run).filter(Run.route_id == route_id, Run.service_date == service_date_str).all()
    ):
        runs_by_trip[run.trip_id][run.source] = run

    actuals: list[float] = []
    scheduleds: list[float] = []
    over_110 = 0
    for sources in runs_by_trip.values():
        prox = sources.get("proximity")
        tu = sources.get("trip_update")

        actual = _trip_actual_duration_sec(prox, tu)
        scheduled = _trip_scheduled_duration_sec(prox, tu)
        if actual is None or scheduled is None:
            continue

        actuals.append(actual)
        scheduleds.append(scheduled)
        if actual > EXCESS_RATIO_THRESHOLD * scheduled:
            over_110 += 1

    n = len(actuals)
    if n == 0:
        return {
            "route_id": route_id,
            "service_date": service_date_str,
            "n_trips": 0,
            "median_actual_sec": None,
            "p95_actual_sec": None,
            "median_scheduled_sec": None,
            "pct_over_110": None,
        }

    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "n_trips": n,
        "median_actual_sec": int(np.percentile(actuals, 50)),
        "p95_actual_sec": int(np.percentile(actuals, 95)),
        "median_scheduled_sec": int(np.percentile(scheduleds, 50)),
        "pct_over_110": round(over_110 * 100 / n, 2),
    }


def compute_excess_trip_time_for_routes(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
) -> list[dict]:
    """Compute excess trip time for every route with runs on `service_date`.

    Pass `route_ids` to restrict; default scans all routes that have any
    runs on the day. Returns one dict per route, sorted by route_id.
    """
    service_date_str = service_date.isoformat()
    if route_ids is None:
        route_ids = sorted(
            r
            for (r,) in db.query(Run.route_id)
            .filter(Run.service_date == service_date_str)
            .distinct()
            .all()
        )
    return [compute_excess_trip_time(db, r, service_date) for r in route_ids]
