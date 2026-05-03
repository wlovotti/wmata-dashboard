"""
Service-delivered ratio over the runs / route_service_profile foundation.

Per (route, service_date): `delivered_trips / scheduled_trips`. Single most
rider-felt failure mode the dashboard currently can't see — most rider pain is
missing buses, not late ones.

  - **scheduled_trips**: sum of `route_service_profile.scheduled_trips` for the
    day_type matching `service_date`. day_type comes from Python
    `date.weekday()` — Mon-Fri → weekday, Sat → saturday, Sun → sunday.
    Holiday awareness via GTFS `calendar_dates` exceptions is a known
    limitation: a Federal-holiday weekday that runs Sunday service will use
    the weekday denominator and look catastrophically under-delivered. Add
    holiday handling when it shows up as a real interpretation problem.

  - **delivered_trips**: `COUNT(DISTINCT trip_id)` over `runs` where **any
    source row** has `stops_observed >= 3` (the RUN_EXISTED filter from the
    Run model docstring). DISTINCT collapses the per-source duplication;
    "any source" is the right rule because TU and proximity have nearly
    inverse blind spots and either source observing ≥3 stops is sufficient
    evidence the trip ran. Caveat: ~3-6% of TU-day trips have no matching
    `vehicle_positions` row and so get dropped by the B1 derivation —
    those look "not delivered" here even if they ran. The dropped set is
    route-concentrated as of 2026-05-03; re-run
    `scripts/probe_dropped_tu_trips.py` periodically against multi-day
    windows to see whether the bias shifts.
"""

from __future__ import annotations

from datetime import date as date_type

from sqlalchemy import distinct, func
from sqlalchemy.orm import Session

from src.models import RouteServiceProfile, Run


def _day_type_for(service_date: date_type) -> str:
    """Map service_date to the day_type bucket route_service_profile uses."""
    wd = service_date.weekday()  # Mon=0 .. Sun=6
    if wd == 5:
        return "saturday"
    if wd == 6:
        return "sunday"
    return "weekday"


def compute_service_delivered(
    db: Session,
    route_id: str,
    service_date: date_type,
) -> dict:
    """Compute service-delivered ratio for one (route, service_date).

    Returns `{route_id, service_date, day_type, scheduled_trips,
    delivered_trips, ratio}`. `ratio` is `None` when `scheduled_trips == 0`
    (no schedule for this route on this day_type — the route may not run
    Sundays, etc.) so callers can distinguish "didn't run any" from "wasn't
    supposed to run any."
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    scheduled = (
        db.query(func.coalesce(func.sum(RouteServiceProfile.scheduled_trips), 0))
        .filter(
            RouteServiceProfile.route_id == route_id,
            RouteServiceProfile.day_type == day_type,
        )
        .scalar()
    )

    delivered = (
        db.query(func.count(distinct(Run.trip_id)))
        .filter(
            Run.route_id == route_id,
            Run.service_date == service_date_str,
            Run.stops_observed >= 3,
        )
        .scalar()
    )

    ratio = round(delivered / scheduled, 4) if scheduled else None

    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "day_type": day_type,
        "scheduled_trips": int(scheduled),
        "delivered_trips": int(delivered),
        "ratio": ratio,
    }


def compute_service_delivered_for_routes(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
) -> list[dict]:
    """Compute service-delivered ratio for every route with a schedule
    or any runs on `service_date`.

    Pass `route_ids` to restrict; default unions every route_id present in
    `route_service_profile` (for the matching day_type) with every route_id
    that has runs on the date — so a route that's running unscheduled
    service still surfaces (delivered>0, scheduled=0, ratio=None) and a
    scheduled route with 0 delivered surfaces (ratio=0). Returns one dict
    per route, sorted by route_id.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)
    if route_ids is None:
        from_profile = {
            r
            for (r,) in db.query(RouteServiceProfile.route_id)
            .filter(RouteServiceProfile.day_type == day_type)
            .distinct()
            .all()
        }
        from_runs = {
            r
            for (r,) in db.query(Run.route_id)
            .filter(Run.service_date == service_date_str)
            .distinct()
            .all()
        }
        route_ids = sorted(from_profile | from_runs)
    return [compute_service_delivered(db, r, service_date) for r in route_ids]
