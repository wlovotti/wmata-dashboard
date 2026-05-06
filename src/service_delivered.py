"""
Service-delivered ratio over the runs / GTFS Trip foundation.

Per (route, service_date): `delivered_trips / scheduled_trips`. Single most
rider-felt failure mode the dashboard currently can't see — most rider pain is
missing buses, not late ones.

  - **scheduled_trips**: `COUNT(DISTINCT Trip.trip_id)` over GTFS `trips`
    for the route, joined to `calendar` filtered to the day_type's
    representative weekday (Tuesday → weekday, Saturday → saturday,
    Sunday → sunday — same convention as `service_profile.py`). day_type
    comes from Python `date.weekday()`. Counts both directions — a
    delivered round-trip is two trips, and missing either direction is
    a delivery failure. (We do NOT use `route_service_profile.scheduled_trips`
    here, despite the name: that field stores trunk-stop arrivals at a
    single unidirectional stop, useful for headway/frequency
    classification but ~half the actual trip count on bidirectional
    routes — would inflate this ratio toward 200%.) Holiday awareness
    via GTFS `calendar_dates` exceptions is a known limitation: a
    Federal-holiday weekday that runs Sunday service will use the
    weekday denominator and look catastrophically under-delivered.
    Add holiday handling when it shows up as a real interpretation
    problem.

  - **delivered_trips**: `COUNT(DISTINCT trip_id)` over `runs` where **any
    source row** has `stops_observed >= 3` (the RUN_EXISTED filter from the
    Run model docstring) AND the trip_id is in GTFS for the day_type's
    representative weekday. DISTINCT collapses the per-source duplication;
    "any source" is the right rule because TU and proximity have nearly
    inverse blind spots and either source observing ≥3 stops is sufficient
    evidence the trip ran. The GTFS-membership filter is load-bearing for
    ratio sanity: without it, real-time-only ADDED trips end up in the
    numerator while the denominator is purely GTFS-derived. Caveat: ~3-6%
    of TU-day trips have no matching `vehicle_positions` row and so get
    dropped by the B1 derivation — those look "not delivered" here even
    if they ran. The dropped set is route-concentrated as of 2026-05-03;
    re-run `scripts/probe_dropped_tu_trips.py` periodically against
    multi-day windows to see whether the bias shifts.

The flat `stops_observed >= 3` threshold is structurally unreachable on
short routes whose GTFS trips have ≤3 stops (NOTES-30, A90 the only
currently-affected route). NOTES-31's `stops_observable` column is now
populated on every run so a follow-up can replace the constant with a
trip-length-aware filter such as `stops_observed >= max(2,
stops_observable // 3)` without needing to change the column or the
write path. Left as-is here to keep this PR's scope tight.
"""

from __future__ import annotations

from datetime import date as date_type

from sqlalchemy import distinct, func
from sqlalchemy.orm import Session

from src.models import Calendar, Run, Trip

# Same day_type → representative-weekday-of-Calendar mapping used in src/ewt.py.
# Kept duplicated rather than imported to avoid circular import risk
# (src.ewt depends on heavier modules); the value is two lines.
_DAY_TYPE_REPRESENTATIVE_FIELD = {
    "weekday": "tuesday",
    "saturday": "saturday",
    "sunday": "sunday",
}


def _day_type_for(service_date: date_type) -> str:
    """Map service_date to the day_type bucket route_service_profile uses."""
    wd = service_date.weekday()  # Mon=0 .. Sun=6
    if wd == 5:
        return "saturday"
    if wd == 6:
        return "sunday"
    return "weekday"


def _scheduled_trip_ids_query(db: Session, route_id: str, day_type: str):
    """Subquery yielding GTFS trip_ids scheduled for `route_id` on `day_type`.

    Used to filter the delivered-trips numerator: a real-time-only ADDED trip
    that's in `runs` but not in GTFS shouldn't count toward "service
    delivered" — the denominator is GTFS-derived, so the numerator must
    match. Without this filter the ratio can exceed 100% on days with
    significant supplementation.
    """
    field = getattr(Calendar, _DAY_TYPE_REPRESENTATIVE_FIELD[day_type])
    return (
        db.query(Trip.trip_id)
        .join(Calendar, Calendar.service_id == Trip.service_id)
        .filter(
            Trip.route_id == route_id,
            Trip.is_current,
            Calendar.is_current,
            field == 1,
        )
    )


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
    scheduled_trip_ids_q = _scheduled_trip_ids_query(db, route_id, day_type)

    scheduled = scheduled_trip_ids_q.distinct().count()

    delivered = (
        db.query(func.count(distinct(Run.trip_id)))
        .filter(
            Run.route_id == route_id,
            Run.service_date == service_date_str,
            Run.stops_observed >= 3,
            Run.trip_id.in_(scheduled_trip_ids_q),
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
        field = getattr(Calendar, _DAY_TYPE_REPRESENTATIVE_FIELD[day_type])
        from_gtfs = {
            r
            for (r,) in db.query(Trip.route_id)
            .join(Calendar, Calendar.service_id == Trip.service_id)
            .filter(Trip.is_current, Calendar.is_current, field == 1)
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
        route_ids = sorted(from_gtfs | from_runs)
    return [compute_service_delivered(db, r, service_date) for r in route_ids]
