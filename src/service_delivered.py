"""
Service-delivered ratio over the runs / GTFS Trip foundation.

Per (route, service_date): `delivered_trips / scheduled_trips`. Single most
rider-felt failure mode the dashboard currently can't see — most rider pain is
missing buses, not late ones.

  - **scheduled_trips**: `COUNT(DISTINCT Trip.trip_id)` over GTFS `trips`
    for the route, joined to `calendar` filtered to the **actual
    day-of-week column for `service_date`** (Mon..Sun). Counts both
    directions — a delivered round-trip is two trips, and missing either
    direction is a delivery failure. (We do NOT use
    `route_service_profile.scheduled_trips` here, despite the name: that
    field stores trunk-stop arrivals at a single unidirectional stop,
    useful for headway/frequency classification but ~half the actual
    trip count on bidirectional routes — would inflate this ratio
    toward 200%.) Honors `calendar_dates` exceptions: type-2 (removed)
    service_ids are subtracted from the base set, type-1 (added) are
    unioned in. So a Federal-holiday weekday that explicitly removes
    weekday service and adds Sunday service via GTFS exceptions
    computes against the schedule that actually ran. Distinct from the
    representative-weekday convention in `service_profile.py` and
    `ewt.py` (both collapse weekdays to a Tuesday sample for cross-day
    aggregation): service-delivered is a per-date question, and
    collapsing to Tuesday silently drops trips on dates where WMATA
    splits weekday service across separate service_ids (Wed-only,
    Fri-only, etc.) — that was NOTES-51, where every Wed/Fri date
    returned `delivered_trips=0` system-wide.

  - **delivered_trips**: `COUNT(DISTINCT trip_id)` over `runs` where **any
    source row** clears a trip-length-aware existence threshold AND the
    trip_id is in GTFS scheduled for the actual `service_date`. The
    threshold is `stops_observed >= max(2, stops_observable // 3)`:
    a hard floor of 2 stops (otherwise a single ghost ping would qualify),
    proportional to roughly a third of the observable stops on longer
    trips. This recovers the original "≥3" semantics on a typical
    30-stop urban route (threshold = 10) while admitting short express
    routes whose entire schedule is 2-3 stops (NOTES-30; A90 was reporting
    0/127 delivered despite 88% OTP because `stops_observed >= 3` is
    structurally unreachable on a 2-stop trip). DISTINCT collapses the
    per-source duplication; "any source" is the right rule because TU
    and proximity have nearly inverse blind spots and either source
    clearing the threshold is sufficient evidence the trip ran. The
    GTFS-membership filter is load-bearing for ratio sanity: without it,
    real-time-only ADDED trips end up in the numerator while the
    denominator is purely GTFS-derived. Caveat: ~3-6% of TU-day trips
    have no matching `vehicle_positions` row and so get dropped by the
    B1 derivation — those look "not delivered" here even if they ran.
    The dropped set is route-concentrated as of 2026-05-03; re-run
    `scripts/probe_dropped_tu_trips.py` periodically against multi-day
    windows to see whether the bias shifts.
"""

from __future__ import annotations

from datetime import date as date_type

from sqlalchemy import case, distinct, func
from sqlalchemy.orm import Session

from src.models import Calendar, CalendarDate, Run, Trip

# Day-of-week column on `Calendar` for each Python weekday (Mon=0..Sun=6).
# Used to filter "what's actually scheduled on THIS service_date." Distinct
# from the representative-weekday convention in `src/ewt.py` and
# `src/service_profile.py`, which deliberately collapse all weekdays to a
# Tuesday sample for cross-day aggregation. Service-delivered is a
# per-date question — collapsing to Tuesday silently drops trips on dates
# where WMATA splits weekday service across multiple service_ids
# (e.g. Wed-only `service_id=10`, Fri-only `service_id=6`). Closes
# NOTES-51, where every Wed/Fri date returned `delivered_trips=0`
# system-wide because the Tuesday filter excluded the Wed/Fri-only
# service_ids that actually ran.
_WEEKDAY_TO_CALENDAR_FIELD = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)


def _day_type_for(service_date: date_type) -> str:
    """Map service_date to the day_type bucket route_service_profile uses.

    Kept for the returned dict's `day_type` key — UI / downstream callers
    classify by weekday/saturday/sunday for grouping. Note: scheduled-trip
    filtering no longer routes through day_type (see
    `_scheduled_trip_ids_query`).
    """
    wd = service_date.weekday()  # Mon=0 .. Sun=6
    if wd == 5:
        return "saturday"
    if wd == 6:
        return "sunday"
    return "weekday"


def _scheduled_trip_ids_query(db: Session, route_id: str, service_date: date_type):
    """Subquery yielding GTFS trip_ids scheduled for `route_id` on `service_date`.

    Used to filter the delivered-trips numerator: a real-time-only ADDED trip
    that's in `runs` but not in GTFS shouldn't count toward "service
    delivered" — the denominator is GTFS-derived, so the numerator must
    match. Without this filter the ratio can exceed 100% on days with
    significant supplementation.

    Filters by the actual day-of-week column for `service_date` (rather
    than a representative weekday) and applies the calendar's
    `start_date <= service_date <= end_date` window. Honors
    `calendar_dates` exceptions: type-1 (added) trips are unioned in,
    type-2 (removed) trips are subtracted out — so federal-holiday
    weekdays that explicitly drop weekday service (and add Sunday
    service via a Sunday service_id) compute against the real running
    schedule. Closes the holiday limitation noted in the module
    docstring.
    """
    service_date_str = service_date.strftime("%Y%m%d")
    weekday_field = getattr(Calendar, _WEEKDAY_TO_CALENDAR_FIELD[service_date.weekday()])

    # Service_ids active by base calendar (day-of-week + date window) MINUS
    # explicit removals, UNION explicit additions for this date. This is
    # the GTFS-spec rule for resolving `calendar` + `calendar_dates`.
    base_service_ids = (
        db.query(Calendar.service_id)
        .filter(
            Calendar.is_current,
            weekday_field == 1,
            Calendar.start_date <= service_date_str,
            Calendar.end_date >= service_date_str,
        )
        .subquery()
    )
    removed_service_ids = (
        db.query(CalendarDate.service_id)
        .filter(
            CalendarDate.is_current,
            CalendarDate.date == service_date_str,
            CalendarDate.exception_type == 2,
        )
        .subquery()
    )
    added_service_ids = (
        db.query(CalendarDate.service_id)
        .filter(
            CalendarDate.is_current,
            CalendarDate.date == service_date_str,
            CalendarDate.exception_type == 1,
        )
        .subquery()
    )

    return db.query(Trip.trip_id).filter(
        Trip.route_id == route_id,
        Trip.is_current,
        (
            (
                Trip.service_id.in_(db.query(base_service_ids.c.service_id))
                & ~Trip.service_id.in_(db.query(removed_service_ids.c.service_id))
            )
            | Trip.service_id.in_(db.query(added_service_ids.c.service_id))
        ),
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
    scheduled_trip_ids_q = _scheduled_trip_ids_query(db, route_id, service_date)

    scheduled = scheduled_trip_ids_q.distinct().count()

    # Trip-length-aware existence threshold (NOTES-30). Floor at 2 to reject
    # single-ping ghost runs; otherwise scale with the source's observable
    # stop count (NOTES-31 column) so short express routes — whose GTFS trips
    # have only 2-3 stops total — aren't structurally excluded the way the
    # old flat `stops_observed >= 3` did. `stops_observable` differs from
    # `stops_scheduled` by 1 on trip_update rows (origin is unobservable);
    # using the per-source observable count keeps the threshold honest.
    # `func.floor(stops_observable / 3.0)` makes the divisor explicit-float
    # so the result is identical across Postgres (where INTEGER / INTEGER
    # would otherwise be integer division) and SQLite (where it's already
    # real division); FLOOR then yields the integer floor on both. The
    # outer CASE pins the floor at 2 — algebraically equivalent to
    # `max(2, floor(stops_observable / 3))`. NULL `stops_observable` falls
    # through the CASE and the comparison fails closed; in practice
    # NOTES-31's backfill plus `aggregate_runs.py` keep the column populated.
    delivered_threshold = case(
        (Run.stops_observable >= 9, func.floor(Run.stops_observable / 3.0)),
        else_=2,
    )

    delivered = (
        db.query(func.count(distinct(Run.trip_id)))
        .filter(
            Run.route_id == route_id,
            Run.service_date == service_date_str,
            Run.stops_observed >= delivered_threshold,
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
    if route_ids is None:
        # Build the GTFS-side route set with the same day-of-week + exception
        # logic the per-route query uses, so the union is consistent — a route
        # whose only Friday-only service_id matches the date should appear
        # here, even though it'd be silently dropped by a Tuesday-rep filter.
        gtfs_date_str = service_date.strftime("%Y%m%d")
        weekday_field = getattr(Calendar, _WEEKDAY_TO_CALENDAR_FIELD[service_date.weekday()])
        base_service_ids = {
            sid
            for (sid,) in db.query(Calendar.service_id)
            .filter(
                Calendar.is_current,
                weekday_field == 1,
                Calendar.start_date <= gtfs_date_str,
                Calendar.end_date >= gtfs_date_str,
            )
            .distinct()
            .all()
        }
        added_service_ids = {
            sid
            for (sid,) in db.query(CalendarDate.service_id)
            .filter(
                CalendarDate.is_current,
                CalendarDate.date == gtfs_date_str,
                CalendarDate.exception_type == 1,
            )
            .distinct()
            .all()
        }
        removed_service_ids = {
            sid
            for (sid,) in db.query(CalendarDate.service_id)
            .filter(
                CalendarDate.is_current,
                CalendarDate.date == gtfs_date_str,
                CalendarDate.exception_type == 2,
            )
            .distinct()
            .all()
        }
        active_service_ids = (base_service_ids - removed_service_ids) | added_service_ids

        if active_service_ids:
            from_gtfs = {
                r
                for (r,) in db.query(Trip.route_id)
                .filter(
                    Trip.is_current,
                    Trip.service_id.in_(active_service_ids),
                )
                .distinct()
                .all()
            }
        else:
            from_gtfs = set()
        from_runs = {
            r
            for (r,) in db.query(Run.route_id)
            .filter(Run.service_date == service_date_str)
            .distinct()
            .all()
        }
        route_ids = sorted(from_gtfs | from_runs)
    return [compute_service_delivered(db, r, service_date) for r in route_ids]
