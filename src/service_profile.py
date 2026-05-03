"""
Compute the per-(route, day_type, hour) scheduled service profile from GTFS.

Used by the GTFS reload to populate the `route_service_profile` table.
Reference data for downstream metrics (NOTES.md #11 service-delivered ratio,
#15 EWT for frequent routes).

Day-type representative-day strategy
------------------------------------
GTFS lets a service_id apply on any subset of weekdays via its calendar.txt
day-of-week flags (plus calendar_dates exceptions, which we don't consult
here). WMATA in particular splits weekday service across multiple
service_ids — Mon/Tue/Thu (service_id 12), Wed (13), Fri (9) — each with
slightly different trip counts. There is no single "weekday" service.

We pick a representative day-of-week per day_type and aggregate every
service_id whose calendar.txt flag covers that day:
- weekday  → Tuesday  (covers WMATA's dominant Mon-Tue-Thu pattern)
- saturday → Saturday
- sunday   → Sunday

This intentionally excludes Wed-only and Fri-only service variants. For
the downstream consumers (frequent-route classification, denominator for
service-delivered ratio) the Tuesday profile is a good steady-state proxy.

Post-midnight times
-------------------
GTFS stop_times.arrival_time may use HH ≥ 24 to represent service that
extends past midnight (a 25:30 trip is physically 01:30 AM on the next
calendar day). We bucket such trips by `hour % 24` so they aggregate with
clock-time peers, but use raw seconds for the headway sort. This produces
the right classification: late-night extensions don't masquerade as
frequent service via stale mod-24 collisions.
"""

from collections import defaultdict
from collections.abc import Iterable

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models import Calendar, StopTime, Trip

# Day-of-week column on `Calendar` chosen to represent each day_type bucket.
DAY_TYPE_REPRESENTATIVE_FIELD = {
    "weekday": "tuesday",
    "saturday": "saturday",
    "sunday": "sunday",
}

FREQUENT_HEADWAY_MIN = 15.0


def _parse_gtfs_time_to_seconds(t: str) -> int:
    """Convert GTFS HH:MM:SS (HH may be ≥ 24) to seconds since service-day start."""
    h, m, s = (int(x) for x in t.split(":"))
    return h * 3600 + m * 60 + s


def _service_ids_for_day_type(db: Session, day_type: str) -> list[str]:
    """Return current service_ids whose calendar.txt flag covers the day_type's representative day."""
    field_name = DAY_TYPE_REPRESENTATIVE_FIELD[day_type]
    field = getattr(Calendar, field_name)
    rows = db.query(Calendar.service_id).filter(Calendar.is_current, field == 1).all()
    return [sid for (sid,) in rows]


def _trip_starts_for_services(db: Session, service_ids: Iterable[str]):
    """Yield (route_id, start_seconds) for each current trip in the given services."""
    rows = (
        db.query(
            Trip.route_id,
            func.min(StopTime.arrival_time).label("start_time"),
        )
        .join(StopTime, StopTime.trip_id == Trip.trip_id)
        .filter(
            Trip.is_current,
            StopTime.is_current,
            Trip.service_id.in_(list(service_ids)),
        )
        .group_by(Trip.trip_id, Trip.route_id)
        .all()
    )
    for route_id, start_time in rows:
        yield route_id, _parse_gtfs_time_to_seconds(start_time)


def compute_route_service_profile(db: Session) -> list[dict]:
    """
    Build one row per (route_id, day_type, hour) describing scheduled service.

    Returns a list of dicts with keys: route_id, day_type, hour,
    scheduled_trips, mean_headway_min (None if scheduled_trips < 2),
    is_frequent.
    """
    results: list[dict] = []
    for day_type in ("weekday", "saturday", "sunday"):
        service_ids = _service_ids_for_day_type(db, day_type)
        if not service_ids:
            continue

        bucket: dict[tuple[str, int], list[int]] = defaultdict(list)
        for route_id, start_seconds in _trip_starts_for_services(db, service_ids):
            hour = (start_seconds // 3600) % 24
            bucket[(route_id, hour)].append(start_seconds)

        for (route_id, hour), starts in bucket.items():
            starts.sort()
            scheduled_trips = len(starts)
            mean_headway_min: float | None = None
            if scheduled_trips >= 2:
                gaps = [starts[i + 1] - starts[i] for i in range(len(starts) - 1)]
                mean_headway_min = sum(gaps) / len(gaps) / 60.0
            is_frequent = (
                mean_headway_min is not None and mean_headway_min <= FREQUENT_HEADWAY_MIN
            )
            results.append(
                {
                    "route_id": route_id,
                    "day_type": day_type,
                    "hour": hour,
                    "scheduled_trips": scheduled_trips,
                    "mean_headway_min": mean_headway_min,
                    "is_frequent": is_frequent,
                }
            )
    return results
