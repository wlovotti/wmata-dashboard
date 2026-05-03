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

Trunk-stop arrivals (not trip starts)
-------------------------------------
Headway is computed from `arrival_time` at the route's trunk stop — the
stop served by the most current trips for that route+day_type. Trip-start
times are unreliable: GTFS encodes block continuations as separate
trip_ids that share the same start minute (e.g., D80 has four trip_ids
all starting at 10:00:00 ±15s), which produces phantom 0-second headways
at the origin. By the time those buses reach a mid-route trunk stop they
spread out and headway calculations become honest.

Because a single stop only sees one direction's buses, this naturally
handles bidirectional routes — `mean_headway_min` reflects the rider
experience at a typical trunk stop in the more-served direction.

Post-midnight times
-------------------
GTFS stop_times.arrival_time may use HH ≥ 24 to represent service that
extends past midnight (a 25:30 trip is physically 01:30 AM on the next
calendar day). We bucket arrivals by `hour % 24` so they aggregate with
clock-time peers. Mixed-hour buckets (rare, only on routes that run
through midnight on both ends of the service day) inflate the headway
via a 24-hour gap, which keeps `is_frequent=False` — the right answer.
"""

from collections import defaultdict
from collections.abc import Iterable

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


def _trunk_stop_arrivals(
    db: Session, service_ids: Iterable[str]
) -> dict[str, list[str]]:
    """
    For every route active in `service_ids`, identify the trunk stop (most-served
    stop_id across current trips) and return its arrival_time list, ordered.

    Returns: {route_id: [arrival_time, ...]}
    """
    service_ids = list(service_ids)
    if not service_ids:
        return {}

    # One pass over the join, materializing (route_id, stop_id, arrival_time).
    rows = (
        db.query(Trip.route_id, StopTime.stop_id, StopTime.arrival_time)
        .join(StopTime, StopTime.trip_id == Trip.trip_id)
        .filter(
            Trip.is_current,
            StopTime.is_current,
            Trip.service_id.in_(service_ids),
        )
        .all()
    )

    # Per route, count arrivals per stop and collect all arrival times.
    per_route_stop_count: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    per_route_stop_arrivals: dict[str, dict[str, list[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for route_id, stop_id, arrival_time in rows:
        per_route_stop_count[route_id][stop_id] += 1
        per_route_stop_arrivals[route_id][stop_id].append(arrival_time)

    # Pick the trunk stop per route — most-served, with stop_id as a stable tiebreaker.
    trunk_arrivals: dict[str, list[str]] = {}
    for route_id, stop_counts in per_route_stop_count.items():
        trunk_stop_id = max(stop_counts.items(), key=lambda kv: (kv[1], kv[0]))[0]
        trunk_arrivals[route_id] = sorted(per_route_stop_arrivals[route_id][trunk_stop_id])
    return trunk_arrivals


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

        trunk_arrivals = _trunk_stop_arrivals(db, service_ids)

        for route_id, arrival_times in trunk_arrivals.items():
            bucket: dict[int, list[int]] = defaultdict(list)
            for t in arrival_times:
                sec = _parse_gtfs_time_to_seconds(t)
                hour = (sec // 3600) % 24
                bucket[hour].append(sec)

            for hour, secs in bucket.items():
                secs.sort()
                scheduled_trips = len(secs)
                mean_headway_min: float | None = None
                if scheduled_trips >= 2:
                    gaps = [secs[i + 1] - secs[i] for i in range(len(secs) - 1)]
                    mean_headway_min = sum(gaps) / len(gaps) / 60.0
                is_frequent = (
                    mean_headway_min is not None
                    and mean_headway_min <= FREQUENT_HEADWAY_MIN
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
