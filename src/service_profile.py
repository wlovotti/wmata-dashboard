"""
Compute the per-(route, day_type, hour) scheduled service profile from GTFS.

Used by the GTFS reload to populate the `route_service_profile` table.
Reference data for downstream metrics (service-delivered ratio, PR #47;
EWT for frequent routes — see `src/ewt.py`).

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

Trunk-stop arrivals (not trip starts), one direction only
---------------------------------------------------------
Headway is computed from `arrival_time` at the route's trunk stop, defined
as the most-served stop on the route that is served by **only one
direction**. The unidirectional constraint matters: many routes have
terminus stops served by every trip in both directions (e.g., D80's
Friendship Heights and Union Station endpoints each see all 268 daily
trips = 134 dir-0 + 134 dir-1), which would double the apparent
frequency. Picking a mid-route stop unique to one direction gives the
honest rider-experience headway. Trip-start times also can't be used —
SQL `MIN(arrival_time)` is a string min, broken on WMATA's unpadded
single-digit hour times (`"10:00:07"` < `"9:58:27"` lexicographically).

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

from src.models import Calendar, Route, RouteServiceProfile, StopTime, Trip

# Day-of-week column on `Calendar` chosen to represent each day_type bucket.
DAY_TYPE_REPRESENTATIVE_FIELD = {
    "weekday": "tuesday",
    "saturday": "saturday",
    "sunday": "sunday",
}

FREQUENT_HEADWAY_MIN = 15.0

# WMATA's published frequency classes mapped to a P90 weekday-headway threshold.
# "or better" wording in WMATA's legend implies the threshold should hold across
# the service day, not just at peak. P90 (vs strict max) absorbs late-night
# single-trip artifacts (e.g., a route with one isolated 2 AM trip producing an
# implausible 700-min "headway" in that hour) without ignoring real off-peak
# slowness. Imperfect against WMATA's branded labels — those are operational
# policy, not strictly derivable from GTFS — but consistent and explainable.
FREQUENCY_CLASS_THRESHOLDS_MIN: list[tuple[float, str]] = [
    (12.0, "high"),
    (20.0, "medium"),
    (30.0, "low"),
]
FREQUENCY_CLASS_LIMITED = "limited"
FREQUENCY_CLASS_LIMITED_STOP = "limited_stop"


def _percentile(sorted_values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile on an already-sorted list. Returns None if empty."""
    if not sorted_values:
        return None
    idx = int(round(pct * (len(sorted_values) - 1)))
    return sorted_values[idx]


def classify_route_frequency(weekday_hour_headways_min: list[float], route_id: str) -> str | None:
    """Classify a route into one of the WMATA frequency bands.

    Inputs: every non-NULL `mean_headway_min` for the route on weekdays, one
    per hour-of-day the route runs. Output: 'high' / 'medium' / 'low' /
    'limited' / 'limited_stop', or None when there's no usable schedule data.
    Limited-stop is detected by the 'X' suffix convention WMATA uses on
    skip-stop variants (A1X, D4X, etc.) — those override frequency-based
    classification per the published map legend.
    """
    if route_id.endswith("X"):
        return FREQUENCY_CLASS_LIMITED_STOP
    if not weekday_hour_headways_min:
        return None
    p90 = _percentile(sorted(weekday_hour_headways_min), 0.90)
    if p90 is None:
        return None
    for threshold, label in FREQUENCY_CLASS_THRESHOLDS_MIN:
        if p90 <= threshold:
            return label
    return FREQUENCY_CLASS_LIMITED


def compute_route_frequency_classes(db: Session) -> dict[str, str]:
    """Return `{route_id: frequency_class}` for every current route.

    One pass over `route_service_profile` (weekday rows) plus one pass over
    `routes` for the X-suffix limited-stop set. Routes without any non-NULL
    `mean_headway_min` get omitted unless they're limited-stop, where the
    suffix alone classifies them.
    """
    rows = (
        db.query(RouteServiceProfile.route_id, RouteServiceProfile.mean_headway_min)
        .filter(
            RouteServiceProfile.day_type == "weekday",
            RouteServiceProfile.mean_headway_min.isnot(None),
        )
        .all()
    )
    by_route: dict[str, list[float]] = defaultdict(list)
    for route_id, headway in rows:
        by_route[route_id].append(float(headway))

    all_route_ids = {r for (r,) in db.query(Route.route_id).filter(Route.is_current).all()}

    classes: dict[str, str] = {}
    for rid in all_route_ids:
        cls = classify_route_frequency(by_route.get(rid, []), rid)
        if cls is not None:
            classes[rid] = cls
    return classes


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


def _trunk_stop_arrivals(db: Session, service_ids: Iterable[str]) -> dict[str, list[str]]:
    """
    For every route active in `service_ids`, pick the most-served stop that
    serves only one direction of the route, and return its arrival_time list.

    The unidirectional constraint avoids termini and other bidirectional hubs
    that would otherwise double the apparent frequency. If no unidirectional
    stop exists (rare, loop routes), the route is skipped.

    Returns: {route_id: [arrival_time, ...]}
    """
    service_ids = list(service_ids)
    if not service_ids:
        return {}

    # One pass over the join, materializing (route_id, stop_id, direction_id,
    # arrival_time). Direction lets us flag bidirectional stops.
    rows = (
        db.query(Trip.route_id, Trip.direction_id, StopTime.stop_id, StopTime.arrival_time)
        .join(StopTime, StopTime.trip_id == Trip.trip_id)
        .filter(
            Trip.is_current,
            StopTime.is_current,
            Trip.service_id.in_(service_ids),
        )
        .all()
    )

    # Per (route, stop): set of directions seen, count of arrivals, list of times.
    stop_dirs: dict[tuple[str, str], set[int]] = defaultdict(set)
    stop_count: dict[tuple[str, str], int] = defaultdict(int)
    stop_times: dict[tuple[str, str], list[str]] = defaultdict(list)
    for route_id, direction_id, stop_id, arrival_time in rows:
        key = (route_id, stop_id)
        stop_dirs[key].add(direction_id)
        stop_count[key] += 1
        stop_times[key].append(arrival_time)

    # Group stops by route, restrict to unidirectional stops, pick the most-served.
    per_route: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for (route_id, stop_id), directions in stop_dirs.items():
        if len(directions) == 1:
            per_route[route_id].append((stop_id, stop_count[(route_id, stop_id)]))

    trunk_arrivals: dict[str, list[str]] = {}
    for route_id, stops in per_route.items():
        trunk_stop_id = max(stops, key=lambda sc: (sc[1], sc[0]))[0]
        trunk_arrivals[route_id] = sorted(stop_times[(route_id, trunk_stop_id)])
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
