"""
Excess Wait Time (EWT) for frequent service — TfL's flagship rider-experience metric.

Per (route, service_date, time_period), aggregated over every (direction, stop, hour)
**cell** on the route whose own scheduled service is frequent (mean scheduled
headway ≤ 15 min):

  - AWT = mean(h²) / (2 · mean(h))  over observed headways at frequent cells
  - SWT = same formula over scheduled headways at frequent cells
  - EWT = AWT − SWT  (in seconds)

Why cell-level frequency
------------------------
The route-level `route_service_profile.is_frequent` flag is a coarse rollup
derived from the most-served unidirectional stop on the route — useful as a
"this route runs frequent service somewhere" signal but **not safe to pool
across all stops** for AWT/SWT. On a route like D40, branch stops can have
~1/4 the scheduled coverage of trunk stops; pooling their sparse-cell
headways drags SWT into the 60-90 minute range and produces nonsense EWT.

The fix here: classify each `(direction, stop, hour)` cell-hour on its own
merits — frequent iff its own mean scheduled headway ≤ 15 min — and pool
only those into the time_period. Branches that aren't frequent at a given
hour drop out automatically; trunks and corridor stops dominate. This is
both more rigorous and more rider-faithful: EWT measures the experience at
**every stop on the route where service is actually frequent**, not just
the canonical trunk.

The route-level `is_frequent` flag is left alone — it's still useful as a
route-level rollup for service_delivered.py and for UI filtering. It just
isn't the right gate for an averaging metric like EWT.

Headway bucketing
-----------------
Headways are computed per (direction_id, stop_id) cell, then bucketed by the
**earlier** arrival's hour. The direction_id grouping is load-bearing:
termini and shared hubs serve both directions under one stop_id and would
silently double-count without it (CLAUDE.md gotcha). Hours ≥ 24 in GTFS
times wrap via `% 24`, so post-midnight service runs aggregate with their
clock-time peers.

Observed timestamps are stored naive UTC; we convert to Eastern via zoneinfo
before bucketing. The existing `analytics.py` time-period bucketing uses
naive `.hour` directly, which is a latent UTC-vs-Eastern bug — corrected
here for new metrics.

Observed vs scheduled sources
-----------------------------
  - Observed: `stop_events` rows with `source='trip_update'` (the primary
    derivation, PR #43) and non-null `observed_arrival_ts`. Skipped trips
    naturally widen the observed gap and bunched arrivals naturally narrow
    it — both feed into AWT correctly.

  - Scheduled: `stop_times` joined to `trips` and `calendar`, filtered by the
    representative weekday for `service_date`'s day_type (Tue → weekday, Sat
    → saturday, Sun → sunday — same convention as `service_profile.py`).
    GTFS `arrival_time` is parsed to seconds before sorting (string MIN/MAX
    is broken on WMATA's unpadded single-digit hours, e.g. `"10:00:07" <
    "9:58:27"` lexicographically).

Aggregation to time_period
--------------------------
Within each time_period (AM Peak, Midday, PM Peak, Evening, Night — all
Eastern), pool every frequent cell-hour's headways and compute AWT and SWT
once over the pooled lists. This is rider-weighted by construction: cells
with more arrivals contribute more headways. Per-cell AWTs are never
averaged together — averaging AWTs is wrong; only the pooled formula gives
the correct rider-weighted aggregate.

Calendar resolution and calendar_dates (NOTES-106)
----------------------------------------------------
The scheduled side pools GTFS `stop_times` for a **representative** weekday
per day_type (Tue for `weekday`, Sat/Sun for the others — see
`DAY_TYPE_REPRESENTATIVE_FIELD`), not the literal `service_date` passed in.
This is deliberate: it lets one schedule fetch serve every observed date of
the same day_type instead of re-querying per date (see
`fetch_scheduled_cell_hours_for_routes`'s module-level cache).

`calendar`'s day-of-week flag is the base source for that representative
set. When it's non-empty, it's used as-is — a single-date `calendar_dates`
exception (e.g. a federal-holiday weekday running Sunday service) is NOT
subtracted, because it's date-specific and the representative aggregate
pools across many dates; consulting it here would perturb every OTHER
date's SWT for one date's schedule swap. This is `service_delivered.py`'s
per-`service_date` resolution — it isn't the right model here.

But some agencies (Muni/SFMTA as of 2026-08) define ENTIRE day_types
purely via `calendar_dates`: `calendar` carries only Saturday/Sunday rows,
and weekday service exists solely as exception_type=1 (added) rows on
specific dates. For such a day_type, the `calendar`-flag base is empty, so
`_resolve_service_ids_for_day_type` falls back to `calendar_dates` itself:
every service_id ever added (type=1) on a date whose weekday matches the
day_type, minus every service_id ever net-removed (type=2) on a matching
date. This is the only case calendar_dates is consulted — a day_type with
zero base coverage has no "single date's exception" to protect against,
so the fallback set doubles as the representative pool.

Known limitations (deferred)
----------------------------
  - `schedule_relationship='ADDED'` trips (real-time-only additions) aren't
    in the scheduled denominator since they aren't in GTFS. Rare, accepted.
  - Single-date holiday `calendar_dates` swaps on an agency whose `calendar`
    already covers the day_type are still not consulted (see above) — a
    federal-holiday weekday running Sunday service will use the weekday
    schedule as the SWT comparison, same caveat as `service_delivered.py`
    had before its own per-date fix.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date as date_type
from datetime import datetime
from threading import Lock
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.frequent_routes import DEFAULT_GATE_SEC, get_cell_hour_gate_sec
from src.gtfs_versioning import gtfs_version_filter
from src.models import Calendar, CalendarDate, GTFSSnapshot, StopEvent, StopTime, Trip
from src.time_periods import is_hour_in_period

UTC = ZoneInfo("UTC")

# Module-level default for callers that don't have a route_id in hand
# (system-wide EWT, ad-hoc analysis). 15 min matches the route-level
# threshold in service_profile.py (FREQUENT_HEADWAY_MIN = 15.0). The
# per-route gate is resolved via `get_cell_hour_gate_sec` so medium-
# frequency routes get a 20-min gate matching their tier (see
# `src/frequent_routes.py` for the tier policy).
FREQUENT_HEADWAY_MAX_SEC = DEFAULT_GATE_SEC

# Eastern-hour boundaries for the period buckets surfaced in API/UI.
# (label, start_hour_inclusive, end_hour_exclusive). Night wraps the day end
# but stays expressible as 0..6 because clock hours never exceed 23.
EWT_TIME_PERIODS: list[tuple[str, int, int]] = [
    ("AM Peak (6-9)", 6, 9),
    ("Midday (9-15)", 9, 15),
    ("PM Peak (15-19)", 15, 19),
    ("Evening (19-24)", 19, 24),
    ("Night (0-6)", 0, 6),
]

# Same Calendar field map service_profile.py uses to pick the representative
# weekday per day_type. Keeps SWT computed against the same schedule as the
# `is_frequent` classification.
DAY_TYPE_REPRESENTATIVE_FIELD = {
    "weekday": "tuesday",
    "saturday": "saturday",
    "sunday": "sunday",
}

CellHour = tuple[int, str, int]  # (direction_id, stop_id, hour)

# Python `date.weekday()` values (Mon=0..Sun=6) that count as a match for
# each day_type, used by the calendar_dates fallback in
# `_resolve_service_ids_for_day_type` below.
_DAY_TYPE_ISO_WEEKDAYS: dict[str, frozenset[int]] = {
    "weekday": frozenset({0, 1, 2, 3, 4}),
    "saturday": frozenset({5}),
    "sunday": frozenset({6}),
}


def _calendar_dates_service_ids_matching_day_type(
    db: Session,
    day_type: str,
    exception_type: int,
    gtfs_snapshot_id: int | None = None,
) -> set[str]:
    """service_ids with a `calendar_dates` exception of `exception_type`
    (1=added, 2=removed) on any date whose weekday matches `day_type`.

    `CalendarDate.date` is a 'YYYYMMDD' string, so the weekday match is done
    in Python after fetching — there's no portable SQL day-of-week
    extraction that works identically on both SQLite (tests) and Postgres
    (prod) without agency-specific functions. The exception rows themselves
    are few (per-agency calendar_dates tables run in the dozens, not
    millions), so this is cheap.
    """
    matched_weekdays = _DAY_TYPE_ISO_WEEKDAYS[day_type]
    rows = (
        db.query(CalendarDate.service_id, CalendarDate.date)
        .filter(
            gtfs_version_filter(CalendarDate, gtfs_snapshot_id),
            CalendarDate.exception_type == exception_type,
        )
        .all()
    )
    out: set[str] = set()
    for service_id, date_str in rows:
        d = datetime.strptime(date_str, "%Y%m%d").date()
        if d.weekday() in matched_weekdays:
            out.add(service_id)
    return out


def _resolve_service_ids_for_day_type(
    db: Session, day_type: str, gtfs_snapshot_id: int | None = None
) -> set[str]:
    """Resolve the representative-day service_id set for `day_type`
    (NOTES-106), honoring `calendar_dates` only when `calendar` itself
    defines no day-of-week service for it. See the module docstring's
    "Calendar resolution and calendar_dates" section for the full
    rationale — in short: a non-empty `calendar` base is used as-is
    (unaffected by any single-date exception); an empty base falls all
    the way back to `calendar_dates` additions minus removals.

    Shared by `_scheduled_headways_by_cell_hour` and
    `fetch_scheduled_cell_hours_for_routes` so bunching (which imports both
    from this module) gets the fix for free rather than duplicating the
    query.
    """
    field_name = DAY_TYPE_REPRESENTATIVE_FIELD[day_type]
    field = getattr(Calendar, field_name)
    base_ids = {
        sid
        for (sid,) in db.query(Calendar.service_id)
        .filter(gtfs_version_filter(Calendar, gtfs_snapshot_id), field == 1)
        .all()
    }
    if base_ids:
        return base_ids

    added = _calendar_dates_service_ids_matching_day_type(db, day_type, 1, gtfs_snapshot_id)
    removed = _calendar_dates_service_ids_matching_day_type(db, day_type, 2, gtfs_snapshot_id)
    return added - removed


def _day_type_for(service_date: date_type) -> str:
    """Map a service_date to the day_type bucket route_service_profile uses."""
    wd = service_date.weekday()  # Mon=0 .. Sun=6
    if wd == 5:
        return "saturday"
    if wd == 6:
        return "sunday"
    return "weekday"


def _hour_in_zone(ts: datetime, tz: ZoneInfo) -> int:
    """Return the local hour-of-day for a naive-UTC timestamp in a pre-built zone.

    Stop_event timestamps are naive UTC by storage convention (timezones.py).
    We re-attach UTC, convert to ``tz``, and take the hour. zoneinfo handles
    DST transitions correctly.

    Takes an already-constructed ``ZoneInfo`` rather than a ``tz_name``
    string so hot loops (one call per stop_event row) can hoist the
    ``ZoneInfo(tz_name)`` construction once per enclosing function instead
    of paying it on every row — `_eastern_hour` below is the convenience
    single-call wrapper for call sites outside a loop.
    """
    return ts.replace(tzinfo=UTC).astimezone(tz).hour


def _eastern_hour(ts: datetime, tz_name: str = "America/New_York") -> int:
    """Return the local hour-of-day for a naive-UTC stop_event timestamp.

    Convenience wrapper around `_hour_in_zone` for single-call (non-loop)
    use — constructs a fresh ``ZoneInfo(tz_name)`` per call. Hot loops
    should call `_hour_in_zone` directly with a ``ZoneInfo`` built once
    outside the loop.

    ``tz_name`` (NOTES-103 multi-agency) defaults to Eastern so every
    existing WMATA call site is unaffected. Despite the name (kept for the
    many existing call sites, including cross-module ones in
    ``api/aggregations.py``), this is the agency-local hour, not
    necessarily Eastern — pass the agency's own IANA zone (e.g.
    ``"America/Los_Angeles"`` for SFMTA) for a non-Eastern agency.
    """
    return _hour_in_zone(ts, ZoneInfo(tz_name))


def _period_for_hour(hour: int) -> str:
    """Map an agency-local hour-of-day (0..23) to its time_period label."""
    for label, start, end in EWT_TIME_PERIODS:
        if start <= hour < end:
            return label
    raise ValueError(f"hour {hour} out of 0..23 range")


def _parse_gtfs_time_to_seconds(t: str) -> int:
    """Convert GTFS HH:MM:SS (HH may be ≥ 24) to seconds since service-day start."""
    h, m, s = (int(x) for x in t.split(":"))
    return h * 3600 + m * 60 + s


def compute_awt(headways_seconds: list[float]) -> float | None:
    """Rider-weighted average wait time from a list of consecutive headways.

    AWT = mean(h²) / (2 · mean(h)). Returns `None` when the list is empty or
    its sum is non-positive (no service for a rider to wait through).

    Why this formula: a rider arriving uniformly random during an interval of
    length h waits h/2 on average; the probability of being in that interval
    is proportional to h; so the expected wait across all riders is
    `Σ(h · h/2) / Σ h = mean(h²) / (2 · mean(h))`. With perfectly even
    headways it reduces to h/2 — but bunching pushes mean(h²) up faster than
    mean(h), so AWT is strictly above mean(h)/2 for any irregular service.
    Note that even a single headway gives a defined AWT (= h/2), so we
    don't gate on `len ≥ 2` here.
    """
    if not headways_seconds:
        return None
    total = sum(headways_seconds)
    if total <= 0:
        return None
    sq = sum(h * h for h in headways_seconds)
    return sq / (2.0 * total)


def _observed_headways_by_cell_hour(
    db: Session,
    route_id: str,
    service_date_str: str,
    tz_name: str = "America/New_York",
) -> dict[CellHour, list[float]]:
    """Compute observed headways per (direction, stop, local_hour) cell.

    Returns `{(direction, stop, hour): [headway_sec, ...]}` where each
    headway is the gap between two consecutive observed arrivals at the same
    (direction, stop), bucketed by the **earlier** arrival's local hour in
    `tz_name` (NOTES-103 multi-agency; defaults to Eastern). Source is
    restricted to `trip_update` (the primary derivation, PR #43) so each
    actual arrival contributes exactly one row.
    """
    rows = (
        db.query(StopEvent.direction_id, StopEvent.stop_id, StopEvent.observed_arrival_ts)
        .filter(
            StopEvent.route_id == route_id,
            StopEvent.service_date == service_date_str,
            StopEvent.source == "trip_update",
            StopEvent.observed_arrival_ts.isnot(None),
        )
        .order_by(StopEvent.direction_id, StopEvent.stop_id, StopEvent.observed_arrival_ts)
        .all()
    )

    tz = ZoneInfo(tz_name)
    by_cell_hour: dict[CellHour, list[float]] = defaultdict(list)
    prev_key: tuple[int, str] | None = None
    prev_ts: datetime | None = None
    for direction_id, stop_id, ts in rows:
        key = (direction_id, stop_id)
        if prev_key == key and prev_ts is not None:
            delta = (ts - prev_ts).total_seconds()
            if delta > 0:
                by_cell_hour[(direction_id, stop_id, _hour_in_zone(prev_ts, tz))].append(delta)
        prev_key = key
        prev_ts = ts
    return by_cell_hour


def _scheduled_headways_by_cell_hour(
    db: Session, route_id: str, day_type: str
) -> dict[CellHour, list[float]]:
    """Compute scheduled headways per (direction, stop, hour) cell.

    Pulls every (direction, stop) cell active on the day_type's representative
    weekday and computes consecutive scheduled headways within that cell.
    Each headway is bucketed by `(parsed_seconds // 3600) % 24` of the
    earlier arrival — same convention `route_service_profile` uses, so the
    frequent threshold has the same units. Hours ≥ 24 in GTFS service-day-
    extending times wrap correctly.

    The representative-day service_id set is resolved via
    `_resolve_service_ids_for_day_type` (NOTES-106) — `calendar`'s
    day-of-week flag when non-empty, falling back to `calendar_dates`
    when it isn't (e.g. a Muni/SFMTA-shaped feed with no weekday `calendar`
    rows at all).
    """
    service_ids = _resolve_service_ids_for_day_type(db, day_type)
    if not service_ids:
        return {}
    rows = (
        db.query(Trip.direction_id, StopTime.stop_id, StopTime.arrival_time)
        .join(StopTime, StopTime.trip_id == Trip.trip_id)
        .filter(
            Trip.route_id == route_id,
            Trip.is_current,
            StopTime.is_current,
            Trip.service_id.in_(service_ids),
        )
        .all()
    )

    by_cell: dict[tuple[int, str], list[int]] = defaultdict(list)
    for direction_id, stop_id, arrival_time in rows:
        if arrival_time is None:
            continue
        by_cell[(direction_id, stop_id)].append(_parse_gtfs_time_to_seconds(arrival_time))

    by_cell_hour: dict[CellHour, list[float]] = defaultdict(list)
    for (direction, stop), secs in by_cell.items():
        secs.sort()
        for i in range(len(secs) - 1):
            delta = secs[i + 1] - secs[i]
            if delta > 0:
                hour = (secs[i] // 3600) % 24
                by_cell_hour[(direction, stop, hour)].append(float(delta))
    return by_cell_hour


def _is_cell_hour_frequent(
    scheduled_headways: list[float],
    gate_sec: int = FREQUENT_HEADWAY_MAX_SEC,
) -> bool:
    """A cell-hour is frequent iff its mean scheduled headway ≤ `gate_sec`.

    `gate_sec` defaults to `FREQUENT_HEADWAY_MAX_SEC` (15 min) for callers
    without route context. Per-route callers resolve their gate via
    `src/frequent_routes.py:get_cell_hour_gate_sec` so medium-frequency
    routes get a 20-min gate matching their tier.

    Excludes cell-hours with no scheduled headways (single-arrival cells, or
    cells with no service in this hour at all) — they can't be classified.
    """
    if not scheduled_headways:
        return False
    return (sum(scheduled_headways) / len(scheduled_headways)) <= gate_sec


def compute_ewt_for_route_date(
    db: Session,
    route_id: str,
    service_date: date_type,
    tz_name: str = "America/New_York",
) -> list[dict]:
    """Compute EWT for one (route, service_date), one row per time_period.

    Each row has keys `route_id, service_date, day_type, time_period,
    awt_seconds, swt_seconds, ewt_seconds, n_observed_headways,
    n_scheduled_headways, frequent_cell_hours`. AWT/SWT/EWT are `None` when
    the corresponding pool is empty. All five time_periods are emitted even
    when the route has no frequent cells in any of them — callers can filter
    by `frequent_cell_hours > 0` to drop the empty rows.

    `tz_name` (NOTES-103 multi-agency) buckets the *observed* side by the
    agency's own local hour; defaults to Eastern. The *scheduled* side is
    always agency-local by construction (GTFS clock time), so this only
    matters for non-Eastern agencies.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    sched_by_cell_hour = _scheduled_headways_by_cell_hour(db, route_id, day_type)
    obs_by_cell_hour = _observed_headways_by_cell_hour(db, route_id, service_date_str, tz_name)
    gate_sec = get_cell_hour_gate_sec(route_id)

    obs_pool: dict[str, list[float]] = defaultdict(list)
    sched_pool: dict[str, list[float]] = defaultdict(list)
    freq_cell_count: dict[str, int] = defaultdict(int)

    for cell_hour, sched_headways in sched_by_cell_hour.items():
        if not _is_cell_hour_frequent(sched_headways, gate_sec):
            continue
        _direction, _stop, hour = cell_hour
        period = _period_for_hour(hour)
        sched_pool[period].extend(sched_headways)
        obs_pool[period].extend(obs_by_cell_hour.get(cell_hour, []))
        freq_cell_count[period] += 1

    rows: list[dict] = []
    for label, _, _ in EWT_TIME_PERIODS:
        obs = obs_pool.get(label, [])
        sched = sched_pool.get(label, [])
        awt = compute_awt(obs)
        swt = compute_awt(sched)
        # Clamp at 0: EWT is rider-felt excess wait. Sparse observation coverage
        # can drive AWT below SWT — a measurement artifact, not a real "service
        # ran better than scheduled" signal. AWT/SWT remain raw. The companion
        # `coverage_ratio` field is what the UI uses to flag the underlying gap.
        ewt = max(0.0, awt - swt) if (awt is not None and swt is not None) else None
        rows.append(
            {
                "route_id": route_id,
                "service_date": service_date_str,
                "day_type": day_type,
                "time_period": label,
                "awt_seconds": round(awt, 2) if awt is not None else None,
                "swt_seconds": round(swt, 2) if swt is not None else None,
                "ewt_seconds": round(ewt, 2) if ewt is not None else None,
                "n_observed_headways": len(obs),
                "n_scheduled_headways": len(sched),
                "coverage_ratio": _coverage_ratio(len(obs), len(sched)),
                "frequent_cell_hours": freq_cell_count.get(label, 0),
            }
        )
    return rows


def compute_ewt_for_routes(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
    tz_name: str = "America/New_York",
) -> list[dict]:
    """Compute EWT for every route seen in `stop_events` on the date, or pass
    `route_ids` to restrict. Returns a flat list — one dict per (route,
    time_period) — sorted by route_id then by the canonical time_period order
    in `EWT_TIME_PERIODS`. Routes with no frequent cell-hours produce
    placeholder rows (all metrics None, frequent_cell_hours=0) so callers can
    distinguish "evaluated, not frequent" from "not evaluated."

    `tz_name` (NOTES-103 multi-agency) is forwarded to
    `compute_ewt_for_route_date` for every route; defaults to Eastern.
    """
    service_date_str = service_date.isoformat()
    if route_ids is None:
        route_ids = sorted(
            r
            for (r,) in db.query(StopEvent.route_id)
            .filter(StopEvent.service_date == service_date_str)
            .distinct()
            .all()
        )
    out: list[dict] = []
    for r in route_ids:
        out.extend(compute_ewt_for_route_date(db, r, service_date, tz_name))
    return out


def _coverage_ratio(n_observed: int, n_scheduled: int) -> float | None:
    """Observed-to-scheduled headway coverage for an EWT pool.

    Returns `n_observed / n_scheduled` clamped into `[0, 1]`, or `None` when
    `n_scheduled == 0` (no frequent cell-hours, so coverage is undefined).
    The clamp is defensive — observed can briefly exceed scheduled when ADDED
    real-time-only trips slot between scheduled buses, and we don't want a
    `> 1` value confusing the "thin data" UI threshold.

    Used by the frontend to flag periods where the EWT clamp at 0
    (NOTES-17) is masking sparse trip_update derivation rather than reflecting
    on-time service. Below ~0.5 the metric is unreliable.
    """
    if n_scheduled <= 0:
        return None
    return min(1.0, max(0.0, n_observed / n_scheduled))


def _ewt_headline_from_pools(
    route_id: str,
    service_date_str: str,
    day_type: str,
    obs_pool: list[float],
    sched_pool: list[float],
    freq_cells: int,
) -> dict:
    """Build the headline result dict from already-pooled observed/scheduled lists.

    Shared by `compute_ewt_headline_for_route` and the vectorized
    `compute_ewt_headline_for_routes` so both produce identical output.

    The `obs_sum_h`, `obs_sum_h_sq`, `sched_sum_h`, `sched_sum_h_sq` fields
    are sufficient statistics for AWT/SWT — they let a windowed aggregator
    pool across multiple service_dates without re-pulling raw headways.
    AWT(window) = Σ obs_sum_h_sq / (2 · Σ obs_sum_h); same shape for SWT.
    """
    obs_sum_h = sum(obs_pool)
    obs_sum_h_sq = sum(h * h for h in obs_pool)
    sched_sum_h = sum(sched_pool)
    sched_sum_h_sq = sum(h * h for h in sched_pool)
    awt = compute_awt(obs_pool)
    swt = compute_awt(sched_pool)
    ewt = (awt - swt) if (awt is not None and swt is not None) else None
    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "day_type": day_type,
        "awt_seconds": round(awt, 2) if awt is not None else None,
        "swt_seconds": round(swt, 2) if swt is not None else None,
        "ewt_seconds": round(ewt, 2) if ewt is not None else None,
        "n_observed_headways": len(obs_pool),
        "n_scheduled_headways": len(sched_pool),
        "obs_sum_h": obs_sum_h,
        "obs_sum_h_sq": obs_sum_h_sq,
        "sched_sum_h": sched_sum_h,
        "sched_sum_h_sq": sched_sum_h_sq,
        "coverage_ratio": _coverage_ratio(len(obs_pool), len(sched_pool)),
        "frequent_cell_hours": freq_cells,
    }


def compute_ewt_headline_for_route(
    db: Session,
    route_id: str,
    service_date: date_type,
    period_key: str = "all",
    tz_name: str = "America/New_York",
) -> dict:
    """Single-route EWT collapsed to one rider-weighted number for the day.

    Pools every frequent (direction, stop, hour) cell on the route into a single
    observed pool and a single scheduled pool, then computes AWT/SWT/EWT once.
    Mathematically equivalent to "EWT across the whole day for this route at
    every cell where service is actually frequent" — non-frequent cell-hours
    drop out by the same gating used in the per-period variant.

    `period_key` (NOTES-41) restricts which local hours feed the pool —
    e.g. `am_peak` keeps only cell-hours with hour in [6, 10). `late`
    wraps midnight so 22..23 and 0..5 both qualify. Default `all` keeps
    every hour. Note this filters the cell-hour bucket, NOT the originating
    arrival time inside it — but `_eastern_hour` already buckets each
    headway by the earlier arrival's clock hour, so it's the same thing.
    The bucket's hour is the *scheduled* side's GTFS clock hour (always
    agency-local); `tz_name` (NOTES-103 multi-agency) controls which local
    hour the *observed* side buckets into so the two sides key-match for
    non-Eastern agencies. Defaults to Eastern.

    Returns the same dict shape as one period row from `compute_ewt_for_route_date`,
    minus the `time_period` key.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    sched_by_cell_hour = _scheduled_headways_by_cell_hour(db, route_id, day_type)
    obs_by_cell_hour = _observed_headways_by_cell_hour(db, route_id, service_date_str, tz_name)
    gate_sec = get_cell_hour_gate_sec(route_id)

    obs_pool: list[float] = []
    sched_pool: list[float] = []
    freq_cells = 0
    for cell_hour, sched_headways in sched_by_cell_hour.items():
        if not _is_cell_hour_frequent(sched_headways, gate_sec):
            continue
        _direction, _stop, hour = cell_hour
        if not is_hour_in_period(hour, period_key):
            continue
        sched_pool.extend(sched_headways)
        obs_pool.extend(obs_by_cell_hour.get(cell_hour, []))
        freq_cells += 1

    return _ewt_headline_from_pools(
        route_id, service_date_str, day_type, obs_pool, sched_pool, freq_cells
    )


# Module-level cache for the scheduled-cell-hour fetch. The schedule depends
# only on the active GTFS snapshot (which versions trips/stop_times/calendar
# via `is_current`), so the result is valid until a new snapshot is loaded.
# Keying by `(day_type, snapshot_id)` means the cache naturally invalidates
# the moment `reload_gtfs_complete.py` writes a new gtfs_snapshots row — no
# TTL or restart needed.
#
# Only the unfiltered (`route_ids is None`) path is cached. The filtered
# path is uncommon and could legitimately collide with a cached entry's key
# space without proper isolation.
_schedule_cache: dict[tuple[str, int], dict[str, dict[CellHour, list[float]]]] = {}
_schedule_cache_lock = Lock()


def fetch_scheduled_cell_hours_for_routes(
    db: Session,
    day_type: str,
    route_ids: list[str] | None = None,
    gtfs_snapshot_id: int | None = None,
) -> dict[str, dict[CellHour, list[float]]]:
    """Vectorized scheduled-headway-per-(direction, stop, hour) for every route.

    Single SQL pass joining `trips`, `stop_times`, and `calendar` for the
    representative weekday of `day_type`. Returns
    `{route_id: {(direction_id, stop_id, hour): [scheduled_headway_sec, ...]}}`
    — each list is consecutive scheduled headways within that cell, bucketed
    by the earlier arrival's hour-of-day.

    Cached at module level by `(day_type, gtfs_snapshot_id)` when called with
    `route_ids=None` (the dashboard path). The cost is ~1.5s for the full
    SQL pass + Python pairing; the cache invalidates automatically when
    `reload_gtfs_complete.py` writes a new `gtfs_snapshots` row, so no
    manual flush is needed after a GTFS refresh.

    Pass `gtfs_snapshot_id` to pin the schedule to a historical snapshot
    (backfill); the default reads the live `is_current` snapshot. Explicit
    snapshots cache under their own id — historical snapshot rows never
    change, so those entries never need invalidating.

    The representative-day service_id set is resolved via
    `_resolve_service_ids_for_day_type` (NOTES-106) — same shared resolver
    `_scheduled_headways_by_cell_hour` uses, so this vectorized path and the
    per-route path never disagree on which schedule "the weekday" means.
    """
    if route_ids is None:
        if gtfs_snapshot_id is not None:
            snapshot_id = gtfs_snapshot_id
        else:
            snapshot_id = db.query(func.max(GTFSSnapshot.snapshot_id)).scalar() or 0
        cache_key = (day_type, snapshot_id)
        with _schedule_cache_lock:
            cached = _schedule_cache.get(cache_key)
        if cached is not None:
            return cached

    service_ids = _resolve_service_ids_for_day_type(db, day_type, gtfs_snapshot_id)

    sched_by_route_cell: dict[tuple[str, int, str], list[int]] = defaultdict(list)
    if service_ids:
        sched_q = (
            db.query(
                Trip.route_id,
                Trip.direction_id,
                StopTime.stop_id,
                StopTime.arrival_time,
            )
            .join(StopTime, StopTime.trip_id == Trip.trip_id)
            .filter(
                gtfs_version_filter(Trip, gtfs_snapshot_id),
                gtfs_version_filter(StopTime, gtfs_snapshot_id),
                Trip.service_id.in_(service_ids),
            )
        )
        if route_ids is not None:
            sched_q = sched_q.filter(Trip.route_id.in_(route_ids))

        for route_id, direction_id, stop_id, arrival_time in sched_q.all():
            if arrival_time is None:
                continue
            sched_by_route_cell[(route_id, direction_id, stop_id)].append(
                _parse_gtfs_time_to_seconds(arrival_time)
            )

    sched_by_route_cell_hour: dict[str, dict[CellHour, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for (route_id, direction, stop), secs in sched_by_route_cell.items():
        secs.sort()
        for i in range(len(secs) - 1):
            delta = secs[i + 1] - secs[i]
            if delta > 0:
                hour = (secs[i] // 3600) % 24
                sched_by_route_cell_hour[route_id][(direction, stop, hour)].append(float(delta))

    if route_ids is None:
        # Stash the unfiltered result and evict any entries from older GTFS
        # snapshots so the cache doesn't accumulate every historical version.
        with _schedule_cache_lock:
            _schedule_cache[cache_key] = sched_by_route_cell_hour
            for k in list(_schedule_cache.keys()):
                if k[1] != snapshot_id:
                    del _schedule_cache[k]
    return sched_by_route_cell_hour


def compute_ewt_headline_for_routes(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
    sched_by_route_cell_hour: dict[str, dict[CellHour, list[float]]] | None = None,
    tz_name: str = "America/New_York",
) -> dict[str, dict]:
    """Vectorized headline EWT for all routes — two SQL passes, no per-route loop.

    Pulls all scheduled stop_times (joined to trips and calendar for the
    representative weekday) and all observed `stop_events` on the date in one
    query each, then groups by (route, direction, stop) in Python and
    aggregates per route.

    Pass `sched_by_route_cell_hour` to skip the scheduled fetch — used by the
    scorecard path to share scheduled data with bunching.

    `tz_name` (NOTES-103 multi-agency) buckets the observed side by the
    agency's own local hour; defaults to Eastern.

    Returns `{route_id: headline_dict}`. Routes with no scheduled service on
    the day_type don't appear; routes with scheduled service but no observed
    arrivals appear with `awt_seconds=None`. Pass `route_ids` to restrict.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    if sched_by_route_cell_hour is None:
        sched_by_route_cell_hour = fetch_scheduled_cell_hours_for_routes(db, day_type, route_ids)

    # All observed stop_events for the date, every route, one query.
    obs_q = (
        db.query(
            StopEvent.route_id,
            StopEvent.direction_id,
            StopEvent.stop_id,
            StopEvent.observed_arrival_ts,
        )
        .filter(
            StopEvent.service_date == service_date_str,
            StopEvent.source == "trip_update",
            StopEvent.observed_arrival_ts.isnot(None),
        )
        .order_by(
            StopEvent.route_id,
            StopEvent.direction_id,
            StopEvent.stop_id,
            StopEvent.observed_arrival_ts,
        )
    )
    if route_ids is not None:
        obs_q = obs_q.filter(StopEvent.route_id.in_(route_ids))

    tz = ZoneInfo(tz_name)
    obs_by_route_cell_hour: dict[str, dict[CellHour, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    prev_key: tuple[str, int, str] | None = None
    prev_ts: datetime | None = None
    for route_id, direction_id, stop_id, ts in obs_q.all():
        key = (route_id, direction_id, stop_id)
        if prev_key == key and prev_ts is not None:
            delta = (ts - prev_ts).total_seconds()
            if delta > 0:
                obs_by_route_cell_hour[route_id][
                    (direction_id, stop_id, _hour_in_zone(prev_ts, tz))
                ].append(delta)
        prev_key = key
        prev_ts = ts

    # Per-route headline aggregation.
    all_routes = set(sched_by_route_cell_hour.keys())
    if route_ids is not None:
        all_routes &= set(route_ids)

    results: dict[str, dict] = {}
    for route_id in all_routes:
        sched_cells = sched_by_route_cell_hour.get(route_id, {})
        obs_cells = obs_by_route_cell_hour.get(route_id, {})
        gate_sec = get_cell_hour_gate_sec(route_id)
        obs_pool: list[float] = []
        sched_pool: list[float] = []
        freq_cells = 0
        for cell_hour, sched_headways in sched_cells.items():
            if not _is_cell_hour_frequent(sched_headways, gate_sec):
                continue
            sched_pool.extend(sched_headways)
            obs_pool.extend(obs_cells.get(cell_hour, []))
            freq_cells += 1
        results[route_id] = _ewt_headline_from_pools(
            route_id, service_date_str, day_type, obs_pool, sched_pool, freq_cells
        )
    return results


def fetch_observed_stop_events_for_window(
    db: Session,
    service_dates: list[date_type],
    route_ids: list[str] | None = None,
) -> list[tuple]:
    """Pull source='trip_update' observed stop_events for the whole window in one query.

    Returns a list of `(service_date_str, route_id, direction_id, stop_id,
    observed_arrival_ts, schedule_relationship)` tuples, ordered for the
    headway-pairing logic in both EWT and bunching. EWT pairs every
    consecutive arrival; bunching pairs only those with
    `schedule_relationship='SCHEDULED'`. Sharing this pull saves the
    duplicate ~9s SQL+materialize cost the two metrics would otherwise pay
    individually.
    """
    if not service_dates:
        return []
    date_strs = [d.isoformat() for d in service_dates]
    q = (
        db.query(
            StopEvent.service_date,
            StopEvent.route_id,
            StopEvent.direction_id,
            StopEvent.stop_id,
            StopEvent.observed_arrival_ts,
            StopEvent.schedule_relationship,
        )
        .filter(
            StopEvent.service_date.in_(date_strs),
            StopEvent.source == "trip_update",
            StopEvent.observed_arrival_ts.isnot(None),
        )
        .order_by(
            StopEvent.service_date,
            StopEvent.route_id,
            StopEvent.direction_id,
            StopEvent.stop_id,
            StopEvent.observed_arrival_ts,
        )
    )
    if route_ids is not None:
        q = q.filter(StopEvent.route_id.in_(route_ids))
    return q.all()


def compute_ewt_headline_for_routes_multi_date(
    db: Session,
    service_dates: list[date_type],
    sched_by_day_type: dict[str, dict[str, dict[CellHour, list[float]]]] | None = None,
    route_ids: list[str] | None = None,
    observed_rows: list[tuple] | None = None,
    tz_name: str = "America/New_York",
) -> dict[str, dict[str, dict]]:
    """Multi-date headline EWT — one SQL pull for the whole window.

    Equivalent to calling `compute_ewt_headline_for_routes` once per date in
    `service_dates`, but collapses the per-date observed-stop_events queries
    into a single SQL pass using `service_date IN (...)`. Returns
    `{service_date_str: {route_id: headline_dict}}` — each per-date inner
    dict is identical to what the single-date function returns for that day.

    Pass `observed_rows` to skip the observed fetch — used by the windowed
    scorecard path so EWT and bunching share one pull. Pass
    `sched_by_day_type` to share schedule fetches the same way. Both are
    auto-fetched when None.

    `tz_name` (NOTES-103 multi-agency) buckets the observed side by the
    agency's own local hour; defaults to Eastern.

    Pairing is strictly within `(service_date, route, direction, stop)` —
    consecutive arrivals never cross a day boundary, so the headway list
    each date produces is identical to what the single-date function would.
    """
    if not service_dates:
        return {}

    date_strs = [d.isoformat() for d in service_dates]
    day_types = {ds: _day_type_for(d) for ds, d in zip(date_strs, service_dates, strict=True)}

    if sched_by_day_type is None:
        sched_by_day_type = {}
        for dt in set(day_types.values()):
            sched_by_day_type[dt] = fetch_scheduled_cell_hours_for_routes(db, dt, route_ids)

    if observed_rows is None:
        observed_rows = fetch_observed_stop_events_for_window(db, service_dates, route_ids)

    # `{(service_date_str, route_id): {cell_hour: [headways]}}` — pairing is
    # reset every time the (service_date, route, direction, stop) key changes,
    # so per-(date, route) pools never cross day boundaries.
    tz = ZoneInfo(tz_name)
    obs_by_date_route_cell_hour: dict[tuple[str, str], dict[CellHour, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    prev_key: tuple[str, str, int, str] | None = None
    prev_ts: datetime | None = None
    for service_date_str, route_id, direction_id, stop_id, ts, _sr in observed_rows:
        key = (service_date_str, route_id, direction_id, stop_id)
        if prev_key == key and prev_ts is not None:
            delta = (ts - prev_ts).total_seconds()
            if delta > 0:
                obs_by_date_route_cell_hour[(service_date_str, route_id)][
                    (direction_id, stop_id, _hour_in_zone(prev_ts, tz))
                ].append(delta)
        prev_key = key
        prev_ts = ts

    results: dict[str, dict[str, dict]] = {ds: {} for ds in date_strs}
    for service_date_str in date_strs:
        day_type = day_types[service_date_str]
        sched_by_route_cell_hour = sched_by_day_type.get(day_type, {})
        all_routes = set(sched_by_route_cell_hour.keys())
        if route_ids is not None:
            all_routes &= set(route_ids)
        for route_id in all_routes:
            sched_cells = sched_by_route_cell_hour.get(route_id, {})
            obs_cells = obs_by_date_route_cell_hour.get((service_date_str, route_id), {})
            gate_sec = get_cell_hour_gate_sec(route_id)
            obs_pool: list[float] = []
            sched_pool: list[float] = []
            freq_cells = 0
            for cell_hour, sched_headways in sched_cells.items():
                if not _is_cell_hour_frequent(sched_headways, gate_sec):
                    continue
                sched_pool.extend(sched_headways)
                obs_pool.extend(obs_cells.get(cell_hour, []))
                freq_cells += 1
            results[service_date_str][route_id] = _ewt_headline_from_pools(
                route_id, service_date_str, day_type, obs_pool, sched_pool, freq_cells
            )
    return results
