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

Known limitations (deferred)
----------------------------
  - `schedule_relationship='ADDED'` trips (real-time-only additions) aren't
    in the scheduled denominator since they aren't in GTFS. Rare, accepted.
  - Holiday awareness (calendar_dates) is not consulted — same caveat as
    `service_delivered.py`. A federal-holiday weekday running Sunday service
    will use the weekday schedule as the SWT comparison.
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
from src.models import Calendar, GTFSSnapshot, StopEvent, StopTime, Trip
from src.time_periods import is_hour_in_period

EASTERN = ZoneInfo("America/New_York")
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


def _day_type_for(service_date: date_type) -> str:
    """Map a service_date to the day_type bucket route_service_profile uses."""
    wd = service_date.weekday()  # Mon=0 .. Sun=6
    if wd == 5:
        return "saturday"
    if wd == 6:
        return "sunday"
    return "weekday"


def _eastern_hour(ts: datetime) -> int:
    """Return the Eastern hour-of-day for a naive-UTC stop_event timestamp.

    Stop_event timestamps are naive UTC by storage convention (timezones.py).
    We re-attach UTC, convert to Eastern, and take the hour. zoneinfo handles
    DST transitions correctly.
    """
    return ts.replace(tzinfo=UTC).astimezone(EASTERN).hour


def _period_for_hour(eastern_hour: int) -> str:
    """Map an Eastern hour-of-day (0..23) to its time_period label."""
    for label, start, end in EWT_TIME_PERIODS:
        if start <= eastern_hour < end:
            return label
    raise ValueError(f"Eastern hour {eastern_hour} out of 0..23 range")


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
    db: Session, route_id: str, service_date_str: str
) -> dict[CellHour, list[float]]:
    """Compute observed headways per (direction, stop, eastern_hour) cell.

    Returns `{(direction, stop, hour): [headway_sec, ...]}` where each
    headway is the gap between two consecutive observed arrivals at the same
    (direction, stop), bucketed by the **earlier** arrival's Eastern hour.
    Source is restricted to `trip_update` (the primary derivation, PR #43)
    so each actual arrival contributes exactly one row.
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

    by_cell_hour: dict[CellHour, list[float]] = defaultdict(list)
    prev_key: tuple[int, str] | None = None
    prev_ts: datetime | None = None
    for direction_id, stop_id, ts in rows:
        key = (direction_id, stop_id)
        if prev_key == key and prev_ts is not None:
            delta = (ts - prev_ts).total_seconds()
            if delta > 0:
                by_cell_hour[(direction_id, stop_id, _eastern_hour(prev_ts))].append(delta)
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
    """
    field_name = DAY_TYPE_REPRESENTATIVE_FIELD[day_type]
    field = getattr(Calendar, field_name)
    rows = (
        db.query(Trip.direction_id, StopTime.stop_id, StopTime.arrival_time)
        .join(StopTime, StopTime.trip_id == Trip.trip_id)
        .join(Calendar, Calendar.service_id == Trip.service_id)
        .filter(
            Trip.route_id == route_id,
            Trip.is_current,
            StopTime.is_current,
            Calendar.is_current,
            field == 1,
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
) -> list[dict]:
    """Compute EWT for one (route, service_date), one row per time_period.

    Each row has keys `route_id, service_date, day_type, time_period,
    awt_seconds, swt_seconds, ewt_seconds, n_observed_headways,
    n_scheduled_headways, frequent_cell_hours`. AWT/SWT/EWT are `None` when
    the corresponding pool is empty. All five time_periods are emitted even
    when the route has no frequent cells in any of them — callers can filter
    by `frequent_cell_hours > 0` to drop the empty rows.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    sched_by_cell_hour = _scheduled_headways_by_cell_hour(db, route_id, day_type)
    obs_by_cell_hour = _observed_headways_by_cell_hour(db, route_id, service_date_str)
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
) -> list[dict]:
    """Compute EWT for every route seen in `stop_events` on the date, or pass
    `route_ids` to restrict. Returns a flat list — one dict per (route,
    time_period) — sorted by route_id then by the canonical time_period order
    in `EWT_TIME_PERIODS`. Routes with no frequent cell-hours produce
    placeholder rows (all metrics None, frequent_cell_hours=0) so callers can
    distinguish "evaluated, not frequent" from "not evaluated."
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
        out.extend(compute_ewt_for_route_date(db, r, service_date))
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
) -> dict:
    """Single-route EWT collapsed to one rider-weighted number for the day.

    Pools every frequent (direction, stop, hour) cell on the route into a single
    observed pool and a single scheduled pool, then computes AWT/SWT/EWT once.
    Mathematically equivalent to "EWT across the whole day for this route at
    every cell where service is actually frequent" — non-frequent cell-hours
    drop out by the same gating used in the per-period variant.

    `period_key` (NOTES-41) restricts which Eastern hours feed the pool —
    e.g. `am_peak` keeps only cell-hours with hour in [6, 10). `late`
    wraps midnight so 22..23 and 0..5 both qualify. Default `all` keeps
    every hour. Note this filters the cell-hour bucket, NOT the originating
    arrival time inside it — but `_eastern_hour` already buckets each
    headway by the earlier arrival's clock hour, so it's the same thing.

    Returns the same dict shape as one period row from `compute_ewt_for_route_date`,
    minus the `time_period` key.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    sched_by_cell_hour = _scheduled_headways_by_cell_hour(db, route_id, day_type)
    obs_by_cell_hour = _observed_headways_by_cell_hour(db, route_id, service_date_str)
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
    """
    if route_ids is None:
        snapshot_id = db.query(func.max(GTFSSnapshot.snapshot_id)).scalar() or 0
        cache_key = (day_type, snapshot_id)
        with _schedule_cache_lock:
            cached = _schedule_cache.get(cache_key)
        if cached is not None:
            return cached

    field_name = DAY_TYPE_REPRESENTATIVE_FIELD[day_type]
    field = getattr(Calendar, field_name)

    sched_q = (
        db.query(
            Trip.route_id,
            Trip.direction_id,
            StopTime.stop_id,
            StopTime.arrival_time,
        )
        .join(StopTime, StopTime.trip_id == Trip.trip_id)
        .join(Calendar, Calendar.service_id == Trip.service_id)
        .filter(
            Trip.is_current,
            StopTime.is_current,
            Calendar.is_current,
            field == 1,
        )
    )
    if route_ids is not None:
        sched_q = sched_q.filter(Trip.route_id.in_(route_ids))

    sched_by_route_cell: dict[tuple[str, int, str], list[int]] = defaultdict(list)
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
) -> dict[str, dict]:
    """Vectorized headline EWT for all routes — two SQL passes, no per-route loop.

    Pulls all scheduled stop_times (joined to trips and calendar for the
    representative weekday) and all observed `stop_events` on the date in one
    query each, then groups by (route, direction, stop) in Python and
    aggregates per route.

    Pass `sched_by_route_cell_hour` to skip the scheduled fetch — used by the
    scorecard path to share scheduled data with bunching.

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
                    (direction_id, stop_id, _eastern_hour(prev_ts))
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
                    (direction_id, stop_id, _eastern_hour(prev_ts))
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
