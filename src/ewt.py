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

  - Scheduled: `stop_times` joined to `trips`, filtered to the service_ids
    resolved for the EXACT `service_date` passed in (see "Calendar
    resolution and calendar_dates" below for exactly how that set is
    computed). GTFS `arrival_time` is parsed to seconds before sorting
    (string MIN/MAX is broken on WMATA's unpadded single-digit hours,
    e.g. `"10:00:07" < "9:58:27"` lexicographically).

Aggregation to time_period
--------------------------
Within each time_period (AM Peak, Midday, PM Peak, Evening, Night — all
Eastern), pool every frequent cell-hour's headways and compute AWT and SWT
once over the pooled lists. This is rider-weighted by construction: cells
with more arrivals contribute more headways. Per-cell AWTs are never
averaged together — averaging AWTs is wrong; only the pooled formula gives
the correct rider-weighted aggregate.

Calendar resolution and calendar_dates (NOTES-106, NOTES-109)
---------------------------------------------------------------
The scheduled side pools GTFS `stop_times` for the service_ids resolved
against the EXACT `service_date` passed in — no day_type/modal layer.
Resolution is `_resolve_service_ids_for_date`, a thin memoizing wrapper
around `src.gtfs_calendar.scheduled_service_ids_for_date` (the same
per-date GTFS-spec rule `service_delivered.py` already uses for its own
per-date resolution, NOTES-51). A Friday resolves against its own
service_id even when it carries no `tuesday=1` flag (WMATA splits weekday
service across a Mon/Tue/Thu-shaped service_id and a separate Wed-only or
Fri-only one — see the NOTES-51 note in `service_delivered.py`), and a
federal holiday resolves against its real `calendar_dates` substitution
instead of a representative weekday's schedule. `day_type`
(`_day_type_for`) still appears in every returned row, but purely as
descriptive metadata (which of the three UI buckets this date falls
into) — it plays no role in which schedule gets pooled.

This is the SECOND fix to how the scheduled pool resolves the service_id
set, and the third rejected design before it (this history matters for
anyone tempted to "simplify" back toward one of the rejected shapes):
  1. (Original, rejected) Union every `calendar_dates` type=1 addition
     matching the day_type's weekday **across the whole feed** and
     subtract every matching type=2 removal, also feed-wide. Bug: a
     single `exception_type=2` row anywhere in the feed evicted a
     service_id from the ENTIRE pool (self-reverting on the very next
     holiday), and an agency whose `calendar_dates`-only day_type spans
     more than one schedule-revision era (SFMTA: service_id `78968`
     covers 7/23-8/14, `82660` covers 8/17-8/28) got both eras blended
     into one schedule.
  2. (Rejected) Pick ONE deterministic date — the most recent
     matching-weekday date the snapshot had ANY evidence for — and
     resolve only that date. Bug, found on real `wmata_dashboard` data:
     anchoring on the feed's most recent date targets the terminal edge
     of the schedule, which is exactly where agencies stack
     schedule-transition `calendar_dates` exceptions. The anchor would
     land on an exception-substitute service_id instead of the stable
     base service — silently changing WMATA's output (663
     frequent-cell-hour flips on the weekday pool alone).
  3. (Shipped in PR #191, NOTES-106) MODAL RESOLUTION
     (`_resolve_service_ids_for_day_type`, still used elsewhere — see
     below): sample every date in the feed's validity window matching
     the day_type's single representative weekday, resolve each
     independently, and take the most common (modal) result. Reproduced
     WMATA's pre-fix output exactly and resolved SFMTA to its
     majority schedule-revision era — but a genuinely majority era can
     still be the "wrong" (superseded) one when a later era is more
     current, and modal resolution is reload-order-sensitive: one more
     week of feed can flip which era is modal, so a historical re-derive
     done today and the same re-derive done after the next GTFS reload
     aren't guaranteed to agree.
  4. (Shipped here, NOTES-109) Exact per-date resolution — no day_type
     sampling, no modal vote, no representative-day fiction at all.
     Fridays get Friday's real schedule, federal holidays get the
     holiday's real substituted schedule, and every schedule-revision
     era resolves on its own exact dates with no majority/minority
     ambiguity — each date always resolves the same way regardless of
     what the rest of a window looks like.

`_resolve_service_ids_for_day_type` and its modal-resolution machinery
are NOT removed by this change — they remain the resolver for
`service_profile.py`'s route-level day_type classification and
`service_level.py`'s day_type-shaped comparison-page stat, neither of
which is "EWT/bunching for one exact service_date." Widening NOTES-109's
fix to those would be an unreviewed scope change to a different metric.

Known limitations (deferred)
----------------------------
  - `schedule_relationship='ADDED'` trips (real-time-only additions) aren't
    in the scheduled denominator since they aren't in GTFS. Rare, accepted.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date as date_type
from datetime import datetime, timedelta
from threading import Lock
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.frequent_routes import DEFAULT_GATE_SEC, get_cell_hour_gate_sec
from src.gtfs_calendar import scheduled_service_ids_for_date
from src.gtfs_versioning import gtfs_version_filter
from src.models import Calendar, CalendarDate, GTFSSnapshot, Route, StopEvent, StopTime, Trip
from src.time_periods import is_hour_in_period

logger = logging.getLogger(__name__)

# GTFS route_type for bus service. Used to restrict cross-route schedule
# pools to bus-only when a feed is mode-mixed -- SFMTA's carries 7 Muni
# Metro light-rail routes (route_type 0) and 3 cable-car routes
# (route_type 5) alongside its 58 bus routes, while WMATA's feed is
# verified 100% route_type 3 (the bus-only comparison filtering, PR #201).
BUS_ROUTE_TYPE = "3"


def bus_route_ids(db: Session, gtfs_snapshot_id: int | None = None) -> set[str]:
    """Route IDs for bus routes (GTFS route_type=3) in this database.

    Used to post-filter the (module-cached) output of
    `fetch_scheduled_cell_hours_for_date` (NOTES-109) down to a bus-only
    pool for the system-level EWT/SWT/bunching rollup
    (`api.aggregations._system_ewt_and_bunching_for_date`), and of the
    day_type-keyed `fetch_scheduled_cell_hours_for_routes` for the
    comparison page's service-level tile
    (`src.service_level.service_level_for_agency`) — see the bus-only
    comparison filtering (PR #201).

    Deliberately a separate query rather than a SQL-level filter inside
    either fetch function: their module-level caches are keyed only by
    `(db_identity, service_date_or_day_type, snapshot_id)` — not by route
    mode — so filtering post-fetch keeps the cache correct for their
    other (unfiltered) callers.

    Args:
        db: SQLAlchemy session.
        gtfs_snapshot_id: Selects the route set from that historical
            snapshot via `gtfs_version_filter` instead of the live
            `is_current` snapshot. Must match whatever snapshot pin the
            caller already used to fetch the schedule pool being
            filtered — `_system_ewt_and_bunching_for_date` backfills
            against a historical snapshot, and intersecting its schedule
            pool against the *current* route set would silently drop
            routes retired since that snapshot. `service_level_for_agency`
            always wants the live snapshot, so it passes nothing.
    """
    return {
        route_id
        for (route_id,) in db.query(Route.route_id)
        .filter(gtfs_version_filter(Route, gtfs_snapshot_id), Route.route_type == BUS_ROUTE_TYPE)
        .all()
    }


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

# Calendar column name -> Python `date.weekday()` index (Mon=0..Sun=6).
_CALENDAR_FIELD_TO_WEEKDAY_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

# The single representative weekday (as a `date.weekday()` index) sampled
# for each day_type, derived from `DAY_TYPE_REPRESENTATIVE_FIELD` above so
# there's one source of truth for the convention. Deliberately ONE weekday
# per day_type, not "any Mon-Fri" — WMATA splits weekday service across
# multiple service_ids (Mon/Tue/Thu vs a separate Wed-only or Fri-only
# service_id; see the NOTES-51 note in `service_delivered.py`), so
# sampling every weekday date would mix distinct services into the same
# modal vote.
_DAY_TYPE_WEEKDAY_INDEX: dict[str, int] = {
    day_type: _CALENDAR_FIELD_TO_WEEKDAY_INDEX[field_name]
    for day_type, field_name in DAY_TYPE_REPRESENTATIVE_FIELD.items()
}


def _try_parse_yyyymmdd(value: str | None, *, context: str) -> date_type | None:
    """Parse a 'YYYYMMDD' string; return `None` (and print a warning) on
    `None` input or a malformed string, instead of raising — one bad date
    column from a feed shouldn't take down EWT/bunching resolution for the
    whole agency.
    """
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        print(f"[ewt] skipping malformed date value={value!r} ({context})")
        return None


def _feed_validity_window(
    db: Session, gtfs_snapshot_id: int | None = None
) -> tuple[date_type, date_type] | None:
    """Return `(earliest, latest)` date the snapshot has ANY `calendar` or
    `calendar_dates` evidence for — the union of every `calendar` row's
    `[start_date, end_date]` range and every `calendar_dates.date`.
    `None` when the snapshot has no calendar data at all.

    GTFS date strings are fixed-width 'YYYYMMDD' (unlike GTFS time
    strings, which are NOT zero-padded — see `_parse_gtfs_time_to_seconds`)
    so SQL `MIN`/`MAX` on the raw string columns is safe and avoids
    pulling every row into Python just to find the extremes.
    """
    cal_start = (
        db.query(func.min(Calendar.start_date))
        .filter(gtfs_version_filter(Calendar, gtfs_snapshot_id))
        .scalar()
    )
    cal_end = (
        db.query(func.max(Calendar.end_date))
        .filter(gtfs_version_filter(Calendar, gtfs_snapshot_id))
        .scalar()
    )
    cd_min = (
        db.query(func.min(CalendarDate.date))
        .filter(gtfs_version_filter(CalendarDate, gtfs_snapshot_id))
        .scalar()
    )
    cd_max = (
        db.query(func.max(CalendarDate.date))
        .filter(gtfs_version_filter(CalendarDate, gtfs_snapshot_id))
        .scalar()
    )

    starts = [
        d
        for d in (
            _try_parse_yyyymmdd(cal_start, context="calendar.start_date"),
            _try_parse_yyyymmdd(cd_min, context="calendar_dates.date"),
        )
        if d is not None
    ]
    ends = [
        d
        for d in (
            _try_parse_yyyymmdd(cal_end, context="calendar.end_date"),
            _try_parse_yyyymmdd(cd_max, context="calendar_dates.date"),
        )
        if d is not None
    ]
    if not starts or not ends:
        return None
    start, end = min(starts), max(ends)
    if start > end:
        return None
    return start, end


def _dates_matching_weekday(
    start: date_type, end: date_type, weekday_index: int
) -> list[date_type]:
    """Every date in `[start, end]` (inclusive) whose `date.weekday()`
    equals `weekday_index` — i.e. every Tuesday (or Saturday, or Sunday) in
    the range, in ascending order.
    """
    if start > end:
        return []
    offset = (weekday_index - start.weekday()) % 7
    d = start + timedelta(days=offset)
    out: list[date_type] = []
    while d <= end:
        out.append(d)
        d += timedelta(days=7)
    return out


def _db_identity(db: Session) -> str:
    """Best-effort identifier for the database `db` is bound to.

    `gtfs_snapshot_id` is `MAX(GTFSSnapshot.snapshot_id)` scoped to
    whichever `db` session is passed in — a per-database sequence, not a
    globally unique id (NOTES-108). Namespacing the module-level schedule
    caches below by this identity, alongside `(day_type, snapshot_id)`,
    means two different databases (`wmata_dashboard` vs `sfmta_dashboard`,
    or any two databases whose `snapshot_id` sequences happen to overlap)
    can never collide, even if a single long-lived process (e.g. a future
    API comparison endpoint) ends up holding sessions against both.

    Renders the bind's URL with the password hidden — safe to use as a
    plain dict key and safe to hold in process memory. Falls through to
    `Connection.engine.url` for sessions bound to a `Connection` rather
    than an `Engine` (e.g. the test suite's transactional fixtures, which
    bind `sessionmaker` to a connection so each test's writes roll back).
    Falls through to `"unknown"` if no bind/url is resolvable at all
    (including `UnboundExecutionError` from `get_bind()` on a session
    with no single bind) — never observed in practice, but a cache key
    helper must not raise.
    """
    try:
        bind = db.get_bind()
    except Exception:
        return "unknown"
    url = getattr(bind, "url", None)
    if url is None:
        url = getattr(getattr(bind, "engine", None), "url", None)
    if url is None:
        return "unknown"
    return url.render_as_string(hide_password=True)


# Module-level memo for the per-day_type modal service_id resolution.
# Keyed `(db_identity, day_type, snapshot_id)` with the SAME
# resolve-then-evict semantics `_schedule_cache` below uses (mirrored
# exactly, not reinvented): `gtfs_snapshot_id=None` resolves to the
# concrete current snapshot id via `MAX(GTFSSnapshot.snapshot_id)` BEFORE
# keying, so the cache naturally invalidates the moment
# `reload_gtfs_complete.py` writes a new `gtfs_snapshots` row — storing a
# fresh entry evicts every OTHER entry for the SAME database keyed to a
# different snapshot_id (entries for other databases are left alone —
# see `_db_identity` above; NOTES-108). Explicit historical
# `gtfs_snapshot_id` values (backfill) cache under their own id and never
# need invalidating.
#
# Needed because `_scheduled_headways_by_cell_hour` (the per-route path,
# called once per route per pass — ~128 routes on WMATA) invokes
# `_resolve_service_ids_for_day_type` fresh every time; unlike
# `fetch_scheduled_cell_hours_for_routes`'s vectorized path, there's no
# other caching upstream of it. Modal resolution costs ~4 window queries
# + 3 queries per sampled representative-weekday date (~12/day_type on
# WMATA's typical window) — unmemoized, that's ~40 queries × ~128 routes
# ≈ 5,100 extra round-trips per day_type per pass, which multiplies badly
# over the SSH tunnel (NOTES-88).
_service_id_resolution_cache: dict[tuple[str, str, int], frozenset[str]] = {}
_service_id_resolution_cache_lock = Lock()


def _resolve_service_ids_for_day_type(
    db: Session, day_type: str, gtfs_snapshot_id: int | None = None
) -> set[str]:
    """Resolve `day_type`'s representative service_id set by MODAL
    RESOLUTION (NOTES-106, second review follow-up — see the module
    docstring's "Calendar resolution and calendar_dates" section for the
    full rationale and the two rejected earlier designs).

    Samples every date in the feed's validity window (`_feed_validity_window`)
    whose weekday matches `day_type`'s single representative weekday
    (`_DAY_TYPE_WEEKDAY_INDEX`), resolves EACH sampled date independently
    via the exact GTFS-spec per-date rule `service_delivered.py` uses —
    `src.gtfs_calendar.scheduled_service_ids_for_date` — and returns the
    MOST COMMON (modal) resolved set. Dates that resolve to an empty set
    are skipped when computing the mode; if every sampled date is empty,
    the result stays empty. Ties break by preferring the set whose most
    recent contributing date is later (a deterministic, data-driven
    tiebreak rather than an arbitrary one).

    Memoized at module level by `(db_identity, day_type, resolved
    snapshot_id)` — see `_service_id_resolution_cache` above for the exact
    invalidation semantics (mirrors `_schedule_cache`) and `_db_identity`
    for why the database component is required (NOTES-108).

    Shared by `_scheduled_headways_by_cell_hour` and
    `fetch_scheduled_cell_hours_for_routes` so bunching (which imports both
    from this module) gets the fix for free rather than duplicating the
    query.
    """
    if gtfs_snapshot_id is not None:
        snapshot_id = gtfs_snapshot_id
    else:
        snapshot_id = db.query(func.max(GTFSSnapshot.snapshot_id)).scalar() or 0
    db_identity = _db_identity(db)
    cache_key = (db_identity, day_type, snapshot_id)
    with _service_id_resolution_cache_lock:
        cached = _service_id_resolution_cache.get(cache_key)
    if cached is not None:
        return set(cached)

    window = _feed_validity_window(db, gtfs_snapshot_id)
    if window is None:
        result: frozenset[str] = frozenset()
    else:
        start, end = window
        weekday_index = _DAY_TYPE_WEEKDAY_INDEX[day_type]
        candidate_dates = _dates_matching_weekday(start, end, weekday_index)

        counts: dict[frozenset[str], int] = defaultdict(int)
        most_recent: dict[frozenset[str], date_type] = {}
        for d in candidate_dates:
            ids = scheduled_service_ids_for_date(db, d, gtfs_snapshot_id)
            if not ids:
                continue
            key = frozenset(ids)
            counts[key] += 1
            if key not in most_recent or d > most_recent[key]:
                most_recent[key] = d

        result = max(counts, key=lambda k: (counts[k], most_recent[k])) if counts else frozenset()

    with _service_id_resolution_cache_lock:
        _service_id_resolution_cache[cache_key] = result
        # Evict this SAME database's entries from older/other GTFS
        # snapshots so the cache doesn't accumulate every historical
        # version — same eviction rule `_schedule_cache` uses below.
        # Scoped to `db_identity` so storing a fresh entry never evicts
        # another database's cached entries (NOTES-108). Trade-off: a
        # database never queried again keeps its entries for the process
        # lifetime — bounded (one small frozenset per day_type per db)
        # and accepted.
        for k in list(_service_id_resolution_cache.keys()):
            if k[0] == db_identity and k[2] != snapshot_id:
                del _service_id_resolution_cache[k]
    return set(result)


# Module-level memo for the per-EXACT-DATE service_id resolution used by
# EWT/bunching's scheduled pool (NOTES-109 — the deferred fourth option
# from NOTES-106, now shipped). Keyed `(db_identity, service_date_iso,
# resolved snapshot_id)` with the SAME resolve-then-evict semantics as
# `_service_id_resolution_cache` above (mirrored, not reinvented) — see
# that cache's comment and `_db_identity` for the full invalidation
# rationale (NOTES-108).
#
# This is a SEPARATE cache from `_service_id_resolution_cache`, not a
# replacement for it: `_resolve_service_ids_for_day_type`'s modal
# resolver stays in place for its other callers
# (`service_profile.py`'s route-level day_type classification and
# `service_level.py`'s day_type-shaped comparison-page stat) — neither of
# those is "EWT/bunching for one exact service_date," so widening this
# fix to them would be an unreviewed scope change to a different metric.
_service_id_resolution_cache_by_date: dict[tuple[str, str, int], frozenset[str]] = {}
_service_id_resolution_cache_by_date_lock = Lock()


def _resolve_service_ids_for_date(
    db: Session, service_date: date_type, gtfs_snapshot_id: int | None = None
) -> set[str]:
    """Resolve the service_id set for the EXACT `service_date` (NOTES-109).

    Thin memoizing wrapper around
    `src.gtfs_calendar.scheduled_service_ids_for_date` — the same per-date
    GTFS-spec rule `service_delivered.py` already uses for its own
    per-date resolution (NOTES-51). No day_type/modal layer: a Friday
    resolves against its own service_id even when that service_id has no
    `tuesday=1` flag (invisible to `_resolve_service_ids_for_day_type`'s
    weekday sampling — see the NOTES-51 note in `service_delivered.py`
    for why WMATA splits weekday service this way), and a federal holiday
    resolves against its real `calendar_dates` substitution instead of
    "whatever the modal Tuesday looks like."

    Memoized at module level by `(db_identity, service_date, resolved
    snapshot_id)` — mirrors `_resolve_service_ids_for_day_type`'s cache
    exactly (see `_service_id_resolution_cache` above and `_db_identity`
    for the invalidation rationale, NOTES-108). Needed for the same
    reason: the per-route path (`_scheduled_headways_by_cell_hour_for_date`)
    calls this fresh once per route per pass.

    Out-of-window fallback (PR #233 review finding 1): an exact
    `service_date` that falls entirely outside the CURRENT feed's
    validity window (`_feed_validity_window` — the union of every
    `calendar` row's `[start_date, end_date]` range and every
    `calendar_dates.date`) has no calendar coverage at all, so the exact
    rule above always resolves it to an empty set — silently, with no
    signal to the caller. This bites real production dates: WMATA's
    `stop_events` history starts 2026-05-02 while a live feed's validity
    window commonly starts weeks later (e.g. 2026-06-21), so an unpinned
    historical re-derive over that gap would NULL out EWT/bunching for
    every pre-window date. When the exact rule resolves EMPTY *and*
    `service_date` is outside the feed's validity window, fall back to
    `_resolve_service_ids_for_day_type`'s modal resolution for that
    date's `day_type` and log a warning — better a representative-day
    approximation (the same one EWT/bunching used before NOTES-109) than
    a silently empty pool for a date the current feed simply doesn't
    cover.

    An in-window date that genuinely resolves empty (a real no-service
    day — e.g. a holiday `calendar_dates` suspension with no substitute
    service_id, validated against EXP's Independence Day suspension in
    the PR body) must NOT fall back: staying empty in that case is
    NOTES-109's entire point, so the fallback is gated strictly on
    "outside the window," never on "empty."
    """
    if gtfs_snapshot_id is not None:
        snapshot_id = gtfs_snapshot_id
    else:
        snapshot_id = db.query(func.max(GTFSSnapshot.snapshot_id)).scalar() or 0
    db_identity = _db_identity(db)
    service_date_iso = service_date.isoformat()
    cache_key = (db_identity, service_date_iso, snapshot_id)
    with _service_id_resolution_cache_by_date_lock:
        cached = _service_id_resolution_cache_by_date.get(cache_key)
    if cached is not None:
        return set(cached)

    result = frozenset(scheduled_service_ids_for_date(db, service_date, gtfs_snapshot_id))

    if not result:
        window = _feed_validity_window(db, gtfs_snapshot_id)
        if window is not None:
            window_start, window_end = window
            if service_date < window_start or service_date > window_end:
                day_type = _day_type_for(service_date)
                fallback = _resolve_service_ids_for_day_type(db, day_type, gtfs_snapshot_id)
                if fallback:
                    logger.warning(
                        "service_date %s falls outside the current feed's validity "
                        "window (%s..%s) -- the date predates or postdates this "
                        "feed's calendar coverage. Falling back to modal day_type "
                        "resolution for %r (%s) instead of an empty scheduled pool.",
                        service_date_iso,
                        window_start.isoformat(),
                        window_end.isoformat(),
                        day_type,
                        sorted(fallback),
                    )
                    result = frozenset(fallback)

    with _service_id_resolution_cache_by_date_lock:
        _service_id_resolution_cache_by_date[cache_key] = result
        # Evict this SAME database's entries from older/other GTFS
        # snapshots — same eviction rule `_service_id_resolution_cache`
        # uses. Scoped to `db_identity` so storing a fresh entry never
        # evicts another database's cached entries (NOTES-108).
        for k in list(_service_id_resolution_cache_by_date.keys()):
            if k[0] == db_identity and k[2] != snapshot_id:
                del _service_id_resolution_cache_by_date[k]
    return set(result)


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
    `_resolve_service_ids_for_day_type` (NOTES-106): modal resolution over
    every matching-weekday date in the feed's validity window, each
    resolved independently via the same per-date GTFS rule
    `service_delivered.py` uses. Handles a Muni/SFMTA-shaped feed (no
    weekday `calendar` rows at all — weekday service exists purely as
    `calendar_dates` additions) the same as a WMATA-shaped one.
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
    return _bucket_scheduled_headways_by_cell(rows)


def _bucket_scheduled_headways_by_cell(
    rows: list[tuple[int, str, str | None]],
) -> dict[CellHour, list[float]]:
    """Shared cell-hour bucketing for a `(direction_id, stop_id,
    arrival_time)` row list — factored out of `_scheduled_headways_by_cell_hour`
    so `_scheduled_headways_by_cell_hour_for_date` (NOTES-109) doesn't
    duplicate the sort/pair/bucket logic. See
    `_scheduled_headways_by_cell_hour` for the bucketing convention.
    """
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


def _scheduled_headways_by_cell_hour_for_date(
    db: Session, route_id: str, service_date: date_type
) -> dict[CellHour, list[float]]:
    """Compute scheduled headways per (direction, stop, hour) cell for the
    EXACT `service_date` (NOTES-109) — the per-date replacement for
    `_scheduled_headways_by_cell_hour`'s day_type/modal layer.

    Same shape and bucketing convention as `_scheduled_headways_by_cell_hour`,
    but resolves the service_id set via `_resolve_service_ids_for_date`
    (the literal GTFS-spec per-date rule) instead of
    `_resolve_service_ids_for_day_type`'s modal weekday sampling — no
    representative-day fiction at all. A Friday sees its own Friday
    service_id; a federal holiday sees its own `calendar_dates`
    substitution.
    """
    service_ids = _resolve_service_ids_for_date(db, service_date)
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
    return _bucket_scheduled_headways_by_cell(rows)


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
    agency: str = "wmata",
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

    `agency` (PR #242 review finding 5) selects the cell-hour gate tier via
    `src/frequent_routes.py:get_cell_hour_gate_sec` — a non-wmata agency
    always gets the default 15-min gate, never WMATA's medium-freq 20-min
    tier for a same-numbered route_id. Defaults to `"wmata"` so every
    existing caller (the daily batch, none of which pass this yet) keeps
    today's behavior unchanged.

    The scheduled side resolves against the EXACT `service_date`
    (NOTES-109 — `_scheduled_headways_by_cell_hour_for_date`), not a
    day_type/modal representative day; `day_type` in the returned rows is
    descriptive metadata only (see `_day_type_for`).
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    sched_by_cell_hour = _scheduled_headways_by_cell_hour_for_date(db, route_id, service_date)
    obs_by_cell_hour = _observed_headways_by_cell_hour(db, route_id, service_date_str, tz_name)
    gate_sec = get_cell_hour_gate_sec(route_id, agency)

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
    agency: str = "wmata",
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

    The scheduled side resolves against the EXACT `service_date`
    (NOTES-109 — `_scheduled_headways_by_cell_hour_for_date`), not a
    day_type/modal representative day; the returned `day_type` is
    descriptive metadata only.

    `agency` (PR #242 review finding 5) selects the cell-hour gate tier —
    see `compute_ewt_for_route_date`'s docstring. This is the live-request
    path behind `/api/routes/{id}` and the scorecard, so a real, non-default
    `agency` reaches here whenever `_compute_single_route_live_metrics` /
    `_compute_live_metrics_uncached` forward the caller's actual agency.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    sched_by_cell_hour = _scheduled_headways_by_cell_hour_for_date(db, route_id, service_date)
    obs_by_cell_hour = _observed_headways_by_cell_hour(db, route_id, service_date_str, tz_name)
    gate_sec = get_cell_hour_gate_sec(route_id, agency)

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
# Keying by `(db_identity, day_type, snapshot_id)` means the cache naturally
# invalidates the moment `reload_gtfs_complete.py` writes a new
# gtfs_snapshots row — no TTL or restart needed — and the `db_identity`
# component (see `_db_identity`) means two different databases can never
# collide even when their `snapshot_id` sequences happen to overlap
# (NOTES-108).
#
# Only the unfiltered (`route_ids is None`) path is cached. The filtered
# path is uncommon and could legitimately collide with a cached entry's key
# space without proper isolation.
_schedule_cache: dict[tuple[str, str, int], dict[str, dict[CellHour, list[float]]]] = {}
_schedule_cache_lock = Lock()


def fetch_scheduled_cell_hours_for_routes(
    db: Session,
    day_type: str,
    route_ids: list[str] | None = None,
    gtfs_snapshot_id: int | None = None,
) -> dict[str, dict[CellHour, list[float]]]:
    """Vectorized scheduled-headway-per-(direction, stop, hour) for every route.

    Resolves the representative-day service_id set once (see
    `_resolve_service_ids_for_day_type`), then a single SQL pass joins
    `trips` and `stop_times` filtered to that service_id set — no `calendar`
    join here; the calendar/calendar_dates resolution happens entirely
    inside the resolver. Returns
    `{route_id: {(direction_id, stop_id, hour): [scheduled_headway_sec, ...]}}`
    — each list is consecutive scheduled headways within that cell, bucketed
    by the earlier arrival's hour-of-day.

    Cached at module level by `(db_identity, day_type, gtfs_snapshot_id)`
    when called with `route_ids=None` (the dashboard path) — see
    `_db_identity` for why the database component is required (NOTES-108).
    The cost is ~1.5s for the full SQL pass + Python pairing; the cache
    invalidates automatically when `reload_gtfs_complete.py` writes a new
    `gtfs_snapshots` row, so no manual flush is needed after a GTFS
    refresh.

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
        db_identity = _db_identity(db)
        cache_key = (db_identity, day_type, snapshot_id)
        with _schedule_cache_lock:
            cached = _schedule_cache.get(cache_key)
        if cached is not None:
            return cached

    service_ids = _resolve_service_ids_for_day_type(db, day_type, gtfs_snapshot_id)
    sched_by_route_cell_hour = _fetch_and_bucket_scheduled_cells(
        db, service_ids, route_ids, gtfs_snapshot_id
    )

    if route_ids is None:
        # Stash the unfiltered result and evict this SAME database's
        # entries from older GTFS snapshots so the cache doesn't
        # accumulate every historical version. Scoped to `db_identity` so
        # storing a fresh entry never evicts another database's cached
        # entries (NOTES-108). Trade-off: a database never queried again
        # keeps its entries for the process lifetime — bounded (at most a
        # few full-schedule payloads per db) and accepted.
        with _schedule_cache_lock:
            _schedule_cache[cache_key] = sched_by_route_cell_hour
            for k in list(_schedule_cache.keys()):
                if k[0] == db_identity and k[2] != snapshot_id:
                    del _schedule_cache[k]
    return sched_by_route_cell_hour


def _fetch_and_bucket_scheduled_cells(
    db: Session,
    service_ids: set[str],
    route_ids: list[str] | None,
    gtfs_snapshot_id: int | None,
) -> dict[str, dict[CellHour, list[float]]]:
    """Shared SQL pass + cell-hour bucketing for a resolved `service_ids`
    set — factored out of `fetch_scheduled_cell_hours_for_routes` so
    `fetch_scheduled_cell_hours_for_date` (NOTES-109) doesn't duplicate
    the query/pairing logic. Both callers only differ in how
    `service_ids` gets resolved (day_type/modal vs. exact-date).
    """
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
    return sched_by_route_cell_hour


# Module-level cache for the per-EXACT-DATE scheduled-cell-hour PAYLOAD
# (NOTES-109) — the per-date replacement for `_schedule_cache` used by
# EWT/bunching specifically. Keyed `(db_identity, frozenset(service_ids),
# resolved snapshot_id)` — the RESOLVED service_id pool, not the literal
# `service_date` (PR #233 review findings 3/4).
#
# Why pool-keyed, not date-keyed: the naive shape (keyed by service_date,
# shipped in the first NOTES-109 cut) paid its own ~4.29s SQL pass +
# bucketing for every distinct date on a cold cache — a 30-date backfill
# cost ~130s serially and retained ~53-55MB per date (~1.6GB for 30
# dates), because each entry holds a full `{route_id: {cell_hour:
# [headways]}}` payload for every bus route. But most dates in a rolling
# window resolve to the SAME service_id pool (an ordinary run of
# weekdays under one `calendar` row, say) — keying by the resolved pool
# instead collapses those dates onto ONE payload entry, restoring
# near-day_type cold cost (~13s) and bounding memory to
# distinct-pools-per-snapshot, comparable to the old day_type cache's
# ≤3-entries rationale.
#
# This is a two-level cache, not one: `_resolve_service_ids_for_date`
# above already memoizes the CHEAP part (date -> resolved
# frozenset(service_ids)) in `_service_id_resolution_cache_by_date`.
# Callers here resolve service_ids for the date FIRST (a cheap in-memory
# lookup after the first hit), then look up the EXPENSIVE payload by the
# resolved pool — so two dates landing on the same pool share the
# expensive fetch even though each keeps its own cheap date->pool memo
# entry. Same resolve-then-evict semantics as `_schedule_cache`
# (mirrored, not reinvented — see that cache's comment and `_db_identity`
# for the full invalidation rationale, NOTES-108).
#
# A SEPARATE cache from `_schedule_cache`, not a replacement: the
# day_type-keyed fetch (`fetch_scheduled_cell_hours_for_routes`) stays in
# place for `service_level.py`'s day_type-shaped comparison-page stat —
# out of NOTES-109's "EWT/bunching" scope.
_schedule_cache_by_service_ids: dict[
    tuple[str, frozenset[str], int], dict[str, dict[CellHour, list[float]]]
] = {}
_schedule_cache_by_service_ids_lock = Lock()


def fetch_scheduled_cell_hours_for_date(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
    gtfs_snapshot_id: int | None = None,
) -> dict[str, dict[CellHour, list[float]]]:
    """Vectorized scheduled-headway-per-(direction, stop, hour) for every
    route, resolved against the EXACT `service_date` (NOTES-109).

    The per-date replacement for `fetch_scheduled_cell_hours_for_routes`'s
    day_type/modal layer, used by EWT/bunching's scheduled pool. Resolves
    the service_id set via `_resolve_service_ids_for_date` — the literal
    GTFS-spec per-date rule, same one `service_delivered.py` and
    `_scheduled_headways_by_cell_hour_for_date` use — instead of modal
    weekday sampling. A Friday gets its own Friday service_id (invisible
    to weekday-Tuesday sampling); a federal holiday gets its own
    `calendar_dates` substitution. Same return shape as
    `fetch_scheduled_cell_hours_for_routes`:
    `{route_id: {(direction_id, stop_id, hour): [scheduled_headway_sec, ...]}}`.

    Cached at module level by `(db_identity, frozenset(service_ids),
    gtfs_snapshot_id)` when called with `route_ids=None` — see
    `_schedule_cache_by_service_ids` above for the exact keying/eviction
    semantics and why the expensive payload is keyed by the RESOLVED
    pool rather than the literal date (PR #233 review findings 3/4):
    most dates in a window share a pool, so this collapses a multi-date
    backfill down to a handful of distinct payload fetches.

    Pass `gtfs_snapshot_id` to pin the schedule to a historical snapshot
    (backfill); the default reads the live `is_current` snapshot.
    """
    if gtfs_snapshot_id is not None:
        snapshot_id = gtfs_snapshot_id
    else:
        snapshot_id = db.query(func.max(GTFSSnapshot.snapshot_id)).scalar() or 0

    # Cheap: hits `_service_id_resolution_cache_by_date` after the first
    # call for this exact date (see that cache's comment). Resolving
    # service_ids BEFORE the expensive-payload cache check is what lets
    # two different dates share one payload fetch below.
    service_ids = _resolve_service_ids_for_date(db, service_date, gtfs_snapshot_id)

    if route_ids is None:
        db_identity = _db_identity(db)
        pool_key = (db_identity, frozenset(service_ids), snapshot_id)
        with _schedule_cache_by_service_ids_lock:
            cached = _schedule_cache_by_service_ids.get(pool_key)
        if cached is not None:
            return cached

    sched_by_route_cell_hour = _fetch_and_bucket_scheduled_cells(
        db, service_ids, route_ids, gtfs_snapshot_id
    )

    if route_ids is None:
        with _schedule_cache_by_service_ids_lock:
            _schedule_cache_by_service_ids[pool_key] = sched_by_route_cell_hour
            # Evict this SAME database's entries from older/other GTFS
            # snapshots so the cache doesn't accumulate every historical
            # version — same eviction rule `_schedule_cache` uses.
            # Scoped to `db_identity` so storing a fresh entry never
            # evicts another database's cached entries (NOTES-108).
            for k in list(_schedule_cache_by_service_ids.keys()):
                if k[0] == db_identity and k[2] != snapshot_id:
                    del _schedule_cache_by_service_ids[k]
    return sched_by_route_cell_hour


def compute_ewt_headline_for_routes(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
    sched_by_route_cell_hour: dict[str, dict[CellHour, list[float]]] | None = None,
    tz_name: str = "America/New_York",
    agency: str = "wmata",
) -> dict[str, dict]:
    """Vectorized headline EWT for all routes — two SQL passes, no per-route loop.

    `agency` (PR #242 review finding 5) selects the cell-hour gate tier per
    route via `src/frequent_routes.py:get_cell_hour_gate_sec` — see
    `compute_ewt_for_route_date`'s docstring. Defaults to `"wmata"`.

    Pulls all scheduled stop_times (joined to trips, filtered to the
    EXACT `service_date`'s resolved service_id set — see
    `_resolve_service_ids_for_date`, NOTES-109) and all observed
    `stop_events` on the date in one query each, then groups by (route,
    direction, stop) in Python and aggregates per route.

    Pass `sched_by_route_cell_hour` to skip the scheduled fetch — used by the
    scorecard path to share scheduled data with bunching.

    `tz_name` (NOTES-103 multi-agency) buckets the observed side by the
    agency's own local hour; defaults to Eastern.

    Returns `{route_id: headline_dict}`. Routes with no scheduled service on
    the date don't appear; routes with scheduled service but no observed
    arrivals appear with `awt_seconds=None`. Pass `route_ids` to restrict.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    if sched_by_route_cell_hour is None:
        sched_by_route_cell_hour = fetch_scheduled_cell_hours_for_date(db, service_date, route_ids)

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
        gate_sec = get_cell_hour_gate_sec(route_id, agency)
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
    sched_by_date: dict[str, dict[str, dict[CellHour, list[float]]]] | None = None,
    route_ids: list[str] | None = None,
    observed_rows: list[tuple] | None = None,
    tz_name: str = "America/New_York",
    agency: str = "wmata",
) -> dict[str, dict[str, dict]]:
    """Multi-date headline EWT — one SQL pull for the whole window.

    `agency` (PR #242 review finding 5) selects the cell-hour gate tier per
    route via `src/frequent_routes.py:get_cell_hour_gate_sec` — see
    `compute_ewt_for_route_date`'s docstring. Defaults to `"wmata"`.

    Equivalent to calling `compute_ewt_headline_for_routes` once per date in
    `service_dates`, but collapses the per-date observed-stop_events queries
    into a single SQL pass using `service_date IN (...)`. Returns
    `{service_date_str: {route_id: headline_dict}}` — each per-date inner
    dict is identical to what the single-date function returns for that day.

    Pass `observed_rows` to skip the observed fetch — used by the windowed
    scorecard path so EWT and bunching share one pull. Pass `sched_by_date`
    (keyed by `service_date.isoformat()`) to share schedule fetches the
    same way. Both are auto-fetched when None.

    Each distinct date resolves its own EXACT scheduled pool via
    `fetch_scheduled_cell_hours_for_date` (NOTES-109) — no day_type/modal
    layer, so a window spanning a Friday or a holiday sees that date's
    real schedule. The expensive payload fetch itself is cached by the
    RESOLVED service_id pool rather than the literal date (PR #233
    review findings 3/4 — see `_schedule_cache_by_service_ids`'s
    docstring), so a window with N distinct dates pays a schedule fetch
    per distinct POOL on a cache miss, not per date — most windows
    collapse onto a handful of pools even though every date still
    resolves its own exact schedule.

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

    if sched_by_date is None:
        sched_by_date = {}
        for ds, d in zip(date_strs, service_dates, strict=True):
            if ds not in sched_by_date:
                sched_by_date[ds] = fetch_scheduled_cell_hours_for_date(db, d, route_ids)

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
        sched_by_route_cell_hour = sched_by_date.get(service_date_str, {})
        all_routes = set(sched_by_route_cell_hour.keys())
        if route_ids is not None:
            all_routes &= set(route_ids)
        for route_id in all_routes:
            sched_cells = sched_by_route_cell_hour.get(route_id, {})
            obs_cells = obs_by_date_route_cell_hour.get((service_date_str, route_id), {})
            gate_sec = get_cell_hour_gate_sec(route_id, agency)
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
