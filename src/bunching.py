"""
Bus bunching rate — the rider-experience tail that headway CV averages away.

Per (route, service_date, time_period), aggregated over every (direction,
stop, hour) cell on the route:

  - count "bunched" observed headway pairs:
        observed_headway < max(0.25 × cell-hour mean scheduled, 120s)
  - bunching_rate = bunched_count / total_observed_pairs

A bunched pair implies a long gap behind it — tight headways are inseparable
from service-gap formation. Reporting this alongside the existing CV at the
route's "most active stop" (`src/analytics.py`) exposes the bad tail that
the average smears out.

Threshold rationale
-------------------
The 0.25× scheduled-headway threshold matches CTA's published Key Routes
methodology (transitchicago.com/performance/). The 120s absolute floor
matches SFMTA Service Gaps and TransitMatters' MBTA work — it keeps the
threshold sensible at high frequency, where 0.25× of a short scheduled
headway is under two minutes (a 5-min route's 0.25× is 75s, but two buses
75s apart is functionally indistinguishable from two buses 119s apart, and
both are bunched in lived experience). Taking the max of the two thresholds
collapses CTA's ratio rule and SFMTA's absolute rule into a single
condition.

The TRB review (Yu et al. 2024) reports thresholds spanning 20s up to ⅓ of
scheduled, plus fixed values like 30s and 1 minute. The 0.25× + 120s floor
sits in the well-supported middle. WMATA itself does not publish a
bunching threshold (their headway-based Service Excellence metric is the
gap side: % timepoint pull-outs ≤ scheduled + 3 min).

No frequent-route gating
------------------------
Unlike EWT (which is only well-defined under the rider-doesn't-consult-schedule
assumption that fails below ~15-min headways), bunching is reported across
all routes. CTA's *intervention program* gates to Key Routes because
holding-based control works there operationally; the *measurement* is
meaningful everywhere — a 6-min observed gap on a 30-min route is a real
failure mode (long gap behind it) even if the agency wouldn't intervene
operationally.

Headway pairing and filters
---------------------------
Observed pairs come from `stop_events` with source='trip_update',
schedule_relationship='SCHEDULED', and observed_arrival_ts not null —
matching EWT's primary-derivation source choice and additionally excluding
ADDED real-time-only trips. An ADDED bus slotting between two scheduled
ones produces a tight observed pair that isn't operationally bunching.

Pairs with gap > 120 min are dropped (service breaks, not headways) — the
same threshold the positions-based CV calc in `src/analytics.py` uses.

Per-cell scheduled headway is the cell-hour mean from
`_scheduled_headways_by_cell_hour` in `src/ewt.py` — same source and
bucketing as EWT, so the bunching-vs-EWT comparison stays apples-to-apples.

Bucketing
---------
Each observed headway is attributed to the **earlier** arrival's Eastern
hour, matching EWT's convention. Hours map to time_period via
`EWT_TIME_PERIODS` (AM Peak / Midday / PM Peak / Evening / Night).

Direction grouping
------------------
Headways are computed within `(direction_id, stop_id)` cells. Termini and
shared hubs serve both directions under one `stop_id` and would silently
double-count without the direction split (CLAUDE.md gotcha).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date as date_type
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from src.ewt import (
    EWT_TIME_PERIODS,
    _day_type_for,
    _period_for_hour,
    _scheduled_headways_by_cell_hour,
    fetch_scheduled_cell_hours_for_routes,
)
from src.models import StopEvent
from src.otp_constants import OTP_EARLY_SEC, OTP_LATE_SEC
from src.time_periods import (
    ALL_DAY_TYPES,
    ALL_HOURS,
    is_hour_in_period,
)

UTC = ZoneInfo("UTC")
EASTERN = ZoneInfo("America/New_York")

# A pair is bunched when observed_headway < max(BUNCHING_RATIO × scheduled,
# BUNCHING_ABSOLUTE_FLOOR_SEC). See module docstring for citation trail.
BUNCHING_RATIO = 0.25
BUNCHING_ABSOLUTE_FLOOR_SEC = 120.0

# Drop observed pairs above this — they're service breaks (depot pull-out,
# overnight quiet), not headways. Matches the filter in
# `src/analytics.py:calculate_headways_batch`.
MAX_OBSERVED_HEADWAY_SEC = 120 * 60

CellHour = tuple[int, str, int]  # (direction_id, stop_id, eastern_hour)


def _eastern_hour(ts: datetime) -> int:
    """Return the Eastern hour-of-day for a naive-UTC stop_event timestamp."""
    return ts.replace(tzinfo=UTC).astimezone(EASTERN).hour


def _scheduled_observed_headways_by_cell_hour(
    db: Session, route_id: str, service_date_str: str
) -> dict[CellHour, list[float]]:
    """Observed-arrival headways per (direction, stop, hour) cell.

    Filters more strictly than EWT's helper: schedule_relationship='SCHEDULED'
    excludes ADDED real-time-only trips, which are service supplementation
    rather than bunching when they slot between two scheduled buses.

    Returns `{(direction, stop, eastern_hour): [headway_sec, ...]}` — bucketed
    by the **earlier** arrival's Eastern hour, matching EWT.
    """
    rows = (
        db.query(StopEvent.direction_id, StopEvent.stop_id, StopEvent.observed_arrival_ts)
        .filter(
            StopEvent.route_id == route_id,
            StopEvent.service_date == service_date_str,
            StopEvent.source == "trip_update",
            StopEvent.schedule_relationship == "SCHEDULED",
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


def _cell_hour_threshold_sec(scheduled_headways: list[float]) -> float | None:
    """Bunching threshold for a cell-hour, in seconds. None when undefined.

    Threshold is `max(BUNCHING_RATIO × mean(scheduled), BUNCHING_ABSOLUTE_FLOOR_SEC)`.
    Returns None for cell-hours with no scheduled headways — there's nothing
    to compare against.
    """
    if not scheduled_headways:
        return None
    mean_sched = sum(scheduled_headways) / len(scheduled_headways)
    return max(BUNCHING_RATIO * mean_sched, BUNCHING_ABSOLUTE_FLOOR_SEC)


def compute_bunching_for_route_date(
    db: Session,
    route_id: str,
    service_date: date_type,
) -> list[dict]:
    """Compute bunching for one (route, service_date), one row per time_period.

    Each row: `{route_id, service_date, day_type, time_period, bunching_count,
    total_headways, bunching_rate}`. `bunching_rate` is `None` when
    `total_headways == 0` (no eligible observed/scheduled pairs in the
    period). All five time_periods are emitted; callers can filter
    `total_headways > 0` to drop empty rows.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    sched_by_cell_hour = _scheduled_headways_by_cell_hour(db, route_id, day_type)
    obs_by_cell_hour = _scheduled_observed_headways_by_cell_hour(db, route_id, service_date_str)

    bunched_by_period: dict[str, int] = defaultdict(int)
    total_by_period: dict[str, int] = defaultdict(int)

    for cell_hour, observed in obs_by_cell_hour.items():
        threshold = _cell_hour_threshold_sec(sched_by_cell_hour.get(cell_hour, []))
        if threshold is None:
            continue
        _direction, _stop, hour = cell_hour
        period = _period_for_hour(hour)
        for headway in observed:
            if headway > MAX_OBSERVED_HEADWAY_SEC:
                continue
            total_by_period[period] += 1
            if headway < threshold:
                bunched_by_period[period] += 1

    rows: list[dict] = []
    for label, _, _ in EWT_TIME_PERIODS:
        total = total_by_period.get(label, 0)
        bunched = bunched_by_period.get(label, 0)
        rate = (bunched / total) if total > 0 else None
        rows.append(
            {
                "route_id": route_id,
                "service_date": service_date_str,
                "day_type": day_type,
                "time_period": label,
                "bunching_count": bunched,
                "total_headways": total,
                "bunching_rate": round(rate, 4) if rate is not None else None,
            }
        )
    return rows


def compute_bunching_for_routes(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
) -> list[dict]:
    """Compute bunching for every route seen in stop_events on the date.

    Pass `route_ids` to restrict. Returns one dict per (route, time_period),
    sorted by route_id then by `EWT_TIME_PERIODS` order. Routes with no
    eligible pairs in any period still emit five placeholder rows
    (total_headways=0, bunching_rate=None) so callers can distinguish
    "evaluated, no data" from "not evaluated."
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
        out.extend(compute_bunching_for_route_date(db, r, service_date))
    return out


def _bunching_headline_from_counts(
    route_id: str,
    service_date_str: str,
    day_type: str,
    bunched: int,
    total: int,
) -> dict:
    """Build the headline result dict from already-summed bunched/total counts.

    Shared by `compute_bunching_headline_for_route` and the vectorized
    `compute_bunching_headline_for_routes` so both produce identical output.
    """
    rate = (bunched / total) if total > 0 else None
    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "day_type": day_type,
        "bunching_count": bunched,
        "total_headways": total,
        "bunching_rate": round(rate, 4) if rate is not None else None,
    }


def compute_bunching_headline_for_route(
    db: Session,
    route_id: str,
    service_date: date_type,
    period_key: str = "all",
) -> dict:
    """Single-route bunching collapsed to one rate for the day.

    Sums bunched pairs and total observed pairs across every cell-hour with a
    defined threshold; rate = bunched / total. Mathematically clean — the
    per-period variant just buckets the same counts and reports the ratio per
    bucket, so summing over all buckets is the natural daily aggregate.

    `period_key` (NOTES-41) restricts which Eastern hours contribute — e.g.
    `pm_peak` keeps only cell-hours in [15, 19). `late` (22-6) wraps
    midnight via `is_hour_in_period`. Default `all` keeps every hour.

    Returns the same dict shape as one period row from `compute_bunching_for_route_date`,
    minus the `time_period` key.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    sched_by_cell_hour = _scheduled_headways_by_cell_hour(db, route_id, day_type)
    obs_by_cell_hour = _scheduled_observed_headways_by_cell_hour(db, route_id, service_date_str)

    bunched = 0
    total = 0
    for cell_hour, observed in obs_by_cell_hour.items():
        threshold = _cell_hour_threshold_sec(sched_by_cell_hour.get(cell_hour, []))
        if threshold is None:
            continue
        _direction, _stop, hour = cell_hour
        if not is_hour_in_period(hour, period_key):
            continue
        for headway in observed:
            if headway > MAX_OBSERVED_HEADWAY_SEC:
                continue
            total += 1
            if headway < threshold:
                bunched += 1

    return _bunching_headline_from_counts(route_id, service_date_str, day_type, bunched, total)


def compute_bunching_headline_for_routes(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
    sched_by_route_cell_hour: dict[str, dict[CellHour, list[float]]] | None = None,
) -> dict[str, dict]:
    """Vectorized headline bunching for all routes — two SQL passes, no per-route loop.

    Mirrors `compute_ewt_headline_for_routes` but with bunching's stricter
    observed filter: `schedule_relationship='SCHEDULED'` excludes ADDED
    real-time-only trips, which are service supplementation rather than
    bunching when they slot between two scheduled buses.

    Pass `sched_by_route_cell_hour` to skip the scheduled fetch — used by the
    scorecard path to share scheduled data with EWT.

    Returns `{route_id: headline_dict}`. Pass `route_ids` to restrict.
    """
    service_date_str = service_date.isoformat()
    day_type = _day_type_for(service_date)

    if sched_by_route_cell_hour is None:
        sched_by_route_cell_hour = fetch_scheduled_cell_hours_for_routes(db, day_type, route_ids)

    # All observed stop_events for the date, every route, one query.
    # Stricter filter than EWT: schedule_relationship='SCHEDULED' only.
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
            StopEvent.schedule_relationship == "SCHEDULED",
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
    all_routes = set(sched_by_route_cell_hour.keys()) | set(obs_by_route_cell_hour.keys())
    if route_ids is not None:
        all_routes &= set(route_ids)

    results: dict[str, dict] = {}
    for route_id in all_routes:
        sched_cells = sched_by_route_cell_hour.get(route_id, {})
        obs_cells = obs_by_route_cell_hour.get(route_id, {})
        bunched = 0
        total = 0
        for cell_hour, observed in obs_cells.items():
            threshold = _cell_hour_threshold_sec(sched_cells.get(cell_hour, []))
            if threshold is None:
                continue
            for headway in observed:
                if headway > MAX_OBSERVED_HEADWAY_SEC:
                    continue
                total += 1
                if headway < threshold:
                    bunched += 1
        results[route_id] = _bunching_headline_from_counts(
            route_id, service_date_str, day_type, bunched, total
        )
    return results


# ---------------------------------------------------------------------------
# Bunching cause decomposition (NOTES-42)
#
# Classical bus-bunching theory (Wikipedia "Bus bunching"; Tandfonline 2024
# review of bus-bunching control strategies) ascribes bunching to a feedback
# loop: a late leader picks up extra passengers (extending dwell, making it
# later) while the trailer encounters fewer passengers and runs early. That
# maps two intervention points:
#
#   - leader-late ⇒ running-time problem (recovery / cycle-time fix)
#   - trailer-early ⇒ dispatch-discipline problem (departure-control fix)
#   - both off ⇒ compounding; both fixes apply
#
# The mapping is textbook. The presentation below — categorizing every
# bunched pair into one of five buckets and reporting the percentage mix —
# is internal dashboard innovation, not a TCRP-named or
# transit-agency-published metric. Frame it accordingly in the UI: useful
# diagnostic, not industry-standard.
#
# Threshold choice: the WMATA OTP window (-2/+7 min) from
# `src/otp_constants.py`. Saved memory `project_otp_window.md` is the
# project standard for "late" / "early" everywhere on the dashboard;
# reusing it keeps the classification coherent with the rest of the
# surface. A symmetric tighter threshold (e.g. ±2 min) would pull more
# pairs into the "off" categories at the cost of inconsistency with OTP.
# ---------------------------------------------------------------------------

# Cause-category labels. These are the keys in the breakdown dict and the
# return values from `classify_bunched_pair`.
CAUSE_LEADER_LATE_ONLY = "leader_late_only"
CAUSE_TRAILER_EARLY_ONLY = "trailer_early_only"
CAUSE_BOTH_OFF = "both_off"
CAUSE_NEITHER_OFF = "neither_off"
CAUSE_UNKNOWN = "unknown"

CAUSE_CATEGORIES: tuple[str, ...] = (
    CAUSE_LEADER_LATE_ONLY,
    CAUSE_TRAILER_EARLY_ONLY,
    CAUSE_BOTH_OFF,
    CAUSE_NEITHER_OFF,
    CAUSE_UNKNOWN,
)


def classify_bunched_pair(
    leader_dev_sec: int | float | None,
    trailer_dev_sec: int | float | None,
) -> str:
    """Categorize one bunched pair by leader/trailer deviation against schedule.

    "Late" means `dev > OTP_LATE_SEC` (+420s, more than 7 minutes behind
    schedule per the WMATA OTP window). "Early" means `dev < OTP_EARLY_SEC`
    (-120s, more than 2 minutes ahead of schedule). The thresholds come from
    `src/otp_constants.py`, which is the project's single source of truth
    for OTP boundaries (saved memory `project_otp_window.md`).

    Returns one of:
      - `leader_late_only`: leader late, trailer not early — recovery /
        running-time problem; the leader fell behind, the trailer is on time
        (or running late itself, just not early), and the gap closed because
        the leader couldn't recover.
      - `trailer_early_only`: leader not late, trailer early — dispatch /
        departure-discipline problem; the leader is on time, the trailer
        rolled out ahead of schedule and caught up.
      - `both_off`: leader late AND trailer early — compounding; both
        interventions apply.
      - `neither_off`: leader on time AND trailer on time — both within
        the WMATA OTP window but the trailer compressed running time
        without crossing the early threshold. Documented but not
        operationally featured; a tighter symmetric threshold would
        re-bucket some of these pairs into the off categories.
      - `unknown`: at least one side has a null deviation_sec — the
        stop_event lacked a scheduled-time match (no GTFS row, post-midnight
        anchor edge cases, or ADDED-trip residue not filtered upstream).

    Pure function — no DB access, easy to test exhaustively.
    """
    if leader_dev_sec is None or trailer_dev_sec is None:
        return CAUSE_UNKNOWN
    leader_late = leader_dev_sec > OTP_LATE_SEC
    trailer_early = trailer_dev_sec < OTP_EARLY_SEC
    if leader_late and trailer_early:
        return CAUSE_BOTH_OFF
    if leader_late:
        return CAUSE_LEADER_LATE_ONLY
    if trailer_early:
        return CAUSE_TRAILER_EARLY_ONLY
    return CAUSE_NEITHER_OFF


def _bunched_pairs_with_deviations(
    db: Session,
    route_id: str,
    start_iso: str,
    end_iso: str,
) -> list[dict]:
    """Yield one record per bunched (leader, trailer) pair across a date window.

    Walks `stop_events` ordered by (service_date, direction_id, stop_id,
    observed_arrival_ts) and emits a record for every consecutive pair
    within the same (date, direction, stop_id) cell whose observed headway
    falls below the cell-hour threshold. Each record carries:

      - `service_date`: pair's service_date (used for day_type filter)
      - `eastern_hour`: leader's Eastern hour (period filter / time-period
        bucketing — matches the EWT/bunching attribution rule)
      - `leader_dev_sec`, `trailer_dev_sec`: schedule deviations for cause
        classification

    Filters mirror `_scheduled_observed_headways_by_cell_hour`:
    `source='trip_update'`, `schedule_relationship='SCHEDULED'`, and
    `observed_arrival_ts IS NOT NULL`. Pairs with gap > 120 min are dropped
    (service breaks).

    The threshold is per-(direction, stop, hour) — same `_cell_hour_threshold_sec`
    formula as `compute_bunching_for_route_date` — so a route's "bunching
    rate" and its "cause breakdown" agree on which pairs are bunched.

    Scheduled headways are pulled per-day_type (using `_day_type_for` on
    each service_date). Caching the per-day_type schedule across the date
    window avoids re-querying GTFS for every day.
    """
    rows = (
        db.query(
            StopEvent.service_date,
            StopEvent.direction_id,
            StopEvent.stop_id,
            StopEvent.observed_arrival_ts,
            StopEvent.deviation_sec,
        )
        .filter(
            StopEvent.route_id == route_id,
            StopEvent.service_date >= start_iso,
            StopEvent.service_date <= end_iso,
            StopEvent.source == "trip_update",
            StopEvent.schedule_relationship == "SCHEDULED",
            StopEvent.observed_arrival_ts.isnot(None),
        )
        .order_by(
            StopEvent.service_date,
            StopEvent.direction_id,
            StopEvent.stop_id,
            StopEvent.observed_arrival_ts,
        )
        .all()
    )

    # Cache the per-day_type scheduled cell-hour map so the repeated calls
    # across a multi-day window don't re-query GTFS for every day. Day_types
    # are at most three values (`weekday` / `saturday` / `sunday`).
    sched_cache: dict[str, dict[CellHour, list[float]]] = {}

    def _sched_for_date(service_date_str: str) -> dict[CellHour, list[float]]:
        """Return cell-hour scheduled headways for the date's day_type, cached."""
        try:
            d = datetime.strptime(service_date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return {}
        day_type = _day_type_for(d)
        cached = sched_cache.get(day_type)
        if cached is None:
            cached = _scheduled_headways_by_cell_hour(db, route_id, day_type)
            sched_cache[day_type] = cached
        return cached

    out: list[dict] = []
    prev_key: tuple[str, int, str] | None = None
    prev_ts: datetime | None = None
    prev_dev: int | None = None
    for service_date, direction_id, stop_id, ts, dev in rows:
        key = (service_date, direction_id, stop_id)
        if prev_key == key and prev_ts is not None:
            delta = (ts - prev_ts).total_seconds()
            if 0 < delta <= MAX_OBSERVED_HEADWAY_SEC:
                hour = _eastern_hour(prev_ts)
                sched = _sched_for_date(service_date)
                threshold = _cell_hour_threshold_sec(sched.get((direction_id, stop_id, hour), []))
                if threshold is not None and delta < threshold:
                    out.append(
                        {
                            "service_date": service_date,
                            "eastern_hour": hour,
                            "leader_dev_sec": prev_dev,
                            "trailer_dev_sec": dev,
                        }
                    )
        prev_key = key
        prev_ts = ts
        prev_dev = dev
    return out


def compute_bunching_cause_breakdown(
    db: Session,
    route_id: str,
    days: int = 30,
    day_type: str = ALL_DAY_TYPES,
    period: str = ALL_HOURS,
) -> dict:
    """Decompose bunched pairs by likely cause over a route-window (NOTES-42).

    For every bunched pair on the route in the past `days` days, classify
    by leader/trailer schedule deviation:

      - `leader_late_only`: running-time / recovery problem
      - `trailer_early_only`: dispatch / departure-discipline problem
      - `both_off`: compounding (both interventions apply)
      - `neither_off`: both within the WMATA OTP window — the trailer
        compressed running time without crossing the early threshold
      - `unknown`: at least one side has no schedule match

    The mechanism is textbook bus-bunching theory (late leaders pick up
    more passengers, extending dwell; trailers run light and catch up).
    The decomposition presented here is internal — not a
    transit-agency-published metric. See module section comment.

    Threshold: the WMATA OTP window (-2/+7 min) from
    `src/otp_constants.py`. See `classify_bunched_pair` for boundaries.

    Args:
        db: SQLAlchemy session.
        route_id: Route identifier (e.g., 'C51').
        days: Window length in days (default 30, end-inclusive on
            today's Eastern service date).
        day_type: One of `all` / `weekday` / `saturday` / `sunday`.
            Filters by `_day_type_for(service_date)`.
        period: One of `all` / `am_peak` / `midday` / `pm_peak` /
            `evening` / `late`. Filters by Eastern hour of the leader's
            observed arrival (matches the EWT/bunching attribution rule).

    Returns:
        Dict with `route_id`, `days`, `day_type`, `period`,
        `n_bunched_pairs`, and `breakdown` — a dict keyed by category
        name with `{"count": int, "pct": float}` values. `pct` is the
        share of bunched pairs in that category (sums to 1.0 across
        all five categories when `n_bunched_pairs > 0`; empty dict
        with zero counts otherwise).
    """
    from src.timezones import eastern_today

    end_date = eastern_today()
    start_date = end_date - timedelta(days=days)
    start_iso = start_date.isoformat()
    end_iso = end_date.isoformat()

    no_day_type_filter = day_type == ALL_DAY_TYPES
    no_period_filter = period == ALL_HOURS

    pairs = _bunched_pairs_with_deviations(db, route_id, start_iso, end_iso)

    counts: dict[str, int] = dict.fromkeys(CAUSE_CATEGORIES, 0)
    n_kept = 0
    for p in pairs:
        # day_type filter — apply against the pair's service_date.
        if not no_day_type_filter:
            try:
                d = datetime.strptime(p["service_date"], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            if _day_type_for(d) != day_type:
                continue
        # period filter — applied to the leader's Eastern hour, matching
        # the EWT/bunching attribution rule (each headway belongs to the
        # earlier arrival's hour bucket).
        if not no_period_filter and not is_hour_in_period(p["eastern_hour"], period):
            continue

        category = classify_bunched_pair(p["leader_dev_sec"], p["trailer_dev_sec"])
        counts[category] += 1
        n_kept += 1

    if n_kept > 0:
        breakdown = {
            c: {"count": counts[c], "pct": round(counts[c] / n_kept, 4)} for c in CAUSE_CATEGORIES
        }
    else:
        breakdown = {c: {"count": 0, "pct": 0.0} for c in CAUSE_CATEGORIES}

    return {
        "route_id": route_id,
        "days": days,
        "day_type": day_type,
        "period": period,
        "n_bunched_pairs": n_kept,
        "breakdown": breakdown,
    }
