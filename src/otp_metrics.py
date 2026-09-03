"""
OTP (on-time performance) metrics computed from the stop_events / runs foundation.

Computes OTP at three levels — origin, destination, and all-timepoints —
for one (route, service_date), returning per-source aggregates.

Origin / destination split (PR #46) requires picking a source per endpoint
because the two derivation sources have nearly inverse blind spots:
  - TripUpdate: ~0% literal-origin coverage (WMATA's TU feed only contains
    trips after the AVL system marks them "active" — typically operator-log-in
    at or after origin departure — and past stops are pruned, so origin's
    StopTimeUpdate is gone by the time the trip first appears).
  - Proximity: 0-5% literal-destination coverage (layover bays are typically
    >50m from the published last-stop point, and ~60s position polling lets
    buses pass and dwell at the last stop without an in-window ping).

So origin OTP reads `proximity` runs and destination OTP reads `trip_update`
runs. See the Run model docstring for the full source-asymmetry write-up.

All-timepoints OTP uses `proximity` stop_events (position-derived, every
observed stop) — comparable to what WMATA publishes. `compute_otp_split`
takes an optional `early_sec`/`late_sec` on-time window (NOTES-144,
NOTES-20) so callers can switch between the official WMATA scorecard
window and the stricter rider-experience window without a second code
path; EWT for frequent routes (see `src/ewt.py`) layers on the same
per-stop deviation data separately.
"""

from __future__ import annotations

from datetime import date as date_type
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from src.ewt import _hour_in_zone
from src.models import Run, StopEvent
from src.otp_constants import OTP_EARLY_SEC, OTP_LATE_SEC
from src.time_periods import ALL_HOURS, is_hour_in_period

UTC = ZoneInfo("UTC")


def _eastern_hour(ts: datetime | None, tz_name: str = "America/New_York") -> int | None:
    """Return the local hour-of-day for a naive-UTC timestamp, or None.

    Convenience wrapper for single-call (non-loop) use — constructs a
    fresh ``ZoneInfo(tz_name)`` per call. Hot loops should call
    `_local_hour_or_none` directly with a ``ZoneInfo`` built once outside
    the loop (see `compute_otp_split`).

    Stop_event and Run timestamps are naive UTC by storage convention
    (timezones.py). We re-attach UTC, convert to `tz_name`, take the hour.
    `zoneinfo` handles DST correctly. Returns None if `ts` is None — the
    caller decides whether a missing timestamp means "exclude" (when
    filtering) or "include" (when no filter set).

    `tz_name` (NOTES-103 multi-agency) defaults to Eastern so every existing
    WMATA call site is unaffected. See `src.ewt._eastern_hour` — the two are
    identical implementations (modulo the None-passthrough) kept as separate
    module-private functions.
    """
    if ts is None:
        return None
    return _hour_in_zone(ts, ZoneInfo(tz_name))


def _local_hour_or_none(ts: datetime | None, tz: ZoneInfo) -> int | None:
    """Return the local hour-of-day in a pre-built zone, or None for a None ts.

    Loop-friendly sibling of `_eastern_hour` — takes an already-constructed
    ``ZoneInfo`` so a hot loop (one call per row) hoists the
    ``ZoneInfo(tz_name)`` construction once instead of paying it per row.
    """
    if ts is None:
        return None
    return _hour_in_zone(ts, tz)


def _aggregate_deviations(
    devs: list[int], early_sec: int = OTP_EARLY_SEC, late_sec: int = OTP_LATE_SEC
) -> dict:
    """Bucket a list of deviation_sec values; return counts + percentages.

    Returns `{"n": 0}` for empty input — caller distinguishes "no data"
    from "data exists but 0% on-time." `early_sec` / `late_sec` (NOTES-144)
    default to the official WMATA window.
    """
    if not devs:
        return {"n": 0}
    early = sum(1 for d in devs if d < early_sec)
    late = sum(1 for d in devs if d > late_sec)
    on_time = len(devs) - early - late
    n = len(devs)
    return {
        "n": n,
        "early": early,
        "on_time": on_time,
        "late": late,
        "early_pct": round(early * 100 / n, 2),
        "on_time_pct": round(on_time * 100 / n, 2),
        "late_pct": round(late * 100 / n, 2),
    }


def compute_otp_split(
    db: Session,
    route_id: str,
    service_date: date_type,
    period_key: str = ALL_HOURS,
    tz_name: str = "America/New_York",
    early_sec: int = OTP_EARLY_SEC,
    late_sec: int = OTP_LATE_SEC,
) -> dict:
    """Compute origin / destination / all-timepoints OTP for one (route, date).

    The three sub-blocks each name the source they used so consumers don't
    have to know the source-asymmetry rules to interpret the numbers.
    All sub-blocks return `{"n": 0}` (no other keys) when no data exists,
    distinguishing absence from a real 0% on-time.

    `period_key` (NOTES-41) restricts which observed timestamps contribute:
      - origin: filter by local hour of the run's `first_obs_ts`
      - destination: filter by local hour of the run's `last_obs_ts`
      - all_timepoints: filter by local hour of `stop_events.observed_arrival_ts`
    Default `all` keeps every hour. Filtering happens in Python after the
    fetch to keep test parity with SQLite (production Postgres could push
    the predicate via `to_eastern_sql`, but the deviation lists are short
    enough that the round-trip cost dominates either way).

    `tz_name` (NOTES-103 multi-agency) is the local zone the hour bucketing
    above uses; defaults to Eastern. Only exercised when `period_key !=
    ALL_HOURS` — the unfiltered default path (what
    `system_metrics_daily.otp_percentage` / `service_delivered_ratio`
    actually consume) never calls the hour helper at all, so it's
    unaffected by this parameter either way.

    `early_sec` / `late_sec` (NOTES-144) are the on-time deviation bounds
    applied to all three sub-blocks, defaulting to the official WMATA
    window. Callers that support the rider-experience window resolve the
    pair via `src.otp_constants.otp_window_bounds` and pass it explicitly;
    `compute_otp_split_for_routes` (the scorecard batch) leaves these at
    their default and is unaffected.
    """
    service_date_str = service_date.isoformat()
    no_filter = period_key == ALL_HOURS
    # Constructed once per call (not per row) — see `_local_hour_or_none`.
    # Cheap even when `no_filter` is True and it goes unused.
    tz = ZoneInfo(tz_name)

    # Origin: proximity runs only (TU has 0% origin coverage by design).
    # Pull `first_obs_ts` alongside dev_sec so we can apply the period filter
    # in Python; the database stores it for free, the cost is one extra column.
    origin_rows = (
        db.query(Run.origin_dev_sec, Run.first_obs_ts)
        .filter(
            Run.route_id == route_id,
            Run.service_date == service_date_str,
            Run.source == "proximity",
            Run.origin_dev_sec.isnot(None),
        )
        .all()
    )
    if no_filter:
        origin_devs = [d for d, _ts in origin_rows]
    else:
        origin_devs = [
            d
            for d, ts in origin_rows
            if (h := _local_hour_or_none(ts, tz)) is not None and is_hour_in_period(h, period_key)
        ]

    # Destination: trip_update runs only (proximity has ~1% destination coverage).
    # Bucket by `last_obs_ts` — the run's destination observation timestamp.
    destination_rows = (
        db.query(Run.destination_dev_sec, Run.last_obs_ts)
        .filter(
            Run.route_id == route_id,
            Run.service_date == service_date_str,
            Run.source == "trip_update",
            Run.destination_dev_sec.isnot(None),
        )
        .all()
    )
    if no_filter:
        destination_devs = [d for d, _ts in destination_rows]
    else:
        destination_devs = [
            d
            for d, ts in destination_rows
            if (h := _local_hour_or_none(ts, tz)) is not None and is_hour_in_period(h, period_key)
        ]

    # All timepoints: proximity stop_events directly (position-derived,
    # every observed stop). Bucket each stop event by its own
    # `observed_arrival_ts`.
    all_rows = (
        db.query(StopEvent.deviation_sec, StopEvent.observed_arrival_ts)
        .filter(
            StopEvent.route_id == route_id,
            StopEvent.service_date == service_date_str,
            StopEvent.source == "proximity",
            StopEvent.deviation_sec.isnot(None),
        )
        .all()
    )
    if no_filter:
        all_devs = [d for d, _ts in all_rows]
    else:
        all_devs = [
            d
            for d, ts in all_rows
            if (h := _local_hour_or_none(ts, tz)) is not None and is_hour_in_period(h, period_key)
        ]

    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "window": {"early_sec": early_sec, "late_sec": late_sec},
        "origin": {
            "source": "proximity",
            **_aggregate_deviations(origin_devs, early_sec=early_sec, late_sec=late_sec),
        },
        "destination": {
            "source": "trip_update",
            **_aggregate_deviations(destination_devs, early_sec=early_sec, late_sec=late_sec),
        },
        "all_timepoints": {
            "source": "proximity",
            **_aggregate_deviations(all_devs, early_sec=early_sec, late_sec=late_sec),
        },
    }


def compute_otp_split_for_routes(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
    tz_name: str = "America/New_York",
) -> list[dict]:
    """Compute the OTP split for every route with stop_events on `service_date`.

    Pass `route_ids` to restrict; default scans all routes that have any
    proximity stop_events on the day. Returns one dict per route, sorted
    by route_id.

    `tz_name` (NOTES-103 multi-agency) is forwarded to `compute_otp_split`
    for every route; defaults to Eastern.
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
    return [compute_otp_split(db, r, service_date, tz_name=tz_name) for r in route_ids]
