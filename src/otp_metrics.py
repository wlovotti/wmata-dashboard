"""
OTP (on-time performance) metrics computed from the stop_events / runs foundation.

This module replaces the OTP parts of the legacy `pipelines/compute_daily_metrics.py`
path (which reads vehicle_positions and writes route_metrics_daily). It computes
OTP at three levels — origin, destination, and all-timepoints — for one
(route, service_date), returning per-source aggregates.

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

All-timepoints OTP uses `proximity` stop_events to match the existing
`route_metrics_daily` semantics (position-derived, comparable to what
WMATA publishes). Future variants (rider-experience window per NOTES-20,
EWT for frequent routes — see `src/ewt.py`) layer on the same per-stop
deviation data.
"""

from __future__ import annotations

from datetime import date as date_type

from sqlalchemy.orm import Session

from src.models import Run, StopEvent
from src.otp_constants import OTP_EARLY_SEC, OTP_LATE_SEC


def _bucket_deviation(dev_sec: int) -> str:
    """Classify one deviation_sec into 'early' / 'on_time' / 'late' per OTP constants."""
    if dev_sec < OTP_EARLY_SEC:
        return "early"
    if dev_sec > OTP_LATE_SEC:
        return "late"
    return "on_time"


def _aggregate_deviations(devs: list[int]) -> dict:
    """Bucket a list of deviation_sec values; return counts + percentages.

    Returns `{"n": 0}` for empty input — caller distinguishes "no data"
    from "data exists but 0% on-time."
    """
    if not devs:
        return {"n": 0}
    early = sum(1 for d in devs if d < OTP_EARLY_SEC)
    late = sum(1 for d in devs if d > OTP_LATE_SEC)
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
) -> dict:
    """Compute origin / destination / all-timepoints OTP for one (route, date).

    The three sub-blocks each name the source they used so consumers don't
    have to know the source-asymmetry rules to interpret the numbers.
    All sub-blocks return `{"n": 0}` (no other keys) when no data exists,
    distinguishing absence from a real 0% on-time.
    """
    service_date_str = service_date.isoformat()

    # Origin: proximity runs only (TU has 0% origin coverage by design).
    origin_devs = [
        d
        for (d,) in db.query(Run.origin_dev_sec)
        .filter(
            Run.route_id == route_id,
            Run.service_date == service_date_str,
            Run.source == "proximity",
            Run.origin_dev_sec.isnot(None),
        )
        .all()
    ]

    # Destination: trip_update runs only (proximity has ~1% destination coverage).
    destination_devs = [
        d
        for (d,) in db.query(Run.destination_dev_sec)
        .filter(
            Run.route_id == route_id,
            Run.service_date == service_date_str,
            Run.source == "trip_update",
            Run.destination_dev_sec.isnot(None),
        )
        .all()
    ]

    # All timepoints: proximity stop_events directly (matches existing
    # route_metrics_daily semantics — position-derived, every observed stop).
    all_devs = [
        d
        for (d,) in db.query(StopEvent.deviation_sec)
        .filter(
            StopEvent.route_id == route_id,
            StopEvent.service_date == service_date_str,
            StopEvent.source == "proximity",
            StopEvent.deviation_sec.isnot(None),
        )
        .all()
    ]

    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "window": {"early_sec": OTP_EARLY_SEC, "late_sec": OTP_LATE_SEC},
        "origin": {"source": "proximity", **_aggregate_deviations(origin_devs)},
        "destination": {"source": "trip_update", **_aggregate_deviations(destination_devs)},
        "all_timepoints": {"source": "proximity", **_aggregate_deviations(all_devs)},
    }


def compute_otp_split_for_routes(
    db: Session,
    service_date: date_type,
    route_ids: list[str] | None = None,
) -> list[dict]:
    """Compute the OTP split for every route with stop_events on `service_date`.

    Pass `route_ids` to restrict; default scans all routes that have any
    proximity stop_events on the day. Returns one dict per route, sorted
    by route_id.
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
    return [compute_otp_split(db, r, service_date) for r in route_ids]
