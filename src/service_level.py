"""
Schedule-derived daytime service-level stats for the agency comparison
page (NOTES-115).

The comparison page's other KPIs measure performance *against* the
schedule; these stats surface the schedule itself — the "promise" term.
Computed from the current GTFS weekday schedule over the daytime window,
trip-weighted via per-route-direction reference stops (see
`compute_service_level_stats`). Random-incidence SWT is not valid for
infrequent service (riders time their arrivals), so this module reports
headway distribution stats instead — see the design spec
`docs/superpowers/specs/2026-08-12-swt-service-level-kpi-design.md`.
"""

from statistics import median

from sqlalchemy.orm import Session

# Daytime window, agency-local clock hours: [start, end) — matches the
# NOTES-115 motivating measurement (weekday 7:00–19:00).
DAYTIME_HOUR_START = 7
DAYTIME_HOUR_END = 19

# Share threshold: fraction of scheduled service at ≤ 15-minute headways.
FREQUENT_SHARE_THRESHOLD_SEC = 900.0


def compute_service_level_stats(
    sched_by_route: dict,
    *,
    hour_start: int = DAYTIME_HOUR_START,
    hour_end: int = DAYTIME_HOUR_END,
    threshold_sec: float = FREQUENT_SHARE_THRESHOLD_SEC,
) -> dict:
    """Trip-weighted daytime headway stats over a scheduled-cell-hour map.

    Args:
        sched_by_route: `{route_id: {(direction_id, stop_id, hour):
            [headway_sec, ...]}}` — the return shape of
            `src.ewt.fetch_scheduled_cell_hours_for_routes`.
        hour_start / hour_end: half-open agency-local hour window
            (GTFS clock hours; the cell key's hour component).
        threshold_sec: headway cutoff for the `pct_at_most_15min` share.

    Weighting: for each (route, direction), only the *reference stop* —
    the stop with the most headway samples inside the window — is pooled,
    so a route-direction's weight is proportional to its scheduled trips
    (TfL-style frequency weighting) rather than its stop count, and
    route-equal averaging (the NYCT Wait Assessment flaw) is avoided.
    Ties on sample count are broken deterministically by the higher
    `stop_id` (string comparison) — the upstream SQL has no ORDER BY, so
    "first-seen" would not be stable across processes.

    Returns:
        Dict with `median_headway_seconds` (float | None),
        `pct_at_most_15min` (0–1 fraction, float | None), and
        `n_headways` (int). Nulls / 0 when no samples survive the window.
    """
    pool: list[float] = []
    for cells in sched_by_route.values():
        # Gather daytime samples per (direction, stop).
        by_dir_stop: dict[tuple, list[float]] = {}
        for (direction_id, stop_id, hour), headways in cells.items():
            if not (hour_start <= hour < hour_end):
                continue
            by_dir_stop.setdefault((direction_id, stop_id), []).extend(headways)
        # Reference stop per direction: max daytime sample count, ties
        # broken by the higher stop_id for a deterministic result that
        # doesn't depend on dict/SQL iteration order.
        best_by_direction: dict = {}
        for (direction_id, stop_id), samples in by_dir_stop.items():
            current = best_by_direction.get(direction_id)
            if current is None or (len(samples), stop_id) > (
                len(current[1]),
                current[0],
            ):
                best_by_direction[direction_id] = (stop_id, samples)
        for _stop_id, samples in best_by_direction.values():
            pool.extend(samples)

    if not pool:
        return {"median_headway_seconds": None, "pct_at_most_15min": None, "n_headways": 0}

    return {
        "median_headway_seconds": round(median(pool), 1),
        "pct_at_most_15min": round(sum(1 for h in pool if h <= threshold_sec) / len(pool), 4),
        "n_headways": len(pool),
    }


def service_level_for_agency(db: Session) -> dict:
    """Bus-only service-level stats for one agency from its current weekday GTFS.

    Thin wrapper over `fetch_scheduled_cell_hours_for_routes(db,
    "weekday")` (module-cached; inherits the NOTES-106 day-type resolver,
    so SFMTA's calendar_dates-only weekday service resolves correctly),
    filtered to `route_type=3` (bus) via `src.ewt.bus_route_ids` before
    pooling — the bus-only comparison filtering (PR #201): SFMTA's feed
    also carries Muni Metro light rail and cable car routes, and pooling
    those in would skew the tile away from a bus-to-bus comparison.
    WMATA's feed is verified 100% route_type 3, so the filter is a no-op
    there.

    Raises whatever the schedule fetch or route lookup raises — the
    comparison endpoint catches and degrades to a null block.
    """
    from src.ewt import bus_route_ids, fetch_scheduled_cell_hours_for_routes

    sched = fetch_scheduled_cell_hours_for_routes(db, "weekday")
    bus_ids = bus_route_ids(db)
    bus_sched = {route_id: cells for route_id, cells in sched.items() if route_id in bus_ids}
    return compute_service_level_stats(bus_sched)
