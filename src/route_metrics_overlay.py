"""
Per-(route, service_date) sufficient-statistics materialization for the
scorecard window endpoint.

Wraps the per-date EWT / bunching / service-delivered / OTP compute
functions and persists their sufficient statistics — not finalized
metrics — into `route_metrics_daily_overlay`. The API's windowed compute
reads from this table and applies the metric formulas at read time, so a
future change to the EWT formula, the OTP window, or the bunching
threshold is a Python edit, not a table migration. See the model
docstring for the design rationale.

Failure semantics match `src/system_metrics.py`: a compute error returns
None and prints a single error line; the daily-batch wrapper treats that
as a soft failure (it logs, sets a non-zero exit, but does not block
other dates).
"""

from datetime import date as date_type

from sqlalchemy.orm import Session

from src.bunching import compute_bunching_headline_for_routes_multi_date
from src.ewt import (
    _day_type_for,
    compute_ewt_headline_for_routes_multi_date,
    fetch_observed_stop_events_for_window,
    fetch_scheduled_cell_hours_for_routes,
)
from src.models import RouteMetricsDailyOverlay
from src.otp_metrics import compute_otp_split_for_routes
from src.service_delivered import compute_service_delivered_for_routes
from src.timezones import utcnow_naive


def compute_route_metrics_overlay_for_date(db: Session, service_date: date_type) -> list[dict]:
    """Compute per-route sufficient statistics for one service_date.

    Returns one dict per route that has any data on the date (scheduled
    service or observed runs). Each dict is shaped to map directly onto
    `RouteMetricsDailyOverlay` columns, plus a `route_id` key. Routes with
    no presence in any of the four sources are not emitted — they'd
    contribute null/zero rows to the overlay without changing the
    aggregator's output.
    """
    day_type = _day_type_for(service_date)
    service_date_iso = service_date.isoformat()

    # Fetch scheduled cell-hours once for this day_type (module-level cache
    # hits on the second call). EWT and bunching share it.
    sched_by_day_type = {day_type: fetch_scheduled_cell_hours_for_routes(db, day_type)}

    # Shared observed-stop_events pull (EWT + bunching consume it).
    observed_rows = fetch_observed_stop_events_for_window(db, [service_date])

    ewt_by_date = compute_ewt_headline_for_routes_multi_date(
        db,
        [service_date],
        sched_by_day_type=sched_by_day_type,
        observed_rows=observed_rows,
    )
    bunching_by_date = compute_bunching_headline_for_routes_multi_date(
        db,
        [service_date],
        sched_by_day_type=sched_by_day_type,
        observed_rows=observed_rows,
    )
    sd_by_route = {r["route_id"]: r for r in compute_service_delivered_for_routes(db, service_date)}
    otp_by_route = {r["route_id"]: r for r in compute_otp_split_for_routes(db, service_date)}

    ewt_by_route = ewt_by_date.get(service_date_iso, {})
    bunching_by_route = bunching_by_date.get(service_date_iso, {})

    # Union of routes that appear in any source — same set the window
    # aggregator would see for this date.
    all_routes = set(ewt_by_route) | set(bunching_by_route) | set(sd_by_route) | set(otp_by_route)

    rows: list[dict] = []
    for route_id in sorted(all_routes):
        otp = otp_by_route.get(route_id) or {}
        sd = sd_by_route.get(route_id) or {}
        ewt = ewt_by_route.get(route_id) or {}
        bun = bunching_by_route.get(route_id) or {}

        # OTP sub-blocks are dicts; missing-data is signalled by `n == 0`,
        # in which case `early`/`on_time`/`late` aren't present. Default
        # to 0 — sufficient stats sum across rows and 0+0=0 is the right
        # identity for "no data."
        origin = otp.get("origin") or {}
        destination = otp.get("destination") or {}
        all_block = otp.get("all_timepoints") or {}

        rows.append(
            {
                "route_id": route_id,
                "service_date": service_date_iso,
                "day_type": day_type,
                "otp_origin_early": origin.get("early", 0),
                "otp_origin_on_time": origin.get("on_time", 0),
                "otp_origin_late": origin.get("late", 0),
                "otp_destination_early": destination.get("early", 0),
                "otp_destination_on_time": destination.get("on_time", 0),
                "otp_destination_late": destination.get("late", 0),
                "otp_all_early": all_block.get("early", 0),
                "otp_all_on_time": all_block.get("on_time", 0),
                "otp_all_late": all_block.get("late", 0),
                "scheduled_trips": sd.get("scheduled_trips", 0),
                "delivered_trips": sd.get("delivered_trips", 0),
                "ewt_obs_sum_h": ewt.get("obs_sum_h", 0.0) or 0.0,
                "ewt_obs_sum_h_sq": ewt.get("obs_sum_h_sq", 0.0) or 0.0,
                "ewt_n_observed_headways": ewt.get("n_observed_headways", 0) or 0,
                "ewt_sched_sum_h": ewt.get("sched_sum_h", 0.0) or 0.0,
                "ewt_sched_sum_h_sq": ewt.get("sched_sum_h_sq", 0.0) or 0.0,
                "ewt_n_scheduled_headways": ewt.get("n_scheduled_headways", 0) or 0,
                "bunching_count": bun.get("bunching_count", 0) or 0,
                "bunching_total_headways": bun.get("total_headways", 0) or 0,
            }
        )
    return rows


def upsert_route_metrics_for_date(db: Session, service_date: date_type) -> int | None:
    """Compute and upsert overlay rows for every route active on `service_date`.

    Idempotent: re-runs against the same date replace the prior rows in
    place. Returns the number of rows written, or None if computation
    raised (matching `upsert_system_metrics_for_date`'s soft-fail contract
    so the daily-batch wrapper can log and continue).

    The completeness guard acts as a *flagger*, not a *gate*: partial days
    are persisted with ``data_quality='partial'`` and their raw
    ``coverage_pct`` so the UI can render an explicit "partial day" badge
    rather than showing a silent gap in the trend strip. Complete days
    receive ``data_quality='complete'``.
    """
    from src.data_completeness import (
        coverage_pct_for_date,
        is_date_sufficiently_complete,
    )

    pct = coverage_pct_for_date(db, service_date)
    is_complete = is_date_sufficiently_complete(db, service_date)
    data_quality = "complete" if is_complete else "partial"

    if not is_complete:
        print(
            f"  ⚠ Route metrics overlay for {service_date.isoformat()}: "
            f"ingest coverage {pct:.1%} below threshold — flagging as partial"
        )

    try:
        rows = compute_route_metrics_overlay_for_date(db, service_date)
    except Exception as exc:
        print(f"  ✗ Route metrics overlay compute failed for {service_date.isoformat()}: {exc}")
        return None

    service_date_iso = service_date.isoformat()
    existing_by_route = {
        row.route_id: row
        for row in db.query(RouteMetricsDailyOverlay)
        .filter(RouteMetricsDailyOverlay.service_date == service_date_iso)
        .all()
    }

    now = utcnow_naive()
    for r in rows:
        existing = existing_by_route.get(r["route_id"])
        if existing is not None:
            for key, value in r.items():
                if key in ("route_id", "service_date"):
                    continue
                setattr(existing, key, value)
            existing.data_quality = data_quality
            existing.coverage_pct = pct
            existing.computed_at = now
        else:
            db.add(
                RouteMetricsDailyOverlay(
                    **r,
                    data_quality=data_quality,
                    coverage_pct=pct,
                    computed_at=now,
                )
            )
    db.commit()

    quality_label = "partial" if not is_complete else "complete"
    print(f"  ✓ Route metrics overlay for {service_date_iso} [{quality_label}]: {len(rows)} rows")
    return len(rows)
