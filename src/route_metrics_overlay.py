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

from src.agency_config import CompletenessPolicy
from src.bunching import compute_bunching_headline_for_routes_multi_date
from src.ewt import (
    _day_type_for,
    compute_ewt_headline_for_routes_multi_date,
    fetch_observed_stop_events_for_window,
    fetch_scheduled_cell_hours_for_date,
)
from src.models import RouteMetricsDailyOverlay
from src.otp_metrics import compute_otp_split_for_routes
from src.service_delivered import compute_service_delivered_for_routes
from src.timezones import utcnow_naive


def compute_route_metrics_overlay_for_date(
    db: Session,
    service_date: date_type,
    gtfs_snapshot_id: int | None = None,
    tz_name: str = "America/New_York",
    agency: str = "wmata",
) -> list[dict]:
    """Compute per-route sufficient statistics for one service_date.

    Returns one dict per route that has any data on the date (scheduled
    service or observed runs). Each dict is shaped to map directly onto
    `RouteMetricsDailyOverlay` columns, plus a `route_id` key. Routes with
    no presence in any of the four sources are not emitted — they'd
    contribute null/zero rows to the overlay without changing the
    aggregator's output.

    `gtfs_snapshot_id` pins the scheduled side (EWT SWT pools,
    service-delivered denominators) to a historical GTFS snapshot when
    backfilling; the default reads the live `is_current` snapshot. OTP and
    bunching counts are observed-side only and unaffected.

    `tz_name` (NOTES-103 multi-agency) buckets the EWT/bunching observed
    side, and the OTP period-filter hour (unused here — the OTP call below
    is always unfiltered), by the agency's own local hour; defaults to
    Eastern.

    The EWT/bunching scheduled side resolves against the EXACT
    `service_date` (NOTES-109, no day_type/modal layer); `day_type`
    (below) is still computed for the output row's descriptive label.

    `agency` (PR #242 round-2 review finding 3) is forwarded to
    `compute_ewt_headline_for_routes_multi_date`'s cell-hour gate lookup —
    the other half of the daily batch alongside
    `src/system_metrics.py:_system_ewt_and_bunching_for_date` (fixed in
    the same PR's round-1 finding 5): without it, a non-wmata route
    materialized into `route_metrics_daily_overlay` could inherit WMATA's
    medium-freq 20-min EWT gate for a same-numbered route_id. Defaults to
    `"wmata"` so existing callers keep today's behavior.
    """
    day_type = _day_type_for(service_date)
    service_date_iso = service_date.isoformat()

    # Fetch scheduled cell-hours once for this exact date (module-level
    # cache hits on the second call). EWT and bunching share it.
    sched_by_date = {
        service_date_iso: fetch_scheduled_cell_hours_for_date(
            db, service_date, gtfs_snapshot_id=gtfs_snapshot_id
        )
    }

    # Shared observed-stop_events pull (EWT + bunching consume it).
    observed_rows = fetch_observed_stop_events_for_window(db, [service_date])

    ewt_by_date = compute_ewt_headline_for_routes_multi_date(
        db,
        [service_date],
        sched_by_date=sched_by_date,
        observed_rows=observed_rows,
        tz_name=tz_name,
        agency=agency,
    )
    bunching_by_date = compute_bunching_headline_for_routes_multi_date(
        db,
        [service_date],
        sched_by_date=sched_by_date,
        observed_rows=observed_rows,
        tz_name=tz_name,
    )
    sd_by_route = {
        r["route_id"]: r
        for r in compute_service_delivered_for_routes(
            db, service_date, gtfs_snapshot_id=gtfs_snapshot_id
        )
    }
    otp_by_route = {
        r["route_id"]: r for r in compute_otp_split_for_routes(db, service_date, tz_name=tz_name)
    }

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


def overlay_required_sources(rows) -> set[str]:
    """Stop-level sources the observed counts in these overlay rows were computed from.

    Origin and all-timepoints OTP come from ``proximity`` runs/stop_events,
    destination OTP / EWT / bunching from ``trip_update``, delivered trips
    from ``runs``. ``scheduled_trips`` and the ``ewt_sched_*`` sums are
    schedule-only and require nothing.
    """
    required: set[str] = set()
    for row in rows:
        if (
            (row.otp_origin_early or 0)
            + (row.otp_origin_on_time or 0)
            + (row.otp_origin_late or 0)
            + (row.otp_all_early or 0)
            + (row.otp_all_on_time or 0)
            + (row.otp_all_late or 0)
        ):
            required.add("proximity")
        if (
            (row.otp_destination_early or 0)
            + (row.otp_destination_on_time or 0)
            + (row.otp_destination_late or 0)
            + (row.ewt_n_observed_headways or 0)
            + (row.bunching_total_headways or 0)
        ):
            required.add("trip_update")
        if row.delivered_trips:
            required.add("runs")
    return required


def upsert_route_metrics_for_date(
    db: Session,
    service_date: date_type,
    gtfs_snapshot_id: int | None = None,
    tz_name: str = "America/New_York",
    completeness_threshold: float | None = None,
    agency: str = "wmata",
    force: bool = False,
    gap_policy: CompletenessPolicy | None = None,
) -> int | None:
    """Compute and upsert overlay rows for every route active on `service_date`.

    Idempotent: re-runs against the same date replace the prior rows in
    place. Returns the number of rows written, or None if computation
    raised (matching `upsert_system_metrics_for_date`'s soft-fail contract
    so the daily-batch wrapper can log and continue).

    `gtfs_snapshot_id` pins the scheduled side to a historical GTFS
    snapshot (backfill); see `compute_route_metrics_overlay_for_date`.

    The completeness guard acts as a *flagger*, not a *gate*: partial days
    are persisted with ``data_quality='partial'`` and their raw
    ``coverage_pct`` so the UI can render an explicit "partial day" badge
    rather than showing a silent gap in the trend strip. Complete days
    receive ``data_quality='complete'``.

    ``tz_name`` (NOTES-100 multi-agency; default Eastern) widens the
    completeness guard's coverage window to the agency's own local day AND
    (as of NOTES-103) is forwarded into
    `compute_route_metrics_overlay_for_date` so EWT/bunching hour-of-day
    bucketing agrees with the agency's own local clock rather than always
    Eastern.

    ``completeness_threshold`` (NOTES-100 multi-agency): ``None`` (the
    default) uses ``src.data_completeness.MIN_COVERAGE_FOR_MATERIALIZATION``
    — correct for WMATA, but a lower cadence-derived value is required
    for any agency that doesn't poll every feed on every collector tick
    (e.g. SFMTA); see ``src.data_completeness.agency_coverage_threshold``.

    ``agency`` (PR #242 round-2 review finding 3), forwarded to
    ``compute_route_metrics_overlay_for_date``'s cell-hour gate lookup.
    Defaults to ``"wmata"``; ``pipelines/upsert_route_metrics_overlay.py``
    passes its ``--agency`` CLI value through here.

    ``force``: overwrite existing rows that carry observed counts even
    though the date has no stop-level rows left; otherwise
    ``src.system_metrics.AggregateOverwriteRefused`` is raised before
    anything is written (retention keeps aggregates forever but prunes
    the rows they were computed from).

    ``gap_policy``: the agency's per-feed gap rule
    (``AgencyConfig.completeness``, issue #270); None uses the defaults
    (90 min inside 05:00–24:00 local). See
    ``src.data_completeness.assess_data_quality`` for the whole rule.
    """
    from src.data_completeness import (
        MIN_COVERAGE_FOR_MATERIALIZATION,
        assess_data_quality,
    )

    threshold = (
        completeness_threshold
        if completeness_threshold is not None
        else MIN_COVERAGE_FOR_MATERIALIZATION
    )
    service_date_iso = service_date.isoformat()
    existing_by_route = {
        row.route_id: row
        for row in db.query(RouteMetricsDailyOverlay)
        .filter(RouteMetricsDailyOverlay.service_date == service_date_iso)
        .all()
    }
    if not force:
        from src.system_metrics import refuse_if_detail_missing

        refuse_if_detail_missing(
            db,
            service_date,
            overlay_required_sources(existing_by_route.values()),
        )
    # Every overlay row for a date carries the same date-level stamp
    # (this function writes them all in one pass), so the lowest route_id
    # is a deterministic representative. It lets assess_data_quality keep
    # an earned stamp when an input (trip_update_state, raw VP) has since
    # been pruned (NOTES-104, issues #270/#272) instead of misreporting an
    # outage.
    prior_row = existing_by_route[min(existing_by_route)] if existing_by_route else None
    prior = (prior_row.data_quality, prior_row.coverage_pct) if prior_row else None
    decision = assess_data_quality(
        db,
        service_date,
        threshold=threshold,
        tz_name=tz_name,
        prior=prior,
        gap_policy=gap_policy,
    )
    data_quality, pct = decision.data_quality, decision.coverage_pct
    is_complete = data_quality == "complete"

    if decision.kept:
        # See the matching note in src/system_metrics.py: the stamp is
        # preserved, the metrics are still re-derived from current rows.
        print(
            f"  ⚠ Route metrics overlay for {service_date_iso}: an input is no longer "
            f"retained ({'; '.join(decision.not_assessed)}); keeping prior "
            f"'{data_quality}' stamp and its stored coverage, but re-deriving metrics "
            f"from current stop_events — check they are non-null"
        )
    elif not is_complete:
        print(
            f"  ⚠ Route metrics overlay for {service_date_iso}: flagging as partial — "
            f"{decision.describe()}"
        )

    try:
        rows = compute_route_metrics_overlay_for_date(
            db, service_date, gtfs_snapshot_id, tz_name=tz_name, agency=agency
        )
    except Exception as exc:
        print(f"  ✗ Route metrics overlay compute failed for {service_date.isoformat()}: {exc}")
        return None

    now = utcnow_naive()
    for r in rows:
        existing = existing_by_route.get(r["route_id"])
        if existing is not None:
            for key, value in r.items():
                if key in ("route_id", "service_date"):
                    continue
                setattr(existing, key, value)
            existing.data_quality = data_quality
            # When an input is pruned the decision carries the date's stored
            # coverage (from the representative row) back unchanged, so this
            # never replaces a real value with an understated re-measurement.
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
