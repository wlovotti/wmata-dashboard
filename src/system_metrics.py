"""
Per-date system-level metric computation (cloud-migration Phase 1).

Wraps the system-rollup helpers in `api/aggregations.py` to compute and
persist the headline metrics — OTP, service-delivered, EWT, SWT,
bunching — for a single service_date.

  - `compute_system_metrics_for_date` returns the computed metrics dict.
    Used by `api/aggregations.get_system_trend_data` for today's live row
    in the hybrid serve path (history from table, today live).
  - `upsert_system_metrics_for_date` calls the compute function then
    persists the result to `system_metrics_daily`. Wired into the daily
    batch via `pipelines/upsert_system_metrics_daily.py`. Re-runs against
    the same date overwrite the prior row in place.
"""

from datetime import date as date_type

from sqlalchemy.orm import Session

from src.agency_config import CompletenessPolicy
from src.models import SystemMetricsDaily
from src.timezones import utcnow_naive


class AggregateOverwriteRefused(RuntimeError):
    """A per-date rollup would replace kept aggregates with values computed from missing rows.

    Each kept aggregate derives from a specific stop-level source: OTP from
    ``proximity`` stop_events/runs, EWT / bunching / destination OTP from
    ``trip_update`` stop_events, service-delivered from ``runs``. Raised
    (before anything is written) when an existing observed value's source
    is absent for the date — e.g. retention pruned the date, or only the
    trip-update replay recreated rows after the VM-era VP was deleted, so
    a recompute would null the proximity-based OTP. Callers that really
    mean to overwrite pass ``force=True`` (CLI ``--force-overwrite-aggregates``).
    """


def system_row_required_sources(row: SystemMetricsDaily | None) -> set[str]:
    """Stop-level sources a ``system_metrics_daily`` row's observed values were computed from.

    SWT is schedule-only and a service-delivered ratio of 0 is what an
    unobserved day computes, so neither requires anything.
    """
    if row is None:
        return set()
    required = set()
    if row.otp_percentage is not None:
        required.add("proximity")
    if row.ewt_seconds is not None or row.bunching_rate is not None:
        required.add("trip_update")
    if (row.service_delivered_ratio or 0) > 0:
        required.add("runs")
    return required


def refuse_if_detail_missing(db: Session, service_date: date_type, required: set[str]) -> None:
    """Raise :class:`AggregateOverwriteRefused` when a ``required`` source is absent for the date."""
    if not required:
        return
    from src.retention import stop_level_sources

    missing = sorted(set(required) - stop_level_sources(db, service_date))
    if missing:
        d = service_date.isoformat()
        raise AggregateOverwriteRefused(
            f"{d} has kept daily aggregates computed from {', '.join(sorted(required))} rows, "
            f"but {', '.join(missing)} rows are not present now (pruned by retention, or only "
            "partly re-derived); recomputing would overwrite them with values from incomplete "
            "data. Re-derive only once the raw inputs are back: inside the S3 JSONL archives "
            f"`bin/pull-and-derive.sh --date {d}` reloads them (it refuses a date whose VP is "
            "unavailable); for older WMATA dates see docs/DEPLOYMENT.md §12.4 (VP parquet "
            "archive). --force-overwrite-aggregates overrides."
        )


def compute_system_metrics_for_date(
    db: Session,
    service_date: date_type,
    gtfs_snapshot_id: int | None = None,
    tz_name: str = "America/New_York",
    agency: str = "wmata",
) -> dict:
    """Compute system-level OTP / service-delivered / EWT / SWT / bunching for one date.

    Returns a dict with keys `otp_percentage`, `service_delivered_ratio`,
    `ewt_seconds`, `swt_seconds`, `bunching_rate`. Any individual value may
    be `None` when the pool is empty for that date (no proximity
    stop_events, no scheduled trips, no eligible observed pairs, etc.).
    `swt_seconds` is schedule-side only (see
    `_system_ewt_and_bunching_for_date`) so it can be populated even when
    `ewt_seconds` is `None` for lack of observed stop_events.

    Args:
        db: SQLAlchemy session bound to the metrics database.
        service_date: Operational date (in `tz_name`) to compute for.
        gtfs_snapshot_id: Pin the scheduled side (service-delivered
            denominators, EWT SWT pools) to a historical GTFS snapshot when
            backfilling a date whose schedule has been superseded; the
            default reads the live `is_current` snapshot. OTP and bunching
            are observed-side only and unaffected.
        tz_name: IANA timezone name (NOTES-103 multi-agency; default
            Eastern). Only affects `ewt_seconds` / `bunching_rate` — both
            bucket observed headways by local hour-of-day and must agree
            with the agency-local hour the scheduled side (GTFS clock
            time) already uses. `otp_percentage` / `service_delivered_ratio`
            don't bucket by hour at all and are unaffected either way.
        agency: Agency name (PR #242 review finding 5), forwarded to
            `_system_ewt_and_bunching_for_date`'s cell-hour gate lookup.
            Defaults to `"wmata"` so existing callers keep today's
            behavior; the daily batch (`pipelines/upsert_system_metrics_daily.py
            --agency sfmta`) and the live "today" hybrid-serve path both
            pass their real agency.

    Returns:
        Dict shaped like a single row of `system_metrics_daily` (minus
        `computed_at` and `service_date`).
    """
    # Local import: api.aggregations imports src.system_metrics in the
    # hybrid serve path, so a top-level import would create a cycle.
    from api.aggregations import (
        _system_ewt_and_bunching_for_date,
        _system_otp_series,
        _system_service_delivered_series,
    )

    otp_by_date = _system_otp_series(db, [service_date])
    sd_by_date = _system_service_delivered_series(db, [service_date], gtfs_snapshot_id)
    sched_by_date: dict[str, dict] = {}
    ewt_seconds, swt_seconds, bunching_rate = _system_ewt_and_bunching_for_date(
        db, service_date, sched_by_date, gtfs_snapshot_id, tz_name=tz_name, agency=agency
    )

    iso = service_date.isoformat()
    return {
        "otp_percentage": otp_by_date.get(iso),
        "service_delivered_ratio": sd_by_date.get(iso),
        "ewt_seconds": ewt_seconds,
        "swt_seconds": swt_seconds,
        "bunching_rate": bunching_rate,
    }


def upsert_system_metrics_for_date(
    db: Session,
    service_date: date_type,
    gtfs_snapshot_id: int | None = None,
    tz_name: str = "America/New_York",
    completeness_threshold: float | None = None,
    agency: str = "wmata",
    force: bool = False,
    gap_policy: CompletenessPolicy | None = None,
) -> dict | None:
    """Compute and upsert one row of `system_metrics_daily` for `service_date`.

    Re-runs against the same date overwrite the prior row in place — the
    upsert is conflict-free since `service_date` is the primary key.

    The completeness guard (see `src/data_completeness.py`) acts as a
    *flagger*, not a *gate*: partial days are persisted with
    ``data_quality='partial'`` and their raw ``coverage_pct`` so the UI
    can render an explicit "partial day" badge instead of a silent gap.
    Complete days receive ``data_quality='complete'``.

    Returns the computed metrics dict, or None if computation raised
    (failures here shouldn't block the rest of the batch).

    Args:
        db: Database session.
        service_date: Service date (in ``tz_name``) to compute and store.
        gtfs_snapshot_id: Optional historical GTFS snapshot to pin the
            scheduled side to (backfill); see
            `compute_system_metrics_for_date`.
        tz_name: IANA timezone name (NOTES-100 multi-agency; default
            Eastern). Widens the completeness guard's coverage window to
            the agency's own local day AND (as of NOTES-103) is forwarded
            into `compute_system_metrics_for_date` so EWT/bunching
            hour-of-day bucketing agrees with the agency's own local
            clock rather than always Eastern.
        completeness_threshold: Minimum coverage fraction to count as
            "complete" (NOTES-100 multi-agency). ``None`` (the default)
            uses ``src.data_completeness.MIN_COVERAGE_FOR_MATERIALIZATION``
            — correct for WMATA, but a lower cadence-derived value is
            required for any agency that doesn't poll every feed on
            every collector tick (e.g. SFMTA) — see
            ``src.data_completeness.agency_coverage_threshold``, which
            callers should compute from the agency's ``AgencyConfig``
            and pass here explicitly.
        agency: Agency name (PR #242 review finding 5), forwarded to
            `compute_system_metrics_for_date`'s cell-hour gate lookup.
            Defaults to `"wmata"`; `pipelines/upsert_system_metrics_daily.py`
            passes its `--agency` CLI value through here.
        force: Overwrite an existing row that carries observed values even
            though the date has no stop-level rows left (see
            :class:`AggregateOverwriteRefused`, raised otherwise — before
            anything is written).
        gap_policy: The agency's per-feed gap rule
            (``AgencyConfig.completeness``, issue #270); None uses the
            defaults (90 min inside 05:00–24:00 local).

    Returns:
        The metrics dict written (includes ``data_quality`` and
        ``coverage_pct`` keys), or None if computation raised.
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
    existing = (
        db.query(SystemMetricsDaily)
        .filter(SystemMetricsDaily.service_date == service_date_iso)
        .first()
    )
    # The prior stamp lets assess_data_quality keep an earned stamp when an
    # input (trip_update_state, raw VP) has since been pruned (NOTES-104,
    # issues #270/#272) instead of misreporting the re-derive as an outage.
    if not force:
        refuse_if_detail_missing(db, service_date, system_row_required_sources(existing))
    prior = (existing.data_quality, existing.coverage_pct) if existing else None
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
        # The stamp is preserved but the metrics below are still
        # re-derived from whatever stop_events/runs exist now. If this
        # re-run follows a runs-delete recovery past the raw retention
        # window, those rows are gone and the metrics will be NULL under
        # the kept stamp (the API's null guards keep them out of windowed
        # means) — hence a warning, not a notice.
        print(
            f"  ⚠ System metrics for {service_date_iso}: an input is no longer retained "
            f"({'; '.join(decision.not_assessed)}); keeping prior '{data_quality}' stamp "
            f"and its stored coverage, but re-deriving metrics from current stop_events "
            f"— check they are non-null"
        )
    elif not is_complete:
        print(
            f"  ⚠ System metrics for {service_date_iso}: flagging as partial — {decision.describe()}"
        )
    print(f"  · completeness {service_date_iso}: {decision.describe()}")

    try:
        metrics = compute_system_metrics_for_date(
            db, service_date, gtfs_snapshot_id, tz_name=tz_name, agency=agency
        )
    except Exception as exc:
        print(f"  ✗ System metrics compute failed for {service_date.isoformat()}: {exc}")
        return None

    if existing:
        existing.otp_percentage = metrics["otp_percentage"]
        existing.service_delivered_ratio = metrics["service_delivered_ratio"]
        existing.ewt_seconds = metrics["ewt_seconds"]
        existing.swt_seconds = metrics["swt_seconds"]
        existing.bunching_rate = metrics["bunching_rate"]
        existing.data_quality = data_quality
        # When an input is pruned the decision carries this row's stored
        # coverage back unchanged (it was the prior), so this write never
        # replaces a real value with an understated re-measurement.
        existing.coverage_pct = pct
        existing.computed_at = utcnow_naive()
    else:
        db.add(
            SystemMetricsDaily(
                service_date=service_date_iso,
                otp_percentage=metrics["otp_percentage"],
                service_delivered_ratio=metrics["service_delivered_ratio"],
                ewt_seconds=metrics["ewt_seconds"],
                swt_seconds=metrics["swt_seconds"],
                bunching_rate=metrics["bunching_rate"],
                data_quality=data_quality,
                coverage_pct=pct,
                computed_at=utcnow_naive(),
            )
        )
    db.commit()

    quality_label = "partial" if not is_complete else "complete"
    print(
        f"  ✓ System metrics for {service_date_iso} [{quality_label}]: "
        f"OTP={metrics['otp_percentage']}, "
        f"SD={metrics['service_delivered_ratio']}, "
        f"SWT={metrics['swt_seconds']}, "
        f"EWT={metrics['ewt_seconds']}, "
        f"BUN={metrics['bunching_rate']}"
    )
    metrics["data_quality"] = data_quality
    metrics["coverage_pct"] = pct
    return metrics
