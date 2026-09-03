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

from src.models import SystemMetricsDaily
from src.timezones import utcnow_naive


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

    Returns:
        The metrics dict written (includes ``data_quality`` and
        ``coverage_pct`` keys), or None if computation raised.
    """
    from src.data_completeness import (
        MIN_COVERAGE_FOR_MATERIALIZATION,
        coverage_pct_for_date,
        is_date_sufficiently_complete,
    )

    threshold = (
        completeness_threshold
        if completeness_threshold is not None
        else MIN_COVERAGE_FOR_MATERIALIZATION
    )
    pct = coverage_pct_for_date(db, service_date, tz_name=tz_name)
    is_complete = is_date_sufficiently_complete(
        db, service_date, threshold=threshold, tz_name=tz_name
    )
    data_quality = "complete" if is_complete else "partial"

    if not is_complete:
        print(
            f"  ⚠ System metrics for {service_date.isoformat()}: "
            f"ingest coverage {pct:.1%} below threshold — flagging as partial"
        )

    try:
        metrics = compute_system_metrics_for_date(
            db, service_date, gtfs_snapshot_id, tz_name=tz_name, agency=agency
        )
    except Exception as exc:
        print(f"  ✗ System metrics compute failed for {service_date.isoformat()}: {exc}")
        return None

    service_date_iso = service_date.isoformat()
    existing = (
        db.query(SystemMetricsDaily)
        .filter(SystemMetricsDaily.service_date == service_date_iso)
        .first()
    )
    if existing:
        existing.otp_percentage = metrics["otp_percentage"]
        existing.service_delivered_ratio = metrics["service_delivered_ratio"]
        existing.ewt_seconds = metrics["ewt_seconds"]
        existing.swt_seconds = metrics["swt_seconds"]
        existing.bunching_rate = metrics["bunching_rate"]
        existing.data_quality = data_quality
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
