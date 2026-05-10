"""
Per-date system-level metric computation (NOTES-48).

Wraps the system-rollup helpers in `api/aggregations.py` to compute and
persist the four headline metrics — OTP, service-delivered, EWT,
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


def compute_system_metrics_for_date(db: Session, service_date: date_type) -> dict:
    """Compute system-level OTP / service-delivered / EWT / bunching for one date.

    Returns a dict with keys `otp_percentage`, `service_delivered_ratio`,
    `ewt_seconds`, `bunching_rate`. Any individual value may be `None` when
    the pool is empty for that date (no proximity stop_events, no scheduled
    trips, no eligible observed pairs, etc.).

    Args:
        db: SQLAlchemy session bound to the metrics database.
        service_date: Eastern operational date to compute for.

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
    sd_by_date = _system_service_delivered_series(db, [service_date])
    sched_by_day_type: dict[str, dict] = {}
    ewt_seconds, bunching_rate = _system_ewt_and_bunching_for_date(
        db, service_date, sched_by_day_type
    )

    iso = service_date.isoformat()
    return {
        "otp_percentage": otp_by_date.get(iso),
        "service_delivered_ratio": sd_by_date.get(iso),
        "ewt_seconds": ewt_seconds,
        "bunching_rate": bunching_rate,
    }


def upsert_system_metrics_for_date(db: Session, service_date: date_type) -> dict | None:
    """Compute and upsert one row of `system_metrics_daily` for `service_date`.

    Re-runs against the same date overwrite the prior row in place — the
    upsert is conflict-free since `service_date` is the primary key.

    Returns the computed metrics dict, or None if computation raised
    (failures here shouldn't block the rest of the batch).

    Args:
        db: Database session.
        service_date: Eastern service date to compute and store.

    Returns:
        The metrics dict written, or None if computation raised.
    """
    try:
        metrics = compute_system_metrics_for_date(db, service_date)
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
        existing.bunching_rate = metrics["bunching_rate"]
        existing.computed_at = utcnow_naive()
    else:
        db.add(
            SystemMetricsDaily(
                service_date=service_date_iso,
                otp_percentage=metrics["otp_percentage"],
                service_delivered_ratio=metrics["service_delivered_ratio"],
                ewt_seconds=metrics["ewt_seconds"],
                bunching_rate=metrics["bunching_rate"],
                computed_at=utcnow_naive(),
            )
        )
    db.commit()

    print(
        f"  ✓ System metrics for {service_date_iso}: "
        f"OTP={metrics['otp_percentage']}, "
        f"SD={metrics['service_delivered_ratio']}, "
        f"EWT={metrics['ewt_seconds']}, "
        f"BUN={metrics['bunching_rate']}"
    )
    return metrics
