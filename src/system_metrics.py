"""
Per-date system-level metric computation (NOTES-48).

Wraps the system-rollup helpers in `api/aggregations.py` to compute the
four headline metrics — OTP, service-delivered, EWT, bunching — for a
single service_date in one shot. Designed to be called from:

  - `pipelines/compute_daily_metrics.py`, after the per-route rollups
    finish, to populate the `system_metrics_daily` table (NOTES-48).
  - `api/aggregations.get_system_trend_data`, to compute today's row
    live in the hybrid serve path (history from table, today live).

Reuses the existing per-date functions rather than reimplementing the
logic, so the materialized values are identical to what the previous
fully-live trend endpoint would have returned.
"""

from datetime import date as date_type

from sqlalchemy.orm import Session


def compute_system_metrics_for_date(db: Session, service_date: date_type) -> dict:
    """Compute system-level OTP / service-delivered / EWT / bunching for one date.

    Returns a dict with keys `otp_percentage`, `service_delivered_ratio`,
    `ewt_seconds`, `bunching_rate`. Any individual value may be `None` when
    the pool is empty for that date (no rows in `route_metrics_daily`,
    no scheduled trips, no eligible observed pairs, etc.).

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
