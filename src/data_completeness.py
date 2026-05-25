"""
Per-service-date ingest completeness check for daily-aggregate pipelines.

The continuous combined collector writes one tick every 30 seconds to
``trip_update_snapshots`` and every 60 seconds to ``vehicle_positions``.
A healthy service day therefore has near-continuous coverage across all
~1,440 minutes of an Eastern operating day. When the collector is down,
both tables stop receiving rows in lockstep.

This module exposes :func:`is_date_sufficiently_complete` so the per-date
upsert pipelines (``src.system_metrics.upsert_system_metrics_for_date``,
``src.route_metrics_overlay.upsert_route_metrics_for_date``) can refuse
to materialize aggregate metrics for a partial day. A partial-day
aggregate is misleading rather than usefully thin: when PM observations
are missing, schedule-anchored denominators (service-delivered, EWT)
collapse to fractions of a percent even though the captured AM portion
is fine. Better to leave the day absent from the materialized tables —
the period-over-period delta code in ``api/aggregations.py`` already
treats absent days as ``None`` and skips them.

Coverage is measured as the union of distinct minute-buckets across
the two ingest tables, so the signal survives the NOTES-72 cutover:
``trip_update_snapshots`` is being retired in Phase F but
``vehicle_positions`` remains. After Phase F the
``trip_update_snapshots`` branch can be removed.
"""

from datetime import date as date_type

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.timezones import eastern_day_bounds_utc

MIN_COVERAGE_FOR_MATERIALIZATION = 0.80


def expected_minutes_for_date(service_date: date_type) -> int:
    """Return the number of clock-minutes in the Eastern service day.

    Normally 1,440 (24 h); on DST spring-forward days the Eastern day
    spans 23 h (1,380 min) and on fall-back days 25 h (1,500 min). Using
    the actual Eastern-day duration as the denominator means the
    coverage threshold stays interpretable across DST transitions.

    Args:
        service_date: Eastern operational date.

    Returns:
        Total minutes between Eastern midnight and the next Eastern midnight.
    """
    start_utc, end_utc = eastern_day_bounds_utc(service_date)
    return int((end_utc - start_utc).total_seconds() // 60)


def _coverage_minutes(db: Session, service_date: date_type) -> int:
    """Count distinct minute-buckets that have at least one ingest row.

    Unions ``trip_update_snapshots.snapshot_ts`` and
    ``vehicle_positions.timestamp`` so the signal survives the NOTES-72
    cutover. Both columns are naive-UTC per the project's storage
    convention. Uses Postgres ``date_trunc``; the SQLite fallback uses
    ``strftime`` so the function still returns a sensible value when
    unit tests run against in-memory SQLite.

    Args:
        db: SQLAlchemy session.
        service_date: Eastern operational date to measure.

    Returns:
        Count of distinct UTC minute-buckets, in ``[0, expected_minutes]``,
        within the Eastern-day window.
    """
    start_utc, end_utc = eastern_day_bounds_utc(service_date)
    dialect = db.bind.dialect.name if db.bind is not None else "postgresql"
    if dialect == "sqlite":
        bucket_expr_snap = "strftime('%Y-%m-%d %H:%M:00', snapshot_ts)"
        bucket_expr_pos = "strftime('%Y-%m-%d %H:%M:00', timestamp)"
    else:
        bucket_expr_snap = "date_trunc('minute', snapshot_ts)"
        bucket_expr_pos = "date_trunc('minute', timestamp)"

    row = db.execute(
        text(
            f"""
            SELECT COUNT(DISTINCT bucket) FROM (
                SELECT {bucket_expr_snap} AS bucket
                FROM trip_update_snapshots
                WHERE snapshot_ts >= :start AND snapshot_ts < :end
                UNION
                SELECT {bucket_expr_pos} AS bucket
                FROM vehicle_positions
                WHERE timestamp >= :start AND timestamp < :end
            ) AS buckets
            """
        ),
        {"start": start_utc, "end": end_utc},
    ).first()
    return int(row[0]) if row and row[0] is not None else 0


def coverage_pct_for_date(db: Session, service_date: date_type) -> float:
    """Return the fraction of in-day minute-buckets with ingest coverage.

    A full healthy day scores ≥ 0.99 in observed history (the collector
    polls every 30 s, so even hours with no active vehicles still
    produce ``trip_update_snapshots`` rows). The 2026-05-24 power-loss
    incident scored ~0.51 (AM-only).

    Args:
        db: SQLAlchemy session.
        service_date: Eastern operational date to evaluate.

    Returns:
        Float in ``[0.0, 1.0]``.
    """
    expected = expected_minutes_for_date(service_date)
    if expected <= 0:
        return 0.0
    return _coverage_minutes(db, service_date) / expected


def is_date_sufficiently_complete(
    db: Session,
    service_date: date_type,
    threshold: float = MIN_COVERAGE_FOR_MATERIALIZATION,
) -> bool:
    """Return True iff the date has enough coverage to materialize aggregates for.

    Threshold defaults to 80% — well above any plausible off-hours dip
    (``trip_update_snapshots`` keeps ticking even when vehicle activity
    is sparse) and well below "healthy day" (which is ≥ 99% in
    observed history). Tune via the ``threshold`` argument if a future
    incident motivates revisiting.

    Args:
        db: SQLAlchemy session.
        service_date: Eastern operational date to check.
        threshold: Minimum coverage fraction to count as "complete".

    Returns:
        True when ``coverage_pct_for_date(db, service_date) >= threshold``.
    """
    return coverage_pct_for_date(db, service_date) >= threshold
