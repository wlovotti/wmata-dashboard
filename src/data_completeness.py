"""
Per-service-date ingest completeness check for daily-aggregate pipelines.

The continuous combined collector writes one heartbeat row every 30 seconds
to ``collector_heartbeats`` and one vehicle-positions row every 60 seconds to
``vehicle_positions``. A healthy service day therefore has near-continuous
coverage across all ~1,440 minutes of an Eastern operating day. When the
collector is down, both tables stop receiving rows in lockstep.

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
the two ingest tables: ``collector_heartbeats`` (primary, every 30 s)
and ``vehicle_positions`` (secondary, every 60 s). The heartbeat table
was introduced in Phase E.2 of NOTES-72 to replace
``trip_update_snapshots`` as the coverage signal once the collector
stopped dual-writing snapshots. The two-source union means a brief
heartbeat gap (e.g., API error on one tick) is covered by the position
signal and vice versa.

Note: ``trip_update_snapshots`` has been retired — its ORM model was
removed and the table is dropped via the manual runbook in
``scripts/migrate_drop_phase_f.py`` (Phase F, PR #155). It is no longer
written to or used for completeness accounting.
"""

from datetime import date as date_type

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.timezones import local_day_bounds_utc

MIN_COVERAGE_FOR_MATERIALIZATION = 0.80


def expected_minutes_for_date(service_date: date_type, tz_name: str = "America/New_York") -> int:
    """Return the number of clock-minutes in the local service day.

    Normally 1,440 (24 h); on DST spring-forward days the local day
    spans 23 h (1,380 min) and on fall-back days 25 h (1,500 min). Using
    the actual local-day duration as the denominator means the
    coverage threshold stays interpretable across DST transitions.

    Args:
        service_date: Operational date in ``tz_name``.
        tz_name: IANA timezone name (NOTES-100 multi-agency; default
            Eastern, matching every WMATA call site).

    Returns:
        Total minutes between local midnight and the next local midnight.
    """
    start_utc, end_utc = local_day_bounds_utc(service_date, tz_name)
    return int((end_utc - start_utc).total_seconds() // 60)


def _coverage_minutes(
    db: Session, service_date: date_type, tz_name: str = "America/New_York"
) -> int:
    """Count distinct minute-buckets that have at least one ingest row.

    Unions ``collector_heartbeats.ts`` and ``vehicle_positions.timestamp``
    so both the collector's 30-second heartbeat and the 60-second position
    signal contribute. Both columns are naive-UTC per the project's storage
    convention. Uses Postgres ``date_trunc``; the SQLite fallback uses
    ``strftime`` so the function still returns a sensible value when
    unit tests run against in-memory SQLite.

    Args:
        db: SQLAlchemy session.
        service_date: Operational date in ``tz_name`` to measure.
        tz_name: IANA timezone name (NOTES-100 multi-agency; default
            Eastern, matching every WMATA call site).

    Returns:
        Count of distinct UTC minute-buckets, in ``[0, expected_minutes]``,
        within the local-day window.
    """
    start_utc, end_utc = local_day_bounds_utc(service_date, tz_name)
    dialect = db.bind.dialect.name if db.bind is not None else "postgresql"
    if dialect == "sqlite":
        bucket_expr_hb = "strftime('%Y-%m-%d %H:%M:00', ts)"
        bucket_expr_pos = "strftime('%Y-%m-%d %H:%M:00', timestamp)"
    else:
        bucket_expr_hb = "date_trunc('minute', ts)"
        bucket_expr_pos = "date_trunc('minute', timestamp)"

    row = db.execute(
        text(
            f"""
            SELECT COUNT(DISTINCT bucket) FROM (
                SELECT {bucket_expr_hb} AS bucket
                FROM collector_heartbeats
                WHERE ts >= :start AND ts < :end
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


def coverage_pct_for_date(
    db: Session, service_date: date_type, tz_name: str = "America/New_York"
) -> float:
    """Return the fraction of in-day minute-buckets with ingest coverage.

    A full healthy day scores ≥ 0.99 in observed history (the collector
    writes a heartbeat every 30 s, so even hours with no active vehicles
    still produce ``collector_heartbeats`` rows). The 2026-05-24 power-loss
    incident scored ~0.51 (AM-only).

    Args:
        db: SQLAlchemy session.
        service_date: Operational date in ``tz_name`` to evaluate.
        tz_name: IANA timezone name (NOTES-100 multi-agency; default
            Eastern, matching every WMATA call site).

    Returns:
        Float in ``[0.0, 1.0]``.
    """
    expected = expected_minutes_for_date(service_date, tz_name)
    if expected <= 0:
        return 0.0
    return _coverage_minutes(db, service_date, tz_name) / expected


def is_date_sufficiently_complete(
    db: Session,
    service_date: date_type,
    threshold: float = MIN_COVERAGE_FOR_MATERIALIZATION,
    tz_name: str = "America/New_York",
) -> bool:
    """Return True iff the date has enough coverage to materialize aggregates for.

    Threshold defaults to 80% — well above any plausible off-hours dip
    (``collector_heartbeats`` keeps ticking even when vehicle activity
    is sparse) and well below "healthy day" (which is ≥ 99% in
    observed history). Tune via the ``threshold`` argument if a future
    incident motivates revisiting.

    Args:
        db: SQLAlchemy session.
        service_date: Operational date in ``tz_name`` to check.
        threshold: Minimum coverage fraction to count as "complete".
        tz_name: IANA timezone name (NOTES-100 multi-agency; default
            Eastern, matching every WMATA call site).

    Returns:
        True when ``coverage_pct_for_date(db, service_date, tz_name) >= threshold``.
    """
    return coverage_pct_for_date(db, service_date, tz_name) >= threshold
