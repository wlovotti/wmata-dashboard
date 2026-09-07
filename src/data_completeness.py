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
three ingest signals (NOTES-104):

- ``collector_heartbeats.ts`` — written by the *live* collector on
  every TripUpdates poll. Only present on a database the collector
  writes to directly; since the stateless-collector cutover (NOTES-95)
  the laptop system of record receives none for either agency.
- ``vehicle_positions.timestamp`` — one row per vehicle per
  VehiclePositions poll, loaded from the S3 archive. Present in every
  regime, but alone it caps at the agency's VP cadence: ~100% for WMATA
  (every 60 s tick) and ~33% for SFMTA (every 3rd 60 s tick).
- ``trip_update_state.final_snapshot_ts`` — the feed timestamp of the
  poll on which each (trip, stop) was last seen. Written by both the
  live collector and ``pipelines/replay_archive_to_state.py``, so it is
  the one poll-time signal available in all three data-arrival regimes
  the NOTES-104 addendum identified: live (heartbeats + VP + TU),
  replayed-only (TU), and archive-loaded-to-laptop (VP + TU). A
  collector outage shows up as a gap in it exactly as it did in
  heartbeats: no poll, no snapshot timestamps; the first poll after the
  outage finalizes every stop passed during it with that single poll's
  timestamp, so it can't back-fill the gap.

The union means a brief gap in one signal (e.g. an API error on one
VP tick) is covered by the others. Measured on real SFMTA dates the
union reaches 0.62–0.81 against a cadence-aware threshold of 0.53
(``agency_coverage_threshold``); VP alone sat at 0.33, which is why
every SFMTA date was stamped ``partial`` before this signal was added.

Note: ``trip_update_snapshots`` has been retired — its ORM model was
removed and the table is dropped via the manual runbook in
``scripts/migrate_drop_phase_f.py`` (Phase F, PR #155). It is no longer
written to or used for completeness accounting.
"""

from datetime import date as date_type
from datetime import timedelta
from math import lcm

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.agency_config import AgencyConfig
from src.timezones import local_day_bounds_utc

MIN_COVERAGE_FOR_MATERIALIZATION = 0.80


def agency_coverage_threshold(cfg: AgencyConfig) -> float:
    """Return the completeness-guard threshold for one agency's polling cadence.

    ``MIN_COVERAGE_FOR_MATERIALIZATION`` (0.80) implicitly assumed a
    collector that can reach ~100% minute-coverage under perfect
    collection -- true for WMATA, where TripUpdates is polled every
    tick (``trip_updates_every_ticks == 1``), so the collector writes a
    heartbeat every tick and every clock-minute is coverable.

    It is NOT true for an agency that polls both TripUpdates and
    VehiclePositions less than once per tick (NOTES-100 follow-up):
    ``_coverage_minutes`` counts the *union* of ``collector_heartbeats``
    and ``vehicle_positions.timestamp`` minute-buckets, and a
    ``collector_heartbeats`` row is only written from the TripUpdates
    poll path (``src/wmata_collector.py:_save_trip_updates``) — so a
    tick only contributes a covered minute when TripUpdates polls that
    tick (via the heartbeat) *or* VehiclePositions polls that tick (via
    its own row landing in the union). So the *theoretical maximum*
    achievable coverage is the fraction of ticks, over one full LCM
    cycle of the two polling periods, on which at least one of the two
    feeds is scheduled to poll. For SFMTA
    (``trip_updates_every_ticks=2``, ``vehicle_positions_every_ticks=3``)
    that ceiling is 4/6 ≈ 66.7% -- meaning the flat 0.80 threshold can
    NEVER be satisfied, even by a perfectly healthy SFMTA day with zero
    collector downtime. Every SFMTA date would be permanently flagged
    `data_quality='partial'`, which defeats the flag's purpose (it's
    supposed to distinguish a real collector gap from a healthy day).

    This function keeps the *same relative safety margin* the original
    constant expressed (a plausible-dip-tolerant, healthy-day-permissive
    80%) but scales it by the agency's own cadence ceiling instead of
    assuming that ceiling is 100%.

    Note: this is exact when ``tick_sec`` is a multiple of 60 (true for
    both configured agencies today, 30s and 60s respectively) or when
    every tick is active (the WMATA case) -- a sub-minute tick_sec with
    a non-trivial cadence would need tick-to-minute deduplication logic
    not implemented here, since no configured agency needs it yet.

    With ``trip_update_state.final_snapshot_ts`` in the numerator union
    (NOTES-104) the *measured* ceiling can exceed this tick-derived one,
    because feed timestamps land at sub-minute offsets and so spread
    across more clock-minutes than the poll cadence alone implies. The
    threshold stays anchored to the conservative tick-derived ceiling;
    exceeding it is headroom, not an error.

    Args:
        cfg: The agency's ``AgencyConfig`` (uses ``tick_sec`` only for
            documentation context; the ceiling itself depends on the
            ``*_every_ticks`` ratio, not the absolute tick length).

    Returns:
        ``MIN_COVERAGE_FOR_MATERIALIZATION`` scaled by the agency's
        cadence-derived coverage ceiling, in ``(0.0, MIN_COVERAGE_FOR_MATERIALIZATION]``.
    """
    tu, vp = cfg.trip_updates_every_ticks, cfg.vehicle_positions_every_ticks
    cycle_ticks = lcm(tu, vp)
    active_ticks = sum(1 for i in range(1, cycle_ticks + 1) if i % tu == 0 or i % vp == 0)
    ceiling = active_ticks / cycle_ticks
    return MIN_COVERAGE_FOR_MATERIALIZATION * ceiling


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

    Unions ``collector_heartbeats.ts``, ``vehicle_positions.timestamp``,
    and ``trip_update_state.final_snapshot_ts`` (see the module docstring
    for why each exists and which regimes it covers). All three columns
    are naive-UTC per the project's storage convention. Uses Postgres
    ``date_trunc``; the SQLite fallback uses ``strftime`` so the function
    still returns a sensible value when unit tests run against in-memory
    SQLite.

    The ``trip_update_state`` leg also filters ``service_date IN
    (service_date - 1, service_date)``: a snapshot that lands inside the
    local clock-day belongs to a trip on that service date or, past
    midnight, the previous one. The predicate lets Postgres use
    ``idx_tus_service_date`` instead of scanning the full retention
    window (WMATA keeps ~30 days, ~4M rows) once per stamped date.

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
        bucket_expr_tus = "strftime('%Y-%m-%d %H:%M:00', final_snapshot_ts)"
    else:
        bucket_expr_hb = "date_trunc('minute', ts)"
        bucket_expr_pos = "date_trunc('minute', timestamp)"
        bucket_expr_tus = "date_trunc('minute', final_snapshot_ts)"

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
                UNION
                SELECT {bucket_expr_tus} AS bucket
                FROM trip_update_state
                WHERE service_date IN (:sd_prev, :sd)
                  AND final_snapshot_ts >= :start AND final_snapshot_ts < :end
            ) AS buckets
            """
        ),
        {
            "start": start_utc,
            "end": end_utc,
            "sd_prev": service_date - timedelta(days=1),
            "sd": service_date,
        },
    ).first()
    return int(row[0]) if row and row[0] is not None else 0


def coverage_pct_for_date(
    db: Session, service_date: date_type, tz_name: str = "America/New_York"
) -> float:
    """Return the fraction of in-day minute-buckets with ingest coverage.

    A full healthy WMATA day scores ≥ 0.99 in observed history (positions
    every 60 s tick plus trip-update snapshots fill every clock-minute;
    under the old live collector, heartbeats alone did). The 2026-05-24
    power-loss incident scored ~0.51 (AM-only). Healthy SFMTA days score
    0.62–0.81 against their cadence-aware threshold of ~0.53.

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
