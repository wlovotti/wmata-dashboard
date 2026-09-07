"""
Per-service-date ingest completeness check for daily-aggregate pipelines.

This module answers one question for the per-date stamping pipelines
(``src.system_metrics.upsert_system_metrics_for_date``,
``src.route_metrics_overlay.upsert_route_metrics_for_date``): did enough
of the service day's polling actually land in this database to trust the
day's aggregates? A day that fails is still materialized, but stamped
``data_quality='partial'`` so the API can exclude it from windowed means
(a collection-outage day's thin PM sample would otherwise stand in for,
or disproportionately swing, a whole window) and the UI can badge it.

Coverage is the fraction of the local clock-day's minutes that have at
least one ingest row, unioned across three signals (NOTES-104):

- ``collector_heartbeats.ts`` — written by a *live* collector on every
  TripUpdates poll. Only present on a database the collector writes to
  directly; since the stateless-collector cutover (NOTES-95) the laptop
  system of record receives none for either agency.
- ``vehicle_positions.timestamp`` — one row per vehicle per
  VehiclePositions poll, loaded from the S3 archive. Present in every
  regime, but alone it caps at the agency's VP cadence: ~100% for WMATA
  (every 60 s tick) and ~33% for SFMTA (every 3rd 60 s tick).
- ``trip_update_state.final_snapshot_ts`` — the feed timestamp of the
  poll on which each (trip, stop) was last seen. Written by both the
  live collector and ``pipelines/replay_archive_to_state.py``, so it is
  the one poll-time signal available in all three data-arrival regimes:
  live (heartbeats + VP + TU), replayed-only (TU), and
  archive-loaded-to-laptop (VP + TU). A total collector outage shows up
  as a gap in it exactly as it did in heartbeats: no poll, no snapshot
  timestamps, and the first poll after the outage finalizes every stop
  passed during it with that single poll's timestamp, so it cannot
  back-fill the gap.

Measured on the system of record: a healthy WMATA day reaches ~1.00
(VP alone does; TU alone reads 0.999). A healthy SFMTA day reaches
0.56–0.81 against a cadence-aware threshold of 0.533 — VP alone sat at
0.33, which is why every SFMTA date was stamped ``partial`` before the
TU leg existed. The SFMTA spread is wide because the two poller streams
(NOTES-133 per-stream isolation) drift in phase relative to each other
and to the clock-minute grid; the headroom above the tick-derived
ceiling is real but variable, not a structural constant.

Two limits worth knowing:

- **The TU signal is subject to retention.** ``trip_update_state`` is
  pruned (7 days for SFMTA in ``bin/pull-and-derive.sh``, 30 for WMATA),
  so re-deriving an old date would see VP only. :func:`resolve_data_quality`
  therefore never downgrades an existing ``complete`` stamp when the TU
  leg has *no rows at all* for the date — that is retention, not an
  outage (an outage day was stamped while the signal was present).
- **A failed VP archive load is no longer caught by this check.** TU
  alone certifies a WMATA day, and ``bin/pull-and-derive.sh`` tolerates a
  VP loader failure by design; proximity-sourced OTP (origin / all-
  timepoints) is computed from ``vehicle_positions`` and would be thin
  on such a day while the stamp reads ``complete``. The loader's own
  summary flags the failure loudly; this module does not.

``trip_update_snapshots`` has been retired (Phase F, PR #155) and plays
no part here.
"""

from datetime import date as date_type
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
    collection — true for WMATA, where both feeds are polled every
    60 s. It is NOT true for an agency that polls both feeds less than
    once per tick (NOTES-100 follow-up): a tick only contributes a
    covered minute when at least one feed polled on it, so the
    *theoretical* ceiling is the fraction of ticks, over one LCM cycle
    of the two polling periods, on which either feed is scheduled. For
    SFMTA (``trip_updates_every_ticks=2``, ``vehicle_positions_every_ticks=3``)
    that is 4/6 ≈ 66.7%, and the flat 0.80 would be unreachable even by
    a perfectly healthy day.

    This function scales the constant by that cadence ceiling, keeping
    the same *relative* safety margin the original expressed.

    Honest caveat (NOTES-104): with trip-update snapshot timestamps in
    the numerator, SFMTA's *measured* healthy-day coverage is 0.56–0.81,
    above the 0.667 tick-derived ceiling, so the effective margin is
    nearer 70–80% of a healthy day than the nominal 80%. Concretely, an
    SFMTA day missing ~6 of 24 hours can still clear 0.533. The threshold
    is deliberately left anchored to the tick-derived ceiling rather than
    re-tuned to the measured one, because the measured ceiling moves with
    poller phase drift (see the module docstring); revisit if an SFMTA
    outage ever slips through as ``complete``.

    Exact when ``tick_sec`` is a multiple of 60 (both configured agencies)
    or when every tick is active (WMATA); a sub-minute tick with a
    non-trivial cadence would need tick-to-minute deduplication not
    implemented here.

    Args:
        cfg: The agency's ``AgencyConfig``; only the ``*_every_ticks``
            ratio matters, not the absolute tick length.

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


def _bucket_exprs(db: Session) -> tuple[str, str, str]:
    """Return the minute-bucket SQL expressions for the session's dialect.

    Postgres uses ``date_trunc``; the SQLite fallback uses ``strftime`` so
    the queries still work when unit tests run against in-memory SQLite.
    """
    dialect = db.bind.dialect.name if db.bind is not None else "postgresql"
    if dialect == "sqlite":
        return (
            "strftime('%Y-%m-%d %H:%M:00', ts)",
            "strftime('%Y-%m-%d %H:%M:00', timestamp)",
            "strftime('%Y-%m-%d %H:%M:00', final_snapshot_ts)",
        )
    return (
        "date_trunc('minute', ts)",
        "date_trunc('minute', timestamp)",
        "date_trunc('minute', final_snapshot_ts)",
    )


def _coverage_minutes(
    db: Session, service_date: date_type, tz_name: str = "America/New_York"
) -> int:
    """Count distinct minute-buckets that have at least one ingest row.

    Unions ``collector_heartbeats.ts``, ``vehicle_positions.timestamp``,
    and ``trip_update_state.final_snapshot_ts`` (see the module docstring
    for why each exists and which regimes it covers). All three columns
    are naive-UTC per the project's storage convention. Each leg is a
    plain range scan on an indexed timestamp column
    (``idx_tus_final_snapshot_ts`` for the trip-update leg); measured at
    ~0.45 s on WMATA's retained window and ~0.2 s on SFMTA's.

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
    bucket_hb, bucket_pos, bucket_tus = _bucket_exprs(db)
    row = db.execute(
        text(
            f"""
            SELECT COUNT(DISTINCT bucket) FROM (
                SELECT {bucket_hb} AS bucket
                FROM collector_heartbeats
                WHERE ts >= :start AND ts < :end
                UNION
                SELECT {bucket_pos} AS bucket
                FROM vehicle_positions
                WHERE timestamp >= :start AND timestamp < :end
                UNION
                SELECT {bucket_tus} AS bucket
                FROM trip_update_state
                WHERE final_snapshot_ts >= :start AND final_snapshot_ts < :end
            ) AS buckets
            """
        ),
        {"start": start_utc, "end": end_utc},
    ).first()
    return int(row[0]) if row and row[0] is not None else 0


def has_trip_update_signal(
    db: Session, service_date: date_type, tz_name: str = "America/New_York"
) -> bool:
    """Return True iff any ``trip_update_state`` row was polled inside the local day.

    Distinguishes "the trip-update signal is absent for this date"
    (retention has pruned it, or the date predates trip-update
    collection) from "the signal is present but thin" (a real outage).
    :func:`resolve_data_quality` uses it to avoid downgrading a stamp
    that was earned while the signal was still available.

    Args:
        db: SQLAlchemy session.
        service_date: Operational date in ``tz_name``.
        tz_name: IANA timezone name.

    Returns:
        True when at least one row has ``final_snapshot_ts`` in the window.
    """
    start_utc, end_utc = local_day_bounds_utc(service_date, tz_name)
    row = db.execute(
        text(
            "SELECT 1 FROM trip_update_state "
            "WHERE final_snapshot_ts >= :start AND final_snapshot_ts < :end LIMIT 1"
        ),
        {"start": start_utc, "end": end_utc},
    ).first()
    return row is not None


def coverage_pct_for_date(
    db: Session, service_date: date_type, tz_name: str = "America/New_York"
) -> float:
    """Return the fraction of in-day minute-buckets with ingest coverage.

    A healthy WMATA day scores ~1.00; the 2026-05-24 power-loss incident
    scored ~0.51 (AM-only). Healthy SFMTA days score 0.56–0.81 against
    their cadence-aware threshold of ~0.53 (see the module docstring).

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

    The default threshold is the flat 80% constant; per-agency callers
    pass :func:`agency_coverage_threshold` instead. This is the raw
    comparison with no memory of a prior stamp — see
    :func:`resolve_data_quality` for the stamp the pipelines actually
    write.

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


def resolve_data_quality(
    db: Session,
    service_date: date_type,
    threshold: float = MIN_COVERAGE_FOR_MATERIALIZATION,
    tz_name: str = "America/New_York",
    prior: tuple[str, float | None] | None = None,
) -> tuple[str, float | None, bool]:
    """Decide the ``data_quality`` stamp for a date, honoring an earned prior.

    The rule:

    1. Measure coverage once; if it meets ``threshold`` the date is
       ``complete``.
    2. Otherwise, if the date already carries a ``complete`` stamp AND
       the trip-update leg has no rows at all for the date, keep the
       prior stamp and its recorded coverage — the signal that earned
       the stamp has since been pruned by retention, and re-measuring
       without it would misreport a healthy day as an outage.
    3. Otherwise the date is ``partial``.

    Rule 2 can never *promote* a date and never fires for a date whose
    trip-update rows are present-but-thin (a genuine outage), so a bad
    day still stamps ``partial`` on first materialization and stays so.

    Args:
        db: SQLAlchemy session.
        service_date: Operational date in ``tz_name``.
        threshold: Minimum coverage fraction for ``complete``.
        tz_name: IANA timezone name.
        prior: ``(data_quality, coverage_pct)`` from the row being
            re-stamped, or None when the date has never been materialized.

    Returns:
        ``(data_quality, coverage_pct, kept)``. When ``kept`` is True the
        returned coverage is the prior's stored value — possibly ``None``
        for rows that predate the column — and callers must NOT overwrite
        the stored coverage with anything else: a kept stamp is
        deliberately not re-measured, so a fresh (signal-less) number
        would be a fabrication.
    """
    pct = coverage_pct_for_date(db, service_date, tz_name)
    if pct >= threshold:
        return "complete", pct, False
    if (
        prior is not None
        and prior[0] == "complete"
        and not has_trip_update_signal(db, service_date, tz_name)
    ):
        return "complete", prior[1], True
    return "partial", pct, False
