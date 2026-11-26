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

A date is ``complete`` only if it passes BOTH legs of the check
(:func:`assess_data_quality`):

1. **Coverage** — the fraction of the local clock-day's minutes that have
   at least one ingest row, unioned across three signals (NOTES-104),
   must reach the agency's cadence-aware threshold
   (:func:`agency_coverage_threshold`).
2. **Per-feed gap** (issue #270) — separately for the trip-update feed
   and the vehicle-position feed, the longest silence inside the
   agency-local service-hours window (default 05:00–24:00) must not
   exceed ``CompletenessPolicy.feed_gap_max_minutes`` (default 90; both
   per-agency in ``config/agencies/*.yaml`` ``completeness:``).

Why the second leg exists: the coverage union cannot see a single-feed
outage. Losing one stream costs SFMTA only ~0.33 × hours/24 of coverage,
so a trip-update blackout of up to ~9 h still clears 0.533 (SFMTA
2026-10-21: a 5.7 h trip-update outage passed at 0.564); at WMATA either
stream alone reaches ~1.00, so an outage of one feed of any length was
invisible. The per-feed gap catches exactly those days. Raising the
coverage threshold was rejected (it flags healthy SFMTA days and does
nothing for WMATA), as was a service-delivered or EWT floor (those would
exclude genuinely bad service days, which are what an advocacy profile
must report, not hide).

The coverage signals:

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

The gap legs' timestamps:

- **Trip updates**: ``trip_update_state.final_snapshot_ts`` of rows whose
  ``service_date`` is the date being stamped. Keying on the row's own
  service date (not on the clock window) makes the leg all-or-nothing
  under every deletion path — per-date and end-of-run
  ``cleanup_trip_update_state`` and retention all delete whole service
  dates — so a pruned date reads as *not assessable* rather than as a
  day-long gap (issue #272: the next date's replay leaves a few rows in
  the earlier date's evening window). In ``pipelines/refresh_by_day.py``
  the date's state is present when it is stamped: the per-date flow
  replays D, derives and aggregates D, and only then deletes state for
  dates *before* D, so D's own stamp — and D-1's re-aggregation by D's
  derive — both see their rows.
- **Vehicle positions**: distinct ``vehicle_positions.collected_at`` (the
  poll time). Retention and the VP loader both work in whole UTC days of
  ``collected_at``, so the leg is assessable only when every UTC day the
  service window overlaps has at least one row.

Honesty about missing inputs: raw VP and ``trip_update_state`` are kept
only 7 days (``retention.raw_days``), so re-aggregating an older date
sees neither. A leg whose inputs are absent is *not assessable*: it never
stamps a date ``partial`` and never upgrades one. When a date that
already carries a stamp is re-evaluated with any leg not assessable, the
stored stamp and its coverage are kept (``kept``), unless an assessable
gap leg fails outright — a real gap is real evidence. A date stamped for
the first time with a leg not assessable is decided by the legs that are
(and the decision records which leg was skipped): the WMATA pull
tolerates a VP loader failure by design, and such a day is still stamped
from the trip-update legs, with a loud warning.

``trip_update_snapshots`` has been retired (Phase F, PR #155) and plays
no part here.
"""

from dataclasses import dataclass, field
from datetime import date as date_type
from datetime import datetime, time, timedelta
from math import lcm
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.agency_config import AgencyConfig, CompletenessPolicy
from src.timezones import UTC, local_day_bounds_utc

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
    poller phase drift (see the module docstring). One did slip through
    (SFMTA 2026-10-21, a 5.7 h trip-update outage at 0.564); the answer
    was the per-feed gap leg in :func:`assess_data_quality` (issue #270),
    not a higher threshold, which would flag healthy SFMTA days.

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
    """Return True iff ``trip_update_state`` holds any row for this service date.

    Distinguishes "the trip-update signal is absent for this date"
    (retention or the refresh's state cleanup has pruned it, or the date
    predates trip-update collection) from "the signal is present but thin"
    (a real outage). :func:`assess_data_quality` uses it to avoid
    re-measuring a stamp that was earned while the signal was available,
    and as the trip-update gap leg's assessability test.

    Keys on the row's ``service_date`` column, not on whether its
    ``final_snapshot_ts`` falls in the local day (issue #272): the next
    date's replay leaves a few rows polled inside this date's evening
    window, which made a date whose own state had been deleted look
    "present but thin" and demoted it. Every deletion path removes whole
    service dates, so this test is all-or-nothing.

    Args:
        db: SQLAlchemy session.
        service_date: Operational date in ``tz_name``.
        tz_name: Unused (kept for call-site compatibility; the
            ``service_date`` column already carries the agency-local date).

    Returns:
        True when at least one ``trip_update_state`` row has
        ``service_date == service_date``.
    """
    del tz_name  # the service_date column is already agency-local
    row = db.execute(
        text("SELECT 1 FROM trip_update_state WHERE service_date = :sd LIMIT 1"),
        {"sd": service_date},
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
    pass :func:`agency_coverage_threshold` instead. This is the coverage
    leg alone, with no memory of a prior stamp and no per-feed gap test —
    see :func:`assess_data_quality` for the stamp the pipelines actually
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


TRIP_UPDATE_FEED = "trip_update"
VEHICLE_POSITION_FEED = "vehicle_position"


def service_window_utc(
    service_date: date_type,
    tz_name: str = "America/New_York",
    policy: CompletenessPolicy | None = None,
) -> tuple[datetime, datetime]:
    """Return the naive-UTC bounds of the agency-local service-hours window.

    The window runs from ``service_start_hour`` to ``service_end_hour``
    agency-local wall-clock time on ``service_date`` (24 = the following
    local midnight). Each bound is converted separately through the zone,
    so the window always starts and ends at those local wall-clock times.
    US transitions happen at 02:00, outside the default 05–24 window, so
    on a DST day that window is still 19 real hours; a configured window
    spanning 02:00 would be one hour shorter or longer that day.

    Args:
        service_date: Operational date in ``tz_name``.
        tz_name: IANA timezone name of the agency.
        policy: Gap policy; None means the defaults.

    Returns:
        ``(start_utc, end_utc)`` as naive UTC datetimes.
    """
    policy = policy or CompletenessPolicy()
    zone = ZoneInfo(tz_name)

    def _at(hour: int) -> datetime:
        """Naive UTC for ``hour`` o'clock local on the service date (24 = next midnight)."""
        day = service_date + timedelta(days=hour // 24)
        local = datetime.combine(day, time(hour % 24), tzinfo=zone)
        return local.astimezone(UTC).replace(tzinfo=None)

    return _at(policy.service_start_hour), _at(policy.service_end_hour)


@dataclass(frozen=True)
class FeedGap:
    """The longest silence of one feed inside a date's service-hours window.

    Attributes:
        feed: ``TRIP_UPDATE_FEED`` or ``VEHICLE_POSITION_FEED``.
        assessable: False when the feed's rows for the date are absent
            (pruned, never loaded, or the date predates collection); the
            gap fields are then None and the leg decides nothing.
        longest_gap_min: Longest gap in minutes, including the stretch from
            the window start to the first poll and from the last poll to the
            window end (so an outage running into midnight counts).
        gap_start_utc: Naive-UTC start of that gap.
        gap_end_utc: Naive-UTC end of that gap.
        note: Why the leg is not assessable (empty when it is).
    """

    feed: str
    assessable: bool
    longest_gap_min: float | None = None
    gap_start_utc: datetime | None = None
    gap_end_utc: datetime | None = None
    note: str = ""

    def describe(self) -> str:
        """One-line human-readable summary for logs and the restamp report."""
        if not self.assessable:
            return f"{self.feed} gap not assessed ({self.note})"
        return (
            f"{self.feed} longest gap {self.longest_gap_min:.0f} min "
            f"({self.gap_start_utc:%H:%M}Z-{self.gap_end_utc:%H:%M}Z)"
        )


def _as_datetime(value) -> datetime:
    """Coerce a DB timestamp to ``datetime`` (SQLite raw SQL returns strings)."""
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def longest_gap(
    timestamps, start_utc: datetime, end_utc: datetime
) -> tuple[float, datetime, datetime]:
    """Return the longest silence within ``[start_utc, end_utc]`` given poll timestamps.

    Edges count: the gap from ``start_utc`` to the first timestamp and from
    the last timestamp to ``end_utc`` are candidates, so a feed that stops
    at 18:00 and never returns shows a gap running to the window end. With
    no timestamps at all the whole window is one gap.

    Args:
        timestamps: Iterable of naive-UTC datetimes (any order; values
            outside the window are ignored).
        start_utc: Window start.
        end_utc: Window end.

    Returns:
        ``(gap_minutes, gap_start, gap_end)``.
    """
    points = sorted(t for t in (_as_datetime(v) for v in timestamps) if start_utc <= t <= end_utc)
    best = (0.0, start_utc, start_utc)
    prev = start_utc
    for t in [*points, end_utc]:
        gap = (t - prev).total_seconds() / 60.0
        if gap > best[0]:
            best = (gap, prev, t)
        prev = t
    return best


def trip_update_feed_gap(
    db: Session,
    service_date: date_type,
    tz_name: str = "America/New_York",
    policy: CompletenessPolicy | None = None,
) -> FeedGap:
    """Measure the trip-update feed's longest service-hours gap for a date.

    Timestamps are ``trip_update_state.final_snapshot_ts`` of the date's
    own rows (``service_date = service_date``) — see the module docstring
    for why the row's service date, not the clock window, decides which
    rows count. Not assessable when the date has no state rows at all.

    Args:
        db: SQLAlchemy session.
        service_date: Operational date in ``tz_name``.
        tz_name: IANA timezone name of the agency.
        policy: Gap policy; None means the defaults.

    Returns:
        A :class:`FeedGap` for ``TRIP_UPDATE_FEED``.
    """
    if not has_trip_update_signal(db, service_date, tz_name):
        return FeedGap(
            TRIP_UPDATE_FEED, False, note="no trip_update_state rows for the date (pruned?)"
        )
    start_utc, end_utc = service_window_utc(service_date, tz_name, policy)
    rows = db.execute(
        text(
            "SELECT DISTINCT final_snapshot_ts FROM trip_update_state "
            "WHERE service_date = :sd AND final_snapshot_ts >= :start "
            "AND final_snapshot_ts <= :end"
        ),
        {"sd": service_date, "start": start_utc, "end": end_utc},
    ).all()
    gap, gap_start, gap_end = longest_gap((r[0] for r in rows), start_utc, end_utc)
    return FeedGap(TRIP_UPDATE_FEED, True, gap, gap_start, gap_end)


def vehicle_position_feed_gap(
    db: Session,
    service_date: date_type,
    tz_name: str = "America/New_York",
    policy: CompletenessPolicy | None = None,
) -> FeedGap:
    """Measure the vehicle-position feed's longest service-hours gap for a date.

    Timestamps are distinct ``vehicle_positions.collected_at`` (poll time).
    VP is loaded and pruned in whole UTC days of ``collected_at``, so the
    leg is assessable only when every UTC day the service window overlaps
    has at least one row; otherwise a pruned or never-loaded day would read
    as a many-hour gap. (A real outage covering an entire UTC day is
    therefore reported as not assessable, not as a gap — the trip-update
    legs still apply.)

    Args:
        db: SQLAlchemy session.
        service_date: Operational date in ``tz_name``.
        tz_name: IANA timezone name of the agency.
        policy: Gap policy; None means the defaults.

    Returns:
        A :class:`FeedGap` for ``VEHICLE_POSITION_FEED``.
    """
    start_utc, end_utc = service_window_utc(service_date, tz_name, policy)
    utc_day = start_utc.date()
    while datetime.combine(utc_day, time()) < end_utc:
        day_start = datetime.combine(utc_day, time())
        present = db.execute(
            text(
                "SELECT 1 FROM vehicle_positions "
                "WHERE collected_at >= :start AND collected_at < :end LIMIT 1"
            ),
            {"start": day_start, "end": day_start + timedelta(days=1)},
        ).first()
        if present is None:
            return FeedGap(
                VEHICLE_POSITION_FEED,
                False,
                note=f"no vehicle_positions collected on UTC {utc_day} (pruned or not loaded)",
            )
        utc_day += timedelta(days=1)
    rows = db.execute(
        text(
            "SELECT DISTINCT collected_at FROM vehicle_positions "
            "WHERE collected_at >= :start AND collected_at <= :end"
        ),
        {"start": start_utc, "end": end_utc},
    ).all()
    gap, gap_start, gap_end = longest_gap((r[0] for r in rows), start_utc, end_utc)
    return FeedGap(VEHICLE_POSITION_FEED, True, gap, gap_start, gap_end)


@dataclass(frozen=True)
class QualityDecision:
    """The ``data_quality`` stamp for one date and everything that decided it.

    Attributes:
        data_quality: ``'complete'`` or ``'partial'``.
        coverage_pct: The coverage to store: the fresh measurement when
            ``coverage_measured``, else the prior row's stored value
            (possibly None for rows that predate the column).
        coverage_measured: False when the coverage was carried over from
            the prior row because an input was absent; callers must then
            leave the stored ``coverage_pct`` untouched.
        kept: True when the prior stamp was kept because a leg could not be
            assessed (the stamp is neither demoted nor upgraded).
        measured_coverage: The raw coverage measured now, for disclosure
            (understated when inputs are pruned).
        threshold: Coverage threshold applied.
        feed_gap_max_minutes: Gap limit applied.
        trip_update_gap: Trip-update gap leg.
        vehicle_position_gap: Vehicle-position gap leg.
        reasons: Why the date is ``partial`` (empty when complete or kept).
        not_assessed: Legs that could not be assessed, with the reason.
    """

    data_quality: str
    coverage_pct: float | None
    coverage_measured: bool
    kept: bool
    measured_coverage: float
    threshold: float
    feed_gap_max_minutes: int
    trip_update_gap: FeedGap
    vehicle_position_gap: FeedGap
    reasons: tuple[str, ...] = field(default_factory=tuple)
    not_assessed: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def fixed(
        cls, data_quality: str, coverage_pct: float | None, kept: bool = False
    ) -> "QualityDecision":
        """A decision with a given stamp and no measured legs (tests and stubs).

        Args:
            data_quality: ``'complete'`` or ``'partial'``.
            coverage_pct: Coverage to report/store.
            kept: When True the coverage counts as carried over (not
                re-measured), like a real kept decision.

        Returns:
            A :class:`QualityDecision` whose gap legs are marked not assessed.
        """
        return cls(
            data_quality=data_quality,
            coverage_pct=coverage_pct,
            coverage_measured=not kept,
            kept=kept,
            measured_coverage=coverage_pct or 0.0,
            threshold=MIN_COVERAGE_FOR_MATERIALIZATION,
            feed_gap_max_minutes=CompletenessPolicy().feed_gap_max_minutes,
            trip_update_gap=FeedGap(TRIP_UPDATE_FEED, False, note="stub"),
            vehicle_position_gap=FeedGap(VEHICLE_POSITION_FEED, False, note="stub"),
        )

    def describe(self) -> str:
        """One-line disclosure of the decision for logs and the restamp report."""
        parts = [
            f"{self.data_quality}{' (kept)' if self.kept else ''}",
            f"coverage {self.measured_coverage:.3f} vs {self.threshold:.3f}",
            self.trip_update_gap.describe(),
            self.vehicle_position_gap.describe(),
            f"gap limit {self.feed_gap_max_minutes} min",
        ]
        if self.reasons:
            parts.append("partial because: " + "; ".join(self.reasons))
        return " | ".join(parts)


def assess_data_quality(
    db: Session,
    service_date: date_type,
    threshold: float = MIN_COVERAGE_FOR_MATERIALIZATION,
    tz_name: str = "America/New_York",
    prior: tuple[str, float | None] | None = None,
    gap_policy: CompletenessPolicy | None = None,
) -> QualityDecision:
    """Decide the ``data_quality`` stamp for a date, honoring an earned prior.

    The rule (see the module docstring for the why):

    1. Measure coverage and both feed-gap legs. A gap leg is assessable
       only when its feed has rows for the date; the coverage leg is
       assessed when every input is present, or when the date has never
       been stamped (``prior`` is None).
    2. Any assessed failure — coverage below ``threshold``, or a feed gap
       longer than ``gap_policy.feed_gap_max_minutes`` — makes the date
       ``partial``. (With a prior and a missing input, the stored coverage
       is carried over rather than replaced by an understated number.)
    3. Otherwise, if every leg was assessed, the date is ``complete``.
    4. Otherwise (a leg is not assessable and nothing assessed failed): a
       date with a prior stamp keeps it and its stored coverage
       (``kept``) — never demoted for missing inputs, never upgraded; a
       date stamped for the first time is ``complete`` on the legs that
       were assessed, and ``not_assessed`` records the skipped leg.

    This keeps the NOTES-104 retention rule (an earned ``complete`` is not
    demoted once its trip-update rows are pruned) and extends it to the gap
    legs; a genuine outage still stamps ``partial`` at first
    materialization, while the inputs are present, and stays so.

    Args:
        db: SQLAlchemy session.
        service_date: Operational date in ``tz_name``.
        threshold: Minimum coverage fraction for ``complete``.
        tz_name: IANA timezone name.
        prior: ``(data_quality, coverage_pct)`` from the row being
            re-stamped, or None when the date has never been materialized.
        gap_policy: Per-agency gap rule (``AgencyConfig.completeness``);
            None means the defaults (90 min, 05:00–24:00 local).

    Returns:
        A :class:`QualityDecision`.
    """
    policy = gap_policy or CompletenessPolicy()
    pct = coverage_pct_for_date(db, service_date, tz_name)
    tu_gap = trip_update_feed_gap(db, service_date, tz_name, policy)
    vp_gap = vehicle_position_feed_gap(db, service_date, tz_name, policy)

    all_assessable = tu_gap.assessable and vp_gap.assessable
    coverage_measured = all_assessable or prior is None
    not_assessed = tuple(f"{g.feed}: {g.note}" for g in (tu_gap, vp_gap) if not g.assessable)
    if not coverage_measured:
        not_assessed += ("coverage: an input is missing; stored value kept",)

    reasons: list[str] = []
    if coverage_measured and pct < threshold:
        reasons.append(f"coverage {pct:.3f} < threshold {threshold:.3f}")
    for g in (tu_gap, vp_gap):
        if g.assessable and g.longest_gap_min > policy.feed_gap_max_minutes:
            reasons.append(
                f"{g.feed} gap {g.longest_gap_min:.0f} min > {policy.feed_gap_max_minutes} min "
                f"({g.gap_start_utc:%Y-%m-%d %H:%M}Z-{g.gap_end_utc:%H:%M}Z)"
            )

    stored = pct if coverage_measured else prior[1]
    common = {
        "measured_coverage": pct,
        "threshold": threshold,
        "feed_gap_max_minutes": policy.feed_gap_max_minutes,
        "trip_update_gap": tu_gap,
        "vehicle_position_gap": vp_gap,
        "not_assessed": not_assessed,
    }
    if reasons:
        return QualityDecision(
            "partial", stored, coverage_measured, False, reasons=tuple(reasons), **common
        )
    if all_assessable or prior is None:
        return QualityDecision("complete", stored, coverage_measured, False, **common)
    return QualityDecision(prior[0], prior[1], False, True, **common)


def resolve_data_quality(
    db: Session,
    service_date: date_type,
    threshold: float = MIN_COVERAGE_FOR_MATERIALIZATION,
    tz_name: str = "America/New_York",
    prior: tuple[str, float | None] | None = None,
    gap_policy: CompletenessPolicy | None = None,
) -> tuple[str, float | None, bool]:
    """Tuple form of :func:`assess_data_quality`: ``(data_quality, coverage_pct, kept)``.

    The third element means "the returned coverage is the prior's stored
    value and must NOT overwrite it" — ``not decision.coverage_measured``.

    Args:
        db: SQLAlchemy session.
        service_date: Operational date in ``tz_name``.
        threshold: Minimum coverage fraction for ``complete``.
        tz_name: IANA timezone name.
        prior: ``(data_quality, coverage_pct)`` of the existing row, or None.
        gap_policy: Per-agency gap rule; None means the defaults.

    Returns:
        ``(data_quality, coverage_pct, coverage_not_remeasured)``.
    """
    d = assess_data_quality(db, service_date, threshold, tz_name, prior, gap_policy)
    return d.data_quality, d.coverage_pct, not d.coverage_measured
