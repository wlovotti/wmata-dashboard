"""Local-DB retention tiers: what may be pruned, and the guards that say no.

The laptop cannot keep every row forever. Raw GTFS-RT lives in S3 and any
service date can be re-derived from it one day at a time
(``pipelines/refresh_by_day.py``), so the local database keeps three tiers
per agency (numbers in ``config/agencies/<agency>.yaml`` → ``retention:``,
parsed into :class:`src.agency_config.RetentionPolicy`):

==========================================  ==============  ==============
data                                        WMATA (deep)    peers (shallow)
==========================================  ==============  ==============
``stop_events``, ``runs``                   100 days        35 days
``vehicle_positions``, ``trip_update_state``  7 days          7 days
daily aggregates                            forever         forever
==========================================  ==============  ==============

**Window anchor.** Both windows count back from the *latest fully derived
service date* (:func:`latest_derived_date`: newest date with a
``complete``, non-null ``system_metrics_daily`` row that still has
``runs``), capped at agency-local yesterday — never from "today". Anchoring
on today would make weeks of not running anything eat the window (after
three idle weeks a 35-day window would hold 14 days of detail), and while a
backfill has only reached mid-October the anchor stays mid-October, so the
tool only ever keeps *more* than a today-anchored window would. With no
derived date at all, nothing is pruned.

**Guards** (each date / UTC day is checked again immediately before its
delete):

- stop-level rows of date D are deleted only when D's daily aggregates are
  all present and final: a ``complete`` ``system_metrics_daily`` row with
  non-null OTP and service-delivered, plus ``route_metrics_daily_overlay``
  and ``route_headway_metrics`` rows (:func:`aggregate_status`); D is not
  an unfinished date in the refresh progress file; and D can be re-derived
  from the S3 archives (its VP window starts at or after the first VP
  archive file the loader manifested — WMATA and SFMTA VP JSONL begins
  2026-08-22 ~17:10 UTC) unless the operator passes ``allow_unrederivable``.
- a VP UTC day is deleted only when every row in it came from manifested
  archive files (rows ≤ manifested row count — the same test
  ``load_vp_archive.reconcile_utc_day`` uses), so the day can be reloaded;
  and no not-yet-derived service date reads it. Manifest rows are kept, so
  ``reconcile_utc_day`` sees "fewer rows than manifested" and reloads the
  whole day when ``refresh_by_day --date D`` needs it again.
- ``runs`` are deleted before ``stop_events`` for each date, so an
  interrupted run never leaves a date that has ``runs`` (which the API and
  the per-date rollup guard treat as "detail present") without its
  ``stop_events``; the remaining ``stop_events`` are picked up next run.

``trip_update_state`` uses the existing
``pipelines/cleanup_trip_update_state.run_cleanup`` (today-anchored; a
date whose state goes before it is derived re-folds from S3 on replay).

Rolling-window tables (``route_diagnostic_*``, ``cross_route_segment_rollup``,
``corridor_slip_rollup``) need nothing: the housekeeping refresh rebuilds
them from the last 30 days of ``stop_events``, which every tier keeps.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from src.agency_config import RetentionPolicy
from src.models import (
    RouteHeadwayMetrics,
    RouteMetricsDailyOverlay,
    Run,
    StopEvent,
    SystemMetricsDaily,
    VehiclePosition,
    VpArchiveLoadedFile,
)
from src.raw_archive import VP_TIMESTAMP_GUARD_SEC, archive_file_date, vp_utc_dates
from src.timezones import (
    from_epoch_naive_utc,
    local_service_date_position_window_utc,
    local_today,
)

DEFAULT_BATCH_SIZE = 50_000

# Refresh progress statuses that mean "this date is not finished" — never
# prune its stop-level rows or the raw VP it reads.
UNFINISHED_STATUSES = ("failed", "held", "deferred", "derived")

STATUS_OK = "ok"

_ARCHIVE_EPOCH_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\.\d+\.(\d+)\.jsonl\.zst$")


# ---------------------------------------------------------------------------
# Small shared queries
# ---------------------------------------------------------------------------


def _iso(d: date) -> str:
    """ISO ``YYYY-MM-DD`` string (the String ``service_date`` columns' format)."""
    return d.isoformat()


def has_stop_level_rows(session: Session, service_date: date) -> bool:
    """True when ``service_date`` has both ``runs`` and ``stop_events`` rows.

    Both lookups ride the ``(service_date, ...)`` unique indexes, so this is
    two index probes. Retention deletes ``runs`` first, so "runs present"
    alone already implies the date was not pruned; requiring ``stop_events``
    too also catches a manual partial delete.
    """
    iso = _iso(service_date)
    has_runs = session.execute(select(Run.id).where(Run.service_date == iso).limit(1)).first()
    if has_runs is None:
        return False
    has_se = session.execute(
        select(StopEvent.id).where(StopEvent.service_date == iso).limit(1)
    ).first()
    return has_se is not None


def latest_derived_date(session: Session, tz_name: str) -> date | None:
    """Newest service date that is fully derived, capped at agency-local yesterday.

    Fully derived = a ``system_metrics_daily`` row stamped ``complete`` with
    non-null ``otp_percentage`` AND ``runs`` rows still present. Partial,
    null, or runs-less dates are skipped (they never anchor a window).
    Returns None when no date qualifies — callers then prune nothing.
    """
    cap = _iso(local_today(tz_name) - timedelta(days=1))
    candidates = session.execute(
        select(SystemMetricsDaily.service_date)
        .where(
            SystemMetricsDaily.service_date <= cap,
            SystemMetricsDaily.data_quality == "complete",
            SystemMetricsDaily.otp_percentage.isnot(None),
        )
        .order_by(SystemMetricsDaily.service_date.desc())
        .limit(60)
    ).all()
    for (iso,) in candidates:
        if session.execute(select(Run.id).where(Run.service_date == iso).limit(1)).first():
            return date.fromisoformat(iso)
    return None


def aggregate_status(session: Session, service_date: date) -> str:
    """Return ``"ok"`` when ``service_date``'s daily aggregates are present and final.

    Otherwise a short reason: ``"no system_metrics_daily row"``,
    ``"system_metrics_daily partial"``, ``"system_metrics_daily null
    OTP/service-delivered"``, ``"no route_metrics_daily_overlay rows"``,
    ``"no route_headway_metrics rows"``. Deleting stop-level rows of a
    date that is not ``ok`` would orphan (or lose) its aggregates.
    """
    iso = _iso(service_date)
    row = session.execute(
        select(
            SystemMetricsDaily.data_quality,
            SystemMetricsDaily.otp_percentage,
            SystemMetricsDaily.service_delivered_ratio,
        ).where(SystemMetricsDaily.service_date == iso)
    ).first()
    if row is None:
        return "no system_metrics_daily row"
    if row.data_quality != "complete":
        return f"system_metrics_daily {row.data_quality}"
    if row.otp_percentage is None or row.service_delivered_ratio is None:
        return "system_metrics_daily null OTP/service-delivered"
    overlay = session.execute(
        select(RouteMetricsDailyOverlay.id)
        .where(RouteMetricsDailyOverlay.service_date == iso)
        .limit(1)
    ).first()
    if overlay is None:
        return "no route_metrics_daily_overlay rows"
    headway = session.execute(
        select(RouteHeadwayMetrics.id).where(RouteHeadwayMetrics.date == iso).limit(1)
    ).first()
    if headway is None:
        return "no route_headway_metrics rows"
    return STATUS_OK


def read_progress_statuses(path: Path | None) -> dict[date, str]:
    """Read ``refresh_progress_<agency>.json`` (read-only) into ``{date: status}``.

    A missing file is empty. An unreadable file raises ``ValueError``:
    retention must not guess which dates are unfinished.
    """
    if path is None or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        entries = data.get("dates", {})
        if not isinstance(entries, dict):
            raise ValueError("unexpected structure")
    except (OSError, ValueError) as exc:
        raise ValueError(f"progress file {path} unreadable ({exc}); refusing to guess") from exc
    out: dict[date, str] = {}
    for key, entry in entries.items():
        if isinstance(entry, dict) and isinstance(entry.get("status"), str):
            try:
                out[date.fromisoformat(key)] = entry["status"]
            except ValueError:
                continue
    return out


def vp_archive_start(session: Session) -> datetime | None:
    """Naive-UTC open time of the earliest manifested VP archive file, or None.

    Files are named ``YYYY-MM-DD.<pid>.<open_epoch>.jsonl.zst``; a file
    opened at epoch E holds only rows collected at or after E, so no
    archive row predates this instant. Legacy ``YYYY-MM-DD.jsonl.zst``
    names count from that UTC midnight.
    """
    starts: list[datetime] = []
    for (name,) in session.execute(select(VpArchiveLoadedFile.filename)):
        m = _ARCHIVE_EPOCH_RE.match(name)
        if m:
            starts.append(from_epoch_naive_utc(int(m.group(1))))
        else:
            d = archive_file_date(name)
            if d is not None:
                starts.append(datetime(d.year, d.month, d.day))
    return min(starts) if starts else None


def is_rederivable(service_date: date, tz_name: str, archive_start: datetime | None) -> bool:
    """True when ``service_date``'s whole VP read window is inside the S3 VP archive.

    The proximity derive (OTP's primary source) needs VP from the window
    start minus the phantom-timestamp guard; if the archive starts later,
    ``refresh_by_day --date D`` cannot rebuild D's ``stop_events``.
    """
    if archive_start is None:
        return False
    start, _end = local_service_date_position_window_utc(service_date, tz_name)
    return start - timedelta(seconds=VP_TIMESTAMP_GUARD_SEC) >= archive_start


def avg_row_bytes(session: Session, table: str) -> float | None:
    """Average on-disk bytes per row (heap + indexes + TOAST), or None off Postgres.

    ``pg_total_relation_size / reltuples`` — an estimate for sizing a
    delete, not an exact figure (bloat, fillfactor and index dedup skew it).
    """
    if session.get_bind().dialect.name != "postgresql":
        return None
    row = session.execute(
        text(
            "SELECT pg_total_relation_size(c.oid)::float8, c.reltuples::float8 "
            "FROM pg_class c WHERE c.oid = to_regclass(:t)"
        ),
        {"t": table},
    ).first()
    if row is None or not row[1] or row[1] <= 0:
        return None
    return row[0] / row[1]


def _distinct_dates_below(session: Session, column, cutoff_iso: str) -> list[str]:
    """Distinct values of a String ``service_date``-like column below ``cutoff_iso``.

    A loose index scan (one ``min()`` probe per distinct value), so it is
    cheap on the 90M-row ``stop_events`` whose unique index leads with
    ``service_date``.
    """
    out: list[str] = []
    current = session.execute(select(func.min(column)).where(column < cutoff_iso)).scalar()
    while current is not None:
        out.append(current)
        current = session.execute(
            select(func.min(column)).where(column > current, column < cutoff_iso)
        ).scalar()
    return out


def _count(session: Session, model, *where) -> int:
    """``SELECT count(*) FROM model WHERE ...``."""
    return session.execute(select(func.count()).select_from(model).where(*where)).scalar_one()


def _batched_delete(
    session: Session, model, where, batch_size: int, log: Callable[[str], None], label: str
) -> int:
    """Delete rows of ``model`` matching ``where`` in ``batch_size`` id batches, committing each.

    ``DELETE ... WHERE id IN (SELECT id ... LIMIT n)`` keeps every statement
    and transaction small (and is dialect-portable for the SQLite tests).
    """
    from sqlalchemy import delete

    total = 0
    while True:
        ids = select(model.id).where(where).limit(batch_size).scalar_subquery()
        deleted = session.execute(delete(model).where(model.id.in_(ids))).rowcount or 0
        session.commit()
        total += deleted
        if deleted:
            log(f"    {label}: deleted {deleted} (running total {total})")
        if deleted < batch_size:
            return total


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------


@dataclass
class DatePlan:
    """One service date's stop-level rows and whether they may be deleted."""

    service_date: date
    stop_events: int
    runs: int
    reason: str | None  # None = deletable; otherwise why it is kept

    @property
    def deletable(self) -> bool:
        """True when every guard passed."""
        return self.reason is None


@dataclass
class VpDayPlan:
    """One UTC day of ``vehicle_positions`` and whether it may be deleted."""

    utc_day: date
    rows: int
    manifested: int
    reason: str | None

    @property
    def deletable(self) -> bool:
        """True when every guard passed."""
        return self.reason is None


@dataclass
class RetentionPlan:
    """Everything one agency's retention run would delete (and keep, and why)."""

    agency: str
    policy: RetentionPolicy
    anchor: date | None
    stop_level_first_kept: date | None = None
    raw_first_kept: date | None = None
    vp_first_kept_utc_day: date | None = None
    dates: list[DatePlan] = field(default_factory=list)
    vp_days: list[VpDayPlan] = field(default_factory=list)
    tus_rows: int = 0
    tus_cutoff: date | None = None
    bytes_per_row: dict[str, float | None] = field(default_factory=dict)

    def deletable_dates(self) -> list[DatePlan]:
        """Stop-level dates that pass every guard."""
        return [d for d in self.dates if d.deletable]

    def deletable_vp_days(self) -> list[VpDayPlan]:
        """VP UTC days that pass every guard."""
        return [d for d in self.vp_days if d.deletable]


def plan_retention(
    session: Session,
    agency: str,
    tz_name: str,
    policy: RetentionPolicy,
    progress: dict[date, str] | None = None,
    allow_unrederivable: bool = False,
    include_unmanifested_vp: bool = False,
    allow_partial_dates: set[date] | None = None,
    today: date | None = None,
) -> RetentionPlan:
    """Work out what retention would delete for one agency (reads only).

    Args:
        session: Session on the agency database.
        agency: Agency name (labels only).
        tz_name: Agency IANA timezone (service-date + VP-window math).
        policy: The agency's retention tiers.
        progress: ``{date: status}`` from the refresh progress file; dates
            in ``UNFINISHED_STATUSES`` are never pruned and their VP days
            are protected.
        allow_unrederivable: Also delete stop-level rows of dates whose VP
            window predates the S3 VP archive (they can never be re-derived
            afterwards — the aggregates remain).
        include_unmanifested_vp: Also delete VP UTC days holding rows the
            archive manifest cannot account for (the VM era, through
            2026-08-22; WMATA's ``wmata-vp-archive/`` parquet covers only
            part of it, so the DB may be the only copy).
        allow_partial_dates: Dates whose aggregates exist but are stamped
            ``partial`` that the operator explicitly accepts pruning.
        today: Agency-local today override (tests).
    """
    progress = progress or {}
    allow_partial_dates = allow_partial_dates or set()
    anchor = latest_derived_date(session, tz_name)
    plan = RetentionPlan(agency=agency, policy=policy, anchor=anchor)
    for table in ("stop_events", "runs", "vehicle_positions", "trip_update_state"):
        plan.bytes_per_row[table] = avg_row_bytes(session, table)
    if anchor is None:
        return plan

    plan.stop_level_first_kept = anchor - timedelta(days=policy.stop_level_days - 1)
    plan.raw_first_kept = anchor - timedelta(days=policy.raw_days - 1)
    archive_start = vp_archive_start(session)

    cutoff_iso = _iso(plan.stop_level_first_kept)
    candidate_isos = sorted(
        set(_distinct_dates_below(session, StopEvent.service_date, cutoff_iso))
        | set(_distinct_dates_below(session, Run.service_date, cutoff_iso))
    )
    for iso in candidate_isos:
        d = date.fromisoformat(iso)
        reason = stop_level_refusal(
            session,
            d,
            tz_name,
            progress,
            archive_start,
            allow_unrederivable=allow_unrederivable,
            allow_partial=d in allow_partial_dates,
        )
        plan.dates.append(
            DatePlan(
                service_date=d,
                stop_events=_count(session, StopEvent, StopEvent.service_date == iso),
                runs=_count(session, Run, Run.service_date == iso),
                reason=reason,
            )
        )

    plan.vp_first_kept_utc_day = min(vp_utc_dates(plan.raw_first_kept, tz_name))
    protected = _vp_days_needed_by_unfinished(session, tz_name, progress, plan, today)
    cutoff_ts = datetime.combine(plan.vp_first_kept_utc_day, datetime.min.time())
    for utc_day in _distinct_vp_days_below(session, cutoff_ts):
        rows = _vp_day_rows(session, utc_day)
        manifested = _manifested_rows(session, utc_day)
        reason = None
        if utc_day in protected:
            reason = f"needed by not-yet-derived {protected[utc_day]}"
        elif rows > manifested and not include_unmanifested_vp:
            reason = (
                f"{rows - manifested} rows not from manifested archive files "
                "(VM era; S3 JSONL cannot reload them)"
            )
        plan.vp_days.append(VpDayPlan(utc_day, rows, manifested, reason))

    from pipelines.cleanup_trip_update_state import run_cleanup

    plan.tus_cutoff = local_today(tz_name) - timedelta(days=policy.raw_days)
    plan.tus_rows = run_cleanup(
        session, retention_days=policy.raw_days, tz_name=tz_name, dry_run=True
    )["deleted"]
    return plan


def stop_level_refusal(
    session: Session,
    service_date: date,
    tz_name: str,
    progress: dict[date, str],
    archive_start: datetime | None,
    allow_unrederivable: bool = False,
    allow_partial: bool = False,
) -> str | None:
    """Return why ``service_date``'s stop-level rows must be kept, or None if deletable."""
    status = progress.get(service_date)
    if status in UNFINISHED_STATUSES:
        return f"refresh progress says {status!r}"
    agg = aggregate_status(session, service_date)
    if agg != STATUS_OK and not (allow_partial and agg == "system_metrics_daily partial"):
        return f"aggregates not final: {agg}"
    if not allow_unrederivable and not is_rederivable(service_date, tz_name, archive_start):
        return "not re-derivable from S3 (VP window predates the VP archive)"
    return None


def _distinct_vp_days_below(session: Session, cutoff_ts: datetime) -> list[date]:
    """UTC days (of ``collected_at``) with any VP row before ``cutoff_ts`` (loose scan)."""
    out: list[date] = []
    ts = session.execute(
        select(func.min(VehiclePosition.collected_at)).where(
            VehiclePosition.collected_at < cutoff_ts
        )
    ).scalar()
    while ts is not None:
        if isinstance(ts, str):  # SQLite returns text
            ts = datetime.fromisoformat(ts)
        day = ts.date()
        out.append(day)
        next_start = datetime.combine(day + timedelta(days=1), datetime.min.time())
        ts = session.execute(
            select(func.min(VehiclePosition.collected_at)).where(
                VehiclePosition.collected_at >= next_start,
                VehiclePosition.collected_at < cutoff_ts,
            )
        ).scalar()
    return out


def _vp_day_bounds(utc_day: date) -> tuple[datetime, datetime]:
    """Half-open naive-UTC ``collected_at`` bounds of one UTC day."""
    start = datetime.combine(utc_day, datetime.min.time())
    return start, start + timedelta(days=1)


def _vp_day_rows(session: Session, utc_day: date) -> int:
    """Rows of ``vehicle_positions`` collected on ``utc_day``."""
    start, end = _vp_day_bounds(utc_day)
    return _count(
        session,
        VehiclePosition,
        VehiclePosition.collected_at >= start,
        VehiclePosition.collected_at < end,
    )


def _manifested_rows(session: Session, utc_day: date) -> int:
    """Rows the manifested archive files named for ``utc_day`` inserted."""
    from pipelines.load_vp_archive import manifested_day

    return sum(manifested_day(session, utc_day).values())


def _vp_days_needed_by_unfinished(
    session: Session,
    tz_name: str,
    progress: dict[date, str],
    plan: RetentionPlan,
    today: date | None,
) -> dict[date, date]:
    """Map each VP UTC day a not-yet-derived service date reads to that date.

    Not yet derived = no ``system_metrics_daily`` row at all, or an
    unfinished progress status. Only dates whose VP window can reach
    below the VP cutoff are examined (later dates are kept anyway).
    """
    yesterday = (today or local_today(tz_name)) - timedelta(days=1)
    first_vp = session.execute(select(func.min(VehiclePosition.collected_at))).scalar()
    if first_vp is None:
        return {}
    if isinstance(first_vp, str):
        first_vp = datetime.fromisoformat(first_vp)
    lo = first_vp.date() - timedelta(days=2)
    hi = min(plan.raw_first_kept, yesterday)
    have_rows = {
        iso
        for (iso,) in session.execute(
            select(SystemMetricsDaily.service_date).where(
                SystemMetricsDaily.service_date >= _iso(lo),
                SystemMetricsDaily.service_date <= _iso(hi),
            )
        )
    }
    needed: dict[date, date] = {}
    d = lo
    while d <= hi:
        if _iso(d) not in have_rows or progress.get(d) in UNFINISHED_STATUSES:
            for u in vp_utc_dates(d, tz_name):
                needed.setdefault(u, d)
        d += timedelta(days=1)
    return needed


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


@dataclass
class RetentionResult:
    """Rows actually deleted by :func:`apply_retention`."""

    stop_events: int = 0
    runs: int = 0
    vehicle_positions: int = 0
    trip_update_state: int = 0
    skipped: list[str] = field(default_factory=list)


def apply_retention(
    session: Session,
    plan: RetentionPlan,
    tz_name: str,
    progress: dict[date, str] | None = None,
    allow_unrederivable: bool = False,
    include_unmanifested_vp: bool = False,
    allow_partial_dates: set[date] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    log: Callable[[str], None] = print,
) -> RetentionResult:
    """Delete what ``plan`` marked deletable, re-checking every guard first.

    Order: stop-level (per date: ``runs`` then ``stop_events``), then VP
    UTC days, then ``trip_update_state``. Every batch commits, so an
    interruption loses nothing that a rerun would not redo. A date or day
    whose guard no longer passes is skipped and listed in ``skipped``.
    """
    progress = progress or {}
    allow_partial_dates = allow_partial_dates or set()
    result = RetentionResult()
    archive_start = vp_archive_start(session)
    first_kept = plan.stop_level_first_kept
    for dp in plan.deletable_dates():
        d = dp.service_date
        reason = None
        if first_kept is None or d >= first_kept:
            reason = "inside the window"
        else:
            reason = stop_level_refusal(
                session,
                d,
                tz_name,
                progress,
                archive_start,
                allow_unrederivable=allow_unrederivable,
                allow_partial=d in allow_partial_dates,
            )
        session.rollback()  # end the read transaction before the deletes
        if reason:
            result.skipped.append(f"{d}: {reason}")
            log(f"  {d}: SKIPPED ({reason})")
            continue
        log(f"  {d}: deleting runs, then stop_events")
        iso = _iso(d)
        result.runs += _batched_delete(
            session, Run, Run.service_date == iso, batch_size, log, f"{d} runs"
        )
        result.stop_events += _batched_delete(
            session, StopEvent, StopEvent.service_date == iso, batch_size, log, f"{d} stop_events"
        )

    vp_cutoff = plan.vp_first_kept_utc_day
    protected = (
        _vp_days_needed_by_unfinished(session, tz_name, progress, plan, None)
        if plan.raw_first_kept
        else {}
    )
    for vp in plan.deletable_vp_days():
        reason = None
        if vp_cutoff is None or vp.utc_day >= vp_cutoff:
            reason = "inside the window"
        elif vp.utc_day in protected:
            reason = f"needed by not-yet-derived {protected[vp.utc_day]}"
        elif (
            _vp_day_rows(session, vp.utc_day) > _manifested_rows(session, vp.utc_day)
            and not include_unmanifested_vp
        ):
            reason = "rows not from manifested archive files"
        session.rollback()
        if reason:
            result.skipped.append(f"VP {vp.utc_day}: {reason}")
            log(f"  VP {vp.utc_day}: SKIPPED ({reason})")
            continue
        start, end = _vp_day_bounds(vp.utc_day)
        log(f"  VP {vp.utc_day}: deleting (manifest rows kept so the day reloads)")
        result.vehicle_positions += _batched_delete(
            session,
            VehiclePosition,
            (VehiclePosition.collected_at >= start) & (VehiclePosition.collected_at < end),
            batch_size,
            log,
            f"VP {vp.utc_day}",
        )

    if plan.anchor is not None:
        from pipelines.cleanup_trip_update_state import run_cleanup

        result.trip_update_state = run_cleanup(
            session, retention_days=plan.policy.raw_days, tz_name=tz_name, batch_size=batch_size
        )["deleted"]
    return result


def stop_level_floor(session: Session) -> date | None:
    """Earliest service date whose stop-level detail (``runs``) is still present.

    ``min(runs.service_date)`` — one probe of the ``(service_date, ...)``
    unique index. Retention deletes ``runs`` before ``stop_events``, so a
    date at or after this floor has its detail (or never had any data).
    """
    iso = session.execute(select(func.min(Run.service_date))).scalar()
    return date.fromisoformat(iso) if iso else None
