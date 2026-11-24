"""Selecting a GTFS version in queries against the versioned schedule tables.

`scripts/reload_gtfs_complete.py` never deletes schedule rows: a reload marks
the previous snapshot's rows `is_current=False` and inserts the new set, so
every historical snapshot's full row set survives in place. Pipelines
normally pin to `is_current=True` (the project-wide convention), but
backfilling a service date whose schedule has since been superseded needs
the snapshot that was in force on that date — e.g. re-deriving June 2026
dates against snapshot 12 after the July reload made snapshot 13 current.

`resolve_gtfs_snapshot_for_date` picks that snapshot per service date (issue
#258): when a reload lands a feed that starts *today* before *yesterday* has
been derived, yesterday must still be derived against the outgoing snapshot
— `is_current` would point every observed trip_id at a schedule that has
none of them (WMATA 2026-09-12: 0/9,917 matched against snapshot 20).
"""

from dataclasses import dataclass
from datetime import date as date_type
from datetime import timedelta

# How a snapshot was chosen for a date; logged by callers.
REASON_COVERS = "covers"
REASON_FALLBACK_LOADED_BY = "fallback_loaded_by_date"
REASON_FALLBACK_OLDEST = "fallback_oldest"
REASON_FALLBACK_CURRENT = "fallback_current"  # only when no snapshot has calendar rows


def gtfs_version_filter(model, gtfs_snapshot_id=None):
    """Return a SQLAlchemy criterion selecting one GTFS version of ``model``.

    With ``gtfs_snapshot_id=None`` (the default), selects the live snapshot
    via ``is_current`` — identical to the conventional filter. With an int,
    selects that historical snapshot's rows regardless of currency.

    ``model`` must be one of the versioned GTFS models (Route, Stop, Trip,
    StopTime, Calendar, CalendarDate) — anything with both ``is_current``
    and ``snapshot_id`` columns.
    """
    if gtfs_snapshot_id is None:
        return model.is_current
    return model.snapshot_id == gtfs_snapshot_id


@dataclass(frozen=True)
class SnapshotChoice:
    """The GTFS snapshot resolved for one service date.

    Attributes:
        snapshot_id: The chosen snapshot, or None when the database has no
            usable snapshot at all (callers then fall back to ``is_current``,
            which selects nothing either).
        current_snapshot_id: The snapshot owning the ``is_current`` rows
            (None if there is none).
        reason: One of ``REASON_COVERS`` / ``REASON_FALLBACK_LOADED_BY`` /
            ``REASON_FALLBACK_OLDEST`` / ``REASON_FALLBACK_CURRENT``.
    """

    snapshot_id: int | None
    current_snapshot_id: int | None
    reason: str

    @property
    def is_current(self) -> bool:
        """True when the chosen snapshot is the live ``is_current`` one."""
        return self.snapshot_id is None or self.snapshot_id == self.current_snapshot_id

    @property
    def override_id(self) -> int | None:
        """The id to pass as ``--gtfs-snapshot-id``, or None to keep ``is_current``.

        Returning None for the current snapshot keeps the steady-state
        (no feed boundary) code path byte-for-byte what it was before
        per-date resolution existed.
        """
        return None if self.is_current else self.snapshot_id


def current_gtfs_snapshot_id(session) -> int | None:
    """Return the snapshot_id owning the ``is_current`` calendar rows, or None.

    Uses ``calendar`` (a few dozen rows per snapshot) rather than
    ``routes``/``trips`` purely for cost; a reload flips every versioned
    table's ``is_current`` in the same transaction, so they agree.
    """
    from sqlalchemy import func

    from src.models import Calendar

    return (
        session.query(func.max(Calendar.snapshot_id))
        .filter(Calendar.is_current, Calendar.snapshot_id.isnot(None))
        .scalar()
    )


def _snapshots_with_calendar_newest_first(session) -> list[int]:
    """Return ids of snapshots that have any ``calendar`` rows, newest load first.

    Newest = latest ``gtfs_snapshots.snapshot_date``, ties broken by the
    higher ``snapshot_id``. Snapshots with no calendar rows (a load that
    never populated the versioned tables, e.g. WMATA snapshot 5) can never
    schedule anything and are excluded everywhere.
    """
    from src.models import Calendar, GTFSSnapshot

    has_calendar = (
        session.query(Calendar.id).filter(Calendar.snapshot_id == GTFSSnapshot.snapshot_id).exists()
    )
    return [
        sid
        for (sid,) in session.query(GTFSSnapshot.snapshot_id)
        .filter(has_calendar)
        .order_by(GTFSSnapshot.snapshot_date.desc(), GTFSSnapshot.snapshot_id.desc())
        .all()
    ]


def resolve_gtfs_snapshot_for_date(
    session, service_date: date_type, tz_name: str = "America/New_York"
) -> SnapshotChoice:
    """Pick the GTFS snapshot to derive ``service_date`` against.

    Rule:

    1. **Covers.** A snapshot covers D when it schedules service on D under
       the GTFS-spec rule the pipelines themselves use —
       `src/gtfs_calendar.py:scheduled_service_ids_for_date(session, D,
       snapshot_id)` is non-empty (calendar day-of-week flag within
       ``[start_date, end_date]``, minus ``calendar_dates`` removals, plus
       additions). Being merely *inside* a calendar's date range is not
       enough: a newer feed back-dated over D whose service there is
       weekday-only or removed by ``exception_type=2`` does not cover D.
       Among covering snapshots the newest wins (latest ``snapshot_date``,
       ties by ``snapshot_id``): several snapshots routinely share one
       feed_version, and a republished feed that still covers D supersedes
       its predecessor.
    2. **Fallback, none covers** (the feed expired before a replacement was
       loaded, or a genuine no-service day): the newest snapshot already
       loaded by the end of D (agency-local midnight after D, in naive UTC,
       against ``gtfs_snapshots.snapshot_date``) — the schedule the
       pipeline would have used had it run on time.
    3. **Fallback, D predates every loaded snapshot**: the OLDEST snapshot —
       the closest in time to D. (SFMTA 2026-07-21/22, observed before
       snapshot 1 was loaded on 8/10, match snapshot 1's trips 100% and the
       current snapshot 4's 0%.)

    Snapshots without calendar rows are never chosen. With no usable
    snapshot at all, returns ``snapshot_id=None`` (callers fall back to
    ``is_current``). Use ``choice.override_id`` for the
    ``--gtfs-snapshot-id`` CLI argument (None when the choice is the
    current snapshot). Costs ~3 small calendar queries per loaded snapshot
    in the worst case — dozens of queries against tables of a few dozen
    rows per snapshot.
    """
    from src.gtfs_calendar import scheduled_service_ids_for_date
    from src.models import GTFSSnapshot
    from src.timezones import local_midnight_as_utc

    current_id = current_gtfs_snapshot_id(session)
    candidates = _snapshots_with_calendar_newest_first(session)
    if not candidates:
        return SnapshotChoice(current_id, current_id, REASON_FALLBACK_CURRENT)

    for sid in candidates:
        if scheduled_service_ids_for_date(session, service_date, sid):
            return SnapshotChoice(sid, current_id, REASON_COVERS)

    end_of_day_utc = local_midnight_as_utc(service_date + timedelta(days=1), tz_name)
    loaded_by = {
        sid
        for (sid,) in session.query(GTFSSnapshot.snapshot_id)
        .filter(GTFSSnapshot.snapshot_date < end_of_day_utc)
        .all()
    }
    for sid in candidates:
        if sid in loaded_by:
            return SnapshotChoice(sid, current_id, REASON_FALLBACK_LOADED_BY)

    return SnapshotChoice(candidates[-1], current_id, REASON_FALLBACK_OLDEST)
