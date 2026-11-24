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
REASON_FALLBACK_CURRENT = "fallback_current"


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
            ``REASON_FALLBACK_CURRENT``.
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


def resolve_gtfs_snapshot_for_date(
    session, service_date: date_type, tz_name: str = "America/New_York"
) -> SnapshotChoice:
    """Pick the GTFS snapshot to derive ``service_date`` against.

    Rule:

    1. **Covers.** A snapshot covers the date when its ``calendar`` has a
       row with ``start_date <= D <= end_date`` or its ``calendar_dates``
       *adds* service on D (``exception_type = 1``). GTFS dates are
       fixed-width ``YYYYMMDD`` strings, so string comparison is a correct
       date comparison here (unlike GTFS *times*, which are unpadded — see
       CLAUDE.md). Among covering snapshots the newest wins (latest
       ``snapshot_date``, ties by ``snapshot_id``): several snapshots
       routinely share one feed_version, and a republished feed that
       still covers D supersedes its predecessor.
    2. **Fallback, none covers** (the feed expired before a replacement
       was loaded, or D predates every loaded feed): the newest snapshot
       that was already loaded by the end of D (agency-local midnight
       after D, in naive UTC, against ``gtfs_snapshots.snapshot_date``) —
       i.e. the schedule the pipeline would have used had it run on time.
       Snapshots with no ``calendar`` rows at all (a load that never
       populated the versioned tables) are never chosen.
    3. **Fallback, nothing loaded by then**: the current snapshot — exactly
       the pre-#258 behaviour.

    Returns a `SnapshotChoice`; use ``choice.override_id`` for the
    ``--gtfs-snapshot-id`` CLI argument (None when the choice is the
    current snapshot).
    """
    from src.models import Calendar, CalendarDate, GTFSSnapshot
    from src.timezones import local_midnight_as_utc

    current_id = current_gtfs_snapshot_id(session)
    d = service_date.strftime("%Y%m%d")

    covering_ids = {
        sid
        for (sid,) in session.query(Calendar.snapshot_id)
        .filter(
            Calendar.snapshot_id.isnot(None),
            Calendar.start_date <= d,
            Calendar.end_date >= d,
        )
        .distinct()
        .all()
    } | {
        sid
        for (sid,) in session.query(CalendarDate.snapshot_id)
        .filter(
            CalendarDate.snapshot_id.isnot(None),
            CalendarDate.date == d,
            CalendarDate.exception_type == 1,
        )
        .distinct()
        .all()
    }

    if covering_ids:
        newest = (
            session.query(GTFSSnapshot.snapshot_id)
            .filter(GTFSSnapshot.snapshot_id.in_(covering_ids))
            .order_by(GTFSSnapshot.snapshot_date.desc(), GTFSSnapshot.snapshot_id.desc())
            .limit(1)
            .scalar()
        )
        # A covering calendar row whose gtfs_snapshots row is missing can't
        # happen under the FK, but don't let a None slip through if it does.
        if newest is None:
            newest = max(covering_ids)
        return SnapshotChoice(newest, current_id, REASON_COVERS)

    end_of_day_utc = local_midnight_as_utc(service_date + timedelta(days=1), tz_name)
    has_calendar = (
        session.query(Calendar.id).filter(Calendar.snapshot_id == GTFSSnapshot.snapshot_id).exists()
    )
    loaded_by = (
        session.query(GTFSSnapshot.snapshot_id)
        .filter(GTFSSnapshot.snapshot_date < end_of_day_utc, has_calendar)
        .order_by(GTFSSnapshot.snapshot_date.desc(), GTFSSnapshot.snapshot_id.desc())
        .limit(1)
        .scalar()
    )
    if loaded_by is not None:
        return SnapshotChoice(loaded_by, current_id, REASON_FALLBACK_LOADED_BY)

    return SnapshotChoice(current_id, current_id, REASON_FALLBACK_CURRENT)
