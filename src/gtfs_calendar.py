"""GTFS-spec `calendar` + `calendar_dates` resolution for one exact date.

Single source of truth for "which `service_id`s are scheduled to run on
`service_date` D" — the canonical GTFS rule: start from `calendar`'s
day-of-week flag intersected with that calendar row's own
`[start_date, end_date]` validity window, then apply `calendar_dates`
exceptions for that **literal date only**: `exception_type=2` (removed)
subtracts, `exception_type=1` (added) unions in.

This resolves strictly per-date — it does NOT pool exceptions across a
date range or a "representative weekday." `src/service_delivered.py`
calls it against the literal observed `service_date` (a genuinely
per-date question — NOTES-51, where collapsing to a representative
weekday silently dropped Wed-only/Fri-only service_ids). `src/ewt.py`
calls it against a single, deterministically-chosen *representative*
date per day_type (see that module's docstring for why EWT/bunching need
a representative day at all) — but the resolution for that one date is
identical to this function's per-date rule, not a separate "pool
everything matching this weekday across the whole feed" shape. A review
follow-up to NOTES-106 (`_resolve_service_ids_for_day_type`'s original
fix) converged both call sites onto this one resolver after the
feed-wide pooling shape was found to reintroduce NOTES-106's own bug
(a single stray `calendar_dates` exception anywhere in the feed could
evict a service_id from the ENTIRE representative pool) and to silently
blend unrelated schedule-revision eras together.
"""

from __future__ import annotations

from datetime import date as date_type

from sqlalchemy.orm import Session

from src.gtfs_versioning import gtfs_version_filter
from src.models import Calendar, CalendarDate

# Day-of-week column on `Calendar`, indexed by Python's `date.weekday()`
# (Mon=0..Sun=6).
_WEEKDAY_TO_CALENDAR_FIELD = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)


def scheduled_service_ids_for_date(
    db: Session, service_date: date_type, gtfs_snapshot_id: int | None = None
) -> set[str]:
    """Return the set of `service_id`s scheduled to run on `service_date`.

    GTFS-spec resolution: (`calendar` day-of-week flag AND `service_date`
    within `[start_date, end_date]`) MINUS `calendar_dates`
    `exception_type=2` (removed) for this exact date, UNION
    `calendar_dates` `exception_type=1` (added) for this exact date.

    `gtfs_snapshot_id` pins the resolution to a historical GTFS snapshot
    (backfill); the default reads the live `is_current` snapshot.
    """
    service_date_str = service_date.strftime("%Y%m%d")
    weekday_field = getattr(Calendar, _WEEKDAY_TO_CALENDAR_FIELD[service_date.weekday()])

    base_ids = {
        sid
        for (sid,) in db.query(Calendar.service_id)
        .filter(
            gtfs_version_filter(Calendar, gtfs_snapshot_id),
            weekday_field == 1,
            Calendar.start_date <= service_date_str,
            Calendar.end_date >= service_date_str,
        )
        .all()
    }
    removed_ids = {
        sid
        for (sid,) in db.query(CalendarDate.service_id)
        .filter(
            gtfs_version_filter(CalendarDate, gtfs_snapshot_id),
            CalendarDate.date == service_date_str,
            CalendarDate.exception_type == 2,
        )
        .all()
    }
    added_ids = {
        sid
        for (sid,) in db.query(CalendarDate.service_id)
        .filter(
            gtfs_version_filter(CalendarDate, gtfs_snapshot_id),
            CalendarDate.date == service_date_str,
            CalendarDate.exception_type == 1,
        )
        .all()
    }
    return (base_ids - removed_ids) | added_ids
