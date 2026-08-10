"""
Unit tests for src/gtfs_calendar.py — the shared GTFS-spec calendar +
calendar_dates resolver for one exact date.

This is the single source of truth `src/service_delivered.py` and
`src/ewt.py` both call (NOTES-106 review follow-up) for "which service_ids
run on date D" — base calendar day-of-week flag bounded by
[start_date, end_date], MINUS calendar_dates type=2 (removed) for that
exact date, UNION calendar_dates type=1 (added) for that exact date.
"""

from __future__ import annotations

from datetime import date

from src.gtfs_calendar import scheduled_service_ids_for_date
from src.models import Calendar, CalendarDate


def _seed_calendar(
    db_session,
    service_id: str,
    *,
    monday: int = 0,
    tuesday: int = 0,
    wednesday: int = 0,
    thursday: int = 0,
    friday: int = 0,
    saturday: int = 0,
    sunday: int = 0,
    start_date: str = "20260101",
    end_date: str = "20261231",
) -> None:
    """Insert a `calendar` row with explicit per-weekday flags (default all 0)."""
    db_session.add(
        Calendar(
            service_id=service_id,
            monday=monday,
            tuesday=tuesday,
            wednesday=wednesday,
            thursday=thursday,
            friday=friday,
            saturday=saturday,
            sunday=sunday,
            start_date=start_date,
            end_date=end_date,
            is_current=True,
        )
    )
    db_session.commit()


def _seed_calendar_date(db_session, service_id: str, date_str: str, exception_type: int) -> None:
    """Insert a `calendar_dates` exception row (1=added, 2=removed)."""
    db_session.add(
        CalendarDate(
            service_id=service_id,
            date=date_str,
            exception_type=exception_type,
            is_current=True,
        )
    )
    db_session.commit()


class TestScheduledServiceIdsForDate:
    def test_base_calendar_flag_within_date_range(self, db_session):
        """A service_id whose weekday flag is set and whose validity window
        covers the date is included with no exceptions in play."""
        _seed_calendar(db_session, "WK", tuesday=1, start_date="20260101", end_date="20261231")
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 14)) == {"WK"}

    def test_base_calendar_excluded_outside_date_range(self, db_session):
        """A service_id with the weekday flag set but an expired validity
        window must NOT be included — the base rule is flag AND date range."""
        _seed_calendar(db_session, "WK", tuesday=1, start_date="20260101", end_date="20260301")
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 14)) == set()

    def test_wrong_weekday_flag_excluded(self, db_session):
        """A calendar row without the matching weekday flag set doesn't
        qualify even though the date falls within its validity window."""
        _seed_calendar(db_session, "SAT", saturday=1, start_date="20260101", end_date="20261231")
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 14)) == set()  # Tuesday

    def test_type1_addition_on_exact_date_unions_in(self, db_session):
        """A calendar_dates type=1 row on the exact date being resolved adds
        a service_id even when calendar itself has no coverage for that
        weekday at all (the Muni/SFMTA shape)."""
        _seed_calendar_date(db_session, "WKADD", "20260414", exception_type=1)  # Tuesday
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 14)) == {"WKADD"}

    def test_type1_addition_on_different_date_does_not_apply(self, db_session):
        """A type=1 addition scoped to one date must not leak into resolution
        for a different date, even one with a matching weekday."""
        _seed_calendar_date(db_session, "WKADD", "20260414", exception_type=1)  # Tuesday
        # 4/21 is also a Tuesday, but the exception is scoped to 4/14 only.
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 21)) == set()

    def test_type2_removal_on_exact_date_subtracts_from_base(self, db_session):
        """A calendar_dates type=2 row on the exact date subtracts a
        service_id that would otherwise qualify via the base calendar flag —
        a one-off holiday removal, scoped to that single date only."""
        _seed_calendar(db_session, "WK", tuesday=1, start_date="20260101", end_date="20261231")
        _seed_calendar_date(db_session, "WK", "20260414", exception_type=2)
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 14)) == set()
        # A different Tuesday, unaffected by the one-off removal.
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 21)) == {"WK"}

    def test_holiday_swap_removes_weekday_adds_sunday(self, db_session):
        """The canonical GTFS holiday idiom: a federal-holiday weekday
        removes the normal weekday service_id and adds the Sunday
        service_id for that one date — both should resolve correctly."""
        _seed_calendar(db_session, "WK", tuesday=1, start_date="20260101", end_date="20261231")
        _seed_calendar(db_session, "SUN", sunday=1, start_date="20260101", end_date="20261231")
        _seed_calendar_date(db_session, "WK", "20260414", exception_type=2)
        _seed_calendar_date(db_session, "SUN", "20260414", exception_type=1)
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 14)) == {"SUN"}

    def test_addition_and_removal_combine_for_distinct_services(self, db_session):
        """A base-covered service_id being removed and a different
        calendar_dates-only service_id being added on the same date both
        apply independently."""
        _seed_calendar(db_session, "WK", tuesday=1, start_date="20260101", end_date="20261231")
        _seed_calendar_date(db_session, "WK", "20260414", exception_type=2)
        _seed_calendar_date(db_session, "EXTRA", "20260414", exception_type=1)
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 14)) == {"EXTRA"}

    def test_no_calendar_or_calendar_dates_data_returns_empty(self, db_session):
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 14)) == set()

    def test_multiple_base_service_ids_all_qualify(self, db_session):
        """Two independent service_ids both covering the same weekday are
        both returned — mirrors WMATA's Mon/Tue/Thu + separate Wed/Fri
        service_id splits."""
        _seed_calendar(db_session, "A", tuesday=1, start_date="20260101", end_date="20261231")
        _seed_calendar(db_session, "B", tuesday=1, start_date="20260101", end_date="20261231")
        assert scheduled_service_ids_for_date(db_session, date(2026, 4, 14)) == {"A", "B"}
