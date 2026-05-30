"""Tests for pipelines.window_derived_tables (tier-2 365-day windowing, NOTES-48).

`stop_events.service_date` / `runs.service_date` are STRING ("YYYY-MM-DD")
columns, so the windowing DELETE is pure-ORM and runs on SQLite — these are
fast smoke tests on the in-memory `db_session` (fresh and empty per test, so
exact-count assertions are safe).
"""

from datetime import timedelta

import pytest

from src.models import Run, StopEvent
from src.timezones import eastern_today


def _stop_event(service_date: str, trip_id: str) -> StopEvent:
    """Build a minimal StopEvent (only NOT NULL columns) for windowing tests."""
    return StopEvent(
        service_date=service_date,
        trip_id=trip_id,
        route_id="R1",
        direction_id=0,
        stop_id="S1",
        stop_sequence=1,
        source="trip_update",
    )


def _run(service_date: str, trip_id: str) -> Run:
    """Build a minimal Run (only NOT NULL columns) for windowing tests."""
    return Run(
        service_date=service_date,
        trip_id=trip_id,
        route_id="R1",
        direction_id=0,
        source="trip_update",
    )


@pytest.mark.smoke
def test_window_deletes_rows_older_than_365_days(db_session):
    """stop_events/runs older than the cutoff are deleted; newer rows stay."""
    from pipelines.window_derived_tables import run_window

    today = eastern_today()
    old = (today - timedelta(days=400)).isoformat()
    recent = (today - timedelta(days=10)).isoformat()
    db_session.add_all(
        [
            _stop_event(old, "T_old"),
            _stop_event(recent, "T_recent"),
            _run(old, "T_old"),
            _run(recent, "T_recent"),
        ]
    )
    db_session.commit()

    counts = run_window(db_session)
    db_session.commit()

    assert counts == {"stop_events": 1, "runs": 1}
    assert {r.service_date for r in db_session.query(StopEvent).all()} == {recent}
    assert {r.service_date for r in db_session.query(Run).all()} == {recent}


@pytest.mark.smoke
def test_window_respects_explicit_retention_days(db_session):
    """A tighter window deletes more rows."""
    from pipelines.window_derived_tables import run_window

    today = eastern_today()
    d100 = (today - timedelta(days=100)).isoformat()
    d10 = (today - timedelta(days=10)).isoformat()
    db_session.add_all([_stop_event(d100, "T100"), _stop_event(d10, "T10")])
    db_session.commit()

    counts = run_window(db_session, retention_days=30)
    db_session.commit()

    assert counts["stop_events"] == 1
    assert {r.service_date for r in db_session.query(StopEvent).all()} == {d10}


@pytest.mark.smoke
def test_window_boundary_is_exclusive(db_session):
    """A row exactly on the cutoff date is KEPT — the cutoff is a strict `<`."""
    from pipelines.window_derived_tables import compute_cutoff_str, run_window

    cutoff = compute_cutoff_str(365)
    db_session.add(_stop_event(cutoff, "T_boundary"))
    db_session.commit()

    run_window(db_session)
    db_session.commit()

    assert {r.trip_id for r in db_session.query(StopEvent).all()} == {"T_boundary"}


@pytest.mark.smoke
def test_window_empty_db_returns_zero_counts(db_session):
    """run_window on an empty DB returns zero counts, not -1 (the rowcount guard)."""
    from pipelines.window_derived_tables import run_window

    counts = run_window(db_session)
    assert counts == {"stop_events": 0, "runs": 0}
