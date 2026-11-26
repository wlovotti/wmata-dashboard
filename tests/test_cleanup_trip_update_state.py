"""Tests for pipelines.cleanup_trip_update_state.

Cleanup is a single date-based rule now: rows whose ``service_date``
is older than the retention window are deleted. The previous two-pass
implementation (derived rows older than 2 days, safety net for
underived rows older than 7 days) collapsed because ``service_date``
is in the PK — there's no need to reason about whether derivation
already ran.
"""

from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import select

from src.models import TripUpdateState
from src.timezones import eastern_today


def _make_state_row(
    trip_id: str,
    stop_sequence: int,
    *,
    service_date: date,
    derived_at: datetime | None = None,
) -> TripUpdateState:
    """Build a TripUpdateState row for cleanup tests.

    Args:
        trip_id: GTFS trip identifier.
        stop_sequence: Stop sequence number within the trip.
        service_date: Eastern service date this row belongs to. Drives
            the cleanup cutoff.
        derived_at: When stop_events were materialized; ``None`` means
            the row has not yet been derived. Preserved as a diagnostic;
            no longer load-bearing for cleanup.

    Returns:
        An unsaved :class:`~src.models.TripUpdateState` instance.
    """
    final_ts = datetime.combine(service_date, datetime.min.time()) + timedelta(hours=18)
    return TripUpdateState(
        trip_id=trip_id,
        stop_sequence=stop_sequence,
        service_date=service_date,
        stop_id="S1",
        vehicle_id="V1",
        final_snapshot_ts=final_ts,
        final_schedule_relationship="SCHEDULED",
        last_pred_snapshot_ts=final_ts,
        last_predicted_arrival_ts=final_ts + timedelta(minutes=5),
        derived_at=derived_at,
    )


@pytest.mark.integration
def test_cleanup_deletes_rows_older_than_retention_window(pg_session):
    """Rows with service_date older than the cutoff are deleted; newer rows stay.

    Cutoff is ``eastern_today() - retention_days``. With the default 7-day
    retention, a row dated 10 days ago is past the cutoff; 3 days ago is
    inside it.
    """
    from pipelines.cleanup_trip_update_state import run_cleanup

    today = eastern_today()
    pg_session.add_all(
        [
            _make_state_row("T_old", 1, service_date=today - timedelta(days=10)),
            _make_state_row("T_recent", 1, service_date=today - timedelta(days=3)),
            _make_state_row("T_today", 1, service_date=today),
        ]
    )
    pg_session.commit()

    counts = run_cleanup(pg_session)
    pg_session.commit()

    # On a populated dev DB, run_cleanup deletes all visible old rows (not just
    # the test's rows). Assert at-least-N to stay portable across populated DBs.
    assert counts["deleted"] >= 1
    remaining = {
        r.trip_id
        for r in pg_session.execute(
            select(TripUpdateState).where(
                TripUpdateState.trip_id.in_(["T_old", "T_recent", "T_today"])
            )
        ).scalars()
    }
    assert remaining == {"T_recent", "T_today"}


@pytest.mark.integration
def test_cleanup_respects_explicit_retention_days(pg_session):
    """A tighter retention window deletes more rows.

    Same fixture as above but called with ``retention_days=2``: anything
    older than 2 days gets deleted, so both T_old and T_recent (3 days
    out) are removed.
    """
    from pipelines.cleanup_trip_update_state import run_cleanup

    today = eastern_today()
    pg_session.add_all(
        [
            _make_state_row("T_old", 1, service_date=today - timedelta(days=10)),
            _make_state_row("T_recent", 1, service_date=today - timedelta(days=3)),
            _make_state_row("T_today", 1, service_date=today),
        ]
    )
    pg_session.commit()

    counts = run_cleanup(pg_session, retention_days=2)
    pg_session.commit()

    # On a populated dev DB, run_cleanup deletes all visible old rows (not just
    # the test's rows). Assert at-least-N to stay portable across populated DBs.
    assert counts["deleted"] >= 2
    remaining = {
        r.trip_id
        for r in pg_session.execute(
            select(TripUpdateState).where(
                TripUpdateState.trip_id.in_(["T_old", "T_recent", "T_today"])
            )
        ).scalars()
    }
    assert remaining == {"T_today"}


@pytest.mark.integration
def test_cleanup_ignores_derived_at(pg_session):
    """A derived row inside the retention window stays; that's the whole point
    of moving the cleanup rule onto service_date.

    Previously, ``derived_at`` set + 2-day window would have deleted a
    recent row. Now only ``service_date`` matters.
    """
    from pipelines.cleanup_trip_update_state import run_cleanup

    today = eastern_today()
    pg_session.add(
        _make_state_row(
            "T_derived_recent",
            1,
            service_date=today - timedelta(days=3),
            derived_at=datetime.now() - timedelta(days=3),
        )
    )
    pg_session.commit()

    counts = run_cleanup(pg_session)
    pg_session.commit()

    # The test verifies that T_derived_recent (inside the retention window) was NOT
    # deleted. On a populated dev DB, run_cleanup also deletes pre-existing old rows
    # so we can't assert an exact count — the remaining-set check is authoritative.
    assert "deleted" in counts
    remaining = {
        r.trip_id
        for r in pg_session.execute(
            select(TripUpdateState).where(TripUpdateState.trip_id == "T_derived_recent")
        ).scalars()
    }
    assert remaining == {"T_derived_recent"}


@pytest.mark.integration
def test_cleanup_tz_name_changes_cutoff(pg_session, monkeypatch):
    """NOTES-100: tz_name changes which calendar day 'today' resolves to,
    hence the retention cutoff. Fixes "today" to a known date per zone
    (via monkeypatch, not the real clock) so the boundary math is exact
    rather than dependent on when the suite happens to run.
    """
    import pipelines.cleanup_trip_update_state as cleanup_module

    fixed_dates = {
        "America/New_York": date(2026, 7, 23),
        "America/Los_Angeles": date(2026, 7, 22),  # a day behind at this instant
    }
    monkeypatch.setattr(cleanup_module, "local_today", lambda tz_name: fixed_dates[tz_name])

    from pipelines.cleanup_trip_update_state import run_cleanup

    # A row dated 2026-07-22 (yesterday Eastern, but "today" Pacific).
    pg_session.add(_make_state_row("T_tz_boundary", 1, service_date=date(2026, 7, 22)))
    pg_session.commit()

    # retention_days=0, Eastern: cutoff is 2026-07-23, so 07-22 is strictly
    # older and gets deleted.
    run_cleanup(pg_session, retention_days=0, tz_name="America/New_York")
    pg_session.commit()
    remaining = {
        r.trip_id
        for r in pg_session.execute(
            select(TripUpdateState).where(TripUpdateState.trip_id == "T_tz_boundary")
        ).scalars()
    }
    assert remaining == set()

    # Re-seed and repeat with Pacific: cutoff is 2026-07-22, so a
    # service_date of 07-22 is NOT strictly older and survives.
    pg_session.add(_make_state_row("T_tz_boundary", 1, service_date=date(2026, 7, 22)))
    pg_session.commit()
    run_cleanup(pg_session, retention_days=0, tz_name="America/Los_Angeles")
    pg_session.commit()
    remaining = {
        r.trip_id
        for r in pg_session.execute(
            select(TripUpdateState).where(TripUpdateState.trip_id == "T_tz_boundary")
        ).scalars()
    }
    assert remaining == {"T_tz_boundary"}


@pytest.mark.integration
def test_cleanup_commits_once_per_service_date_in_batches(pg_session, monkeypatch):
    """PR #229 review kept statements bounded (``batch_size``); issue #270's
    review requires each service date to be deleted in ONE transaction, so
    an interrupted cleanup never leaves a remnant of a date. Two eligible
    dates of 7 rows each with batch_size=3: every row goes, and one commit
    happens per date although each date takes three statements.
    """
    from pipelines.cleanup_trip_update_state import run_cleanup

    today = eastern_today()
    for days in (10, 11):
        d = today - timedelta(days=days)
        pg_session.add_all(
            [_make_state_row(f"T_batch_{days}_{i}", 1, service_date=d) for i in range(7)]
        )
    pg_session.commit()
    eligible_dates = len(
        pg_session.execute(
            select(TripUpdateState.service_date)
            .where(TripUpdateState.service_date < today - timedelta(days=7))
            .distinct()
        ).all()
    )

    commit_count = 0
    original_commit = pg_session.commit

    def _counting_commit():
        """Count commits issued from inside run_cleanup."""
        nonlocal commit_count
        commit_count += 1
        return original_commit()

    monkeypatch.setattr(pg_session, "commit", _counting_commit)

    counts = run_cleanup(pg_session, batch_size=3)

    remaining = pg_session.execute(
        select(TripUpdateState).where(TripUpdateState.trip_id.like("T_batch_%"))
    ).all()
    assert remaining == []
    assert counts["deleted"] >= 14
    assert commit_count == eligible_dates  # == 2 on a clean test DB; per-batch would be >= 6


@pytest.mark.integration
def test_interrupted_cleanup_never_leaves_a_partial_date(pg_session, monkeypatch):
    """A cleanup that dies while deleting the second date leaves the first
    date fully gone and the second fully present — never a scattered
    remnant that the completeness check would read as a sparse feed
    (issue #270 review item 2). Dates are deleted oldest first.
    """
    from pipelines.cleanup_trip_update_state import run_cleanup

    today = eastern_today()
    older, newer = today - timedelta(days=12), today - timedelta(days=11)
    for d in (older, newer):
        pg_session.add_all([_make_state_row(f"T_int_{d}_{i}", 1, service_date=d) for i in range(5)])
    pg_session.commit()

    calls = 0
    original_commit = pg_session.commit

    def _dies_on_second_commit():
        """Commit the first date, then simulate a crash before the second commits."""
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated crash mid-cleanup")
        return original_commit()

    monkeypatch.setattr(pg_session, "commit", _dies_on_second_commit)
    with pytest.raises(RuntimeError):
        run_cleanup(pg_session, batch_size=2)
    pg_session.rollback()

    def _left(d):
        """Rows of this test left for service date ``d``."""
        return len(
            pg_session.execute(
                select(TripUpdateState).where(
                    TripUpdateState.trip_id.like("T_int_%"), TripUpdateState.service_date == d
                )
            ).all()
        )

    assert (_left(older), _left(newer)) == (0, 5)


@pytest.mark.integration
def test_cleanup_dry_run_reports_total_without_deleting(pg_session):
    """dry_run must still report the full eligible count, and must not
    delete or commit anything -- batching must not change this contract.
    """
    from pipelines.cleanup_trip_update_state import run_cleanup

    today = eastern_today()
    old_date = today - timedelta(days=10)
    pg_session.add_all([_make_state_row(f"T_dry_{i}", 1, service_date=old_date) for i in range(5)])
    pg_session.commit()

    counts = run_cleanup(pg_session, dry_run=True, batch_size=2)

    assert counts["deleted"] >= 5
    remaining = {
        r.trip_id
        for r in pg_session.execute(
            select(TripUpdateState).where(TripUpdateState.trip_id.like("T_dry_%"))
        ).scalars()
    }
    assert remaining == {f"T_dry_{i}" for i in range(5)}


def test_main_targets_agency_database(db_session, monkeypatch):
    """--agency threads through to the DB session resolution, matching
    every other NOTES-100 pipeline entry point -- this closes the gap
    where a WMATA-only cleanup run against `--agency sfmta` would have
    silently pruned the WMATA table instead (see run_daily_batch.py's
    corrected housekeeping-skip message)."""
    import sys

    import pipelines.cleanup_trip_update_state as cleanup_module

    monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///sfmta_test")
    seen_db_urls = []

    def _fake_get_session(db_url=None):
        seen_db_urls.append(db_url)
        return db_session

    monkeypatch.setattr(cleanup_module, "get_session", _fake_get_session)
    monkeypatch.setattr(sys, "argv", ["cleanup_trip_update_state.py", "--agency", "sfmta"])

    cleanup_module.main()

    assert seen_db_urls == ["postgresql:///sfmta_test"]


def test_main_default_agency_is_wmata(db_session, monkeypatch):
    """Omitting --agency keeps today's DATABASE_URL behavior unchanged."""
    import sys

    import pipelines.cleanup_trip_update_state as cleanup_module

    seen_db_urls = []

    def _fake_get_session(db_url=None):
        seen_db_urls.append(db_url)
        return db_session

    monkeypatch.setattr(cleanup_module, "get_session", _fake_get_session)
    monkeypatch.setattr(sys, "argv", ["cleanup_trip_update_state.py"])

    cleanup_module.main()

    assert len(seen_db_urls) == 1  # resolve_agency_db_url(wmata_cfg) — whatever DATABASE_URL is


@pytest.mark.integration
def test_cleanup_before_date_keeps_the_date_just_derived_and_later(pg_session):
    """Per-date cleanup (PR #264 review): cutoff = min(before_date, today - retention).

    During a backfill of a weeks-old date D, rows for dates before D go —
    D itself (whose state D+1's completeness check can still read) and
    everything after it stay, even though all of them are past the normal
    7-day window.
    """
    from pipelines.cleanup_trip_update_state import run_cleanup

    today = eastern_today()
    d = today - timedelta(days=40)
    pg_session.add_all(
        [
            _make_state_row("T_bd_m2", 1, service_date=d - timedelta(days=2)),
            _make_state_row("T_bd_m1", 1, service_date=d - timedelta(days=1)),
            _make_state_row("T_bd_d", 1, service_date=d),
            _make_state_row("T_bd_p1", 1, service_date=d + timedelta(days=1)),
            _make_state_row("T_bd_recent", 1, service_date=today - timedelta(days=3)),
        ]
    )
    pg_session.commit()
    ids = ["T_bd_m2", "T_bd_m1", "T_bd_d", "T_bd_p1", "T_bd_recent"]

    def _remaining() -> set[str]:
        """Which of this test's rows survive."""
        stmt = select(TripUpdateState).where(TripUpdateState.trip_id.in_(ids))
        return {r.trip_id for r in pg_session.execute(stmt).scalars()}

    run_cleanup(pg_session, before_date=d)
    assert _remaining() == {"T_bd_d", "T_bd_p1", "T_bd_recent"}

    # A recent date keeps the normal retention window instead.
    run_cleanup(pg_session, before_date=today - timedelta(days=1))
    assert _remaining() == {"T_bd_recent"}
