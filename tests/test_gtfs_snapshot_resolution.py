"""Per-service-date GTFS snapshot resolution (issue #258).

WMATA 2026-09-13: the reload landed snapshot 20 (feed S1000251, calendar
starting 2026-09-13) before 2026-09-12 was derived. Every one of 9/12's
observed trip_ids lived in snapshot 19, none in snapshot 20, so deriving
9/12 against `is_current` matched 0 trips. These tests pin
`resolve_gtfs_snapshot_for_date` (which snapshot a date is derived
against) and its wiring into `run_daily_batch.run_batch` and the
post-replay canary.
"""

from datetime import date, datetime

import pytest

from src.gtfs_versioning import (
    REASON_COVERS,
    REASON_FALLBACK_CURRENT,
    REASON_FALLBACK_LOADED_BY,
    REASON_FALLBACK_OLDEST,
    SnapshotChoice,
    resolve_gtfs_snapshot_for_date,
)
from src.models import Calendar, CalendarDate, GTFSSnapshot


def _snapshot(snapshot_id: int, loaded: datetime) -> GTFSSnapshot:
    """Build a gtfs_snapshots row loaded at ``loaded`` (naive UTC)."""
    return GTFSSnapshot(
        snapshot_id=snapshot_id, snapshot_date=loaded, feed_version=f"F{snapshot_id}"
    )


def _calendar(snapshot_id: int, start: str, end: str, is_current: bool) -> Calendar:
    """Build an every-day calendar row for ``snapshot_id`` spanning start..end (YYYYMMDD)."""
    return Calendar(
        service_id=f"SVC{snapshot_id}",
        monday=1,
        tuesday=1,
        wednesday=1,
        thursday=1,
        friday=1,
        saturday=1,
        sunday=1,
        start_date=start,
        end_date=end,
        snapshot_id=snapshot_id,
        valid_from=datetime(2026, 1, 1),
        is_current=is_current,
    )


def _seed_feed_boundary(session) -> None:
    """Seed the 2026-09 WMATA shape: two copies of the outgoing feed + the new one.

    Snapshot 18 and 19 are reloads of the same outgoing feed (calendar
    2026-06-21..2026-09-12); snapshot 20 is the incoming feed (calendar
    2026-09-13..2027-03-27), loaded the morning of 9/13 and current.
    """
    session.add_all(
        [
            _snapshot(18, datetime(2026, 8, 29, 23, 5)),
            _snapshot(19, datetime(2026, 9, 7, 20, 6)),
            _snapshot(20, datetime(2026, 9, 13, 8, 15)),
        ]
    )
    session.flush()
    session.add_all(
        [
            _calendar(18, "20260621", "20260912", is_current=False),
            _calendar(19, "20260621", "20260912", is_current=False),
            _calendar(20, "20260913", "20270327", is_current=True),
        ]
    )
    session.commit()


@pytest.mark.smoke
def test_date_covered_only_by_older_snapshots_pins_the_newest_of_them(db_session):
    """9/12 is covered by 18 and 19 (not the current 20) -> 19, pinned (not is_current)."""
    _seed_feed_boundary(db_session)

    choice = resolve_gtfs_snapshot_for_date(db_session, date(2026, 9, 12))

    assert choice == SnapshotChoice(19, 20, REASON_COVERS)
    assert choice.override_id == 19
    assert not choice.is_current


@pytest.mark.smoke
def test_date_covered_only_by_an_older_snapshot(db_session):
    """A single older snapshot covering the date is chosen over the current one."""
    db_session.add_all([_snapshot(1, datetime(2026, 6, 1)), _snapshot(2, datetime(2026, 7, 1))])
    db_session.flush()
    db_session.add_all(
        [
            _calendar(1, "20260301", "20260620", is_current=False),
            _calendar(2, "20260621", "20260912", is_current=True),
        ]
    )
    db_session.commit()

    choice = resolve_gtfs_snapshot_for_date(db_session, date(2026, 6, 20))

    assert choice == SnapshotChoice(1, 2, REASON_COVERS)
    assert choice.override_id == 1


@pytest.mark.smoke
def test_date_covered_by_current_snapshot_needs_no_override(db_session):
    """9/13 resolves to the current snapshot -> override_id None (is_current path unchanged)."""
    _seed_feed_boundary(db_session)

    choice = resolve_gtfs_snapshot_for_date(db_session, date(2026, 9, 13))

    assert choice.snapshot_id == 20
    assert choice.is_current
    assert choice.override_id is None


@pytest.mark.smoke
def test_date_covered_by_two_overlapping_feeds_prefers_newest(db_session):
    """When an old and a new feed both cover the date, the newer load wins."""
    db_session.add_all([_snapshot(1, datetime(2026, 6, 1)), _snapshot(2, datetime(2026, 9, 1))])
    db_session.flush()
    db_session.add_all(
        [
            _calendar(1, "20260301", "20260930", is_current=False),
            _calendar(2, "20260901", "20261231", is_current=True),
        ]
    )
    db_session.commit()

    choice = resolve_gtfs_snapshot_for_date(db_session, date(2026, 9, 15))

    assert choice == SnapshotChoice(2, 2, REASON_COVERS)


@pytest.mark.smoke
def test_newest_is_by_load_time_not_id(db_session):
    """Ordering is snapshot_date first: a lower id loaded later still wins."""
    db_session.add_all([_snapshot(5, datetime(2026, 9, 1)), _snapshot(4, datetime(2026, 9, 5))])
    db_session.flush()
    db_session.add_all(
        [
            _calendar(5, "20260801", "20261231", is_current=False),
            _calendar(4, "20260801", "20261231", is_current=True),
        ]
    )
    db_session.commit()

    assert resolve_gtfs_snapshot_for_date(db_session, date(2026, 9, 10)).snapshot_id == 4


@pytest.mark.smoke
def test_calendar_dates_added_service_counts_as_coverage(db_session):
    """An exception_type=1 calendar_dates row covers a date outside the calendar range;
    an exception_type=2 (removed service) row does not."""
    _seed_feed_boundary(db_session)
    db_session.add_all(
        [
            CalendarDate(
                service_id="HOL",
                date="20260601",
                exception_type=1,
                snapshot_id=18,
                valid_from=datetime(2026, 1, 1),
                is_current=False,
            ),
            CalendarDate(
                service_id="HOL",
                date="20260602",
                exception_type=2,
                snapshot_id=18,
                valid_from=datetime(2026, 1, 1),
                is_current=False,
            ),
        ]
    )
    db_session.commit()

    assert resolve_gtfs_snapshot_for_date(db_session, date(2026, 6, 1)) == SnapshotChoice(
        18, 20, REASON_COVERS
    )
    assert resolve_gtfs_snapshot_for_date(db_session, date(2026, 6, 2)).reason != REASON_COVERS


@pytest.mark.smoke
def test_date_covered_by_none_falls_back_to_newest_loaded_by_that_date(db_session):
    """A gap no calendar covers -> the newest snapshot already loaded by the end of
    that service date (what an on-time run would have used), not the current one."""
    db_session.add_all([_snapshot(1, datetime(2026, 6, 1)), _snapshot(2, datetime(2026, 8, 1))])
    db_session.flush()
    db_session.add_all(
        [
            _calendar(1, "20260301", "20260620", is_current=False),
            _calendar(2, "20260801", "20261231", is_current=True),
        ]
    )
    db_session.commit()

    # 2026-07-04: feed 1 expired 6/20, feed 2 starts 8/01 and was loaded 8/01.
    choice = resolve_gtfs_snapshot_for_date(db_session, date(2026, 7, 4))
    assert choice == SnapshotChoice(1, 2, REASON_FALLBACK_LOADED_BY)

    # 2027-01-05: every feed has expired; the newest loaded is the current one.
    choice = resolve_gtfs_snapshot_for_date(db_session, date(2027, 1, 5))
    assert choice == SnapshotChoice(2, 2, REASON_FALLBACK_LOADED_BY)
    assert choice.override_id is None


@pytest.mark.smoke
def test_date_predating_every_snapshot_falls_back_to_the_oldest(db_session):
    """D before any snapshot was loaded and no calendar covers -> the OLDEST snapshot
    (closest in time), not the current one. SFMTA 7/21-22 shape: observed before
    snapshot 1 was loaded, matching snapshot 1 100% and the current one 0%."""
    _seed_feed_boundary(db_session)

    choice = resolve_gtfs_snapshot_for_date(db_session, date(2026, 1, 15))

    assert choice == SnapshotChoice(18, 20, REASON_FALLBACK_OLDEST)
    assert choice.override_id == 18


@pytest.mark.smoke
def test_fallback_skips_snapshots_without_calendar_rows(db_session):
    """A gtfs_snapshots row whose versioned tables were never populated is never chosen."""
    _seed_feed_boundary(db_session)
    db_session.add(_snapshot(21, datetime(2026, 9, 20)))  # no calendar rows
    db_session.commit()

    choice = resolve_gtfs_snapshot_for_date(db_session, date(2027, 6, 1))

    assert choice.snapshot_id == 20


@pytest.mark.smoke
def test_empty_database_resolves_to_none(db_session):
    """No snapshots at all -> snapshot None, no override (is_current selects nothing anyway)."""
    choice = resolve_gtfs_snapshot_for_date(db_session, date(2026, 9, 12))

    assert choice.snapshot_id is None
    assert choice.override_id is None


# ---------------------------------------------------------------------------
# run_daily_batch wiring
# ---------------------------------------------------------------------------


class _FakeCompletedProcess:
    """Minimal stand-in for subprocess.CompletedProcess."""

    returncode = 0


def _run_batch_capturing(
    tmp_path,
    monkeypatch,
    target_dates,
    resolver,
    agency="sfmta",
    match_guard=lambda d, choice: None,
    dry_run=False,
    expected_failures=0,
):
    """Run run_batch with subprocess stubbed out; return (captured_cmds, log_text).

    ``agency="sfmta"`` by default so the WMATA-only housekeeping loop is
    skipped and only per-date pipeline commands are captured. The default
    ``match_guard`` returns None ("nothing to check") so no DB is touched.
    """
    import pipelines.run_daily_batch as run_daily_batch_module

    captured = []

    def _fake_subprocess_run(cmd, **kwargs):
        """Record the command instead of running it."""
        captured.append(cmd)
        return _FakeCompletedProcess()

    monkeypatch.setattr(run_daily_batch_module.subprocess, "run", _fake_subprocess_run)
    log_path = tmp_path / "batch.log"
    with log_path.open("a") as log_handle:
        failures = run_daily_batch_module.run_batch(
            target_dates,
            log_handle,
            dry_run=dry_run,
            agency=agency,
            snapshot_resolver=resolver,
            match_guard=match_guard,
        )
    assert failures == expected_failures
    return captured, log_path.read_text()


@pytest.mark.smoke
def test_run_batch_pins_every_per_date_pipeline_to_the_resolved_snapshot(tmp_path, monkeypatch):
    """9/12 resolves to non-current snapshot 19 -> all six per-date pipelines get
    --gtfs-snapshot-id 19; 9/13 resolves to the current snapshot -> none do."""
    from pipelines.run_daily_batch import PIPELINES

    choices = {
        date(2026, 9, 12): SnapshotChoice(19, 20, REASON_COVERS),
        date(2026, 9, 13): SnapshotChoice(20, 20, REASON_COVERS),
    }
    captured, log_text = _run_batch_capturing(
        tmp_path, monkeypatch, [date(2026, 9, 12), date(2026, 9, 13)], choices.__getitem__
    )

    by_date: dict[str, dict[str, list[str]]] = {}
    for cmd in captured:
        by_date.setdefault(cmd[cmd.index("--date") + 1], {})[cmd[2]] = cmd

    all_modules = {p["module"] for p in PIPELINES}
    assert set(by_date["2026-09-12"]) == all_modules
    for module, cmd in by_date["2026-09-12"].items():
        assert cmd[cmd.index("--gtfs-snapshot-id") + 1] == "19", module
    for module, cmd in by_date["2026-09-13"].items():
        assert "--gtfs-snapshot-id" not in cmd, module
    assert "service_date=2026-09-12 snapshot_id=19" in log_text
    assert "PINNED" in log_text


@pytest.mark.smoke
def test_run_batch_resolver_error_falls_back_to_is_current(tmp_path, monkeypatch):
    """A resolution failure is logged loudly and the date runs against is_current."""

    def _boom(service_date):
        """Simulate a DB error during resolution."""
        raise RuntimeError("db unavailable")

    captured, log_text = _run_batch_capturing(tmp_path, monkeypatch, [date(2026, 9, 12)], _boom)

    assert captured
    assert all("--gtfs-snapshot-id" not in cmd for cmd in captured)
    assert "WARN gtfs_snapshot: resolution failed" in log_text


@pytest.mark.smoke
def test_run_batch_dry_run_shows_pinned_snapshot(tmp_path, monkeypatch):
    """--dry-run prints the per-date snapshot pin in its plan lines, runs nothing,
    and never calls the match guard."""

    def _guard_must_not_run(d, choice):
        """Fail if the guard is consulted under dry-run."""
        raise AssertionError("match guard ran under dry_run")

    captured, log_text = _run_batch_capturing(
        tmp_path,
        monkeypatch,
        [date(2026, 9, 12)],
        lambda d: SnapshotChoice(19, 20, REASON_COVERS),
        match_guard=_guard_must_not_run,
        dry_run=True,
    )
    assert captured == []
    assert "DRY-RUN would run pipelines.aggregate_runs --all-routes --date 2026-09-12 " in log_text
    assert "--gtfs-snapshot-id 19 --agency sfmta" in log_text
    assert "match_guard" not in log_text


@pytest.mark.smoke
def test_run_batch_match_guard_skips_a_collapsed_catch_up_date(tmp_path, monkeypatch):
    """The 9/29 catch-up shape: a date whose observed trip_ids don't exist in its
    snapshot is skipped entirely (no derive, no aggregate, counted as a failure)
    while a healthy date in the same batch still runs."""
    from src.trip_match_rate import TripMatchResult

    results = {
        date(2026, 9, 12): TripMatchResult(9917, 0, 0.0, gtfs_snapshot_id=21),
        date(2026, 9, 13): TripMatchResult(9800, 9700, 9700 / 9800, gtfs_snapshot_id=21),
    }
    captured, log_text = _run_batch_capturing(
        tmp_path,
        monkeypatch,
        [date(2026, 9, 12), date(2026, 9, 13)],
        lambda d: SnapshotChoice(21, 21, REASON_FALLBACK_CURRENT),
        match_guard=lambda d, choice: results[d],
        expected_failures=0,  # guard skips are reported separately (guard_skipped), not as failures
    )
    dates_run = {cmd[cmd.index("--date") + 1] for cmd in captured}
    assert dates_run == {"2026-09-13"}
    assert "SKIP all pipelines for 2026-09-12" in log_text
    assert "0/9917 observed trip_ids in snapshot 21" in log_text


@pytest.mark.smoke
def test_run_batch_match_guard_no_observations_or_error_still_derives(tmp_path, monkeypatch):
    """rate None (nothing observed) and a guard exception both let the date derive."""
    from src.trip_match_rate import TripMatchResult

    def _guard(d, choice):
        """No observations for 9/12; a DB error for 9/13."""
        if d == date(2026, 9, 12):
            return TripMatchResult(0, 0, None)
        raise RuntimeError("db unavailable")

    captured, log_text = _run_batch_capturing(
        tmp_path,
        monkeypatch,
        [date(2026, 9, 12), date(2026, 9, 13)],
        lambda d: None,
        match_guard=_guard,
    )
    dates_run = {cmd[cmd.index("--date") + 1] for cmd in captured}
    assert dates_run == {"2026-09-12", "2026-09-13"}
    assert "WARN match_guard: could not check 2026-09-13" in log_text


# ---------------------------------------------------------------------------
# canary wiring
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_canary_checks_against_the_dates_snapshot_not_is_current(db_session):
    """The 9/12 shape: observed trip_ids exist only in outgoing snapshot 19. Against
    is_current (20) that is a 0% 'collapse'; against the date's snapshot it's healthy."""
    from scripts.gtfs_trip_match_canary import compute_trip_match_rate
    from src.models import Trip, VehiclePosition
    from src.timezones import local_midnight_as_utc

    _seed_feed_boundary(db_session)
    d = date(2026, 9, 12)
    ts = local_midnight_as_utc(d, "America/New_York").replace(hour=14)
    db_session.add_all(
        [
            Trip(trip_id="old-1", route_id="R1", snapshot_id=19, is_current=False),
            Trip(trip_id="old-2", route_id="R1", snapshot_id=19, is_current=False),
            Trip(trip_id="new-1", route_id="R1", snapshot_id=20, is_current=True),
        ]
        + [
            VehiclePosition(
                vehicle_id=f"v{i}",
                route_id="R1",
                trip_id=tid,
                trip_start_date="20260912",
                latitude=0.0,
                longitude=0.0,
                timestamp=ts,
            )
            for i, tid in enumerate(["old-1", "old-2"])
        ]
    )
    db_session.commit()

    result = compute_trip_match_rate(db_session, d, "America/New_York")

    assert result.gtfs_snapshot_id == 19
    assert result.matched_count == 2
    assert result.rate == 1.0


# ---------------------------------------------------------------------------
# Postgres: the YYYYMMDD string comparisons behave the same on the real engine
# ---------------------------------------------------------------------------


def test_resolver_on_postgres(pg_session):
    """Same 9/12 boundary scenario on Postgres (varchar comparison, exists() subquery)."""
    from sqlalchemy import text

    # Use ids far above anything a scratch DB is likely to hold; the
    # pg_session fixture rolls everything back.
    base = 900_000
    max_existing = pg_session.execute(text("SELECT MAX(snapshot_id) FROM gtfs_snapshots")).scalar()
    assert max_existing is None or max_existing < base
    # Mark any pre-existing current calendar rows non-current inside the
    # rolled-back transaction so "current" is unambiguous for this test.
    pg_session.execute(text("UPDATE calendar SET is_current = FALSE WHERE is_current"))
    # Load times later than any real snapshot so pre-existing rows in a
    # non-empty scratch DB can't out-rank these as "newest".
    pg_session.add_all(
        [
            _snapshot(base + 19, datetime(2031, 1, 1)),
            _snapshot(base + 20, datetime(2031, 1, 2)),
        ]
    )
    pg_session.flush()
    pg_session.add_all(
        [
            _calendar(base + 19, "20260621", "20260912", is_current=False),
            _calendar(base + 20, "20260913", "20270327", is_current=True),
        ]
    )
    pg_session.flush()

    assert resolve_gtfs_snapshot_for_date(pg_session, date(2026, 9, 12)) == SnapshotChoice(
        base + 19, base + 20, REASON_COVERS
    )
    assert resolve_gtfs_snapshot_for_date(pg_session, date(2026, 9, 13)).override_id is None


# ---------------------------------------------------------------------------
# "covers" = schedules service on D (gtfs_calendar rule), not "inside a range"
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_back_dated_newer_calendar_with_removals_does_not_cover(db_session):
    """A newer feed whose calendar range is back-dated over D but whose only
    service is removed on D (exception_type=2) does not cover D; the older
    feed that really runs on D wins."""
    db_session.add_all([_snapshot(1, datetime(2026, 6, 1)), _snapshot(2, datetime(2026, 9, 6))])
    db_session.flush()
    db_session.add_all(
        [
            _calendar(1, "20260301", "20260912", is_current=False),
            _calendar(2, "20260901", "20270327", is_current=True),
            CalendarDate(
                service_id="SVC2",
                date="20260905",
                exception_type=2,
                snapshot_id=2,
                valid_from=datetime(2026, 1, 1),
                is_current=True,
            ),
        ]
    )
    db_session.commit()

    assert resolve_gtfs_snapshot_for_date(db_session, date(2026, 9, 5)) == SnapshotChoice(
        1, 2, REASON_COVERS
    )
    # A date the newer feed really serves still goes to the newer feed.
    assert resolve_gtfs_snapshot_for_date(db_session, date(2026, 9, 6)).snapshot_id == 2


@pytest.mark.smoke
def test_newer_calendar_without_that_weekday_does_not_cover(db_session):
    """Day-of-week matters: a newer weekday-only feed back-dated over a Saturday
    doesn't cover it; the older every-day feed does."""
    db_session.add_all([_snapshot(1, datetime(2026, 6, 1)), _snapshot(2, datetime(2026, 9, 6))])
    db_session.flush()
    weekday_only = _calendar(2, "20260901", "20270327", is_current=True)
    weekday_only.saturday = 0
    weekday_only.sunday = 0
    db_session.add_all([_calendar(1, "20260301", "20260912", is_current=False), weekday_only])
    db_session.commit()

    saturday = date(2026, 9, 5)
    assert saturday.weekday() == 5
    assert resolve_gtfs_snapshot_for_date(db_session, saturday).snapshot_id == 1
    assert resolve_gtfs_snapshot_for_date(db_session, date(2026, 9, 4)).snapshot_id == 2


# ---------------------------------------------------------------------------
# guard: skipped dates stay skipped; None choice -> is_current
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_guard_skipped_date_is_not_aggregated_even_if_a_later_derive_writes_onto_it(
    tmp_path, monkeypatch
):
    """Targets D (0% match, skipped) and D+1 (healthy) where D+1's derive reports
    an owl-corrected write onto D: nothing downstream may run for D."""
    import pipelines.run_daily_batch as run_daily_batch_module
    from src.trip_match_rate import TripMatchResult

    captured = []

    def _fake_subprocess_run(cmd, **kwargs):
        """Record the command; D+1's derive claims it also wrote D."""
        captured.append(cmd)
        if cmd[2] == "pipelines.derive_stop_events_from_state":
            kwargs["stdout"].write("SERVICE_DATES_WRITTEN:2026-09-12,2026-09-13\n")
            kwargs["stdout"].flush()
        return _FakeCompletedProcess()

    monkeypatch.setattr(run_daily_batch_module.subprocess, "run", _fake_subprocess_run)
    results = {
        date(2026, 9, 12): TripMatchResult(9917, 0, 0.0, gtfs_snapshot_id=21),
        date(2026, 9, 13): TripMatchResult(100, 100, 1.0, gtfs_snapshot_id=21),
    }
    skipped: set = set()
    with (tmp_path / "b.log").open("a") as log_handle:
        failures = run_daily_batch_module.run_batch(
            [date(2026, 9, 12), date(2026, 9, 13)],
            log_handle,
            agency="sfmta",
            snapshot_resolver=lambda d: None,
            match_guard=lambda d, choice: results[d],
            guard_skipped=skipped,
        )

    assert failures == 0
    assert skipped == {date(2026, 9, 12)}
    assert all(cmd[cmd.index("--date") + 1] == "2026-09-13" for cmd in captured)


@pytest.mark.smoke
def test_match_guard_with_failed_resolution_checks_is_current(db_session, monkeypatch):
    """choice=None (resolution failed -> derive uses is_current) must be checked
    against is_current, not re-resolved to some other snapshot."""
    import pipelines.run_daily_batch as run_daily_batch_module
    from src.models import Trip, VehiclePosition
    from src.timezones import local_midnight_as_utc

    _seed_feed_boundary(db_session)
    d = date(2026, 9, 12)
    db_session.add_all(
        [
            Trip(trip_id="old-1", route_id="R1", snapshot_id=19, is_current=False),
            Trip(trip_id="new-1", route_id="R1", snapshot_id=20, is_current=True),
            VehiclePosition(
                vehicle_id="v1",
                route_id="R1",
                trip_id="old-1",
                trip_start_date="20260912",
                latitude=0.0,
                longitude=0.0,
                timestamp=local_midnight_as_utc(d, "America/New_York").replace(hour=14),
            ),
        ]
    )
    db_session.commit()

    class _NoClose:
        """Keep the fixture session open across the guard's close()."""

        def __getattr__(self, name):
            """Delegate to the test session."""
            return getattr(db_session, name)

        def close(self):
            """No-op."""

    monkeypatch.setattr(run_daily_batch_module, "get_session", lambda db_url=None: _NoClose())
    guard = run_daily_batch_module.make_match_guard("wmata")

    # Re-resolving would pick snapshot 19 (100%); the derive will use is_current (0%).
    assert guard(d, None).rate == 0.0
    assert guard(d, SnapshotChoice(19, 20, REASON_COVERS)).rate == 1.0


@pytest.mark.smoke
def test_main_exit_code_3_when_only_guard_skips(tmp_path, monkeypatch):
    """main() returns 3 (not 1) when no pipeline failed but a date was held back."""
    import pipelines.run_daily_batch as run_daily_batch_module

    def _fake_run_batch(
        target_dates, log_handle, dry_run=False, agency="wmata", guard_skipped=None
    ):
        """Pretend the guard held back the only date."""
        guard_skipped.add(target_dates[0])
        return 0

    monkeypatch.setattr(run_daily_batch_module, "LOGS_DIR", tmp_path)
    monkeypatch.setattr(
        run_daily_batch_module, "determine_target_dates", lambda **k: [date(2026, 9, 12)]
    )
    monkeypatch.setattr(run_daily_batch_module, "list_active_route_ids", lambda **k: [])
    monkeypatch.setattr(run_daily_batch_module, "run_batch", _fake_run_batch)
    monkeypatch.setattr("sys.argv", ["run_daily_batch.py"])

    assert run_daily_batch_module.main() == run_daily_batch_module.EXIT_GUARD_SKIPS_ONLY == 3


# ---------------------------------------------------------------------------
# bunching's scheduled side follows the pinned snapshot (blocking review fix)
# ---------------------------------------------------------------------------


def _seed_two_schedules(session):
    """Route R: snapshot 19 runs 08:00/08:10 at stop X (600s headway); current
    snapshot 20 runs 08:00/08:30 (1800s) on the same date."""
    from src.models import StopTime, Trip

    session.add_all([_snapshot(19, datetime(2026, 9, 7)), _snapshot(20, datetime(2026, 9, 13))])
    session.flush()
    session.add_all(
        [
            _calendar(19, "20260621", "20260912", is_current=False),
            _calendar(20, "20260601", "20270327", is_current=True),
        ]
    )
    for snap, current, times in (
        (19, False, ("08:00:00", "08:10:00")),
        (20, True, ("08:00:00", "08:30:00")),
    ):
        for i, t in enumerate(times):
            trip_id = f"T{snap}-{i}"
            session.add(
                Trip(
                    trip_id=trip_id,
                    route_id="R",
                    service_id=f"SVC{snap}",
                    direction_id=0,
                    snapshot_id=snap,
                    is_current=current,
                )
            )
            session.add(
                StopTime(
                    trip_id=trip_id,
                    stop_id="X",
                    arrival_time=t,
                    departure_time=t,
                    stop_sequence=1,
                    snapshot_id=snap,
                    is_current=current,
                )
            )
    session.commit()


@pytest.mark.smoke
def test_bunching_scheduled_headways_follow_the_pinned_snapshot(db_session):
    """Without the fix this either raises (no gtfs_snapshot_id parameter) or
    returns the current feed's 1800s headway for a date pinned to snapshot 19."""
    from src.ewt import _scheduled_headways_by_cell_hour_for_date

    _seed_two_schedules(db_session)
    d = date(2026, 9, 12)

    pinned = _scheduled_headways_by_cell_hour_for_date(db_session, "R", d, gtfs_snapshot_id=19)
    current = _scheduled_headways_by_cell_hour_for_date(db_session, "R", d)

    assert [h for hs in pinned.values() for h in hs] == [600.0]
    assert [h for hs in current.values() for h in hs] == [1800.0]


@pytest.mark.smoke
def test_compute_bunching_cli_threads_snapshot_into_the_computation(db_session, monkeypatch):
    """materialize_for_routes -> compute_bunching_for_route_date receives the id."""
    import pipelines.compute_bunching as compute_bunching_module

    seen = []

    def _fake_compute(
        db, route_id, service_date, tz_name="America/New_York", gtfs_snapshot_id=None
    ):
        """Record the snapshot id the bunching computation was asked for."""
        seen.append(gtfs_snapshot_id)
        return []

    monkeypatch.setattr(compute_bunching_module, "compute_bunching_for_route_date", _fake_compute)
    compute_bunching_module.materialize_for_routes(
        db_session, ["R"], [date(2026, 9, 12)], gtfs_snapshot_id=19
    )
    assert seen == [19]
