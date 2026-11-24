"""Tests for pipelines/refresh_by_day.py — per-date orchestration, resume, failures (#261).

Every step is a ``FakeSteps`` method that records calls and returns
scripted results, so these exercise the orchestration (ordering,
progress file vs DB, stop/continue policy, per-date state cleanup,
end-of-run housekeeping) without S3, subprocesses, or a database.
"""

from datetime import date, datetime, timedelta

import pytest

from pipelines.refresh_by_day import (
    DERIVED,
    DONE,
    EXIT_FAILED,
    EXIT_HELD_ONLY,
    EXIT_OK,
    FAILED,
    HELD,
    AgencyRun,
    Progress,
    default_dates,
    last_complete_date,
    run_refresh,
)
from src.raw_archive import ArchiveStream

D1, D2, D3 = date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)


class Interrupted(Exception):
    """Stands in for Ctrl-C / a crash mid-run."""


class FakeSteps:
    """Scripted per-step results; records every call in order."""

    def __init__(self, derive_rc=None, load_rc=None, fetch_error=None, interrupt=None):
        """Maps of (agency, date) → rc / exception; ``interrupt`` = (step, agency, date)."""
        self.derive_rc = derive_rc or {}
        self.load_rc = load_rc or {}
        self.fetch_error = fetch_error or {}
        self.interrupt = interrupt
        self.calls: list[tuple] = []
        self.excludes: dict[tuple, list] = {}

    def _maybe_interrupt(self, step, run, d):
        """Raise ``Interrupted`` when this call matches ``interrupt``."""
        if self.interrupt == (step, run.name, d):
            raise Interrupted(step)

    def fetch(self, run, d):
        """Record; optionally raise."""
        self.calls.append(("fetch", run.name, d))
        if (run.name, d) in self.fetch_error:
            raise RuntimeError(self.fetch_error[(run.name, d)])

    def load_vp(self, run, d):
        """Record; scripted rc."""
        self.calls.append(("load_vp", run.name, d))
        return self.load_rc.get((run.name, d), 0)

    def replay(self, run, d, force):
        """Record (with force flag)."""
        self.calls.append(("replay", run.name, d, force))
        return 0

    def derive(self, run, d, exclude):
        """Record (and the exclude list); scripted rc; optional interrupt."""
        self.calls.append(("derive", run.name, d))
        self.excludes[(run.name, d)] = list(exclude)
        self._maybe_interrupt("derive", run, d)
        return self.derive_rc.get((run.name, d), 0)

    def clean_state(self, run, d):
        """Record."""
        self.calls.append(("clean_state", run.name, d))
        return 0

    def prune(self, run, d):
        """Record; optional interrupt."""
        self.calls.append(("prune", run.name, d))
        self._maybe_interrupt("prune", run, d)

    def housekeeping(self):
        """Record."""
        self.calls.append(("housekeeping",))
        return 0

    def cleanup(self, agency):
        """Record."""
        self.calls.append(("cleanup", agency))
        return 0

    def retention(self, agency):
        """Record; scripted rc via ``retention_rc``."""
        self.calls.append(("retention", agency))
        return getattr(self, "retention_rc", 0)

    def steps_for(self, name, step):
        """Dates a given step ran for one agency, in order."""
        return [c[2] for c in self.calls if c[0] == step and c[1] == name]


def _run(tmp_path, name, dates, with_runs=()) -> AgencyRun:
    """An AgencyRun whose progress file lives under ``tmp_path``."""
    tu = ArchiveStream(name, "tu", tmp_path / name / "tu", "b", "p/", "America/New_York")
    vp = ArchiveStream(name, "vp", tmp_path / name / "vp", "b", "p/vp/", "America/New_York")
    progress = Progress(tmp_path / f"refresh_progress_{name}.json")
    return AgencyRun(name, "America/New_York", tu, vp, progress, list(dates), set(with_runs))


@pytest.mark.smoke
def test_dates_run_oldest_first_interleaved_with_steps_in_order(tmp_path):
    """Each date: fetch→load→replay→derive→clean_state→prune; agencies interleave by date."""
    steps = FakeSteps()
    runs = [_run(tmp_path, "wmata", [D1, D2]), _run(tmp_path, "sfmta", [D1, D2])]
    assert run_refresh(runs, steps, force=False, housekeeping=True) == EXIT_OK
    assert [c for c in steps.calls if c[0] == "fetch"] == [
        ("fetch", "wmata", D1),
        ("fetch", "sfmta", D1),
        ("fetch", "wmata", D2),
        ("fetch", "sfmta", D2),
    ]
    first = [c[0] for c in steps.calls if c[1:3] == ("wmata", D1)]
    assert first == ["fetch", "load_vp", "replay", "derive", "clean_state", "prune"]
    assert steps.calls[-2:] == [("housekeeping",), ("cleanup", "sfmta")]
    assert runs[0].progress.status(D2) == DONE


@pytest.mark.smoke
def test_resume_after_interruption_starts_at_first_unfinished_date(tmp_path):
    """A crash mid-D2 leaves D1 done; the rerun skips D1 (done + runs) and redoes D2."""
    dates = [D1, D2, D3]
    with pytest.raises(Interrupted):
        run_refresh(
            [_run(tmp_path, "wmata", dates)],
            FakeSteps(interrupt=("derive", "wmata", D2)),
            force=False,
            housekeeping=True,
        )
    rerun = FakeSteps()
    run = _run(tmp_path, "wmata", dates, with_runs={D1})  # re-reads the progress file
    assert run.progress.status(D1) == DONE
    assert run_refresh([run], rerun, force=False, housekeeping=True) == EXIT_OK
    assert rerun.steps_for("wmata", "fetch") == [D2, D3]


@pytest.mark.smoke
def test_progress_done_without_runs_is_not_done(tmp_path):
    """The DB wins: a 'done' date with zero runs is processed again."""
    run_refresh([_run(tmp_path, "wmata", [D1])], FakeSteps(), force=False, housekeeping=False)
    rerun = FakeSteps()
    run_refresh([_run(tmp_path, "wmata", [D1])], rerun, force=False, housekeeping=False)
    assert rerun.steps_for("wmata", "derive") == [D1]


@pytest.mark.smoke
def test_resume_between_derive_and_prune_only_reprunes(tmp_path):
    """A crash after the derive committed resumes with just the prune for that date."""
    with pytest.raises(Interrupted):
        run_refresh(
            [_run(tmp_path, "wmata", [D1, D2])],
            FakeSteps(interrupt=("prune", "wmata", D1)),
            force=False,
            housekeeping=False,
        )
    rerun = FakeSteps()
    run = _run(tmp_path, "wmata", [D1, D2], with_runs={D1})
    assert run.progress.status(D1) == DERIVED
    run_refresh([run], rerun, force=False, housekeeping=False)
    assert [c for c in rerun.calls if c[2] == D1] == [("prune", "wmata", D1)]
    assert rerun.steps_for("wmata", "derive") == [D2]


@pytest.mark.smoke
def test_derived_entry_without_runs_is_processed_in_full(tmp_path):
    """A 'derived' progress entry is only trusted when runs exist for the date."""
    run = _run(tmp_path, "wmata", [D1])
    run.progress.record(D1, DERIVED)
    steps = FakeSteps()
    run_refresh([run], steps, force=False, housekeeping=False)
    assert steps.steps_for("wmata", "derive") == [D1]


@pytest.mark.smoke
def test_corrupt_progress_file_is_set_aside(tmp_path):
    """An unreadable progress file warns, is moved aside, and the run starts from the DB."""
    path = tmp_path / "refresh_progress_wmata.json"
    path.write_text("{not json")
    p = Progress(path)
    assert p.status(D1) is None
    assert not path.exists()
    assert list(tmp_path.glob("refresh_progress_wmata.json.corrupt-*"))
    # read-only (dry run) leaves it alone
    path.write_text("[]")
    Progress(path, read_only=True)
    assert path.exists()


@pytest.mark.smoke
def test_failed_date_stops_that_agency_only_and_keeps_completed_dates(tmp_path):
    """WMATA fails at D2 → D3 not attempted, D1 stays done, no WMATA housekeeping.

    The failed date gets neither state cleanup nor prune (its files and
    state are kept for the retry). SFMTA runs all dates and its cleanup
    still runs because SFMTA itself had no failure.
    """
    steps = FakeSteps(derive_rc={("wmata", D2): 1})
    wmata = _run(tmp_path, "wmata", [D1, D2, D3])
    sfmta = _run(tmp_path, "sfmta", [D1, D2, D3])
    assert run_refresh([wmata, sfmta], steps, force=False, housekeeping=True) == EXIT_FAILED
    assert steps.steps_for("wmata", "derive") == [D1, D2]
    assert steps.steps_for("wmata", "clean_state") == [D1]
    assert steps.steps_for("wmata", "prune") == [D1]
    assert steps.steps_for("sfmta", "derive") == [D1, D2, D3]
    assert wmata.progress.status(D1) == DONE
    assert wmata.progress.status(D2) == FAILED
    assert wmata.progress.status(D3) is None
    assert ("cleanup", "sfmta") in steps.calls
    assert ("housekeeping",) not in steps.calls


@pytest.mark.smoke
def test_sfmta_failure_skips_its_cleanup(tmp_path):
    """A hard SFMTA failure leaves trip_update_state alone for the retry."""
    steps = FakeSteps(load_rc={("sfmta", D1): 1})
    run_refresh([_run(tmp_path, "sfmta", [D1, D2])], steps, force=False, housekeeping=True)
    assert ("cleanup", "sfmta") not in steps.calls
    assert steps.steps_for("sfmta", "replay") == []


@pytest.mark.smoke
def test_fetch_error_fails_the_date(tmp_path):
    """An S3/disk problem at fetch fails the date before anything touches the DB."""
    steps = FakeSteps(fetch_error={("wmata", D1): "only 3.0 GB free"})
    run = _run(tmp_path, "wmata", [D1, D2])
    assert run_refresh([run], steps, force=False, housekeeping=True) == EXIT_FAILED
    assert steps.steps_for("wmata", "load_vp") == []
    assert "only 3.0 GB free" in run.progress.data["dates"][D1.isoformat()]["detail"]
    assert ("housekeeping",) not in steps.calls


@pytest.mark.smoke
def test_held_date_is_reported_excluded_downstream_and_run_continues(tmp_path):
    """Exit 3 marks the date held, later derives exclude it, and the run exits 3."""
    steps = FakeSteps(derive_rc={("wmata", D1): 3})
    run = _run(tmp_path, "wmata", [D1, D2])
    assert run_refresh([run], steps, force=False, housekeeping=True) == EXIT_HELD_ONLY
    assert run.progress.status(D1) == HELD
    assert run.progress.status(D2) == DONE
    assert steps.excludes[("wmata", D2)] == [D1]
    assert steps.excludes[("wmata", D1)] == []
    assert steps.steps_for("wmata", "prune") == [D1, D2]
    rerun = FakeSteps()
    run_refresh(
        [_run(tmp_path, "wmata", [D1, D2], with_runs={D2})], rerun, force=False, housekeeping=False
    )
    assert rerun.steps_for("wmata", "derive") == [D1]


@pytest.mark.smoke
def test_guard_error_derives_but_fails_the_run(tmp_path):
    """run_daily_batch exit 4 (guard could not run): date done, warning, exit 1, no stop."""
    steps = FakeSteps(derive_rc={("wmata", D1): 4})
    run = _run(tmp_path, "wmata", [D1, D2])
    assert run_refresh([run], steps, force=False, housekeeping=False) == EXIT_FAILED
    assert run.progress.status(D1) == DONE
    assert steps.steps_for("wmata", "derive") == [D1, D2]
    assert any("guard could not run" in w for w in run.warnings)


@pytest.mark.smoke
def test_force_reprocesses_done_dates_and_forces_replay(tmp_path):
    """--force re-runs a done date and passes force to the replay step."""
    run_refresh([_run(tmp_path, "wmata", [D1])], FakeSteps(), force=False, housekeeping=False)
    steps = FakeSteps()
    run_refresh(
        [_run(tmp_path, "wmata", [D1], with_runs={D1})], steps, force=True, housekeeping=False
    )
    assert ("replay", "wmata", D1, True) in steps.calls


@pytest.mark.smoke
def test_skip_housekeeping(tmp_path):
    """--skip-housekeeping suppresses WMATA's end-of-run housekeeping."""
    steps = FakeSteps()
    run_refresh([_run(tmp_path, "wmata", [D1])], steps, force=False, housekeeping=False)
    assert ("housekeeping",) not in steps.calls


@pytest.mark.smoke
def test_retention_is_opt_in_and_runs_after_housekeeping(tmp_path):
    """Retention runs only for named agencies, after housekeeping/cleanup."""
    steps = FakeSteps()
    runs = [_run(tmp_path, "wmata", [D1]), _run(tmp_path, "sfmta", [D1])]
    assert run_refresh(runs, steps, force=False, housekeeping=True) == EXIT_OK
    assert not [c for c in steps.calls if c[0] == "retention"]

    steps = FakeSteps()
    runs = [_run(tmp_path / "b", "wmata", [D1]), _run(tmp_path / "b", "sfmta", [D1])]
    rc = run_refresh(
        runs, steps, force=False, housekeeping=True, retention=frozenset({"wmata", "sfmta"})
    )
    assert rc == EXIT_OK
    tail = steps.calls[-4:]
    assert tail == [
        ("housekeeping",),
        ("retention", "wmata"),
        ("cleanup", "sfmta"),
        ("retention", "sfmta"),
    ]


@pytest.mark.smoke
def test_retention_skipped_after_a_failure_and_failure_propagates(tmp_path):
    """A failed date skips that agency's retention; a failing retention fails the run."""
    steps = FakeSteps(derive_rc={("wmata", D1): 1})
    run_refresh(
        [_run(tmp_path, "wmata", [D1])],
        steps,
        force=False,
        housekeeping=True,
        retention=frozenset({"wmata"}),
    )
    assert ("retention", "wmata") not in steps.calls

    steps = FakeSteps()
    steps.retention_rc = 1
    rc = run_refresh(
        [_run(tmp_path / "b", "sfmta", [D1])],
        steps,
        force=False,
        housekeeping=True,
        retention=frozenset({"sfmta"}),
    )
    assert rc == EXIT_FAILED


@pytest.mark.smoke
def test_default_dates_never_treat_pruned_dates_as_holes(db_session):
    """A lookback wider than the stop-level window stops at the window start."""
    today = date(2026, 10, 20)
    # Runs only in the last 10 days; days 11..60 back have none (pruned).
    _add_runs(db_session, [today - timedelta(days=n) for n in range(1, 11)])
    sel = default_dates(db_session, 60, today, stop_level_days=35)
    assert sel.dates and min(sel.dates) == today - timedelta(days=35)
    sel = default_dates(db_session, 60, today)  # no window: old behaviour
    assert min(sel.dates) == today - timedelta(days=60)


def _add_runs(db_session, dates):
    """Insert one runs row per date."""
    from src.models import Run

    for i, d in enumerate(dates):
        db_session.add(
            Run(
                route_id="R",
                trip_id=f"t{i}",
                direction_id=0,
                source="trip_update",
                service_date=d.isoformat(),
            )
        )
    db_session.flush()


@pytest.mark.smoke
def test_default_dates_continue_after_latest_runs_and_catch_holes(db_session):
    """Default range = latest runs date + 1 .. yesterday, plus zero-runs holes in lookback."""
    today = date(2026, 10, 20)
    _add_runs(db_session, [today - timedelta(days=n) for n in (12, 11, 9, 8, 7, 6)])
    sel = default_dates(db_session, 14, today)
    dates = sel.dates
    assert dates[0] == today - timedelta(days=14)
    assert today - timedelta(days=10) in dates
    assert today - timedelta(days=9) not in dates
    assert dates[-1] == today - timedelta(days=1)
    assert today - timedelta(days=5) in dates
    assert sel.deferred == []


@pytest.mark.smoke
def test_default_dates_add_unfinished_progress_and_defer_incomplete(db_session, tmp_path):
    """Failed dates (even with runs, any age) and recent held dates are re-selected.

    Old held dates are reported, not retried; dates past the completeness
    cap are deferred.
    """
    today = date(2026, 10, 20)
    old_failed = today - timedelta(days=30)  # has runs (failed after aggregate_runs)
    recent_held = today - timedelta(days=3)
    old_held = today - timedelta(days=40)
    _add_runs(db_session, [old_failed] + [today - timedelta(days=n) for n in range(2, 15)])
    progress = Progress(tmp_path / "p.json")
    progress.record(old_failed, FAILED, "derive rc=1")
    progress.record(recent_held, HELD)
    progress.record(old_held, HELD)
    sel = default_dates(
        db_session, 14, today, last_complete=today - timedelta(days=2), progress=progress
    )
    assert old_failed in sel.dates
    assert recent_held in sel.dates
    assert old_held not in sel.dates and sel.stale_held == [old_held]
    assert sel.deferred == [today - timedelta(days=1)]
    assert today - timedelta(days=1) not in sel.dates


@pytest.mark.smoke
def test_default_dates_requires_some_runs(db_session):
    """An empty runs table has no anchor — the caller must pass a range."""
    with pytest.raises(RuntimeError, match="runs is empty"):
        default_dates(db_session, 14, date(2026, 10, 20))


@pytest.mark.smoke
@pytest.mark.parametrize(
    ("tz", "margin_h", "now_utc", "expected"),
    [
        # WMATA (EST, UTC-5) 5 h margin: 11/23 completes at 11/24 05:00 ET = 10:00 UTC
        ("America/New_York", 5, datetime(2026, 11, 24, 9, 59), date(2026, 11, 22)),
        ("America/New_York", 5, datetime(2026, 11, 24, 10, 0), date(2026, 11, 23)),
        # SFMTA (PST, UTC-8) 9 h margin: 11/23 completes at 11/24 09:00 PT = 17:00 UTC
        ("America/Los_Angeles", 9, datetime(2026, 11, 24, 16, 59), date(2026, 11, 22)),
        ("America/Los_Angeles", 9, datetime(2026, 11, 24, 17, 0), date(2026, 11, 23)),
        # 01:00 ET run: yesterday (11/23) is not complete
        ("America/New_York", 5, datetime(2026, 11, 24, 6, 0), date(2026, 11, 22)),
    ],
)
def test_last_complete_date(tz, margin_h, now_utc, expected):
    """A date is complete only after local midnight following it + the agency margin."""
    assert last_complete_date(tz, timedelta(hours=margin_h), now_utc) == expected


@pytest.mark.smoke
def test_window_margins_cover_last_trip_plus_upload_lag():
    """WMATA 26:58 / SFMTA 30:53 last arrivals + ~16 min upload lag leave >= 1.5 h slack."""
    from pipelines.refresh_by_day import WINDOW_MARGIN

    upload = timedelta(minutes=16)
    assert WINDOW_MARGIN["wmata"] - timedelta(hours=2, minutes=58) - upload >= timedelta(hours=1.5)
    assert WINDOW_MARGIN["sfmta"] - timedelta(hours=6, minutes=53) - upload >= timedelta(hours=1.5)


@pytest.mark.smoke
def test_failed_after_aggregate_retry_refolds_after_state_was_cleaned(tmp_path):
    """Review finding A: F fails in the TU derive (runs already written by aggregate_runs).

    A later run derives another date, whose per-date cleanup deletes F's
    state. The retry of F must replay with --force (re-fold from raw files)
    and then derive fully — not derive from empty state.
    """
    steps1 = FakeSteps(derive_rc={("wmata", D1): 1})
    run1 = _run(tmp_path, "wmata", [D1])
    run_refresh([run1], steps1, force=False, housekeeping=False)
    assert run1.progress.status(D1) == FAILED

    steps2 = FakeSteps()  # a separate, explicit run of a later date
    run_refresh(
        [_run(tmp_path, "wmata", [D3], with_runs={D1})], steps2, force=False, housekeeping=False
    )
    assert steps2.steps_for("wmata", "clean_state") == [D3]  # cutoff D3 > D1: F's state gone
    assert steps2.excludes[("wmata", D3)] == [D1]

    steps3 = FakeSteps()
    run3 = _run(tmp_path, "wmata", [D1], with_runs={D1, D3})
    assert run_refresh([run3], steps3, force=False, housekeeping=False) == EXIT_OK
    assert ("replay", "wmata", D1, True) in steps3.calls
    assert steps3.steps_for("wmata", "derive") == [D1]
    assert run3.progress.status(D1) == DONE


@pytest.mark.smoke
def test_plain_new_date_replays_without_force(tmp_path):
    """Only previously failed/held/abandoned dates force the replay."""
    steps = FakeSteps()
    run_refresh([_run(tmp_path, "wmata", [D1])], steps, force=False, housekeeping=False)
    assert ("replay", "wmata", D1, False) in steps.calls


@pytest.mark.smoke
def test_abandon_date_escape(tmp_path, db_session, monkeypatch):
    """--abandon-date: recorded, skipped by default selection, excluded downstream, summarized."""
    import pipelines.refresh_by_day as mod

    monkeypatch.setattr(mod.run_lock, "LOCK_DIR", tmp_path / "lock.d")
    monkeypatch.delenv(mod.run_lock.LOCK_ENV, raising=False)
    monkeypatch.setattr("src.database.get_session", lambda db_url=None: db_session)

    # F failed every time: default selection keeps returning to it.
    progress = Progress(tmp_path / "refresh_progress_wmata.json")
    progress.record(D1, FAILED, "derive rc=1")
    today = date(2026, 10, 20)
    _add_runs(db_session, [today - timedelta(days=n) for n in range(2, 15)])
    sel = default_dates(db_session, 14, today, progress=progress)
    assert D1 in sel.dates

    rc = mod.main(
        [
            "--agency",
            "wmata",
            "--abandon-date",
            D1.isoformat(),
            "--progress-dir",
            str(tmp_path),
        ]
    )
    assert rc == EXIT_OK
    progress = Progress(tmp_path / "refresh_progress_wmata.json")
    assert progress.status(D1) == mod.ABANDONED
    assert not (tmp_path / "lock.d").exists()  # lock taken and released
    assert D1 not in default_dates(db_session, 14, today, progress=progress).dates

    # excluded from neighbours' aggregation; listed in the summary
    steps = FakeSteps()
    run = _run(tmp_path, "wmata", [D2])
    run_refresh([run], steps, force=False, housekeeping=False)
    assert steps.excludes[("wmata", D2)] == [D1]


@pytest.mark.smoke
def test_abandoned_next_date_is_skipped_by_main_range(db_session, tmp_path):
    """Re-review: F = max(runs)+1 always fails; once abandoned, default runs go past it."""
    import pipelines.refresh_by_day as mod

    today = date(2026, 10, 20)
    f = today - timedelta(days=6)
    _add_runs(db_session, [f - timedelta(days=n) for n in range(1, 10)])  # max(runs) = F-1
    progress = Progress(tmp_path / "refresh_progress_wmata.json")
    progress.record(f, FAILED, "derive rc=1")
    assert default_dates(db_session, 14, today, progress=progress).dates[0] == f

    progress.record(f, mod.ABANDONED)
    dates = default_dates(db_session, 14, today, progress=progress).dates
    assert f not in dates
    assert dates == [f + timedelta(days=n) for n in range(1, 6)]

    # the run proceeds to F+1 and beyond; F is listed as abandoned in the summary
    run = _run(tmp_path, "wmata", dates)
    steps = FakeSteps()
    assert run_refresh([run], steps, force=False, housekeeping=False) == EXIT_OK
    assert steps.steps_for("wmata", "derive") == dates
    assert all(steps.excludes[("wmata", d)] == [f] for d in dates)
    assert run.progress.dates_with(mod.ABANDONED) == [f]


@pytest.mark.smoke
def test_abandon_refuses_a_derived_date_without_confirmation(tmp_path, db_session):
    """Abandoning a date that already has runs (or is done) needs --abandon-derived."""
    import pipelines.refresh_by_day as mod

    _add_runs(db_session, [D1])
    args = mod.build_parser().parse_args(["--agency", "wmata", "--abandon-date", D1.isoformat()])
    rc = mod.abandon(args, tmp_path, session_factory=lambda: db_session)
    assert rc == mod.EXIT_USAGE
    assert Progress(tmp_path / "refresh_progress_wmata.json").status(D1) is None

    args = mod.build_parser().parse_args(
        ["--agency", "wmata", "--abandon-date", D1.isoformat(), "--abandon-derived"]
    )
    assert mod.abandon(args, tmp_path, session_factory=lambda: db_session) == EXIT_OK
    assert Progress(tmp_path / "refresh_progress_wmata.json").status(D1) == mod.ABANDONED
    assert mod.main(["--check-args", "--abandon-derived"]) == mod.EXIT_USAGE


@pytest.mark.smoke
def test_abandoned_zero_runs_date_is_not_a_hole(db_session, tmp_path):
    """An abandoned date with no runs inside the lookback is not re-selected as a hole."""
    import pipelines.refresh_by_day as mod

    today = date(2026, 10, 20)
    hole = today - timedelta(days=5)
    _add_runs(db_session, [today - timedelta(days=n) for n in range(2, 15) if n != 5])
    progress = Progress(tmp_path / "p.json")
    assert hole in default_dates(db_session, 14, today, progress=progress).dates
    progress.record(hole, mod.ABANDONED)
    assert hole not in default_dates(db_session, 14, today, progress=progress).dates


@pytest.mark.smoke
def test_deferred_dates_are_recorded_and_always_returned_to(db_session, tmp_path):
    """A recorded 'deferred' date is selected later even after newer dates were derived."""
    import pipelines.refresh_by_day as mod

    today = date(2026, 11, 20)
    deferred = date(2026, 10, 1)  # long outside the 14-day hole lookback
    _add_runs(db_session, [today - timedelta(days=n) for n in range(1, 15)])
    progress = Progress(tmp_path / "p.json")
    run = _run(tmp_path, "wmata", [])
    run.progress = progress
    run.to_defer = [deferred]
    mod.record_deferrals([run])
    assert progress.status(deferred) == mod.DEFERRED
    assert deferred in default_dates(db_session, 14, today, progress=progress).dates


@pytest.mark.smoke
def test_check_args_rejects_typos_and_bad_combinations(tmp_path):
    """--check-args validates without touching anything; argparse typos exit 2."""
    import pipelines.refresh_by_day as mod

    assert mod.main(["--check-args", "--agency", "wmata", "--date", "2026-10-01"]) == EXIT_OK
    with pytest.raises(SystemExit) as exc:
        mod.main(["--check-args", "--dryrun"])
    assert exc.value.code == 2
    assert mod.main(["--check-args", "--start", "2026-10-01"]) == mod.EXIT_USAGE
    assert mod.main(["--check-args", "--abandon-date", "2026-10-01"]) == mod.EXIT_USAGE


@pytest.mark.smoke
def test_real_steps_command_lines(tmp_path, monkeypatch):
    """RealSteps: VP D..D+2, SFMTA --allow-empty, one-date derive with excludes, state cleanup."""
    import pipelines.refresh_by_day as mod

    cmds = []

    class _Done:
        """A successful subprocess result."""

        returncode = 0

    monkeypatch.setattr(mod.subprocess, "run", lambda cmd, **kw: cmds.append(cmd[2:]) or _Done())
    steps = mod.RealSteps.__new__(mod.RealSteps)  # skip boto3 client creation
    sfmta = _run(tmp_path, "sfmta", [D1])
    steps.load_vp(sfmta, D1)
    steps.replay(sfmta, D1, force=True)
    steps.derive(sfmta, D1, [date(2026, 9, 30)])
    steps.clean_state(sfmta, D1)
    load, replay, derive, clean = cmds
    assert load[0] == "pipelines.load_vp_archive"
    assert [load[i + 1] for i, a in enumerate(load) if a == "--utc-date"] == [
        "2026-10-01",
        "2026-10-02",
        "2026-10-03",
    ]
    assert replay[:3] == ["pipelines.replay_archive_to_state", "--date", "2026-10-01"]
    assert "--allow-empty" in replay and "--force" in replay
    assert derive == [
        "pipelines.run_daily_batch",
        "--agency",
        "sfmta",
        "--date",
        "2026-10-01",
        "--skip-housekeeping",
        "--exclude-date",
        "2026-09-30",
    ]
    assert clean == [
        "pipelines.cleanup_trip_update_state",
        "--agency",
        "sfmta",
        "--before-date",
        "2026-10-01",
        "--vacuum",
    ]


@pytest.mark.smoke
def test_explicit_date_without_vp_source_is_refused(tmp_path, monkeypatch):
    """Review S1: a VM-era date whose VP was pruned is refused (TU-only would null OTP)."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    import pipelines.refresh_by_day as mod
    import src.database
    from src.models import Base, VpArchiveLoadedFile

    engine = create_engine(f"sqlite:///{tmp_path / 'r.db'}")
    Base.metadata.create_all(engine)
    maker = sessionmaker(bind=engine)
    s = maker()
    s.add(VpArchiveLoadedFile(filename="2026-08-22.7414.1787432050.jsonl.zst", row_count=1))
    s.commit()
    s.close()
    monkeypatch.setattr(src.database, "get_session", lambda db_url=None: maker())
    now = datetime(2026, 11, 24, 12)

    def plan(*extra):
        """plan_runs for one explicit date."""
        args = mod.build_parser().parse_args(
            ["--agency", "wmata", "--date", "2026-07-15", "--progress-dir", str(tmp_path), *extra]
        )
        return mod.plan_runs(args, mod.explicit_dates(args), now)

    assert plan() == mod.EXIT_USAGE
    runs = plan("--allow-missing-vp")
    assert [r.dates for r in runs] == [[date(2026, 7, 15)]]
