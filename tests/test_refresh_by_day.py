"""Tests for pipelines/refresh_by_day.py — per-date orchestration, resume, failures (#261).

Every step is a ``FakeSteps`` method that records calls and returns
scripted results, so these exercise the orchestration (ordering,
progress file, stop/continue policy, end-of-run housekeeping) without
S3, subprocesses, or a database.
"""

from datetime import date, timedelta

import pytest

from pipelines.refresh_by_day import (
    DONE,
    EXIT_FAILED,
    EXIT_HELD_ONLY,
    EXIT_OK,
    FAILED,
    HELD,
    AgencyRun,
    Progress,
    default_dates,
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

    def derive(self, run, d):
        """Record; scripted rc; optional interrupt."""
        self.calls.append(("derive", run.name, d))
        self._maybe_interrupt("derive", run, d)
        return self.derive_rc.get((run.name, d), 0)

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

    def steps_for(self, name, step):
        """Dates a given step ran for one agency, in order."""
        return [c[2] for c in self.calls if c[0] == step and c[1] == name]


def _run(tmp_path, name, dates) -> AgencyRun:
    """An AgencyRun whose progress file lives under ``tmp_path``."""
    tu = ArchiveStream(name, "tu", tmp_path / name / "tu", "b", "p/", "America/New_York")
    vp = ArchiveStream(name, "vp", tmp_path / name / "vp", "b", "p/vp/", "America/New_York")
    progress = Progress(tmp_path / f"refresh_progress_{name}.json")
    return AgencyRun(name, "America/New_York", tu, vp, progress, list(dates))


@pytest.mark.smoke
def test_dates_run_oldest_first_interleaved_with_steps_in_order(tmp_path):
    """Each date runs fetch→load→replay→derive→prune; agencies interleave by date."""
    steps = FakeSteps()
    runs = [_run(tmp_path, "wmata", [D1, D2]), _run(tmp_path, "sfmta", [D1, D2])]
    assert run_refresh(runs, steps, force=False, housekeeping=True) == EXIT_OK
    per_date = [c for c in steps.calls if c[0] in ("fetch",)]
    assert per_date == [
        ("fetch", "wmata", D1),
        ("fetch", "sfmta", D1),
        ("fetch", "wmata", D2),
        ("fetch", "sfmta", D2),
    ]
    first = [c[0] for c in steps.calls if c[1:3] == ("wmata", D1)]
    assert first == ["fetch", "load_vp", "replay", "derive", "prune"]
    # housekeeping / cleanup exactly once, after every date
    assert steps.calls[-2:] == [("housekeeping",), ("cleanup", "sfmta")]
    assert runs[0].progress.status(D2) == DONE


@pytest.mark.smoke
def test_resume_after_interruption_starts_at_first_unfinished_date(tmp_path):
    """A crash mid-D2 leaves D1 done; the rerun skips D1 and redoes D2 from the top."""
    dates = [D1, D2, D3]
    with pytest.raises(Interrupted):
        run_refresh(
            [_run(tmp_path, "wmata", dates)],
            FakeSteps(interrupt=("derive", "wmata", D2)),
            force=False,
            housekeeping=True,
        )
    rerun = FakeSteps()
    run = _run(tmp_path, "wmata", dates)  # re-reads the progress file
    assert run.progress.status(D1) == DONE
    assert run_refresh([run], rerun, force=False, housekeeping=True) == EXIT_OK
    assert rerun.steps_for("wmata", "fetch") == [D2, D3]


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
    run_refresh([_run(tmp_path, "wmata", [D1, D2])], rerun, force=False, housekeeping=False)
    assert [c for c in rerun.calls if c[2] == D1] == [("prune", "wmata", D1)]
    assert rerun.steps_for("wmata", "derive") == [D2]


@pytest.mark.smoke
def test_failed_date_stops_that_agency_only_and_keeps_completed_dates(tmp_path):
    """WMATA fails at D2 → D3 not attempted, D1 stays done; SFMTA runs all dates.

    The failed date is not pruned (its files are kept for the retry), and
    SFMTA's cleanup still runs because SFMTA itself had no failure.
    """
    steps = FakeSteps(derive_rc={("wmata", D2): 1})
    wmata = _run(tmp_path, "wmata", [D1, D2, D3])
    sfmta = _run(tmp_path, "sfmta", [D1, D2, D3])
    assert run_refresh([wmata, sfmta], steps, force=False, housekeeping=True) == EXIT_FAILED
    assert steps.steps_for("wmata", "derive") == [D1, D2]
    assert steps.steps_for("wmata", "prune") == [D1]
    assert steps.steps_for("sfmta", "derive") == [D1, D2, D3]
    assert wmata.progress.status(D1) == DONE
    assert wmata.progress.status(D2) == FAILED
    assert wmata.progress.status(D3) is None
    assert ("cleanup", "sfmta") in steps.calls


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
    assert ("housekeeping",) not in steps.calls  # nothing derived


@pytest.mark.smoke
def test_held_date_is_reported_and_run_continues(tmp_path):
    """run_daily_batch exit 3 marks the date held, keeps going, and exits 3."""
    steps = FakeSteps(derive_rc={("wmata", D1): 3})
    run = _run(tmp_path, "wmata", [D1, D2])
    assert run_refresh([run], steps, force=False, housekeeping=True) == EXIT_HELD_ONLY
    assert run.progress.status(D1) == HELD
    assert run.progress.status(D2) == DONE
    assert steps.steps_for("wmata", "prune") == [D1, D2]
    # a held date is retried on the next run (only DONE is skipped)
    rerun = FakeSteps()
    run_refresh([_run(tmp_path, "wmata", [D1, D2])], rerun, force=False, housekeeping=False)
    assert rerun.steps_for("wmata", "derive") == [D1]


@pytest.mark.smoke
def test_force_reprocesses_done_dates_and_forces_replay(tmp_path):
    """--force re-runs a done date and passes force to the replay step."""
    run_refresh([_run(tmp_path, "wmata", [D1])], FakeSteps(), force=False, housekeeping=False)
    steps = FakeSteps()
    run_refresh([_run(tmp_path, "wmata", [D1])], steps, force=True, housekeeping=False)
    assert ("replay", "wmata", D1, True) in steps.calls


@pytest.mark.smoke
def test_skip_housekeeping(tmp_path):
    """--skip-housekeeping suppresses WMATA's end-of-run housekeeping."""
    steps = FakeSteps()
    run_refresh([_run(tmp_path, "wmata", [D1])], steps, force=False, housekeeping=False)
    assert ("housekeeping",) not in steps.calls


@pytest.mark.smoke
def test_default_dates_continue_after_latest_runs_and_catch_holes(db_session):
    """Default range = latest runs date + 1 .. yesterday, plus zero-runs holes in lookback."""
    from src.models import Run

    today = date(2026, 10, 20)
    have = [today - timedelta(days=n) for n in (12, 11, 9, 8, 7, 6)]  # hole at -10; latest -6
    for i, d in enumerate(have):
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
    dates = default_dates(db_session, 14, today)
    assert dates[0] == today - timedelta(days=14)  # no runs at all that far back → hole
    assert today - timedelta(days=10) in dates
    assert today - timedelta(days=9) not in dates
    assert dates[-1] == today - timedelta(days=1)
    assert today - timedelta(days=5) in dates


@pytest.mark.smoke
def test_default_dates_requires_some_runs(db_session):
    """An empty runs table has no anchor — the caller must pass a range."""
    with pytest.raises(RuntimeError, match="runs is empty"):
        default_dates(db_session, 14, date(2026, 10, 20))


@pytest.mark.smoke
def test_real_steps_command_lines(tmp_path, monkeypatch):
    """RealSteps scopes VP to D..D+2, keeps SFMTA --allow-empty, derives one date sans housekeeping."""
    import pipelines.refresh_by_day as mod

    cmds = []

    class _Done:
        returncode = 0

    monkeypatch.setattr(mod.subprocess, "run", lambda cmd, **kw: cmds.append(cmd[2:]) or _Done())
    steps = mod.RealSteps.__new__(mod.RealSteps)  # skip boto3 client creation
    sfmta = _run(tmp_path, "sfmta", [D1])
    steps.load_vp(sfmta, D1)
    steps.replay(sfmta, D1, force=True)
    steps.derive(sfmta, D1)
    load, replay, derive = cmds
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
    ]
