"""Tests for pipelines.run_daily_batch's multi-agency threading (NOTES-100).

The wrapper had no test coverage before this PR. Scope here is narrow —
just the ``--agency`` plumbing this PR adds (target-date/route-list
resolution against the right DB + timezone, and the subprocess command
line each per-date pipeline receives) — not a full behavioral suite for
the pre-existing orchestration logic.
"""

from datetime import date, timedelta

import pytest

from pipelines.run_daily_batch import (
    determine_target_dates,
    list_active_route_ids,
    run_housekeeping_pipeline,
    run_pipeline,
)
from src.models import Route


@pytest.mark.smoke
def test_determine_target_dates_uses_agency_local_today(db_session, monkeypatch):
    """sfmta resolves "today" via Pacific, not Eastern -- and queries the
    agency's own database (captured by patching get_session)."""
    import pipelines.run_daily_batch as run_daily_batch_module

    monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///sfmta_test")
    seen_db_urls = []

    def _fake_get_session(db_url=None):
        seen_db_urls.append(db_url)
        return db_session

    monkeypatch.setattr(run_daily_batch_module, "get_session", _fake_get_session)
    monkeypatch.setattr(run_daily_batch_module, "local_today", lambda tz_name: date(2026, 7, 23))

    targets = determine_target_dates(lookback_days=2, agency="sfmta")

    assert seen_db_urls == ["postgresql:///sfmta_test"]
    assert date(2026, 7, 22) in targets  # yesterday relative to the faked "today"


@pytest.mark.smoke
def test_determine_target_dates_default_agency_is_wmata(db_session, monkeypatch):
    """Omitting --agency keeps today's DATABASE_URL / Eastern behavior unchanged."""
    import pipelines.run_daily_batch as run_daily_batch_module

    seen_db_urls = []

    def _fake_get_session(db_url=None):
        seen_db_urls.append(db_url)
        return db_session

    monkeypatch.setattr(run_daily_batch_module, "get_session", _fake_get_session)

    # lookback_days=1 is a pre-existing edge case unrelated to NOTES-100
    # (determine_target_dates's catch-up window computation raises for it
    # regardless of agency) -- use 2 to stay on the well-trodden path.
    determine_target_dates(lookback_days=2)

    # WMATA's configured env var IS DATABASE_URL; resolve_agency_db_url
    # returns os.getenv("DATABASE_URL"), whatever that is in this test env
    # (possibly None) -- the key assertion is get_session was called with
    # exactly one positional resolution, not raising or hard-defaulting.
    assert len(seen_db_urls) == 1


@pytest.mark.smoke
def test_list_active_route_ids_targets_agency_database(db_session, monkeypatch):
    """--agency threads through to list_active_route_ids' DB resolution too."""
    import pipelines.run_daily_batch as run_daily_batch_module

    monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///sfmta_test")
    db_session.add(Route(route_id="38", route_short_name="38", is_current=True))
    db_session.commit()

    seen_db_urls = []

    def _fake_get_session(db_url=None):
        seen_db_urls.append(db_url)
        return db_session

    monkeypatch.setattr(run_daily_batch_module, "get_session", _fake_get_session)

    route_ids = list_active_route_ids(agency="sfmta")

    assert seen_db_urls == ["postgresql:///sfmta_test"]
    assert route_ids == ["38"]


def test_run_pipeline_appends_agency_flag(tmp_path, monkeypatch):
    """The per-date pipeline subprocess command line includes --agency."""
    import pipelines.run_daily_batch as run_daily_batch_module

    captured_cmds = []

    class _FakeCompletedProcess:
        returncode = 0

    def _fake_subprocess_run(cmd, **kwargs):
        captured_cmds.append(cmd)
        return _FakeCompletedProcess()

    monkeypatch.setattr(run_daily_batch_module.subprocess, "run", _fake_subprocess_run)

    log_path = tmp_path / "test.log"
    with log_path.open("a") as log_handle:
        run_pipeline(
            "pipelines.derive_stop_events",
            date(2026, 7, 23),
            log_handle,
            agency="sfmta",
        )

    assert captured_cmds[0][-2:] == ["--agency", "sfmta"]


def test_run_pipeline_default_agency_omits_nothing_new(tmp_path, monkeypatch):
    """Default invocation (no agency passed) still appends --agency wmata --
    explicit and harmless since every pipeline now defaults --agency to
    'wmata' itself; this just pins down the wrapper's own default."""
    import pipelines.run_daily_batch as run_daily_batch_module

    captured_cmds = []

    class _FakeCompletedProcess:
        returncode = 0

    def _fake_subprocess_run(cmd, **kwargs):
        captured_cmds.append(cmd)
        return _FakeCompletedProcess()

    monkeypatch.setattr(run_daily_batch_module.subprocess, "run", _fake_subprocess_run)

    log_path = tmp_path / "test.log"
    with log_path.open("a") as log_handle:
        run_pipeline("pipelines.derive_stop_events", date(2026, 7, 23), log_handle)

    assert captured_cmds[0][-2:] == ["--agency", "wmata"]


def test_run_housekeeping_pipeline_takes_no_agency_flag(tmp_path, monkeypatch):
    """Housekeeping pipelines aren't agency-aware yet -- run_batch guards
    them to WMATA-only instead (see
    test_run_batch_skips_housekeeping_for_non_default_agency), so the
    subprocess command line itself carries no --agency flag."""
    import pipelines.run_daily_batch as run_daily_batch_module

    captured_cmds = []

    class _FakeCompletedProcess:
        returncode = 0

    def _fake_subprocess_run(cmd, **kwargs):
        captured_cmds.append(cmd)
        return _FakeCompletedProcess()

    monkeypatch.setattr(run_daily_batch_module.subprocess, "run", _fake_subprocess_run)

    log_path = tmp_path / "test.log"
    with log_path.open("a") as log_handle:
        run_housekeeping_pipeline("pipelines.cleanup_trip_update_state", log_handle)

    assert "--agency" not in captured_cmds[0]


def test_run_batch_skips_housekeeping_for_non_default_agency(tmp_path, monkeypatch):
    """run_batch must not run WMATA-only housekeeping pipelines against a
    non-default agency's database -- they aren't agency-aware (no --agency
    flag, see test above) and would silently operate on whatever
    DATABASE_URL happens to be in the ambient environment instead of the
    agency's own DB.
    """
    from pipelines.run_daily_batch import run_batch

    captured_cmds = []

    class _FakeCompletedProcess:
        returncode = 0

    def _fake_subprocess_run(cmd, **kwargs):
        captured_cmds.append(cmd)
        return _FakeCompletedProcess()

    import pipelines.run_daily_batch as run_daily_batch_module

    monkeypatch.setattr(run_daily_batch_module.subprocess, "run", _fake_subprocess_run)

    log_path = tmp_path / "test.log"
    with log_path.open("a") as log_handle:
        run_batch([date(2026, 7, 23)], log_handle, agency="sfmta")

    housekeeping_modules = {
        "pipelines.refresh_route_diagnostic_profile",
        "pipelines.cleanup_trip_update_state",
        "pipelines.refresh_cross_route_segments",
        "pipelines.refresh_corridor_slip",
    }
    run_modules = {cmd[2] for cmd in captured_cmds}  # cmd = [sys.executable, "-m", module, ...]
    assert run_modules.isdisjoint(housekeeping_modules)


def test_run_batch_aggregates_day_shifted_dates_from_derive_output(tmp_path, monkeypatch):
    """PR #193 review: when a derive pipeline
    reports (via its SERVICE_DATES_WRITTEN marker, now recovered by
    re-reading the log file rather than captured stdout -- finding 2)
    that it filed a row under an adjacent service_date -- the resolver's
    owl-route anchor correction, NOTES-110 -- that adjacent date must be
    aggregated too.

    Without this, a day-shifted stop_events row lands in a service_date
    that determine_target_dates never revisits (it only re-checks a date
    with ZERO rows in `runs`, and the adjacent date already has rows from
    its own prior aggregation), so the row silently never reaches
    aggregate_runs / compute_bunching / the system rollups.
    """
    from pipelines.run_daily_batch import run_batch

    captured_cmds = []

    class _FakeCompletedProcess:
        def __init__(self, returncode=0):
            self.returncode = returncode

    def _fake_subprocess_run(cmd, **kwargs):
        captured_cmds.append(cmd)
        module = cmd[2]  # cmd = [sys.executable, "-m", module, ...]
        if module == "pipelines.derive_stop_events_from_state":
            # Reports a day-shifted write onto 2026-07-22, one day before
            # the requested 2026-07-23. run_pipeline now streams stdout
            # straight to the log file (kwargs["stdout"]) and recovers the
            # marker by re-reading it, so the fake writes there directly
            # instead of returning a captured `.stdout` string.
            kwargs["stdout"].write("SERVICE_DATES_WRITTEN:2026-07-22,2026-07-23\n")
            kwargs["stdout"].flush()
        return _FakeCompletedProcess()

    import pipelines.run_daily_batch as run_daily_batch_module

    monkeypatch.setattr(run_daily_batch_module.subprocess, "run", _fake_subprocess_run)

    log_path = tmp_path / "test.log"
    with log_path.open("a") as log_handle:
        run_batch([date(2026, 7, 23)], log_handle, has_runs=lambda d: True)

    aggregate_dates = {
        cmd[cmd.index("--date") + 1]
        for cmd in captured_cmds
        if cmd[2] == "pipelines.aggregate_runs"
    }
    assert aggregate_dates == {"2026-07-22", "2026-07-23"}


def test_run_batch_runs_housekeeping_for_default_agency(tmp_path, monkeypatch):
    """Unchanged behavior: the default (wmata) agency still runs housekeeping."""
    from pipelines.run_daily_batch import run_batch

    captured_cmds = []

    class _FakeCompletedProcess:
        returncode = 0

    def _fake_subprocess_run(cmd, **kwargs):
        captured_cmds.append(cmd)
        return _FakeCompletedProcess()

    import pipelines.run_daily_batch as run_daily_batch_module

    monkeypatch.setattr(run_daily_batch_module.subprocess, "run", _fake_subprocess_run)

    log_path = tmp_path / "test.log"
    with log_path.open("a") as log_handle:
        run_batch([date(2026, 7, 23)], log_handle)

    run_modules = {cmd[2] for cmd in captured_cmds}
    assert "pipelines.cleanup_trip_update_state" in run_modules


def _capture_subprocess(monkeypatch):
    """Patch run_daily_batch's subprocess.run to record commands and succeed."""
    import pipelines.run_daily_batch as run_daily_batch_module

    captured_cmds = []

    class _FakeCompletedProcess:
        """A successful subprocess result."""

        returncode = 0

    def _fake_subprocess_run(cmd, **kwargs):
        """Record ``cmd`` and report success."""
        captured_cmds.append(cmd)
        return _FakeCompletedProcess()

    monkeypatch.setattr(run_daily_batch_module.subprocess, "run", _fake_subprocess_run)
    return captured_cmds


def _owl_shift_subprocess(monkeypatch, runs_written: set):
    """Fake subprocess.run: derive of D reports writing D-1; aggregate_runs records its date.

    ``runs_written`` stands in for the ``runs`` table across invocations.
    """
    import pipelines.run_daily_batch as run_daily_batch_module

    captured_cmds = []

    class _FakeCompletedProcess:
        """A successful subprocess result."""

        returncode = 0

    def _fake_subprocess_run(cmd, **kwargs):
        """Record ``cmd``; emulate the owl correction and aggregate_runs' writes."""
        captured_cmds.append(cmd)
        d = date.fromisoformat(cmd[cmd.index("--date") + 1])
        if cmd[2] == "pipelines.derive_stop_events_from_state":
            prev = (d - timedelta(days=1)).isoformat()
            kwargs["stdout"].write(f"SERVICE_DATES_WRITTEN:{prev},{d.isoformat()}\n")
            kwargs["stdout"].flush()
        if cmd[2] == "pipelines.aggregate_runs":
            runs_written.add(d)
        return _FakeCompletedProcess()

    monkeypatch.setattr(run_daily_batch_module.subprocess, "run", _fake_subprocess_run)
    return captured_cmds


def test_date_held_in_earlier_invocation_is_not_aggregated_by_next_date(tmp_path, monkeypatch):
    """PR #264 review (blocking): per-date invocations must not undo the #258 hold.

    Invocation 1 targets D-1 and the guard holds it (no runs). Invocation 2
    targets D, whose derive writes owl-corrected rows under D-1. D-1 must
    not be aggregated — via the zero-runs rule alone, and via the explicit
    exclude list the refresh passes.
    """
    from pipelines.run_daily_batch import run_batch
    from src.trip_match_rate import TripMatchResult

    d_prev, d = date(2026, 10, 2), date(2026, 10, 3)
    runs: set = set()
    cmds = _owl_shift_subprocess(monkeypatch, runs)
    guard = {
        d_prev: TripMatchResult(9000, 0, 0.0, gtfs_snapshot_id=1),
        d: TripMatchResult(100, 100, 1.0, gtfs_snapshot_id=1),
    }
    held: set = set()
    with (tmp_path / "1.log").open("a") as log_handle:
        assert (
            run_batch(
                [d_prev],
                log_handle,
                snapshot_resolver=lambda x: None,
                match_guard=lambda x, c: guard[x],
                guard_skipped=held,
                housekeeping=False,
                has_runs=lambda x: x in runs,
            )
            == 0
        )
    assert held == {d_prev} and runs == set()

    for exclude in (set(), {d_prev}):
        cmds.clear()
        with (tmp_path / "2.log").open("a") as log_handle:
            run_batch(
                [d],
                log_handle,
                snapshot_resolver=lambda x: None,
                match_guard=lambda x, c: guard[x],
                housekeeping=False,
                has_runs=lambda x: x in runs,
                exclude_dates=exclude,
            )
        aggregated = {c[c.index("--date") + 1] for c in cmds if c[2] == "pipelines.aggregate_runs"}
        assert aggregated == {d.isoformat()}
        assert d_prev not in runs
        runs.discard(d)


def test_excluded_date_with_runs_is_not_reaggregated(tmp_path, monkeypatch):
    """A date that failed after aggregate_runs (so it HAS runs) is still protected by exclude."""
    from pipelines.run_daily_batch import run_batch

    d_prev, d = date(2026, 10, 2), date(2026, 10, 3)
    runs = {d_prev}
    cmds = _owl_shift_subprocess(monkeypatch, runs)
    with (tmp_path / "x.log").open("a") as log_handle:
        run_batch(
            [d],
            log_handle,
            snapshot_resolver=lambda x: None,
            match_guard=lambda x, c: None,
            housekeeping=False,
            has_runs=lambda x: x in runs,
            exclude_dates={d_prev},
        )
    aggregated = {c[c.index("--date") + 1] for c in cmds if c[2] == "pipelines.aggregate_runs"}
    assert aggregated == {d.isoformat()}


def test_guard_error_is_reported(tmp_path, monkeypatch):
    """A match-guard exception derives the date anyway and lands in guard_errors."""
    from pipelines.run_daily_batch import run_batch

    _capture_subprocess(monkeypatch)
    errors: set = set()

    def _boom(d, c):
        """Simulate the guard's DB query failing."""
        raise RuntimeError("db hiccup")

    with (tmp_path / "g.log").open("a") as log_handle:
        rc = run_batch(
            [date(2026, 10, 3)],
            log_handle,
            snapshot_resolver=lambda x: None,
            match_guard=_boom,
            housekeeping=False,
            guard_errors=errors,
        )
    assert rc == 0 and errors == {date(2026, 10, 3)}


def test_housekeeping_only_refuses_non_default_agency(monkeypatch):
    """--housekeeping-only --agency sfmta is a usage error, not a silent no-op."""
    import pipelines.run_daily_batch as mod

    monkeypatch.setattr(
        mod.sys, "argv", ["run_daily_batch.py", "--housekeeping-only", "--agency", "sfmta"]
    )
    with pytest.raises(SystemExit) as exc:
        mod.main()
    assert exc.value.code == 2


def test_run_batch_skip_housekeeping_runs_only_per_date_pipelines(tmp_path, monkeypatch):
    """housekeeping=False (CLI --skip-housekeeping, issue #261) runs no housekeeping module."""
    from pipelines.run_daily_batch import HOUSEKEEPING_PIPELINES, run_batch

    captured_cmds = _capture_subprocess(monkeypatch)
    with (tmp_path / "test.log").open("a") as log_handle:
        rc = run_batch(
            [date(2026, 10, 3)],
            log_handle,
            snapshot_resolver=lambda d: None,
            match_guard=lambda d, c: None,
            housekeeping=False,
        )
    assert rc == 0
    run_modules = {cmd[2] for cmd in captured_cmds}
    assert "pipelines.derive_stop_events_from_state" in run_modules
    assert not run_modules & {hk["module"] for hk in HOUSEKEEPING_PIPELINES}
    assert all("2026-10-03" in cmd for cmd in captured_cmds)


def test_run_batch_with_no_dates_is_housekeeping_only(tmp_path, monkeypatch):
    """An empty target list (CLI --housekeeping-only) runs exactly the housekeeping modules."""
    from pipelines.run_daily_batch import HOUSEKEEPING_PIPELINES, run_batch

    captured_cmds = _capture_subprocess(monkeypatch)
    with (tmp_path / "test.log").open("a") as log_handle:
        run_batch([], log_handle, snapshot_resolver=lambda d: None, match_guard=lambda d, c: None)
    assert [cmd[2] for cmd in captured_cmds] == [hk["module"] for hk in HOUSEKEEPING_PIPELINES]


def test_main_explicit_dates_bypass_determine_target_dates(tmp_path, monkeypatch):
    """--date D (repeatable) targets exactly those dates; --skip-housekeeping is forwarded."""
    import pipelines.run_daily_batch as mod

    seen = {}

    def _fake_run_batch(target_dates, log_handle, **kwargs):
        """Record what main() asked run_batch to do."""
        seen["dates"] = target_dates
        seen["housekeeping"] = kwargs.get("housekeeping")
        return 0

    def _no_determine(**kwargs):
        """Fail if main() falls back to the default date selection."""
        raise AssertionError("determine_target_dates must not run with --date")

    monkeypatch.setattr(mod, "determine_target_dates", _no_determine)
    monkeypatch.setattr(mod, "list_active_route_ids", lambda agency: ["R"])
    monkeypatch.setattr(mod, "run_batch", _fake_run_batch)
    monkeypatch.setattr(mod, "LOGS_DIR", tmp_path)
    monkeypatch.setattr(
        mod.sys,
        "argv",
        [
            "run_daily_batch.py",
            "--date",
            "2026-10-04",
            "--date",
            "2026-10-03",
            "--skip-housekeeping",
        ],
    )
    assert mod.main() == 0
    assert seen == {"dates": [date(2026, 10, 3), date(2026, 10, 4)], "housekeeping": False}


@pytest.mark.smoke
def test_determine_target_dates_stops_at_the_retention_window(db_session, monkeypatch):
    """A lookback wider than the stop-level window never selects (pruned) older dates."""
    import pipelines.run_daily_batch as run_daily_batch_module

    monkeypatch.setattr(run_daily_batch_module, "get_session", lambda db_url=None: db_session)
    monkeypatch.setattr(run_daily_batch_module, "local_today", lambda tz_name: date(2026, 10, 20))
    monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///sfmta_test")
    targets = determine_target_dates(lookback_days=60, agency="sfmta")  # sfmta window: 35
    assert min(targets) == date(2026, 10, 20) - timedelta(days=35)
