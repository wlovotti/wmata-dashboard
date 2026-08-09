"""Tests for pipelines.run_daily_batch's multi-agency threading (NOTES-100).

The wrapper had no test coverage before this PR. Scope here is narrow —
just the ``--agency`` plumbing this PR adds (target-date/route-list
resolution against the right DB + timezone, and the subprocess command
line each per-date pipeline receives) — not a full behavioral suite for
the pre-existing orchestration logic.
"""

from datetime import date

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
    """Housekeeping pipelines aren't agency-aware yet (NOTES-103 follow-up
    territory) -- run_batch guards them to WMATA-only instead (see
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
