"""Tests for pipelines.compute_bunching's CLI argument guards (NOTES-100)
and its tz_name forwarding (NOTES-103 follow-up)."""

import sys
from datetime import date

import pytest


@pytest.mark.smoke
def test_days_without_date_rejected_for_non_default_agency(monkeypatch):
    """--days without --date resolves via iter_recent_eastern_dates, which
    is hardcoded to eastern_today() -- silently wrong for a non-WMATA
    agency's backfill window. Must fail loudly (parser.error -> SystemExit)
    rather than compute the wrong date range.
    """
    import pipelines.compute_bunching as compute_bunching_module

    monkeypatch.setattr(
        sys, "argv", ["compute_bunching.py", "--all-routes", "--days", "3", "--agency", "sfmta"]
    )

    with pytest.raises(SystemExit):
        compute_bunching_module.main()


@pytest.mark.smoke
def test_days_without_date_allowed_for_default_agency(db_session, monkeypatch):
    """The same combination is unchanged for the default (wmata) agency --
    this guard must not regress the existing --days-alone usage. Runs
    against an empty in-memory DB (zero routes), so it completes cleanly
    rather than raising -- proving argparse accepted the combination.
    """
    import pipelines.compute_bunching as compute_bunching_module

    monkeypatch.setattr(sys, "argv", ["compute_bunching.py", "--all-routes", "--days", "3"])
    monkeypatch.setattr(compute_bunching_module, "get_session", lambda db_url=None: db_session)

    compute_bunching_module.main()  # must not raise SystemExit(2) from parser.error


@pytest.mark.smoke
def test_materialize_forwards_tz_name_to_compute_bunching(db_session, monkeypatch):
    """NOTES-103 follow-up: `materialize_bunching_for_route_date` must
    forward `tz_name` into `compute_bunching_for_route_date` -- otherwise
    a `--agency sfmta` run of `run_daily_batch.py` silently computes
    `route_headway_metrics` on the hardcoded Eastern default even though
    `main()` already resolves the agency's own timezone via `cfg.timezone`
    (used for the `--date`-less default), producing
    `total_headways=0` / `bunching_rate=NULL` for a healthy SFMTA day.
    """
    import pipelines.compute_bunching as compute_bunching_module

    seen_tz_names = []

    def _fake_compute_bunching(db, route_id, service_date, tz_name="America/New_York"):
        seen_tz_names.append(tz_name)
        return []  # empty -> materialize skips upsert_rows entirely

    monkeypatch.setattr(
        compute_bunching_module, "compute_bunching_for_route_date", _fake_compute_bunching
    )

    compute_bunching_module.materialize_bunching_for_route_date(
        db_session, "R1", date(2026, 7, 23), tz_name="America/Los_Angeles"
    )

    assert seen_tz_names == ["America/Los_Angeles"]


@pytest.mark.smoke
def test_materialize_defaults_tz_name_to_eastern(db_session, monkeypatch):
    """Omitting `tz_name` keeps existing WMATA behavior unchanged."""
    import pipelines.compute_bunching as compute_bunching_module

    seen_tz_names = []

    def _fake_compute_bunching(db, route_id, service_date, tz_name="America/New_York"):
        seen_tz_names.append(tz_name)
        return []

    monkeypatch.setattr(
        compute_bunching_module, "compute_bunching_for_route_date", _fake_compute_bunching
    )

    compute_bunching_module.materialize_bunching_for_route_date(db_session, "R1", date(2026, 5, 3))

    assert seen_tz_names == ["America/New_York"]


@pytest.mark.smoke
def test_materialize_for_routes_forwards_tz_name(db_session, monkeypatch):
    """`materialize_for_routes` must pass `tz_name` through
    `run_route_date_grid`'s kwargs down to each per-(route, date) call --
    the grid iterator forwards `**kwargs` verbatim, so this only requires
    that `materialize_for_routes` itself accepts and passes it along.
    """
    import pipelines.compute_bunching as compute_bunching_module

    seen_tz_names = []

    def _fake_compute_bunching(db, route_id, service_date, tz_name="America/New_York"):
        seen_tz_names.append(tz_name)
        return []

    monkeypatch.setattr(
        compute_bunching_module, "compute_bunching_for_route_date", _fake_compute_bunching
    )

    compute_bunching_module.materialize_for_routes(
        db_session, ["R1", "R2"], [date(2026, 7, 23)], tz_name="America/Los_Angeles"
    )

    assert seen_tz_names == ["America/Los_Angeles", "America/Los_Angeles"]


@pytest.mark.smoke
def test_main_forwards_cfg_timezone_to_materialize_for_routes(db_session, monkeypatch):
    """End-to-end CLI wiring: `main()` already resolves `cfg.timezone` for
    the `--date`-less default (see the existing `--days` guard tests
    above) -- it must also pass that same `cfg.timezone` into
    `materialize_for_routes` so a `--agency sfmta` invocation buckets
    bunching by Pacific, not Eastern.
    """
    import pipelines.compute_bunching as compute_bunching_module

    captured = {}

    def _fake_materialize_for_routes(db, route_ids, service_dates, tz_name="America/New_York"):
        captured["tz_name"] = tz_name
        return []

    monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///sfmta_test")
    monkeypatch.setattr(sys, "argv", ["compute_bunching.py", "--route", "R1", "--agency", "sfmta"])
    monkeypatch.setattr(compute_bunching_module, "get_session", lambda db_url=None: db_session)
    monkeypatch.setattr(
        compute_bunching_module, "materialize_for_routes", _fake_materialize_for_routes
    )

    compute_bunching_module.main()

    assert captured["tz_name"] == "America/Los_Angeles"
