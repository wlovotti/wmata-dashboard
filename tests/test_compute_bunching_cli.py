"""Tests for pipelines.compute_bunching's CLI argument guards (NOTES-100)."""

import sys

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
