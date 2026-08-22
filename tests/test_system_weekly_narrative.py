"""
Tests for the system-level weekly narrative feature (PR #219).

Sibling to tests/test_diagnosis_narrative.py (PR #141), covering the same
three layers for the system-wide weekly narrative instead of the per-route
diagnosis:

  - ``src/diagnosis_hash.py``: ``compute_system_snapshot_hash`` determinism,
    row-order independence, stable across equivalent representations.
  - ``scripts/generate_system_weekly_narrative.py``: subprocess invocation,
    error handling, dry-run behavior, ``claude`` PATH check.
  - API endpoint ``GET /api/system/weekly-narrative``:
      - 404 when no narrative is cached.
      - 200 with ``is_stale=False`` when hash matches and no newer week exists.
      - 200 with ``is_stale=True`` when the snapshot hash has changed.
      - 200 with ``is_stale=True`` when a newer week's data has landed.
"""

import types
from datetime import date, timedelta
from unittest.mock import patch

import pytest

from src.diagnosis_hash import compute_system_snapshot_hash
from src.models import SystemMetricsDaily, SystemWeeklyNarrative
from src.timezones import utcnow_naive

# ---------------------------------------------------------------------------
# compute_system_snapshot_hash tests
# ---------------------------------------------------------------------------


class TestComputeSystemSnapshotHash:
    """Unit tests for ``src/diagnosis_hash.compute_system_snapshot_hash``."""

    _ROW = {
        "service_date": "2026-08-10",
        "otp_percentage": 85.0,
        "service_delivered_ratio": 0.95,
        "ewt_seconds": 60.0,
        "swt_seconds": 120.0,
        "bunching_rate": 0.05,
        "data_quality": "complete",
    }
    _PRIOR_ROW = {**_ROW, "service_date": "2026-08-03", "otp_percentage": 80.0}

    def test_deterministic_same_input(self):
        """Same input always produces the same hash."""
        h1 = compute_system_snapshot_hash([self._ROW], [self._PRIOR_ROW])
        h2 = compute_system_snapshot_hash([self._ROW], [self._PRIOR_ROW])
        assert h1 == h2

    def test_hex_string_format(self):
        """Result is a 64-character lowercase hex string (SHA-256)."""
        h = compute_system_snapshot_hash([self._ROW], [self._PRIOR_ROW])
        assert len(h) == 64
        assert h == h.lower()
        assert all(c in "0123456789abcdef" for c in h)

    def test_row_order_independent(self):
        """Hash is identical regardless of the order rows are supplied within a window."""
        row2 = {**self._ROW, "service_date": "2026-08-11"}
        h_forward = compute_system_snapshot_hash([self._ROW, row2], [self._PRIOR_ROW])
        h_reversed = compute_system_snapshot_hash([row2, self._ROW], [self._PRIOR_ROW])
        assert h_forward == h_reversed

    def test_empty_inputs(self):
        """Empty current and prior week lists produce a stable, non-empty hash."""
        h = compute_system_snapshot_hash([], [])
        assert len(h) == 64

    def test_different_data_different_hash(self):
        """Changing any content field changes the hash."""
        h_original = compute_system_snapshot_hash([self._ROW], [self._PRIOR_ROW])
        modified = {**self._ROW, "otp_percentage": 50.0}
        h_modified = compute_system_snapshot_hash([modified], [self._PRIOR_ROW])
        assert h_original != h_modified

    def test_current_vs_prior_week_not_interchangeable(self):
        """Swapping current_week_rows and prior_week_rows changes the hash."""
        h_a = compute_system_snapshot_hash([self._ROW], [self._PRIOR_ROW])
        h_b = compute_system_snapshot_hash([self._PRIOR_ROW], [self._ROW])
        assert h_a != h_b

    def test_stable_across_equivalent_dicts_with_extra_keys(self):
        """Adding irrelevant keys (like id or computed_at) does not affect the hash."""
        row_with_extra = {**self._ROW, "id": 42, "computed_at": "2026-01-01"}
        h_clean = compute_system_snapshot_hash([self._ROW], [self._PRIOR_ROW])
        h_extra = compute_system_snapshot_hash([row_with_extra], [self._PRIOR_ROW])
        assert h_clean == h_extra


# ---------------------------------------------------------------------------
# _generate_narrative subprocess tests
# ---------------------------------------------------------------------------


def _make_subprocess_result(stdout="Test narrative text.", stderr="", returncode=0):
    """Build a minimal CompletedProcess-like object for mocking subprocess.run."""
    return types.SimpleNamespace(stdout=stdout, stderr=stderr, returncode=returncode)


class TestGenerateNarrative:
    """Unit tests for ``scripts.generate_system_weekly_narrative._generate_narrative``."""

    def test_returns_narrative_and_model_id(self):
        """Successful subprocess returns (narrative, MODEL_ID)."""
        from scripts.generate_system_weekly_narrative import MODEL_ID, _generate_narrative

        mock_result = _make_subprocess_result(stdout="  The network ran a bit late.  ")
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            narrative, model_id = _generate_narrative(
                date(2026, 8, 16), [], [], {"contributors": []}, {"contributors": []}
            )

        assert narrative == "The network ran a bit late."
        assert model_id == MODEL_ID
        mock_run.assert_called_once()

    def test_subprocess_called_with_correct_flags(self):
        """``claude -p`` is invoked with --system-prompt, --model, --tools, etc."""
        from scripts.generate_system_weekly_narrative import (
            MODEL_ID,
            SYSTEM_PROMPT,
            _generate_narrative,
        )

        mock_result = _make_subprocess_result(stdout="narrative")
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            _generate_narrative(
                date(2026, 8, 16), [], [], {"contributors": []}, {"contributors": []}
            )

        args, kwargs = mock_run.call_args
        cmd = args[0]
        assert cmd[0] == "claude"
        assert cmd[1] == "-p"
        assert "--system-prompt" in cmd
        assert SYSTEM_PROMPT in cmd
        assert "--model" in cmd
        assert MODEL_ID in cmd
        assert "--tools" in cmd
        assert "--disable-slash-commands" in cmd
        assert kwargs.get("capture_output") is True
        assert kwargs.get("text") is True
        assert kwargs.get("check") is False

    def test_non_zero_exit_raises_system_exit(self):
        """Non-zero returncode from ``claude`` causes a SystemExit."""
        from scripts.generate_system_weekly_narrative import _generate_narrative

        mock_result = _make_subprocess_result(stdout="", stderr="auth error", returncode=1)
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(SystemExit) as exc_info:
                _generate_narrative(
                    date(2026, 8, 16), [], [], {"contributors": []}, {"contributors": []}
                )
        assert exc_info.value.code == 1

    def test_stdout_is_stripped(self):
        """Leading/trailing whitespace is stripped from the returned narrative."""
        from scripts.generate_system_weekly_narrative import _generate_narrative

        mock_result = _make_subprocess_result(stdout="\n\n  Some narrative.\n\n")
        with patch("subprocess.run", return_value=mock_result):
            narrative, _ = _generate_narrative(
                date(2026, 8, 16), [], [], {"contributors": []}, {"contributors": []}
            )
        assert narrative == "Some narrative."

    def test_empty_stdout_raises_system_exit(self):
        """An exit-0-but-empty `claude` run raises SystemExit rather than
        returning an empty narrative for the caller to upsert (PR #219
        review finding 3)."""
        from scripts.generate_system_weekly_narrative import _generate_narrative

        mock_result = _make_subprocess_result(stdout="", stderr="", returncode=0)
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(SystemExit) as exc_info:
                _generate_narrative(
                    date(2026, 8, 16), [], [], {"contributors": []}, {"contributors": []}
                )
        assert exc_info.value.code != 0

    def test_whitespace_only_stdout_raises_system_exit(self):
        """Whitespace-only output is treated the same as empty output."""
        from scripts.generate_system_weekly_narrative import _generate_narrative

        mock_result = _make_subprocess_result(stdout="   \n\n  ", stderr="", returncode=0)
        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(SystemExit):
                _generate_narrative(
                    date(2026, 8, 16), [], [], {"contributors": []}, {"contributors": []}
                )


class TestMainClaudePathCheck:
    """Unit tests for the ``claude`` PATH check in ``main()``."""

    def test_missing_claude_returns_error_code(self):
        """``main()`` returns 1 when ``claude`` is not on PATH and --dry-run is not set."""
        from scripts.generate_system_weekly_narrative import main

        with patch("shutil.which", return_value=None):
            exit_code = main([])
        assert exit_code == 1

    def test_bad_as_of_format_errors_before_db(self, monkeypatch):
        """An invalid --as-of value is rejected without touching Claude or the DB."""
        from scripts.generate_system_weekly_narrative import main

        monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
        exit_code = main(["--as-of", "not-a-date", "--dry-run"])
        assert exit_code == 1


# ---------------------------------------------------------------------------
# _process_week dry-run test (exercises the prompt builder against a real
# SQLite session via the shared db_session fixture)
# ---------------------------------------------------------------------------


def _insert_week(db_session, start: date, otp: float, quality: str = "complete"):
    """Insert 7 consecutive SystemMetricsDaily rows starting at `start`."""
    for i in range(7):
        d = start + timedelta(days=i)
        db_session.add(
            SystemMetricsDaily(
                service_date=d.isoformat(),
                otp_percentage=otp,
                service_delivered_ratio=0.95,
                ewt_seconds=60.0,
                bunching_rate=0.05,
                data_quality=quality,
                computed_at=utcnow_naive(),
            )
        )
    db_session.commit()


class TestProcessWeekDryRun:
    """``_process_week`` with ``dry_run=True`` prints the prompt without calling Claude."""

    def test_dry_run_prints_prompt(self, db_session, capsys):
        from scripts.generate_system_weekly_narrative import _process_week

        _insert_week(db_session, date(2026, 8, 10), otp=85.0)

        with patch("api.aggregations.get_route_contributors", return_value={"contributors": []}):
            _process_week(db_session, date(2026, 8, 16), force=False, dry_run=True)

        captured = capsys.readouterr()
        assert "DRY RUN prompt" in captured.out
        assert "2026-08-16" in captured.out
        assert "85.0%" in captured.out

    def test_no_data_in_window_skips(self, db_session, capsys):
        """No system_metrics_daily rows in the window prints a skip message and does nothing else."""
        from scripts.generate_system_weekly_narrative import _process_week

        _process_week(db_session, date(2026, 8, 16), force=False, dry_run=True)

        captured = capsys.readouterr()
        assert "no system_metrics_daily rows" in captured.out


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_weekly_narrative_404_when_no_narrative(client):
    """GET /api/system/weekly-narrative returns 404 when no narrative is cached."""
    response = client.get("/api/system/weekly-narrative")
    assert response.status_code == 404
    assert "weekly narrative" in response.json()["detail"].lower()


@pytest.mark.api
def test_weekly_narrative_404_when_table_missing(client, db_session):
    """GET /api/system/weekly-narrative returns 404, not 500, when
    ``system_weekly_narrative`` doesn't exist yet (PR #219 review finding
    4) -- e.g. between a merge that adds this endpoint and the one-time
    migration script that creates the table. Simulated here by dropping
    the table the test schema created.

    ``test_engine`` is session-scoped (an in-memory SQLite DB shared, via
    StaticPool, by every test in the process), so the DROP must be undone
    before this test returns -- otherwise every later test in the module
    that needs the table would fail with the same "no such table" error.
    """
    from sqlalchemy import text

    from src.models import SystemWeeklyNarrative

    db_session.execute(text("DROP TABLE system_weekly_narrative"))
    try:
        response = client.get("/api/system/weekly-narrative")
        assert response.status_code == 404
        assert "does not exist" in response.json()["detail"].lower()
    finally:
        SystemWeeklyNarrative.__table__.create(bind=db_session.get_bind())


@pytest.mark.api
def test_weekly_narrative_200_not_stale(client, db_session):
    """200 with is_stale=False when the stored hash matches the current snapshot
    and no newer week's data has landed."""
    as_of = date(2026, 8, 16)
    prior_start = date(2026, 8, 3)
    _insert_week(db_session, prior_start, otp=80.0)
    _insert_week(db_session, date(2026, 8, 10), otp=85.0)

    current_rows = [
        {
            "service_date": (date(2026, 8, 10) + timedelta(days=i)).isoformat(),
            "otp_percentage": 85.0,
            "service_delivered_ratio": 0.95,
            "ewt_seconds": 60.0,
            "swt_seconds": None,
            "bunching_rate": 0.05,
            "data_quality": "complete",
        }
        for i in range(7)
    ]
    prior_rows = [
        {
            "service_date": (prior_start + timedelta(days=i)).isoformat(),
            "otp_percentage": 80.0,
            "service_delivered_ratio": 0.95,
            "ewt_seconds": 60.0,
            "swt_seconds": None,
            "bunching_rate": 0.05,
            "data_quality": "complete",
        }
        for i in range(7)
    ]
    current_hash = compute_system_snapshot_hash(current_rows, prior_rows)

    db_session.add(
        SystemWeeklyNarrative(
            as_of_date=as_of.isoformat(),
            narrative="Riders on frequent routes waited a bit longer this week.",
            generated_at=utcnow_naive(),
            model_id="claude-sonnet-4-6",
            prompt_version="v1",
            metrics_snapshot_hash=current_hash,
        )
    )
    db_session.commit()

    response = client.get("/api/system/weekly-narrative")
    assert response.status_code == 200
    data = response.json()
    assert data["as_of_date"] == "2026-08-16"
    assert data["narrative"] == "Riders on frequent routes waited a bit longer this week."
    assert data["is_stale"] is False
    assert data["model_id"] == "claude-sonnet-4-6"
    assert data["prompt_version"] == "v1"
    assert "generated_at" in data


@pytest.mark.api
def test_weekly_narrative_200_is_stale_when_hash_differs(client, db_session):
    """200 with is_stale=True when the stored hash doesn't match the current
    (same-week) snapshot -- e.g. a late backfill revised the week's numbers."""
    as_of = date(2026, 8, 16)
    _insert_week(db_session, date(2026, 8, 10), otp=85.0)

    stale_hash = "a" * 64  # deliberate mismatch
    db_session.add(
        SystemWeeklyNarrative(
            as_of_date=as_of.isoformat(),
            narrative="Stale narrative.",
            generated_at=utcnow_naive(),
            model_id="claude-sonnet-4-6",
            prompt_version="v1",
            metrics_snapshot_hash=stale_hash,
        )
    )
    db_session.commit()

    response = client.get("/api/system/weekly-narrative")
    assert response.status_code == 200
    assert response.json()["is_stale"] is True


@pytest.mark.api
def test_weekly_narrative_stale_when_newer_week_available(client, db_session):
    """200 with is_stale=True when system_metrics_daily has a later date than
    the cached narrative's as_of_date -- a new week's data has landed."""
    cached_as_of = date(2026, 8, 9)
    _insert_week(db_session, date(2026, 8, 3), otp=85.0)  # ends 2026-08-09

    # A later week's data has since landed.
    _insert_week(db_session, date(2026, 8, 10), otp=90.0)  # ends 2026-08-16

    db_session.add(
        SystemWeeklyNarrative(
            as_of_date=cached_as_of.isoformat(),
            narrative="Narrative for the week ending 8/9.",
            generated_at=utcnow_naive(),
            model_id="claude-sonnet-4-6",
            prompt_version="v1",
            metrics_snapshot_hash="irrelevant" + "0" * 55,
        )
    )
    db_session.commit()

    response = client.get("/api/system/weekly-narrative")
    assert response.status_code == 200
    data = response.json()
    assert data["as_of_date"] == "2026-08-09"
    assert data["is_stale"] is True


@pytest.mark.api
def test_weekly_narrative_not_stale_when_single_day_lands(client, db_session):
    """200 with is_stale=False when only a few extra daily rows have landed
    since generation -- not a whole newer week (PR #219 review finding 2,
    part a). The nightly batch inserts a system_metrics_daily row every day
    while narrative generation is manual, so day-granularity churn must not
    flag stale on its own."""
    as_of = date(2026, 8, 9)
    _insert_week(db_session, date(2026, 8, 3), otp=85.0)  # ends 2026-08-09

    current_rows = [
        {
            "service_date": (date(2026, 8, 3) + timedelta(days=i)).isoformat(),
            "otp_percentage": 85.0,
            "service_delivered_ratio": 0.95,
            "ewt_seconds": 60.0,
            "swt_seconds": None,
            "bunching_rate": 0.05,
            "data_quality": "complete",
        }
        for i in range(7)
    ]
    current_hash = compute_system_snapshot_hash(current_rows, [])

    db_session.add(
        SystemWeeklyNarrative(
            as_of_date=as_of.isoformat(),
            narrative="Narrative for the week ending 8/9.",
            generated_at=utcnow_naive(),
            model_id="claude-sonnet-4-6",
            prompt_version="v1",
            metrics_snapshot_hash=current_hash,
        )
    )
    db_session.commit()

    # One extra day's row lands (the nightly batch), well short of a full
    # newer 7-day week.
    db_session.add(
        SystemMetricsDaily(
            service_date=date(2026, 8, 10).isoformat(),
            otp_percentage=85.0,
            service_delivered_ratio=0.95,
            ewt_seconds=60.0,
            bunching_rate=0.05,
            data_quality="complete",
            computed_at=utcnow_naive(),
        )
    )
    db_session.commit()

    response = client.get("/api/system/weekly-narrative")
    assert response.status_code == 200
    assert response.json()["is_stale"] is False


@pytest.mark.api
def test_weekly_narrative_not_forced_stale_when_db_lags_narrative(client, db_session):
    """200 with is_stale=False when the local DB lags the narrative's own
    as_of_date but the (partial) data it does have still matches the stored
    hash (PR #219 review finding 2, part b) -- e.g. a scratch DB restored
    from a snapshot older than the narrative it was seeded alongside. The
    old `latest_available != row.as_of_date` comparison forced is_stale=True
    here regardless of the hash; it must fall through to the hash check
    instead."""
    as_of = date(2026, 8, 16)

    # Only a partial window (8/10-8/12) is present locally -- the DB lags
    # the narrative's own as_of_date of 8/16.
    partial_rows = []
    for d in (date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12)):
        db_session.add(
            SystemMetricsDaily(
                service_date=d.isoformat(),
                otp_percentage=85.0,
                service_delivered_ratio=0.95,
                ewt_seconds=60.0,
                bunching_rate=0.05,
                data_quality="complete",
                computed_at=utcnow_naive(),
            )
        )
        partial_rows.append(
            {
                "service_date": d.isoformat(),
                "otp_percentage": 85.0,
                "service_delivered_ratio": 0.95,
                "ewt_seconds": 60.0,
                "swt_seconds": None,
                "bunching_rate": 0.05,
                "data_quality": "complete",
            }
        )
    db_session.commit()

    # Stored hash matches exactly what the reader will recompute from this
    # (partial) local snapshot.
    matching_hash = compute_system_snapshot_hash(partial_rows, [])

    db_session.add(
        SystemWeeklyNarrative(
            as_of_date=as_of.isoformat(),
            narrative="Narrative for the week ending 8/16.",
            generated_at=utcnow_naive(),
            model_id="claude-sonnet-4-6",
            prompt_version="v1",
            metrics_snapshot_hash=matching_hash,
        )
    )
    db_session.commit()

    response = client.get("/api/system/weekly-narrative")
    assert response.status_code == 200
    assert response.json()["is_stale"] is False
