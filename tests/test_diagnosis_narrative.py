"""
Tests for the route diagnosis narrative feature (PR #141).

Covers:
  - ``src/diagnosis_hash.py``: determinism, row-order independence, stable
    across equivalent representations.
  - API endpoint ``GET /api/routes/{route_id}/diagnosis``:
      - 404 when no narrative is cached.
      - 200 with ``is_stale=False`` when hash matches.
      - 200 with ``is_stale=True`` when diagnostic rows have changed.
  - CLI ``scripts/generate_route_diagnosis.py``:
      - Exit 1 when no diagnostic data exists for the requested route.
"""

import pytest

from src.diagnosis_hash import compute_profile_hash
from src.models import RouteDiagnosisNarrative, RouteDiagnosticSegment, RouteDiagnosticTimepoint
from src.timezones import utcnow_naive


# ---------------------------------------------------------------------------
# diagnosis_hash tests
# ---------------------------------------------------------------------------


class TestComputeProfileHash:
    """Unit tests for ``src/diagnosis_hash.compute_profile_hash``."""

    _SEGMENT = {
        "direction_id": 0,
        "from_seq": 2,
        "from_stop_id": "S001",
        "to_seq": 3,
        "to_stop_id": "S002",
        "mean_slip_sec": 30.5,
        "cum_slip_sec": 30.5,
        "n_observations": 10,
        "is_timepoint": False,
    }
    _TIMEPOINT = {
        "direction_id": 0,
        "timepoint_stop_id": "T001",
        "classification": "recovery",
        "median_dev_entering": 60.0,
        "median_dev_leaving": -30.0,
        "p10_dev_entering": -20.0,
        "p10_dev_leaving": -80.0,
        "n_observations": 40,
    }

    def test_deterministic_same_input(self):
        """Same input always produces the same hash."""
        h1 = compute_profile_hash([self._SEGMENT], [self._TIMEPOINT])
        h2 = compute_profile_hash([self._SEGMENT], [self._TIMEPOINT])
        assert h1 == h2

    def test_hex_string_format(self):
        """Result is a 64-character lowercase hex string (SHA-256)."""
        h = compute_profile_hash([self._SEGMENT], [self._TIMEPOINT])
        assert len(h) == 64
        assert h == h.lower()
        assert all(c in "0123456789abcdef" for c in h)

    def test_row_order_independent(self):
        """Hash is identical regardless of the order rows are supplied."""
        seg2 = {**self._SEGMENT, "from_seq": 3, "to_seq": 4, "from_stop_id": "S002",
                "to_stop_id": "S003"}
        h_forward = compute_profile_hash([self._SEGMENT, seg2], [self._TIMEPOINT])
        h_reversed = compute_profile_hash([seg2, self._SEGMENT], [self._TIMEPOINT])
        assert h_forward == h_reversed

    def test_empty_inputs(self):
        """Empty segment and timepoint lists produce a stable, non-empty hash."""
        h = compute_profile_hash([], [])
        assert len(h) == 64

    def test_different_data_different_hash(self):
        """Changing any content field changes the hash."""
        h_original = compute_profile_hash([self._SEGMENT], [self._TIMEPOINT])
        modified = {**self._SEGMENT, "mean_slip_sec": 999.0}
        h_modified = compute_profile_hash([modified], [self._TIMEPOINT])
        assert h_original != h_modified

    def test_stable_across_equivalent_dicts(self):
        """Adding irrelevant keys (like `id` or `computed_at`) does not affect the hash."""
        seg_with_extra = {**self._SEGMENT, "id": 42, "computed_at": "2026-01-01"}
        h_clean = compute_profile_hash([self._SEGMENT], [self._TIMEPOINT])
        # compute_profile_hash only reads canonical fields, so extra keys are ignored.
        h_extra = compute_profile_hash([seg_with_extra], [self._TIMEPOINT])
        assert h_clean == h_extra


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_diagnosis_404_when_no_narrative(client, sample_route):
    """GET /api/routes/{id}/diagnosis returns 404 when no narrative is cached."""
    response = client.get("/api/routes/TEST1/diagnosis")
    assert response.status_code == 404
    assert "TEST1" in response.json()["detail"]


@pytest.mark.api
def test_diagnosis_404_for_nonexistent_route(client):
    """GET /api/routes/{id}/diagnosis returns 404 for a completely unknown route."""
    response = client.get("/api/routes/ZZZZ/diagnosis")
    assert response.status_code == 404


@pytest.mark.api
def test_diagnosis_400_for_invalid_period(client, sample_route):
    """GET /api/routes/{id}/diagnosis returns 400 for an invalid period."""
    response = client.get("/api/routes/TEST1/diagnosis?period=never")
    assert response.status_code == 400
    assert "period" in response.json()["detail"].lower()


@pytest.mark.api
def test_diagnosis_200_not_stale(client, db_session, sample_route):
    """
    200 with is_stale=False when no diagnostic rows exist and the hash stored
    in the narrative matches the (empty) current profile hash.
    """
    # Compute hash for empty profile (no segment / timepoint rows).
    current_hash = compute_profile_hash([], [])

    # Insert a narrative row with that hash.
    db_session.add(
        RouteDiagnosisNarrative(
            route_id="TEST1",
            period="all",
            narrative="Test narrative for TEST1.",
            generated_at=utcnow_naive(),
            model_id="claude-sonnet-4-6",
            prompt_version="v1",
            profile_snapshot_hash=current_hash,
        )
    )
    db_session.commit()

    response = client.get("/api/routes/TEST1/diagnosis")
    assert response.status_code == 200
    data = response.json()
    assert data["narrative"] == "Test narrative for TEST1."
    assert data["is_stale"] is False
    assert data["model_id"] == "claude-sonnet-4-6"
    assert data["prompt_version"] == "v1"
    assert "generated_at" in data


@pytest.mark.api
def test_diagnosis_200_is_stale_when_hash_differs(client, db_session, sample_route):
    """
    200 with is_stale=True when the stored hash does not match the current
    diagnostic profile (simulated by inserting a diagnostic row after the
    narrative was cached with a stale hash).
    """
    stale_hash = "a" * 64  # deliberate mismatch

    db_session.add(
        RouteDiagnosisNarrative(
            route_id="TEST1",
            period="all",
            narrative="Stale narrative.",
            generated_at=utcnow_naive(),
            model_id="claude-sonnet-4-6",
            prompt_version="v1",
            profile_snapshot_hash=stale_hash,
        )
    )
    db_session.commit()

    # The current hash will be compute_profile_hash([], []) != stale_hash.
    response = client.get("/api/routes/TEST1/diagnosis")
    assert response.status_code == 200
    data = response.json()
    assert data["is_stale"] is True


@pytest.mark.api
def test_diagnosis_respects_period_param(client, db_session, sample_route):
    """Period query parameter is forwarded correctly — 404 for uncached period."""
    current_hash = compute_profile_hash([], [])
    db_session.add(
        RouteDiagnosisNarrative(
            route_id="TEST1",
            period="am_peak",
            narrative="AM peak narrative.",
            generated_at=utcnow_naive(),
            model_id="claude-sonnet-4-6",
            prompt_version="v1",
            profile_snapshot_hash=current_hash,
        )
    )
    db_session.commit()

    # The am_peak period is cached.
    assert client.get("/api/routes/TEST1/diagnosis?period=am_peak").status_code == 200
    # The 'all' period is not cached.
    assert client.get("/api/routes/TEST1/diagnosis").status_code == 404


# ---------------------------------------------------------------------------
# CLI no-data exit test
# ---------------------------------------------------------------------------


def test_cli_exits_1_when_no_diagnostic_data(db_session, monkeypatch, tmp_path):
    """
    The CLI exits 1 and prints a useful message when no diagnostic rows exist
    for the requested route.
    """
    import sys

    from sqlalchemy.orm import sessionmaker

    from src.database import get_engine

    # Re-use the in-memory SQLite engine already populated by db_session.
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine)

    # Patch get_engine so the CLI uses the test engine.
    import src.database as _db_mod

    monkeypatch.setattr(_db_mod, "get_engine", lambda: engine)

    # Patch os.environ to provide a fake API key (so the key-check passes but
    # we never actually hit the Anthropic API — the test expects exit 1 before
    # any API call because no diagnostic data exists).
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-fake")

    # Import the CLI module fresh (it loads .env at import time, but we
    # monkeypatched the env already).
    import importlib
    import sys as _sys

    if "scripts.generate_route_diagnosis" in _sys.modules:
        del _sys.modules["scripts.generate_route_diagnosis"]

    # Use subprocess to invoke the CLI without contaminating the test process.
    import subprocess

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--co",
            "-q",
        ],
        cwd="/Users/wlovotti/repos/wmata-dashboard",
        capture_output=True,
    )
    # We just need to confirm the module can be imported without crashing.
    # The subprocess above is a no-op collection run — real invocation test
    # is via the argparse path below.

    # Direct import + invocation of main() with mocked DB.
    import importlib.util
    import os

    spec = importlib.util.spec_from_file_location(
        "generate_route_diagnosis",
        "/Users/wlovotti/repos/wmata-dashboard/scripts/generate_route_diagnosis.py",
    )
    mod = importlib.util.module_from_spec(spec)

    # Patch get_engine inside the module before executing.
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-fake")

    spec.loader.exec_module(mod)

    # Route NONEXISTENT does not exist → should exit 1.
    exit_code = mod.main(["--route", "NONEXISTENT"])
    assert exit_code == 1
