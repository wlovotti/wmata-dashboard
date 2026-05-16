"""
Unit tests for `src/route_targets.py` (NOTES-47 closing PR).

Covers loader semantics (default file, env override, missing file,
malformed YAML), per-route fallback to system default, and the
ewt_minutes → seconds conversion that the loader does on read.
"""

import os
from pathlib import Path

import pytest

from src import route_targets


@pytest.fixture
def isolated_targets(tmp_path, monkeypatch):
    """Point the loader at a tmp YAML file and reset the cache.

    Yields the tmp path so the test can write into it and trigger a
    reload. Cleans up by resetting the cache so the next test starts
    fresh against the real config file.
    """
    path = tmp_path / "route_targets.yaml"
    monkeypatch.setenv("WMATA_ROUTE_TARGETS_PATH", str(path))
    route_targets.reset_cache_for_tests()
    yield path
    route_targets.reset_cache_for_tests()


def _write_yaml(path: Path, body: str) -> None:
    """Write YAML body to `path` and bump mtime so the loader picks it up."""
    path.write_text(body, encoding="utf-8")


@pytest.mark.smoke
def test_default_config_file_loads():
    """The checked-in config/route_targets.yaml parses and yields all four metrics."""
    route_targets.reset_cache_for_tests()
    targets = route_targets.get_system_targets()
    # All four metric keys present.
    assert set(targets.keys()) == {"otp", "service_delivered", "ewt", "bunching"}
    # OTP is a percentage 0-100; sd is a fraction 0-1; ewt is seconds; bunching 0-1.
    assert 0 < targets["otp"] <= 100
    assert 0 < targets["service_delivered"] <= 1
    assert targets["ewt"] > 0  # seconds, not minutes
    assert 0 < targets["bunching"] <= 1


@pytest.mark.smoke
def test_ewt_minutes_converted_to_seconds(isolated_targets):
    """`ewt_minutes` in the YAML surfaces as seconds via get_*_target('ewt')."""
    _write_yaml(
        isolated_targets,
        """
system_default:
  otp: 80
  service_delivered: 0.95
  ewt_minutes: 2.5
  bunching_pct: 0.03
routes: {}
""",
    )
    assert route_targets.get_system_target("ewt") == 2.5 * 60.0
    assert route_targets.get_system_target("otp") == 80.0
    assert route_targets.get_system_target("service_delivered") == 0.95
    assert route_targets.get_system_target("bunching") == 0.03


def test_per_route_override_falls_back_to_system(isolated_targets):
    """A route override may set a subset of metrics; the rest inherit system."""
    _write_yaml(
        isolated_targets,
        """
system_default:
  otp: 75
  service_delivered: 0.95
  ewt_minutes: 3
  bunching_pct: 0.05
routes:
  "30N":
    otp: 85
""",
    )
    assert route_targets.get_target("30N", "otp") == 85.0
    # Missing per-route entries fall back to system default.
    assert route_targets.get_target("30N", "service_delivered") == 0.95
    assert route_targets.get_target("30N", "ewt") == 180.0
    assert route_targets.get_target("30N", "bunching") == 0.05
    # A route with no overrides at all returns system defaults.
    assert route_targets.get_target("99X", "otp") == 75.0


def test_missing_file_returns_none(tmp_path, monkeypatch):
    """A missing YAML file resolves to all-None targets, never raises."""
    missing = tmp_path / "does_not_exist.yaml"
    monkeypatch.setenv("WMATA_ROUTE_TARGETS_PATH", str(missing))
    route_targets.reset_cache_for_tests()
    try:
        assert route_targets.get_system_target("otp") is None
        assert route_targets.get_target("30N", "ewt") is None
    finally:
        route_targets.reset_cache_for_tests()


def test_malformed_yaml_returns_none(isolated_targets):
    """A malformed YAML file resolves to all-None without raising."""
    isolated_targets.write_text("this: is: not: valid: yaml\n", encoding="utf-8")
    # The loader prints a warning to stdout; targets resolve to None.
    assert route_targets.get_system_target("otp") is None
    assert route_targets.get_target("30N", "otp") is None


def test_unknown_metric_returns_none(isolated_targets):
    """Asking for a metric the API doesn't support returns None, not a KeyError."""
    _write_yaml(
        isolated_targets,
        """
system_default:
  otp: 80
""",
    )
    assert route_targets.get_system_target("speed_kmh") is None
    assert route_targets.get_target("30N", "speed_kmh") is None


def test_reload_picks_up_mtime_change(isolated_targets):
    """Editing the YAML between calls produces fresh values without restart."""
    _write_yaml(
        isolated_targets,
        """
system_default:
  otp: 70
""",
    )
    assert route_targets.get_system_target("otp") == 70.0
    # Advance mtime explicitly — on fast filesystems two writes inside the
    # same second can share an mtime, so bump it deliberately.
    new_mtime = isolated_targets.stat().st_mtime + 5
    _write_yaml(
        isolated_targets,
        """
system_default:
  otp: 88
""",
    )
    os.utime(isolated_targets, (new_mtime, new_mtime))
    assert route_targets.get_system_target("otp") == 88.0


def test_get_targets_for_route_emits_all_four_keys(isolated_targets):
    """The convenience helper for API payloads always emits all four metric keys."""
    _write_yaml(
        isolated_targets,
        """
system_default:
  otp: 75
""",
    )
    targets = route_targets.get_targets_for_route("30N")
    assert set(targets.keys()) == {"otp", "service_delivered", "ewt", "bunching"}
    # Only OTP is set in the YAML; the rest are None.
    assert targets["otp"] == 75.0
    assert targets["service_delivered"] is None
    assert targets["ewt"] is None
    assert targets["bunching"] is None
