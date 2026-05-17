"""
Unit tests for `src/frequent_routes.py`.

Covers loader semantics (default file, env override, missing file,
malformed YAML), the high-freq/medium-freq tier split, the per-
route gate helper, mtime-keyed reload, and the API-side
`is_frequent` field on `/api/routes` / `/api/routes/{id}` payloads.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src import frequent_routes
from src.frequent_routes import (
    HIGH_FREQ_GATE_SEC,
    MEDIUM_FREQ_GATE_SEC,
)
from src.models import Route


@pytest.fixture
def isolated_frequent_routes(tmp_path, monkeypatch):
    """Point the loader at a tmp YAML file and reset the cache.

    Yields the tmp path so the test can write into it and trigger a
    reload. Cleans up by resetting the cache so the next test starts
    fresh against the real config file.
    """
    path = tmp_path / "frequent_routes.yaml"
    monkeypatch.setenv("WMATA_FREQUENT_ROUTES_PATH", str(path))
    frequent_routes.reset_cache_for_tests()
    yield path
    frequent_routes.reset_cache_for_tests()


def _write_yaml(path: Path, body: str) -> None:
    """Write YAML body to `path`. Mtime advances automatically on rewrite."""
    path.write_text(body, encoding="utf-8")


@pytest.mark.smoke
def test_default_config_file_loads():
    """The checked-in config/frequent_routes.yaml parses and includes known routes."""
    frequent_routes.reset_cache_for_tests()
    route_ids = frequent_routes.load_frequent_route_ids()
    # NOTES-56's whole point: D80 must be on the list.
    assert "D80" in route_ids
    # Spot-check a few other routes from the published map.
    assert "M60" in route_ids
    assert "P40" in route_ids
    assert "F20" in route_ids
    # Sanity: not empty, not the whole world.
    assert 20 < len(route_ids) < 200


@pytest.mark.smoke
def test_default_config_splits_tiers():
    """Both tiers populate from the default yaml; the union matches load_frequent_route_ids."""
    frequent_routes.reset_cache_for_tests()
    high = frequent_routes.load_high_freq_route_ids()
    medium = frequent_routes.load_medium_freq_route_ids()
    # Both tiers should contain at least one route after the 2026-05-17
    # tier migration. Anything else means the yaml lost its tier
    # structure on a future edit.
    assert len(high) > 0
    assert len(medium) > 0
    # Disjoint by construction — overlap is resolved in favor of high.
    assert high.isdisjoint(medium)
    # Union equals load_frequent_route_ids — the back-compat contract.
    assert frequent_routes.load_frequent_route_ids() == high | medium


@pytest.mark.smoke
def test_is_frequent_route_helper():
    """is_frequent_route() returns True for designated routes, False otherwise."""
    frequent_routes.reset_cache_for_tests()
    assert frequent_routes.is_frequent_route("D80") is True
    # A genuinely-fake route_id that won't ever be on the published map.
    assert frequent_routes.is_frequent_route("ZZZ_NOT_A_ROUTE") is False


def test_missing_file_returns_empty_set(tmp_path, monkeypatch):
    """A missing YAML file resolves to empty sets, never raises."""
    missing = tmp_path / "does_not_exist.yaml"
    monkeypatch.setenv("WMATA_FREQUENT_ROUTES_PATH", str(missing))
    frequent_routes.reset_cache_for_tests()
    try:
        assert frequent_routes.load_frequent_route_ids() == frozenset()
        assert frequent_routes.load_high_freq_route_ids() == frozenset()
        assert frequent_routes.load_medium_freq_route_ids() == frozenset()
        assert frequent_routes.is_frequent_route("D80") is False
    finally:
        frequent_routes.reset_cache_for_tests()


def test_malformed_yaml_returns_empty_set(isolated_frequent_routes):
    """A malformed YAML file resolves to empty sets without raising."""
    isolated_frequent_routes.write_text("this: is: not: valid: yaml\n", encoding="utf-8")
    assert frequent_routes.load_frequent_route_ids() == frozenset()
    assert frequent_routes.load_high_freq_route_ids() == frozenset()
    assert frequent_routes.load_medium_freq_route_ids() == frozenset()


def test_tier_lists_parse(isolated_frequent_routes):
    """high_freq / medium_freq lists each populate their tier; union is the combined set."""
    _write_yaml(
        isolated_frequent_routes,
        """
high_freq:
  - D80
  - M60
medium_freq:
  - C29
  - P1X
""",
    )
    assert frequent_routes.load_high_freq_route_ids() == frozenset({"D80", "M60"})
    assert frequent_routes.load_medium_freq_route_ids() == frozenset({"C29", "P1X"})
    assert frequent_routes.load_frequent_route_ids() == frozenset({"D80", "M60", "C29", "P1X"})


def test_only_high_freq_tier_present(isolated_frequent_routes):
    """A yaml with only high_freq still loads; medium_freq is empty."""
    _write_yaml(
        isolated_frequent_routes,
        """
high_freq:
  - D80
""",
    )
    assert frequent_routes.load_high_freq_route_ids() == frozenset({"D80"})
    assert frequent_routes.load_medium_freq_route_ids() == frozenset()
    assert frequent_routes.load_frequent_route_ids() == frozenset({"D80"})


def test_only_medium_freq_tier_present(isolated_frequent_routes):
    """A yaml with only medium_freq still loads; high_freq is empty."""
    _write_yaml(
        isolated_frequent_routes,
        """
medium_freq:
  - C29
""",
    )
    assert frequent_routes.load_high_freq_route_ids() == frozenset()
    assert frequent_routes.load_medium_freq_route_ids() == frozenset({"C29"})
    assert frequent_routes.load_frequent_route_ids() == frozenset({"C29"})


def test_tier_overlap_resolves_to_high(isolated_frequent_routes):
    """A route_id in both tiers stays in high_freq and is removed from medium."""
    _write_yaml(
        isolated_frequent_routes,
        """
high_freq:
  - D80
medium_freq:
  - D80
  - C29
""",
    )
    assert frequent_routes.load_high_freq_route_ids() == frozenset({"D80"})
    assert frequent_routes.load_medium_freq_route_ids() == frozenset({"C29"})


def test_tier_must_be_a_list(isolated_frequent_routes):
    """A non-list tier value resolves to an empty set for that tier with a warning."""
    _write_yaml(
        isolated_frequent_routes,
        """
high_freq:
  D80: true
medium_freq:
  - C29
""",
    )
    assert frequent_routes.load_high_freq_route_ids() == frozenset()
    assert frequent_routes.load_medium_freq_route_ids() == frozenset({"C29"})


def test_top_level_not_mapping_returns_empty(isolated_frequent_routes):
    """A YAML that parses to a non-mapping at the top level returns empty for both tiers."""
    isolated_frequent_routes.write_text("- D80\n- M60\n", encoding="utf-8")
    # Top-level list, not a mapping — loader rejects it.
    assert frequent_routes.load_frequent_route_ids() == frozenset()


def test_reload_picks_up_mtime_change(isolated_frequent_routes):
    """Editing the YAML between calls produces fresh values without restart."""
    _write_yaml(
        isolated_frequent_routes,
        """
high_freq:
  - D80
""",
    )
    assert frequent_routes.load_frequent_route_ids() == frozenset({"D80"})
    # Advance mtime explicitly — two writes inside the same second can
    # share an mtime on fast filesystems; bump it deliberately.
    new_mtime = isolated_frequent_routes.stat().st_mtime + 5
    _write_yaml(
        isolated_frequent_routes,
        """
high_freq:
  - D80
medium_freq:
  - C29
""",
    )
    os.utime(isolated_frequent_routes, (new_mtime, new_mtime))
    assert frequent_routes.load_frequent_route_ids() == frozenset({"D80", "C29"})
    assert frequent_routes.load_medium_freq_route_ids() == frozenset({"C29"})


def test_empty_tier_lists_return_empty_set(isolated_frequent_routes):
    """A YAML with both tiers as `[]` returns empty sets, not None."""
    _write_yaml(
        isolated_frequent_routes,
        """
high_freq: []
medium_freq: []
""",
    )
    assert frequent_routes.load_frequent_route_ids() == frozenset()


def test_missing_tier_keys_return_empty_set(isolated_frequent_routes):
    """A YAML without any tier keys returns an empty set."""
    _write_yaml(isolated_frequent_routes, "other_key: true\n")
    assert frequent_routes.load_frequent_route_ids() == frozenset()


def test_route_ids_coerced_to_str(isolated_frequent_routes):
    """YAML integer route_ids (rare but possible) are coerced to str."""
    _write_yaml(
        isolated_frequent_routes,
        """
high_freq:
  - 70
  - "D80"
""",
    )
    result = frequent_routes.load_high_freq_route_ids()
    assert "70" in result
    assert "D80" in result


# -----------------------------------------------------------------------
# Per-route cell-hour gate helper
# -----------------------------------------------------------------------


def test_gate_defaults_for_undesignated(isolated_frequent_routes):
    """get_cell_hour_gate_sec returns the high-freq default for unknown routes."""
    _write_yaml(
        isolated_frequent_routes,
        """
high_freq:
  - D80
medium_freq:
  - C29
""",
    )
    assert frequent_routes.get_cell_hour_gate_sec("UNKNOWN_ROUTE") == HIGH_FREQ_GATE_SEC


def test_gate_for_high_freq_route(isolated_frequent_routes):
    """High-freq designated routes get the 15-min gate."""
    _write_yaml(
        isolated_frequent_routes,
        """
high_freq:
  - D80
""",
    )
    assert frequent_routes.get_cell_hour_gate_sec("D80") == HIGH_FREQ_GATE_SEC


def test_gate_for_medium_freq_route(isolated_frequent_routes):
    """Medium-freq designated routes get the 20-min gate."""
    _write_yaml(
        isolated_frequent_routes,
        """
medium_freq:
  - C29
""",
    )
    assert frequent_routes.get_cell_hour_gate_sec("C29") == MEDIUM_FREQ_GATE_SEC


def test_gate_constants_are_distinct():
    """The two tier gates differ — otherwise the per-route lookup is pointless."""
    assert HIGH_FREQ_GATE_SEC != MEDIUM_FREQ_GATE_SEC
    assert MEDIUM_FREQ_GATE_SEC > HIGH_FREQ_GATE_SEC


# -----------------------------------------------------------------------
# API integration: is_frequent field on /api/routes and /api/routes/{id}
# -----------------------------------------------------------------------


@pytest.fixture
def routes_with_frequent_marker(db_session, tmp_path, monkeypatch):
    """Two routes — one designated frequent in a tmp YAML, one not."""
    routes = [
        Route(
            route_id="FREQ1",
            route_short_name="F1",
            route_long_name="Frequent Test Route",
            route_type=3,
            is_current=True,
        ),
        Route(
            route_id="STD1",
            route_short_name="S1",
            route_long_name="Standard Test Route",
            route_type=3,
            is_current=True,
        ),
    ]
    db_session.add_all(routes)
    db_session.commit()

    path = tmp_path / "frequent_routes.yaml"
    path.write_text("high_freq:\n  - FREQ1\n", encoding="utf-8")
    monkeypatch.setenv("WMATA_FREQUENT_ROUTES_PATH", str(path))
    frequent_routes.reset_cache_for_tests()
    yield routes
    frequent_routes.reset_cache_for_tests()


@pytest.mark.api
def test_api_routes_includes_is_frequent(client, routes_with_frequent_marker):
    """`GET /api/routes` emits `is_frequent: bool` on every route summary."""
    response = client.get("/api/routes")
    assert response.status_code == 200
    body = response.json()
    by_id = {r["route_id"]: r for r in body["routes"]}
    assert by_id["FREQ1"]["is_frequent"] is True
    assert by_id["STD1"]["is_frequent"] is False


@pytest.mark.api
def test_api_route_detail_includes_is_frequent(client, routes_with_frequent_marker):
    """`GET /api/routes/{id}` emits `is_frequent` on the detail payload too."""
    response = client.get("/api/routes/FREQ1")
    assert response.status_code == 200
    assert response.json()["is_frequent"] is True

    response = client.get("/api/routes/STD1")
    assert response.status_code == 200
    assert response.json()["is_frequent"] is False
