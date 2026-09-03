"""
Unit tests for the `agency` parameter on the per-agency route config
loaders (NOTES-143, the agency switch UI).

`src/frequent_routes.py` and `src/route_targets.py` gain an `agency`
parameter that replaces the PR #236 API-layer stopgap (which
unconditionally forced `is_frequent=False` / `targets=None` for any
non-wmata agency): `config/frequent_routes.yaml` and
`config/route_targets.yaml` are keyed by WMATA `route_id`, and
route_ids overlap across agencies (SFMTA has its own "1", "9", "14",
...), so a real per-agency answer means "wmata reads the file
unchanged" and "any other agency gets an empty designation /
system-default-only targets (no per-route entries)" -- not silently
reusing WMATA's per-route config for the wrong agency's route.
"""

from __future__ import annotations

import pytest

from src import frequent_routes, route_targets
from src.frequent_routes import DEFAULT_GATE_SEC, MEDIUM_FREQ_GATE_SEC


@pytest.fixture
def isolated_frequent_routes(tmp_path, monkeypatch):
    """Point the frequent-routes loader at a tmp YAML file and reset its cache."""
    path = tmp_path / "frequent_routes.yaml"
    monkeypatch.setenv("WMATA_FREQUENT_ROUTES_PATH", str(path))
    frequent_routes.reset_cache_for_tests()
    yield path
    frequent_routes.reset_cache_for_tests()


@pytest.fixture
def isolated_targets(tmp_path, monkeypatch):
    """Point the route-targets loader at a tmp YAML file and reset its cache."""
    path = tmp_path / "route_targets.yaml"
    monkeypatch.setenv("WMATA_ROUTE_TARGETS_PATH", str(path))
    route_targets.reset_cache_for_tests()
    yield path
    route_targets.reset_cache_for_tests()


# ---------------------------------------------------------------------------
# src/frequent_routes.py
# ---------------------------------------------------------------------------


def test_wmata_agency_reads_real_designation(isolated_frequent_routes):
    """`agency="wmata"` (default and explicit) reads the YAML unchanged."""
    isolated_frequent_routes.write_text("high_freq:\n  - D80\nmedium_freq:\n  - X2\n")

    assert frequent_routes.load_frequent_route_ids() == {"D80", "X2"}
    assert frequent_routes.load_frequent_route_ids("wmata") == {"D80", "X2"}
    assert frequent_routes.load_high_freq_route_ids("wmata") == {"D80"}
    assert frequent_routes.load_medium_freq_route_ids("wmata") == {"X2"}
    assert frequent_routes.is_frequent_route("D80", "wmata") is True


def test_non_wmata_agency_returns_empty_designation(isolated_frequent_routes):
    """Any non-wmata `agency` gets an empty designation, even when the (WMATA)
    YAML classifies the same route_id as frequent.

    This is the crux of NOTES-143: SFMTA has its own route "D80"-shaped
    IDs too (a real SFMTA route could collide with a WMATA route_id
    that IS on the frequent list), so the gate must be unconditional --
    not "empty because the route_id happens not to be on the list."
    """
    isolated_frequent_routes.write_text("high_freq:\n  - D80\nmedium_freq:\n  - X2\n")

    assert frequent_routes.load_frequent_route_ids("sfmta") == frozenset()
    assert frequent_routes.load_high_freq_route_ids("sfmta") == frozenset()
    assert frequent_routes.load_medium_freq_route_ids("sfmta") == frozenset()
    assert frequent_routes.is_frequent_route("D80", "sfmta") is False
    # Confirm the gate fires even though "D80" really is on the (wmata)
    # list -- otherwise this would pass trivially for any unknown route_id.
    assert "D80" in frequent_routes.load_frequent_route_ids("wmata")


def test_non_wmata_agency_ignores_a_missing_or_malformed_file(tmp_path, monkeypatch):
    """A non-wmata agency short-circuits before ever touching the YAML, so a
    missing/malformed file that would otherwise print a warning or raise
    for `agency="wmata"` is simply irrelevant.
    """
    missing = tmp_path / "does_not_exist.yaml"
    monkeypatch.setenv("WMATA_FREQUENT_ROUTES_PATH", str(missing))
    frequent_routes.reset_cache_for_tests()
    try:
        assert frequent_routes.load_frequent_route_ids("sfmta") == frozenset()
        assert frequent_routes.is_frequent_route("D80", "sfmta") is False
    finally:
        frequent_routes.reset_cache_for_tests()


def test_non_wmata_agency_never_gets_the_medium_freq_gate(isolated_frequent_routes):
    """`get_cell_hour_gate_sec` (PR #242 review finding 5) is gated by
    `agency` the same way `is_frequent`/`load_frequent_route_ids` are.

    The overlapping-id case: X2 is WMATA's own medium-frequency route
    (20-min EWT gate). If SFMTA ever runs a route also numbered "X2", it
    must NOT inherit WMATA's medium-freq gate -- it always gets the
    default 15-min gate, exactly as if it weren't on the list at all.
    """
    isolated_frequent_routes.write_text("high_freq: []\nmedium_freq:\n  - X2\n")

    # wmata's own X2 really is medium-freq.
    assert frequent_routes.get_cell_hour_gate_sec("X2", "wmata") == MEDIUM_FREQ_GATE_SEC
    assert frequent_routes.get_cell_hour_gate_sec("X2") == MEDIUM_FREQ_GATE_SEC  # default agency

    # SFMTA's same-numbered "X2" gets the default gate, not WMATA's tier.
    assert frequent_routes.get_cell_hour_gate_sec("X2", "sfmta") == DEFAULT_GATE_SEC


# ---------------------------------------------------------------------------
# src/route_targets.py
# ---------------------------------------------------------------------------


def _write_targets_yaml(path, otp_override: float | None) -> None:
    body = (
        "system_default:\n"
        "  otp: 75.0\n"
        "  service_delivered: 0.95\n"
        "  ewt_minutes: 3.0\n"
        "  bunching_pct: 0.04\n"
        "routes:\n"
    )
    if otp_override is not None:
        body += f"  D80:\n    otp: {otp_override}\n"
    path.write_text(body)


def test_wmata_agency_reads_per_route_override(isolated_targets):
    """`agency="wmata"` (default and explicit) applies the per-route override."""
    _write_targets_yaml(isolated_targets, otp_override=90.0)

    assert route_targets.get_target("D80", "otp") == 90.0
    assert route_targets.get_target("D80", "otp", "wmata") == 90.0
    assert route_targets.get_targets_for_route("D80")["otp"] == 90.0
    assert route_targets.get_targets_for_route("D80", "wmata")["otp"] == 90.0


def test_non_wmata_agency_falls_back_to_system_default(isolated_targets):
    """A non-wmata agency ignores the per-route override but still gets the
    (agency-agnostic) system default -- "system-default-only targets", not
    `None`.
    """
    _write_targets_yaml(isolated_targets, otp_override=90.0)

    # System default (75.0), NOT the WMATA-route-D80-specific override (90.0).
    assert route_targets.get_target("D80", "otp", "sfmta") == 75.0
    all_targets = route_targets.get_targets_for_route("D80", "sfmta")
    assert all_targets["otp"] == 75.0
    assert all_targets["service_delivered"] == 0.95
    # Same route_id, wmata agency, still sees the real per-route override --
    # confirms the difference is the agency gate, not a config change.
    assert route_targets.get_target("D80", "otp", "wmata") == 90.0


def test_non_wmata_agency_with_no_override_matches_wmata_default(isolated_targets):
    """When there's no per-route override at all, every agency sees the same
    system default -- the gate only matters once an override exists.
    """
    _write_targets_yaml(isolated_targets, otp_override=None)

    assert route_targets.get_target("D80", "otp", "wmata") == 75.0
    assert route_targets.get_target("D80", "otp", "sfmta") == 75.0
