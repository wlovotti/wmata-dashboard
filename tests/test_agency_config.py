"""Tests for the per-agency configuration loader."""

import pytest

from src.agency_config import load_agency_config, request_kwargs


def test_load_sfmta_config():
    """sfmta.yaml loads with the spec'd cadences and Pacific timezone."""
    cfg = load_agency_config("sfmta")
    assert cfg.name == "sfmta"
    assert cfg.timezone == "America/Los_Angeles"
    assert cfg.tick_sec == 60
    assert cfg.trip_updates_every_ticks == 2  # 120s
    assert cfg.vehicle_positions_every_ticks == 3  # 180s
    assert cfg.auth_style == "query"
    assert cfg.extra_params == {"agency": "SF"}
    assert cfg.api_key_env == "SFMTA_API_KEY"
    assert cfg.database_url_env == "SFMTA_DATABASE_URL"


def test_load_wmata_config_captures_current_defaults():
    """wmata.yaml documents today's implicit collector settings."""
    cfg = load_agency_config("wmata")
    assert cfg.timezone == "America/New_York"
    assert cfg.tick_sec == 30
    assert cfg.trip_updates_every_ticks == 1  # 30s
    assert cfg.vehicle_positions_every_ticks == 2  # 60s
    assert cfg.auth_style == "header"
    assert cfg.database_url_env == "DATABASE_URL"


def test_load_unknown_agency_raises():
    """A missing yaml file is a clear error, not a silent default."""
    with pytest.raises(FileNotFoundError):
        load_agency_config("bart")


def test_request_kwargs_query_auth():
    """Query-auth agencies put the key and extra params in the query string."""
    cfg = load_agency_config("sfmta")
    kwargs = request_kwargs(cfg, api_key="SECRET")
    assert kwargs == {"params": {"api_key": "SECRET", "agency": "SF"}}


def test_request_kwargs_header_auth():
    """Header-auth agencies (WMATA) put the key in headers, params empty."""
    cfg = load_agency_config("wmata")
    kwargs = request_kwargs(cfg, api_key="SECRET")
    assert kwargs == {"headers": {"api_key": "SECRET"}}
