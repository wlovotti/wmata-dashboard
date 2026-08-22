"""Tests for the per-agency configuration loader."""

import pytest

from src.agency_config import (
    MissingAgencyDatabaseUrlError,
    load_agency_config,
    request_kwargs,
    resolve_agency_db_url,
)


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
    assert cfg.static_gtfs_url == "https://api.511.org/transit/datafeeds"
    assert cfg.static_gtfs_params == {"operator_id": "SF"}


def test_load_wmata_config_captures_current_defaults():
    """wmata.yaml documents today's implicit collector settings."""
    cfg = load_agency_config("wmata")
    assert cfg.timezone == "America/New_York"
    assert cfg.tick_sec == 30
    assert cfg.trip_updates_every_ticks == 1  # 30s
    assert cfg.vehicle_positions_every_ticks == 2  # 60s
    assert cfg.auth_style == "header"
    assert cfg.database_url_env == "DATABASE_URL"
    assert cfg.static_gtfs_url == "https://api.wmata.com/gtfs/bus-gtfs-static.zip"
    assert cfg.static_gtfs_params == {}


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


def test_resolve_agency_db_url_raises_when_env_var_unset(monkeypatch):
    """Non-WMATA agencies must fail loudly rather than fall back to DATABASE_URL.

    NOTES-100: this is the shared home for the guard originally introduced
    in ``pipelines/replay_archive_to_state.py`` (NOTES-96) -- now every
    agency-aware pipeline (derivation, aggregation, GTFS reload) imports
    it from here instead of duplicating the check per module.
    """
    monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
    cfg = load_agency_config("sfmta")

    with pytest.raises(MissingAgencyDatabaseUrlError, match="SFMTA_DATABASE_URL"):
        resolve_agency_db_url(cfg)


def test_resolve_agency_db_url_returns_value_when_set(monkeypatch):
    """When the agency's env var IS set, its value is returned untouched."""
    monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///sfmta_test")
    cfg = load_agency_config("sfmta")

    assert resolve_agency_db_url(cfg) == "postgresql:///sfmta_test"


def test_resolve_agency_db_url_wmata_unset_is_not_an_error(monkeypatch):
    """WMATA's own env var IS DATABASE_URL, so an unset value is not a
    misrouting risk -- it hits ``get_session``'s ordinary (pre-existing)
    failure mode, not the multi-agency guard.
    """
    monkeypatch.delenv("DATABASE_URL", raising=False)
    cfg = load_agency_config("wmata")

    assert resolve_agency_db_url(cfg) is None


def test_vp_and_s3_fields_load_for_both_agencies():
    """The stateless collector's VP-archive dirs and S3 prefixes come from yaml."""
    from src.agency_config import load_agency_config

    wmata = load_agency_config("wmata")
    assert wmata.vp_archive_dir == "archive/vp_snapshots"
    assert wmata.s3_bucket == "wmata-dashboard-backups"
    assert wmata.s3_tu_prefix == "raw-jsonl-archive/"
    assert wmata.s3_vp_prefix == "raw-jsonl-archive/vp/"

    sfmta = load_agency_config("sfmta")
    assert sfmta.vp_archive_dir == "archive/sfmta_vp_snapshots"
    assert sfmta.s3_tu_prefix == "raw-jsonl-archive/sfmta/"
    assert sfmta.s3_vp_prefix == "raw-jsonl-archive/sfmta_vp/"
