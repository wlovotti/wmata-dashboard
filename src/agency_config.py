"""Per-agency collector configuration loaded from config/agencies/*.yaml.

Each yaml captures everything agency-specific about data collection: feed
URLs, auth placement, polling cadences, archive/pid paths, and env-var
names for secrets. The dataclass is the seed of the multi-agency engine
(NOTES-95): anything that can't be expressed here is, by definition, a
code change that engine needs.
"""

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "agencies"


@dataclass(frozen=True)
class AgencyConfig:
    """Immutable snapshot of one agency's collection parameters."""

    name: str
    display_name: str
    timezone: str
    api_key_env: str
    auth_style: str  # "header" | "query"
    trip_updates_url: str
    vehicle_positions_url: str
    extra_params: dict
    static_gtfs_url: str
    static_gtfs_params: dict
    tick_sec: int
    trip_updates_every_ticks: int
    vehicle_positions_every_ticks: int
    archive_dir: str
    vp_archive_dir: str
    pid_file: str
    heartbeat_name: str
    database_url_env: str
    healthcheck_url_env: str
    s3_bucket: str
    s3_tu_prefix: str
    s3_vp_prefix: str


def load_agency_config(name: str) -> AgencyConfig:
    """Load ``config/agencies/<name>.yaml`` into an AgencyConfig.

    Raises FileNotFoundError for unknown agencies and ValueError for an
    unrecognized ``api.auth`` style, so misconfiguration fails loudly at
    startup rather than silently at request time.
    """
    path = CONFIG_DIR / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"No agency config at {path}")
    raw = yaml.safe_load(path.read_text())

    auth_style = raw["api"]["auth"]
    if auth_style not in ("header", "query"):
        raise ValueError(f"Unknown auth style {auth_style!r} in {path}")

    return AgencyConfig(
        name=raw["name"],
        display_name=raw["display_name"],
        timezone=raw["timezone"],
        api_key_env=raw["api"]["key_env"],
        auth_style=auth_style,
        trip_updates_url=raw["api"]["trip_updates_url"],
        vehicle_positions_url=raw["api"]["vehicle_positions_url"],
        extra_params=raw["api"].get("extra_params") or {},
        static_gtfs_url=raw["api"]["static_gtfs_url"],
        static_gtfs_params=raw["api"].get("static_gtfs_params") or {},
        tick_sec=raw["collector"]["tick_sec"],
        trip_updates_every_ticks=raw["collector"]["trip_updates_every_ticks"],
        vehicle_positions_every_ticks=raw["collector"]["vehicle_positions_every_ticks"],
        archive_dir=raw["collector"]["archive_dir"],
        vp_archive_dir=raw["collector"]["vp_archive_dir"],
        pid_file=raw["collector"]["pid_file"],
        heartbeat_name=raw["collector"]["heartbeat_name"],
        database_url_env=raw["database"]["url_env"],
        healthcheck_url_env=raw["healthcheck"]["url_env"],
        s3_bucket=raw["s3"]["bucket"],
        s3_tu_prefix=raw["s3"]["tu_prefix"],
        s3_vp_prefix=raw["s3"]["vp_prefix"],
    )


class MissingAgencyDatabaseUrlError(RuntimeError):
    """Raised when a non-WMATA agency's database env var isn't set.

    ``get_session(db_url=None)`` silently falls back to ``DATABASE_URL``
    (the WMATA default). Without this guard, an agency-aware pipeline run
    (replay, derivation, aggregation, GTFS reload, ...) with e.g.
    ``--agency sfmta`` and ``SFMTA_DATABASE_URL`` unset would silently
    write into the WMATA production database instead of failing — a
    correctness-critical footgun, not a convenience default. WMATA itself
    is exempt: its configured env var IS ``DATABASE_URL``, so an unset
    value there is the same, already-understood failure mode
    ``get_session``/``get_engine`` have always had.

    Originally introduced (NOTES-96) as a private helper in
    ``pipelines/replay_archive_to_state.py``; moved here (NOTES-100) once
    a second pipeline needed the identical check — every agency-aware
    entry point imports it from this one shared home instead of
    redefining it per module.
    """


def resolve_agency_db_url(cfg: AgencyConfig) -> str | None:
    """Return the DB URL env var value for ``cfg``, failing loudly if missing.

    See ``MissingAgencyDatabaseUrlError`` for why this doesn't just let
    ``get_session`` fall back to ``DATABASE_URL`` for non-WMATA agencies.
    """
    db_url = os.getenv(cfg.database_url_env)
    if not db_url and cfg.database_url_env != "DATABASE_URL":
        raise MissingAgencyDatabaseUrlError(
            f"{cfg.database_url_env} is not set. Running agency "
            f"{cfg.name!r} would otherwise silently fall back to "
            "DATABASE_URL (the WMATA default) and could write into the "
            "wrong database."
        )
    return db_url


def request_kwargs(cfg: AgencyConfig, api_key: str) -> dict:
    """Build the requests.get kwargs that authenticate against this agency.

    Header-auth agencies (WMATA) send the key as an ``api_key`` header;
    query-auth agencies (511.org) send it as an ``api_key`` query param
    merged with the agency's ``extra_params`` (e.g. ``agency=SF``).
    """
    if cfg.auth_style == "header":
        return {"headers": {"api_key": api_key}}
    return {"params": {"api_key": api_key, **cfg.extra_params}}
