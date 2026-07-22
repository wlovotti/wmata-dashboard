"""Per-agency collector configuration loaded from config/agencies/*.yaml.

Each yaml captures everything agency-specific about data collection: feed
URLs, auth placement, polling cadences, archive/pid paths, and env-var
names for secrets. The dataclass is the seed of the multi-agency engine
(NOTES-95): anything that can't be expressed here is, by definition, a
code change that engine needs.
"""

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
    tick_sec: int
    trip_updates_every_ticks: int
    vehicle_positions_every_ticks: int
    archive_dir: str
    pid_file: str
    heartbeat_name: str
    database_url_env: str
    healthcheck_url_env: str


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
        tick_sec=raw["collector"]["tick_sec"],
        trip_updates_every_ticks=raw["collector"]["trip_updates_every_ticks"],
        vehicle_positions_every_ticks=raw["collector"]["vehicle_positions_every_ticks"],
        archive_dir=raw["collector"]["archive_dir"],
        pid_file=raw["collector"]["pid_file"],
        heartbeat_name=raw["collector"]["heartbeat_name"],
        database_url_env=raw["database"]["url_env"],
        healthcheck_url_env=raw["healthcheck"]["url_env"],
    )


def request_kwargs(cfg: AgencyConfig, api_key: str) -> dict:
    """Build the requests.get kwargs that authenticate against this agency.

    Header-auth agencies (WMATA) send the key as an ``api_key`` header;
    query-auth agencies (511.org) send it as an ``api_key`` query param
    merged with the agency's ``extra_params`` (e.g. ``agency=SF``).
    """
    if cfg.auth_style == "header":
        return {"headers": {"api_key": api_key}}
    return {"params": {"api_key": api_key, **cfg.extra_params}}
