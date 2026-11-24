"""Per-agency collector configuration loaded from config/agencies/*.yaml.

Each yaml captures everything agency-specific about data collection: feed
URLs, auth placement, polling cadences, archive/pid paths, and env-var
names for secrets. The dataclass is the seed of the multi-agency stateless
collector: anything that can't be expressed here is, by definition, a
code change that collector needs.
"""

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "agencies"


@dataclass(frozen=True)
class RetentionPolicy:
    """How long one agency keeps each data tier in its local database.

    Read from the optional ``retention:`` block of the agency YAML (see
    ``config/agencies/*.yaml`` and ``docs/DEPLOYMENT.md`` §13). Daily
    aggregates (``system_metrics_daily``, ``route_metrics_daily_overlay``,
    ``route_headway_metrics``) are never pruned, so they have no knob.

    Attributes:
        stop_level_days: Service dates of ``stop_events`` + ``runs`` kept,
            counted back from (and including) the latest fully derived
            service date. Must exceed the refresh's zero-runs lookback (14)
            so a pruned date can never look like a hole.
        raw_days: Service dates whose raw inputs (``vehicle_positions``
            UTC days, ``trip_update_state``) are kept. At least 3: the VP
            reader window for date D reaches agency-local midnight D+2.
        apply_after_refresh: When true, ``pipelines/refresh_by_day.py``
            applies retention after its end-of-run housekeeping. Default
            false — deletes need the owner's explicit opt-in.
    """

    stop_level_days: int = 35
    raw_days: int = 7
    apply_after_refresh: bool = False


# Floors enforced on every RetentionPolicy (see its docstring for why).
MIN_STOP_LEVEL_DAYS = 15
MIN_RAW_DAYS = 3


def parse_retention(raw: dict | None, source: str = "<config>") -> RetentionPolicy:
    """Build a RetentionPolicy from a YAML ``retention:`` block (None = shallow default).

    Raises ValueError for unknown keys or values below the floors, so a
    typo can never silently shorten (or disable) a retention window.
    """
    if not raw:
        return RetentionPolicy()
    allowed = {"stop_level_days", "raw_days", "apply_after_refresh"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"Unknown retention key(s) {sorted(unknown)} in {source}")
    policy = RetentionPolicy(
        stop_level_days=int(raw.get("stop_level_days", RetentionPolicy.stop_level_days)),
        raw_days=int(raw.get("raw_days", RetentionPolicy.raw_days)),
        apply_after_refresh=bool(raw.get("apply_after_refresh", False)),
    )
    if policy.stop_level_days < MIN_STOP_LEVEL_DAYS:
        raise ValueError(
            f"retention.stop_level_days={policy.stop_level_days} in {source} is below "
            f"{MIN_STOP_LEVEL_DAYS} (must exceed the refresh's 14-day zero-runs lookback)"
        )
    if policy.raw_days < MIN_RAW_DAYS:
        raise ValueError(
            f"retention.raw_days={policy.raw_days} in {source} is below {MIN_RAW_DAYS} "
            "(the VP window for date D reads UTC days D..D+2)"
        )
    return policy


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
    # Local-DB retention tiers (optional YAML block; shallow-tier default).
    retention: RetentionPolicy = RetentionPolicy()


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
        retention=parse_retention(raw.get("retention"), str(path)),
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
