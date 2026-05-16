"""
WMATA-designated frequent-route list loader (NOTES-56).

Reads `config/frequent_routes.yaml` and exposes a single function that
returns the set of GTFS `route_id`s WMATA publishes as frequent service.
This is the *route-level* designation that drives headline-KPI choice
on the frontend (EWT for frequent routes, OTP for the rest) — distinct
from `src/ewt.py:FREQUENT_HEADWAY_MAX_SEC`, which is a per-cell-hour
gate for the EWT computation itself.

The loader follows the same pattern as `src/route_targets.py`:
- file-mtime keyed cache so edits to the YAML take effect on the next
  call without a server restart
- never raises into the caller; if the YAML is missing or malformed the
  function returns an empty set and prints a warning so the API stays up
- environment override `WMATA_FREQUENT_ROUTES_PATH` for tests / future
  migrations
"""

from __future__ import annotations

import os
from pathlib import Path
from threading import Lock
from typing import Any

import yaml


def _default_config_path() -> Path:
    """Return the default YAML path: `<repo_root>/config/frequent_routes.yaml`."""
    return Path(__file__).resolve().parent.parent / "config" / "frequent_routes.yaml"


def _config_path_for_env() -> Path:
    """Resolve the YAML path, honoring `WMATA_FREQUENT_ROUTES_PATH` if set.

    The env override exists for tests and future migrations; production
    code reads from the default repo-relative location.
    """
    env_path = os.environ.get("WMATA_FREQUENT_ROUTES_PATH")
    if env_path:
        return Path(env_path)
    return _default_config_path()


class _FrequentRoutesCache:
    """File-mtime-keyed cache for the parsed frequent-routes YAML.

    Reload happens lazily on `_load_if_stale` when the file mtime
    advances. The lock only protects cache assignment, not the parse,
    so concurrent reads after first load are uncontended.
    """

    def __init__(self) -> None:
        """Initialize an empty cache; first read triggers a parse."""
        self._path: Path | None = None
        self._mtime: float | None = None
        self._route_ids: frozenset[str] = frozenset()
        self._lock = Lock()

    def _load_if_stale(self, path: Path) -> None:
        """Re-read the YAML if its mtime advanced since the last load.

        Resets the cached set to empty if the file is missing or fails
        to parse — never raises into the caller. Parse failures emit a
        single-line warning to stdout so the operator editing the file
        sees the problem on the next API hit.
        """
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            with self._lock:
                self._path = path
                self._mtime = None
                self._route_ids = frozenset()
            return

        if self._path == path and self._mtime == mtime:
            return

        try:
            with path.open("r", encoding="utf-8") as f:
                raw: Any = yaml.safe_load(f) or {}
        except (OSError, yaml.YAMLError) as exc:
            print(f"[frequent_routes] failed to parse {path}: {exc}")
            with self._lock:
                self._path = path
                self._mtime = mtime
                self._route_ids = frozenset()
            return

        if not isinstance(raw, dict):
            print(f"[frequent_routes] {path} is not a mapping at the top level; ignoring")
            raw = {}

        routes = raw.get("routes")
        if routes is None:
            route_ids: frozenset[str] = frozenset()
        elif isinstance(routes, list):
            # Coerce each entry to str so YAML integer route_ids (rare
            # but possible) don't slip through as a different type than
            # the GTFS route_id strings they're compared against.
            route_ids = frozenset(
                str(r).strip() for r in routes if r is not None and str(r).strip()
            )
        else:
            print(f"[frequent_routes] {path}: `routes` must be a list; ignoring")
            route_ids = frozenset()

        with self._lock:
            self._path = path
            self._mtime = mtime
            self._route_ids = route_ids

    def get_route_ids(self, path: Path | None = None) -> frozenset[str]:
        """Return the cached set, reloading if the file changed."""
        resolved = path or _default_config_path()
        self._load_if_stale(resolved)
        return self._route_ids


_CACHE = _FrequentRoutesCache()


def load_frequent_route_ids() -> frozenset[str]:
    """Return the set of WMATA-designated frequent `route_id`s.

    Reads `config/frequent_routes.yaml` (or the path in
    `WMATA_FREQUENT_ROUTES_PATH` when set). Returns an empty frozenset
    when the file is missing or malformed so callers can rely on a
    safe default — `is_frequent` falls back to `False` for every
    route rather than 500'ing the API.

    The result is mtime-cached; calling this in a hot loop is cheap.
    """
    return _CACHE.get_route_ids(_config_path_for_env())


def is_frequent_route(route_id: str) -> bool:
    """Return True iff `route_id` is on WMATA's frequent-service map.

    Convenience wrapper over `load_frequent_route_ids()`.
    """
    return route_id in load_frequent_route_ids()


def reset_cache_for_tests() -> None:
    """Drop the in-memory cache. Tests call this between fixture swaps."""
    global _CACHE
    _CACHE = _FrequentRoutesCache()
