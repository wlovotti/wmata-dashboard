"""
Per-route and system-default performance targets (NOTES-47).

Targets live in `config/route_targets.yaml`. This module loads them
lazily, caches by file mtime so a reload happens automatically when the
YAML changes on disk, and exposes a small API for callers:

- `get_system_target(metric)` returns the system-default for one metric.
- `get_target(route_id, metric)` returns the per-route override, falling
  back to the system default.
- `get_all_targets()` returns the full loaded payload (used by API
  endpoints that ship targets to the frontend).

`metric` is one of:

  - `"otp"`               -> percent, 0-100, higher is better.
  - `"service_delivered"` -> fraction, 0-1, higher is better.
  - `"ewt"`               -> seconds, lower is better. The YAML stores
                             this as `ewt_minutes` for human-edit
                             friendliness; this module converts to
                             seconds so the value compares directly to
                             the `ewt_seconds` columns in the DB / API.
  - `"bunching"`          -> fraction of headway pairs, 0-1, lower is
                             better.

Returns `None` from `get_target` / `get_system_target` when the YAML
either does not define the metric or does not parse — never raises in
the hot read path so a malformed checked-in YAML doesn't take down the
API. Parse warnings go to stdout.
"""

from __future__ import annotations

import os
from pathlib import Path
from threading import Lock
from typing import Any

import yaml

# Public metric keys callers pass in. The YAML's `system_default` block
# uses different key names for two of them (`ewt_minutes`, `bunching_pct`)
# so operators editing the file see units in the key name; this map keeps
# the on-disk-friendly names and converts on read.
_METRIC_KEY_MAP: dict[str, tuple[str, float]] = {
    # public_metric: (yaml_key, multiplier_to_canonical_unit)
    "otp": ("otp", 1.0),
    "service_delivered": ("service_delivered", 1.0),
    # YAML stores minutes; canonical unit is seconds to match `ewt_seconds`
    # on the API payloads and `SystemMetricsDaily.ewt_seconds` column.
    "ewt": ("ewt_minutes", 60.0),
    "bunching": ("bunching_pct", 1.0),
}

VALID_METRICS = tuple(_METRIC_KEY_MAP.keys())


def _default_config_path() -> Path:
    """Return the default YAML path: `<repo_root>/config/route_targets.yaml`."""
    return Path(__file__).resolve().parent.parent / "config" / "route_targets.yaml"


class _TargetsCache:
    """File-mtime-keyed cache for the parsed YAML.

    Reload happens lazily on `_load_if_stale` when the file mtime
    advances. Two readers racing both produce a valid cached payload —
    the lock only protects the cache assignment, not the parse, so
    concurrent reads after first load are uncontended.
    """

    def __init__(self) -> None:
        """Initialize an empty cache; first read triggers a parse."""
        self._path: Path | None = None
        self._mtime: float | None = None
        self._payload: dict[str, Any] = {"system_default": {}, "routes": {}}
        self._lock = Lock()

    def _load_if_stale(self, path: Path) -> None:
        """Re-read the YAML if its mtime advanced since the last load.

        Resets the cached payload to an empty default if the file is
        missing or fails to parse — never raises into the caller.
        """
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            with self._lock:
                self._path = path
                self._mtime = None
                self._payload = {"system_default": {}, "routes": {}}
            return

        if self._path == path and self._mtime == mtime:
            return

        try:
            with path.open("r", encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
        except (OSError, yaml.YAMLError) as exc:
            # Malformed YAML or unreadable file — fall back to empty so
            # endpoints keep working. Surface to stdout so an operator
            # editing the file sees the problem on the next API hit.
            print(f"[route_targets] failed to parse {path}: {exc}")
            with self._lock:
                self._path = path
                self._mtime = mtime
                self._payload = {"system_default": {}, "routes": {}}
            return

        if not isinstance(raw, dict):
            print(f"[route_targets] {path} is not a mapping at the top level; ignoring")
            raw = {}

        system_default = raw.get("system_default") or {}
        routes = raw.get("routes") or {}
        if not isinstance(system_default, dict):
            system_default = {}
        if not isinstance(routes, dict):
            routes = {}

        with self._lock:
            self._path = path
            self._mtime = mtime
            self._payload = {"system_default": system_default, "routes": routes}

    def get_payload(self, path: Path | None = None) -> dict[str, Any]:
        """Return the current cached payload, reloading if the file changed."""
        resolved = path or _default_config_path()
        self._load_if_stale(resolved)
        return self._payload


_CACHE = _TargetsCache()


def _config_path_for_env() -> Path:
    """Resolve the YAML path, honoring `WMATA_ROUTE_TARGETS_PATH` if set.

    The env override is purely for tests / future migrations — the
    production read path uses the default repo-relative location.
    """
    env_path = os.environ.get("WMATA_ROUTE_TARGETS_PATH")
    if env_path:
        return Path(env_path)
    return _default_config_path()


def _coerce_float(value: Any) -> float | None:
    """Best-effort float cast; returns None on type errors / NaN."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:  # NaN
        return None
    return out


def get_system_target(metric: str) -> float | None:
    """Return the system-default target for one metric, in canonical units.

    Canonical units: OTP percent (0-100), service_delivered fraction
    (0-1), EWT seconds, bunching fraction (0-1). Returns None when the
    YAML omits the metric, or for any unknown metric key.
    """
    if metric not in _METRIC_KEY_MAP:
        return None
    yaml_key, multiplier = _METRIC_KEY_MAP[metric]
    payload = _CACHE.get_payload(_config_path_for_env())
    raw = payload.get("system_default", {}).get(yaml_key)
    value = _coerce_float(raw)
    if value is None:
        return None
    return value * multiplier


def get_target(route_id: str, metric: str) -> float | None:
    """Return the target for `route_id` / `metric`, in canonical units.

    Lookup order: per-route override -> system default -> None.
    Per-route overrides may set a subset of metrics; missing entries
    inherit the system default.
    """
    if metric not in _METRIC_KEY_MAP:
        return None
    yaml_key, multiplier = _METRIC_KEY_MAP[metric]
    payload = _CACHE.get_payload(_config_path_for_env())
    route_block = payload.get("routes", {}).get(route_id) or {}
    if isinstance(route_block, dict) and yaml_key in route_block:
        value = _coerce_float(route_block.get(yaml_key))
        if value is not None:
            return value * multiplier
    return get_system_target(metric)


def get_targets_for_route(route_id: str) -> dict[str, float | None]:
    """Return all four targets for one route, falling back to system defaults.

    Convenience for the API payload builders — emits the same key shape
    every route, so the frontend can render the field unconditionally.
    """
    return {metric: get_target(route_id, metric) for metric in VALID_METRICS}


def get_system_targets() -> dict[str, float | None]:
    """Return all four system-default targets."""
    return {metric: get_system_target(metric) for metric in VALID_METRICS}


def reset_cache_for_tests() -> None:
    """Drop the in-memory cache. Tests call this between fixture swaps."""
    global _CACHE
    _CACHE = _TargetsCache()
