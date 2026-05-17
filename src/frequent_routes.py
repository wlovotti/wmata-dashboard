"""
WMATA-designated frequent-route list loader (NOTES-56).

Reads `config/frequent_routes.yaml` and exposes the route-level
designation that drives headline-KPI choice on the frontend (EWT for
frequent routes, OTP for the rest) and the per-route EWT cell-hour
gate.

The yaml splits the designation into two tiers WMATA publishes
separately — `high_freq` (≤ 12 min headway) and `medium_freq`
(≤ 20 min headway). Both tiers count as frequent for the UI flip;
they differ only in the cell-hour gate used by `src/ewt.py`:

  - high-freq / undesignated route → 15 min gate (legacy default)
  - medium-freq route              → 20 min gate

The 20-min gate exists because WMATA schedules medium-frequency
routes at exactly the 20-min headway threshold — a stricter gate
excludes nearly every cell-hour on those routes from EWT (see
`analysis/frequent_routes_audit_detail.py` for the empirical
finding that motivated the split).

The loader follows the same pattern as `src/route_targets.py`:
- file-mtime keyed cache so edits to the YAML take effect on the next
  call without a server restart
- never raises into the caller; if the YAML is missing or malformed the
  function returns empty sets and prints a warning so the API stays up
- environment override `WMATA_FREQUENT_ROUTES_PATH` for tests / future
  migrations
"""

from __future__ import annotations

import os
from pathlib import Path
from threading import Lock
from typing import Any

import yaml

# Cell-hour gates per tier (seconds). Kept as module-level constants
# so callers that need the raw value (tests, analysis scripts) don't
# have to encode the policy themselves.
HIGH_FREQ_GATE_SEC = 15 * 60
MEDIUM_FREQ_GATE_SEC = 20 * 60
DEFAULT_GATE_SEC = HIGH_FREQ_GATE_SEC


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


def _coerce_routes_list(value: Any, path: Path, key: str) -> frozenset[str]:
    """Parse one yaml tier list into a frozenset of route_id strings.

    Returns an empty frozenset (with a stdout warning) when the value
    is missing, is not a list, or contains no usable entries. Each
    surviving entry is coerced to str and stripped, so a YAML integer
    route_id (rare but possible) does not slip through as a different
    type than the GTFS route_id strings it's compared against.
    """
    if value is None:
        return frozenset()
    if not isinstance(value, list):
        print(f"[frequent_routes] {path}: `{key}` must be a list; ignoring")
        return frozenset()
    return frozenset(str(r).strip() for r in value if r is not None and str(r).strip())


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
        self._high_freq: frozenset[str] = frozenset()
        self._medium_freq: frozenset[str] = frozenset()
        self._lock = Lock()

    def _load_if_stale(self, path: Path) -> None:
        """Re-read the YAML if its mtime advanced since the last load.

        Resets both tier sets to empty if the file is missing or fails
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
                self._high_freq = frozenset()
                self._medium_freq = frozenset()
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
                self._high_freq = frozenset()
                self._medium_freq = frozenset()
            return

        if not isinstance(raw, dict):
            print(f"[frequent_routes] {path} is not a mapping at the top level; ignoring")
            raw = {}

        high = _coerce_routes_list(raw.get("high_freq"), path, "high_freq")
        medium = _coerce_routes_list(raw.get("medium_freq"), path, "medium_freq")

        # A route appearing in both tiers is a YAML authoring mistake.
        # Resolve to high_freq (the stricter gate) and warn so the
        # operator notices on the next reload.
        overlap = high & medium
        if overlap:
            print(
                f"[frequent_routes] {path}: route_id(s) appear in both tiers "
                f"({sorted(overlap)}); treating as high_freq"
            )
            medium = medium - overlap

        with self._lock:
            self._path = path
            self._mtime = mtime
            self._high_freq = high
            self._medium_freq = medium

    def get_high_freq(self, path: Path | None = None) -> frozenset[str]:
        """Return the high-freq tier set, reloading if the file changed."""
        resolved = path or _default_config_path()
        self._load_if_stale(resolved)
        return self._high_freq

    def get_medium_freq(self, path: Path | None = None) -> frozenset[str]:
        """Return the medium-freq tier set, reloading if the file changed."""
        resolved = path or _default_config_path()
        self._load_if_stale(resolved)
        return self._medium_freq


_CACHE = _FrequentRoutesCache()


def load_high_freq_route_ids() -> frozenset[str]:
    """Return the set of WMATA-designated high-frequency `route_id`s (≤ 12 min)."""
    return _CACHE.get_high_freq(_config_path_for_env())


def load_medium_freq_route_ids() -> frozenset[str]:
    """Return the set of WMATA-designated medium-frequency `route_id`s (≤ 20 min)."""
    return _CACHE.get_medium_freq(_config_path_for_env())


def load_frequent_route_ids() -> frozenset[str]:
    """Return the union of high-freq + medium-freq designated `route_id`s.

    This is the "is this route on WMATA's frequent map at all?" answer
    callers want for headline-KPI selection. Returns an empty frozenset
    when the file is missing or malformed so `is_frequent` falls back
    to `False` for every route rather than 500'ing the API.
    """
    return load_high_freq_route_ids() | load_medium_freq_route_ids()


def is_frequent_route(route_id: str) -> bool:
    """Return True iff `route_id` is on WMATA's frequent-service map (either tier)."""
    return route_id in load_frequent_route_ids()


def get_cell_hour_gate_sec(route_id: str) -> int:
    """Return the EWT cell-hour scheduled-headway gate (seconds) for `route_id`.

    `MEDIUM_FREQ_GATE_SEC` for routes WMATA publishes as medium-frequency;
    `DEFAULT_GATE_SEC` (= `HIGH_FREQ_GATE_SEC`) for high-frequency and
    undesignated routes. The per-route lookup is what `src/ewt.py`'s
    cell-hour classifier uses to decide whether a (direction, stop, hour)
    cell contributes to EWT.
    """
    if route_id in load_medium_freq_route_ids():
        return MEDIUM_FREQ_GATE_SEC
    return DEFAULT_GATE_SEC


def reset_cache_for_tests() -> None:
    """Drop the in-memory cache. Tests call this between fixture swaps."""
    global _CACHE
    _CACHE = _FrequentRoutesCache()
