# SFMTA Sidecar Collector Implementation Plan (Plan 1 of 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a Muni (SFMTA) data collector on the Lightsail VM polling 511.org GTFS-RT feeds into a separate `sfmta_dashboard` database, so comparison data starts accruing.

**Architecture:** Sidecar instance of the existing collector: a new thin script (`scripts/sfmta_collector.py`) drives the existing `WMATADataCollector` class, parameterized with 511.org feed URLs, query-style auth, Pacific service-date fallback, and a separate archive dir + database. Agency parameters live in `config/agencies/*.yaml` (the seed of the multi-agency engine). Spec: `docs/superpowers/specs/2026-07-21-sfmta-comparison-design.md`.

**Tech Stack:** Python 3.12, SQLAlchemy, `gtfs-realtime-bindings` (`google.transit.gtfs_realtime_pb2`), requests, PyYAML, zstandard, pytest.

## Global Constraints

- Cadence budget (511 default cap 60 req/rolling-hour): TripUpdates every **120 s** (30/hr), VehiclePositions every **180 s** (20/hr), total 50/hr with 10/hr slack. Implemented as `tick_sec: 60`, TU every 2nd tick, VP every 3rd tick.
- WMATA behavior must be byte-for-byte unchanged: every new constructor/function parameter defaults to today's behavior. Existing tests must pass untouched.
- Datetime storage stays naive UTC everywhere; service-date fallback for SFMTA uses `America/Los_Angeles`.
- Before every commit run: `uv run pytest -m smoke && uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/` — all three must pass (CI gates).
- All new functions/classes/methods get docstrings (user's global standard).
- Branch: `feature/sfmta-sidecar-collector` off `main`. One PR for this whole plan. PR body MUST cite `docs/DEPLOYMENT.md` §2 for the systemd unit install steps (do not inline daemon-reload instructions).
- Non-code prerequisites (user, not implementer): register a free 511.org API token; send the rate-increase request email to 511sfbaydeveloperresources@googlegroups.com (do not include the key); create a second healthchecks.io check for the Muni collector.

---

### Task 1: Agency config files + loader

**Files:**
- Create: `config/agencies/wmata.yaml`
- Create: `config/agencies/sfmta.yaml`
- Create: `src/agency_config.py`
- Test: `tests/test_agency_config.py`

**Interfaces:**
- Consumes: nothing (leaf module; PyYAML already a dependency via `src/frequent_routes.py`).
- Produces: `AgencyConfig` frozen dataclass; `load_agency_config(name: str) -> AgencyConfig`; `request_kwargs(cfg: AgencyConfig, api_key: str) -> dict` returning kwargs to splat into `requests.get` (`{"headers": {...}}` for header auth, `{"params": {...}}` for query auth). Tasks 5–7 rely on these exact names.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_agency_config.py
"""Tests for the per-agency configuration loader."""

import pytest

from src.agency_config import AgencyConfig, load_agency_config, request_kwargs


def test_load_sfmta_config():
    """sfmta.yaml loads with the spec'd cadences and Pacific timezone."""
    cfg = load_agency_config("sfmta")
    assert cfg.name == "sfmta"
    assert cfg.timezone == "America/Los_Angeles"
    assert cfg.tick_sec == 60
    assert cfg.trip_updates_every_ticks == 2       # 120s
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
    assert cfg.trip_updates_every_ticks == 1       # 30s
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_agency_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.agency_config'`

- [ ] **Step 3: Write the yaml files**

```yaml
# config/agencies/sfmta.yaml
# SFMTA (Muni) via 511.org — MTC's regional clearinghouse, the only
# first-party GTFS-RT source for Muni. Default token cap: 60 req/rolling
# hour; cadences below budget 50/hr (TU 120s + VP 180s) leaving 10/hr
# slack for retries and ad-hoc calls. See spec
# docs/superpowers/specs/2026-07-21-sfmta-comparison-design.md.
name: sfmta
display_name: SFMTA (Muni)
timezone: America/Los_Angeles
api:
  key_env: SFMTA_API_KEY
  auth: query
  trip_updates_url: https://api.511.org/transit/tripupdates
  vehicle_positions_url: https://api.511.org/transit/vehiclepositions
  extra_params:
    agency: SF
collector:
  tick_sec: 60
  trip_updates_every_ticks: 2
  vehicle_positions_every_ticks: 3
  archive_dir: archive/sfmta_raw_snapshots
  pid_file: logs/sfmta_collector.pid
  heartbeat_name: sfmta-combined
database:
  url_env: SFMTA_DATABASE_URL
healthcheck:
  url_env: SFMTA_COLLECTOR_HEALTHCHECK_URL
```

```yaml
# config/agencies/wmata.yaml
# Documents the WMATA collector's current implicit settings. NOT yet
# consumed by scripts/continuous_combined_collector.py (that migration is
# the NOTES-95 multi-agency rewrite); exists so every implicit assumption
# is written down and tested loadable.
name: wmata
display_name: WMATA Metrobus
timezone: America/New_York
api:
  key_env: WMATA_API_KEY
  auth: header
  trip_updates_url: https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb
  vehicle_positions_url: https://api.wmata.com/gtfs/bus-gtfsrt-vehiclepositions.pb
  extra_params: {}
collector:
  tick_sec: 30
  trip_updates_every_ticks: 1
  vehicle_positions_every_ticks: 2
  archive_dir: archive/raw_snapshots
  pid_file: logs/collector.pid
  heartbeat_name: combined
database:
  url_env: DATABASE_URL
healthcheck:
  url_env: COLLECTOR_HEALTHCHECK_URL
```

- [ ] **Step 4: Write the loader**

```python
# src/agency_config.py
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_agency_config.py -v`
Expected: 5 passed

- [ ] **Step 6: Run gates and commit**

```bash
uv run pytest -m smoke && uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add config/agencies/ src/agency_config.py tests/test_agency_config.py
git commit -m "feat: per-agency collector config (config/agencies/*.yaml + loader)"
```

---

### Task 2: Import-safe collector module + timezone-generic service-date fallback

**Files:**
- Modify: `src/wmata_collector.py:41-48` (module-level API-key raise) and `src/wmata_collector.py:22-38` (`_service_date_for_row`)
- Modify: `src/timezones.py` (add `local_date_from_naive_utc`)
- Test: `tests/test_timezones_local_date.py`

**Interfaces:**
- Consumes: existing `src/timezones.py` module constants (`UTC`, `EASTERN`).
- Produces: `local_date_from_naive_utc(naive_utc_dt: datetime, tz_name: str) -> date` in `src/timezones.py`; `_service_date_for_row(row: dict, tz_name: str = "America/New_York") -> date` in `src/wmata_collector.py`; `import src.wmata_collector` succeeds with no `WMATA_API_KEY` in the environment. Tasks 3 and 6 rely on all three.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_timezones_local_date.py
"""Tests for timezone-parameterized service-date derivation (SFMTA spec §2)."""

from datetime import date, datetime

from src.timezones import local_date_from_naive_utc


def test_local_date_pacific_vs_eastern_split():
    """05:30 UTC is the next day in Eastern but still 'yesterday' in Pacific.

    2026-07-22 05:30 UTC = 01:30 EDT Jul 22 = 22:30 PDT Jul 21.
    """
    ts = datetime(2026, 7, 22, 5, 30, 0)
    assert local_date_from_naive_utc(ts, "America/New_York") == date(2026, 7, 22)
    assert local_date_from_naive_utc(ts, "America/Los_Angeles") == date(2026, 7, 21)


def test_service_date_fallback_respects_tz():
    """_service_date_for_row uses the agency timezone when start_date is absent."""
    from src.wmata_collector import _service_date_for_row

    row = {"trip_start_date": None, "snapshot_ts": datetime(2026, 7, 22, 5, 30, 0)}
    assert _service_date_for_row(row) == date(2026, 7, 22)  # Eastern default
    assert _service_date_for_row(row, tz_name="America/Los_Angeles") == date(2026, 7, 21)


def test_wmata_collector_importable_without_key(monkeypatch):
    """Importing the module must not require WMATA_API_KEY (SFMTA-only hosts)."""
    import importlib
    import sys

    monkeypatch.delenv("WMATA_API_KEY", raising=False)
    sys.modules.pop("src.wmata_collector", None)
    mod = importlib.import_module("src.wmata_collector")
    assert hasattr(mod, "WMATADataCollector")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_timezones_local_date.py -v`
Expected: FAIL — `ImportError: cannot import name 'local_date_from_naive_utc'`

- [ ] **Step 3: Add the timezone helper**

Append to `src/timezones.py` (near `eastern_date_from_naive_utc`):

```python
def local_date_from_naive_utc(naive_utc_dt, tz_name):
    """Return the calendar date of a naive-UTC datetime in an arbitrary IANA zone.

    Timezone-generic sibling of ``eastern_date_from_naive_utc`` for
    multi-agency service-date fallback (an SF trip observed at 05:30 UTC
    belongs to the *previous* Pacific service date). ``tz_name`` is an
    IANA name like ``America/Los_Angeles``.
    """
    from zoneinfo import ZoneInfo

    return naive_utc_dt.replace(tzinfo=UTC).astimezone(ZoneInfo(tz_name)).date()
```

(If `src/timezones.py` already imports `ZoneInfo` at module top, use that import instead of the local one — match the file's existing style.)

- [ ] **Step 4: Make wmata_collector import-safe and tz-parameterized**

In `src/wmata_collector.py`, replace the module-level key check (lines ~44-48):

```python
# Your WMATA API key from environment. May legitimately be absent on
# hosts that only run other agencies' collectors (e.g. the SFMTA
# sidecar) — callers that need it (main() below, WMATA scripts) must
# check for None themselves.
API_KEY = os.getenv("WMATA_API_KEY")
```

(Delete the `if not API_KEY: raise ValueError(...)` block. Then add the equivalent guard at the top of this module's `main()`:)

```python
def main():
    if not API_KEY:
        raise ValueError("WMATA_API_KEY not found in environment variables")
```

Replace `_service_date_for_row` with:

```python
def _service_date_for_row(row: dict, tz_name: str = "America/New_York"):
    """Return the local-zone service_date for a trip-update row.

    Prefers ``trip_start_date`` (YYYYMMDD string from GTFS-RT
    ``tripDescriptor.start_date``) when present and parseable; otherwise
    falls back to the agency-local calendar day of ``snapshot_ts``
    (``tz_name``, default Eastern — WMATA's zone — so all existing call
    sites keep their behavior).

    Module-level (not a method) so the replay tool can reuse it without
    pulling in the WMATADataCollector context.
    """
    raw = row.get("trip_start_date")
    if raw:
        try:
            return datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            pass  # fall through to snapshot_ts inference
    return local_date_from_naive_utc(row["snapshot_ts"], tz_name)
```

Update the import on line 18 to include the new helper:

```python
from src.timezones import (
    eastern_date_from_naive_utc,
    from_epoch_naive_utc,
    local_date_from_naive_utc,
    utcnow_naive,
)
```

(If `eastern_date_from_naive_utc` is now unused in this file, remove it from the import — ruff will flag it.)

- [ ] **Step 5: Run tests + existing collector tests**

Run: `uv run pytest tests/test_timezones_local_date.py -v && uv run pytest -m smoke -v -k "collector or timezone"`
Expected: new tests PASS; no existing test regresses.

- [ ] **Step 6: Run gates and commit**

```bash
uv run pytest -m smoke && uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add src/wmata_collector.py src/timezones.py tests/test_timezones_local_date.py
git commit -m "refactor: import-safe wmata_collector; tz-generic service-date fallback"
```

---

### Task 3: Parameterize WMATADataCollector feeds (URLs, auth kwargs, heartbeat name, service-date tz)

**Files:**
- Modify: `src/wmata_collector.py:54-84` (`__init__`), `:405-420` (`get_realtime_vehicle_positions`), `:508-530` (`get_realtime_trip_updates`), `:604-672` (`_save_trip_updates`)
- Test: `tests/test_collector_feed_params.py`

**Interfaces:**
- Consumes: Task 2's `_service_date_for_row(row, tz_name)`.
- Produces: `WMATADataCollector.__init__` gains keyword-only args `tu_feed_url: str | None = None`, `vp_feed_url: str | None = None`, `request_params: dict | None = None`, `service_date_tz: str = "America/New_York"`, `heartbeat_name: str = "combined"`. Defaults reproduce current WMATA behavior exactly. Task 6 constructs the collector through these args.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_collector_feed_params.py
"""Feed-parameterization tests: the collector class must serve any GTFS-RT agency."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from google.transit import gtfs_realtime_pb2

from src.wmata_collector import WMATADataCollector


def _tu_feed_bytes(trip_id="muni_trip_1", stop_id="S1", ts=1784500000):
    """Build a minimal serialized TripUpdates FeedMessage for mocking."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = ts
    ent = feed.entity.add()
    ent.id = "1"
    ent.trip_update.trip.trip_id = trip_id
    stu = ent.trip_update.stop_time_update.add()
    stu.stop_id = stop_id
    stu.stop_sequence = 3
    stu.arrival.time = ts + 120
    return feed.SerializeToString()


def _make_collector(tmp_path, **kwargs):
    """Collector with archive redirected into tmp_path (required in tests)."""
    return WMATADataCollector("KEY", archive_root=tmp_path, **kwargs)


def test_custom_tu_url_and_query_params_used(tmp_path):
    """A 511-style collector fetches the configured URL with query params."""
    collector = _make_collector(
        tmp_path,
        tu_feed_url="https://api.511.org/transit/tripupdates",
        request_params={"api_key": "KEY", "agency": "SF"},
    )
    resp = MagicMock(status_code=200, content=_tu_feed_bytes())
    with patch("src.wmata_collector.requests.get", return_value=resp) as mock_get:
        snapshot_ts, rows = collector.get_realtime_trip_updates()
    assert mock_get.call_args.args[0] == "https://api.511.org/transit/tripupdates"
    assert mock_get.call_args.kwargs["params"] == {"api_key": "KEY", "agency": "SF"}
    assert len(rows) == 1 and rows[0]["trip_id"] == "muni_trip_1"


def test_default_urls_unchanged_for_wmata(tmp_path):
    """No params -> exact current WMATA URL and no query params (regression guard)."""
    collector = _make_collector(tmp_path)
    resp = MagicMock(status_code=200, content=_tu_feed_bytes())
    with patch("src.wmata_collector.requests.get", return_value=resp) as mock_get:
        collector.get_realtime_trip_updates()
    assert mock_get.call_args.args[0] == "https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb"
    assert "params" not in mock_get.call_args.kwargs or mock_get.call_args.kwargs["params"] is None


def test_save_trip_updates_uses_tz_and_heartbeat_name(tmp_path, db_session):
    """Pacific service-date fallback + custom heartbeat collector_name land in the DB."""
    from src.models import CollectorHeartbeat, TripUpdateState

    collector = _make_collector(
        tmp_path,
        service_date_tz="America/Los_Angeles",
        heartbeat_name="sfmta-combined",
    )
    collector.db = db_session
    # 05:30 UTC on Jul 22 = 22:30 PDT Jul 21 -> Pacific service_date must be Jul 21.
    snapshot = datetime(2026, 7, 22, 5, 30, 0)
    rows = [
        {
            "snapshot_ts": snapshot,
            "trip_id": "muni_trip_1",
            "route_id": "38R",
            "vehicle_id": "v1",
            "stop_id": "S1",
            "stop_sequence": 3,
            "predicted_arrival_ts": None,
            "predicted_departure_ts": None,
            "schedule_relationship": "UNSET",
            "trip_start_date": None,
        }
    ]
    collector._save_trip_updates(rows)
    state = db_session.query(TripUpdateState).one()
    assert state.service_date.isoformat() == "2026-07-21"
    hb = db_session.query(CollectorHeartbeat).one()
    assert hb.collector_name == "sfmta-combined"
```

Note for the implementer: `db_session` is the SQLite-in-memory fixture from `tests/conftest.py`. If `upsert_trip_update_state` is Postgres-only (`pg_insert`), switch this test to the `pg_session` fixture instead — check `src/upsert_helpers.py:upsert_trip_update_state` first; follow whichever engine existing tests of `_save_trip_updates` use.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_collector_feed_params.py -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'tu_feed_url'`

- [ ] **Step 3: Implement the parameterization**

In `WMATADataCollector.__init__`, add keyword-only params after the existing ones and store them (docstring updated accordingly):

```python
    def __init__(
        self,
        api_key,
        db_session: Session = None,
        archive_root: Path | str | None = None,
        healthcheck_url: str | None = None,
        *,
        tu_feed_url: str | None = None,
        vp_feed_url: str | None = None,
        request_params: dict | None = None,
        service_date_tz: str = "America/New_York",
        heartbeat_name: str = "combined",
    ):
```

with (inside the body, after `self._healthcheck_url = healthcheck_url`):

```python
        # Feed parameterization (SFMTA sidecar, spec 2026-07-21): None keeps
        # the WMATA defaults so existing call sites are untouched.
        self.tu_feed_url = tu_feed_url or f"{BASE_URL}/bus-gtfsrt-tripupdates.pb"
        self.vp_feed_url = vp_feed_url or f"{BASE_URL}/bus-gtfsrt-vehiclepositions.pb"
        self.request_params = request_params
        self.service_date_tz = service_date_tz
        self.heartbeat_name = heartbeat_name
```

In `get_realtime_trip_updates`, replace `url = f"{BASE_URL}/bus-gtfsrt-tripupdates.pb"` with `url = self.tu_feed_url` and the request line with:

```python
            response = requests.get(
                url, headers=self.headers, params=self.request_params, timeout=timeout
            )
```

In `get_realtime_vehicle_positions`, same change: `url = self.vp_feed_url`, and add `params=self.request_params` to the `requests.get` call.

In `_save_trip_updates`:
- `"service_date": _service_date_for_row(r)` → `"service_date": _service_date_for_row(r, self.service_date_tz)`
- `CollectorHeartbeat(ts=tick_ts, collector_name="combined")` → `CollectorHeartbeat(ts=tick_ts, collector_name=self.heartbeat_name)`

- [ ] **Step 4: Run new + existing tests**

Run: `uv run pytest tests/test_collector_feed_params.py -v && uv run pytest -m smoke`
Expected: all PASS (`test_default_urls_unchanged_for_wmata` is the no-regression witness).

- [ ] **Step 5: Run gates and commit**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add src/wmata_collector.py tests/test_collector_feed_params.py
git commit -m "feat: parameterize collector feeds (url/auth/tz/heartbeat) for multi-agency use"
```

---

### Task 4: Database URL override in src/database.py

**Files:**
- Modify: `src/database.py`
- Test: `tests/test_database_url_override.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `get_engine(db_url: str | None = None)`, `get_session(db_url: str | None = None)`, `init_db(engine=None, db_url: str | None = None)` — `None` preserves today's env-var behavior. Task 6 calls `get_session(db_url=...)` and `init_db(db_url=...)`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_database_url_override.py
"""The session factory must accept an explicit URL for sidecar databases."""

from src.database import get_engine


def test_get_engine_honors_explicit_url():
    """An explicit db_url wins over the DATABASE_URL environment default."""
    engine = get_engine(db_url="sqlite:///:memory:")
    assert str(engine.url) == "sqlite:///:memory:"


def test_get_engine_default_unchanged():
    """No argument -> engine built from DATABASE_URL env (conftest sets sqlite)."""
    engine = get_engine()
    assert engine.url is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_database_url_override.py -v`
Expected: FAIL — `TypeError: get_engine() got an unexpected keyword argument 'db_url'`

- [ ] **Step 3: Implement**

Modify the three functions in `src/database.py` (module-level `DATABASE_URL` attr stays for any importers; note `pool_pre_ping`/pool args are harmless no-ops on sqlite):

```python
def get_engine(db_url: str | None = None):
    """Create a SQLAlchemy engine.

    ``db_url=None`` (the default) reads ``DATABASE_URL`` from the
    environment at call time — the historical single-database behavior.
    Sidecar collectors (e.g. SFMTA) pass an explicit URL so one process
    can target a different database than the rest of the codebase.
    """
    url = db_url or os.getenv("DATABASE_URL")
    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600,
        echo=False,
    )
    return engine


def init_db(engine=None, db_url: str | None = None):
    """Create all tables on the given engine (or one built from db_url/env)."""
    if engine is None:
        engine = get_engine(db_url)
    Base.metadata.create_all(bind=engine)
    print(f"Database initialized at: {engine.url}")


def get_session(db_url: str | None = None) -> Session:
    """Get a new database session, optionally against an explicit URL."""
    engine = get_engine(db_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()
```

Implementer note: `init_db` previously printed the module-level `DATABASE_URL`; printing `engine.url` instead also stops leaking the password when the URL carries one — verify no test asserts on that print string (`grep -rn "Database initialized" tests/`).

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_database_url_override.py -v && uv run pytest -m smoke`
Expected: all PASS.

- [ ] **Step 5: Run gates and commit**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add src/database.py tests/test_database_url_override.py
git commit -m "feat: explicit db_url override in database session factory"
```

---

### Task 5: Extract pid-file helpers into src/pidfile.py

**Files:**
- Create: `src/pidfile.py`
- Modify: `scripts/continuous_combined_collector.py:73-138` (delete the three pid helpers; import from `src.pidfile`)
- Test: `tests/test_pidfile.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `acquire_pid_file(pid_file: Path) -> None` (raises `SystemExit(1)` if a live other process holds it; registers atexit cleanup) and `release_pid_file(pid_file: Path) -> None` (idempotent). Both are path-parameterized versions of the WMATA collector's current module-level helpers. Task 6 uses them with `logs/sfmta_collector.pid`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_pidfile.py
"""Path-parameterized pid-file guard (extracted from the WMATA collector)."""

import os

import pytest

from src.pidfile import acquire_pid_file, release_pid_file


def test_acquire_writes_own_pid(tmp_path):
    """Fresh acquire writes this process's pid."""
    pf = tmp_path / "x.pid"
    acquire_pid_file(pf)
    assert pf.read_text().strip() == str(os.getpid())
    release_pid_file(pf)
    assert not pf.exists()


def test_acquire_overwrites_stale_pid(tmp_path):
    """A pid file pointing at a dead process is silently overwritten."""
    pf = tmp_path / "x.pid"
    pf.write_text("999999999")  # certainly dead
    acquire_pid_file(pf)
    assert pf.read_text().strip() == str(os.getpid())
    release_pid_file(pf)


def test_acquire_refuses_live_other_process(tmp_path):
    """A pid file pointing at a live foreign process aborts startup."""
    pf = tmp_path / "x.pid"
    pf.write_text("1")  # pid 1 is always alive and never us
    with pytest.raises(SystemExit):
        acquire_pid_file(pf)


def test_release_leaves_foreign_pid_file(tmp_path):
    """release only deletes a file containing OUR pid."""
    pf = tmp_path / "x.pid"
    pf.write_text("1")
    release_pid_file(pf)
    assert pf.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pidfile.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.pidfile'`

- [ ] **Step 3: Create src/pidfile.py**

Move the three helpers from `scripts/continuous_combined_collector.py` verbatim, parameterizing the path (keep their docstrings, adjusted). The module needs `atexit`, `functools.partial`, `os`, `pathlib.Path`:

```python
# src/pidfile.py
"""Pid-file guard shared by all collector processes.

Extracted from scripts/continuous_combined_collector.py (which hardcoded
logs/collector.pid) so sidecar collectors (SFMTA) can guard their own
pid path with identical semantics: stale/malformed files are silently
overwritten; a live foreign pid aborts startup with SystemExit(1).
"""

import atexit
import os
from functools import partial
from pathlib import Path


def _is_pid_alive(pid: int) -> bool:
    """Return True if *pid* refers to a live process on this machine.

    Uses ``os.kill(pid, 0)`` which sends no signal but raises if the
    process does not exist or if we lack permission to signal it.

    - ``ProcessLookupError`` (ESRCH) — process does not exist; return False.
    - ``PermissionError`` (EPERM) — process exists but is owned by another
      user; treat as live (return True) to avoid clobbering.
    - Any other ``OSError`` — treated conservatively as live.
    """
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except (PermissionError, OSError):
        return True


def release_pid_file(pid_file: Path) -> None:
    """Remove *pid_file* if it still contains this process's pid (idempotent)."""
    if pid_file.exists():
        try:
            if pid_file.read_text().strip() == str(os.getpid()):
                pid_file.unlink(missing_ok=True)
        except OSError:
            pass


def acquire_pid_file(pid_file: Path) -> None:
    """Write the current pid to *pid_file*, refusing if a live collector holds it.

    Raises ``SystemExit(1)`` if the file points to a live process that is
    not this process. Stale or malformed files are silently overwritten.
    Registers ``release_pid_file`` via ``atexit`` for cleanup on any
    normal exit.
    """
    my_pid = os.getpid()
    if pid_file.exists():
        raw = pid_file.read_text().strip()
        try:
            existing_pid = int(raw)
        except ValueError:
            existing_pid = None

        if existing_pid is not None and existing_pid != my_pid and _is_pid_alive(existing_pid):
            print(
                f"ERROR: collector already running as pid {existing_pid}. "
                f"Refusing to start a second instance. "
                f"If you are sure no collector is running, remove {pid_file} and retry."
            )
            raise SystemExit(1)

    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(str(my_pid))
    atexit.register(partial(release_pid_file, pid_file))
```

In `scripts/continuous_combined_collector.py`: delete `_is_pid_alive`, `_release_pid_file`, `_acquire_pid_file`; add `from src.pidfile import acquire_pid_file, release_pid_file`; replace the two call sites: `_acquire_pid_file()` → `acquire_pid_file(PID_FILE)` (in `main`), `_release_pid_file()` → `release_pid_file(PID_FILE)` (in the `finally` block). Delete the now-unused `atexit` import if ruff flags it.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_pidfile.py -v && uv run pytest -m smoke`
Expected: all PASS.

- [ ] **Step 5: Run gates and commit**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add src/pidfile.py scripts/continuous_combined_collector.py tests/test_pidfile.py
git commit -m "refactor: extract path-parameterized pid-file guard to src/pidfile.py"
```

---

### Task 6: scripts/sfmta_collector.py — the sidecar loop

**Files:**
- Create: `scripts/sfmta_collector.py`
- Test: `tests/test_sfmta_collector_tick.py`

**Interfaces:**
- Consumes: `load_agency_config`/`request_kwargs` (Task 1), parameterized `WMATADataCollector` (Task 3), `get_session(db_url=)`/`init_db(db_url=)` (Task 4), `acquire_pid_file`/`release_pid_file` (Task 5).
- Produces: the deployable entrypoint `uv run python scripts/sfmta_collector.py`. `run_one_tick(tick_idx, collector, db_url)` is module-level for testability.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_sfmta_collector_tick.py
"""Tick-schedule test: TU every 2nd tick, VP every 3rd tick (spec cadence)."""

from unittest.mock import MagicMock, patch

from scripts.sfmta_collector import run_one_tick


def _tick(tick_idx, collector):
    """Run one tick with the DB session factory patched out."""
    with patch("scripts.sfmta_collector.get_session", return_value=MagicMock()):
        run_one_tick(tick_idx, collector, db_url="sqlite:///:memory:")


def test_tick_schedule_matches_cadence_budget():
    """Over 6 ticks (one full cycle): 3 TU fetches, 2 VP fetches."""
    collector = MagicMock()
    collector.get_realtime_trip_updates.return_value = (None, [])
    collector.get_realtime_vehicle_positions.return_value = []
    for i in range(6):
        _tick(i, collector)
    assert collector.get_realtime_trip_updates.call_count == 3   # ticks 0,2,4
    assert collector.get_realtime_vehicle_positions.call_count == 2  # ticks 0,3
```

Implementer note: `scripts/` may not be an importable package. If `from scripts.sfmta_collector import ...` fails, follow whatever pattern existing tests use to import scripts (check `grep -rn "scripts\." tests/ | head`); if none exists, add `sys.path` handling consistent with repo conventions rather than inventing a new mechanism.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_sfmta_collector_tick.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the script**

```python
# scripts/sfmta_collector.py
"""SFMTA (Muni) sidecar collector — 511.org GTFS-RT TripUpdates + VehiclePositions.

Sibling of scripts/continuous_combined_collector.py, driven by
config/agencies/sfmta.yaml. Writes to the SFMTA_DATABASE_URL database
(``sfmta_dashboard``) — never the WMATA one. Cadence budget against
511.org's default 60 req/rolling-hour token cap:

    tick 60s; TripUpdates every 2nd tick (120s, 30/hr);
    VehiclePositions every 3rd tick (180s, 20/hr); total 50/hr.

Raw TU rows archive to archive/sfmta_raw_snapshots/ (UTC-date-bucketed
jsonl.zst, same writer as WMATA). Service-date fallback uses Pacific time
(spec 2026-07-21 §2). Run with:

    uv run python scripts/sfmta_collector.py
"""

import os
import signal
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from src.agency_config import load_agency_config, request_kwargs
from src.database import get_session, init_db
from src.pidfile import acquire_pid_file, release_pid_file
from src.wmata_collector import WMATADataCollector

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
CFG = load_agency_config("sfmta")
PID_FILE = REPO_ROOT / CFG.pid_file


def now_str() -> str:
    """Local-time stamp prefix used in console logs."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_collector() -> WMATADataCollector:
    """Construct the parameterized collector for 511.org/SFMTA.

    Reads the 511 token and healthcheck URL from the env vars named in
    config/agencies/sfmta.yaml; raises early if the token is missing.
    """
    api_key = os.getenv(CFG.api_key_env)
    if not api_key:
        raise ValueError(f"{CFG.api_key_env} not found in environment variables")
    auth = request_kwargs(CFG, api_key)
    return WMATADataCollector(
        api_key,
        archive_root=REPO_ROOT / CFG.archive_dir,
        healthcheck_url=os.getenv(CFG.healthcheck_url_env),
        tu_feed_url=CFG.trip_updates_url,
        vp_feed_url=CFG.vehicle_positions_url,
        request_params=auth.get("params"),
        service_date_tz=CFG.timezone,
        heartbeat_name=CFG.heartbeat_name,
    )


def run_one_tick(tick_idx: int, collector: WMATADataCollector, db_url: str) -> None:
    """Run one 60s tick: TU on even ticks, VP on every 3rd tick.

    Opens a fresh DB session per tick (same stale-connection defense as
    the WMATA loop). Each feed's errors are caught independently so one
    failing feed never starves the other.
    """
    db = get_session(db_url=db_url)
    try:
        collector.db = db

        if tick_idx % CFG.trip_updates_every_ticks == 0:
            try:
                _, rows = collector.get_realtime_trip_updates()
                saved = collector._save_trip_updates(rows) if rows else 0
                print(f"[{now_str()}] tick={tick_idx} trip_updates rows={saved}")
            except Exception as e:
                print(f"[{now_str()}] tick={tick_idx} trip_updates ERROR: {e}")

        if tick_idx % CFG.vehicle_positions_every_ticks == 0:
            try:
                vehicles = collector.get_realtime_vehicle_positions()
                if vehicles:
                    collector._save_vehicle_positions(vehicles)
                print(f"[{now_str()}] tick={tick_idx} vehicle_positions rows={len(vehicles)}")
            except Exception as e:
                print(f"[{now_str()}] tick={tick_idx} vehicle_positions ERROR: {e}")

    finally:
        db.close()


def main() -> None:
    """Run the SFMTA polling loop until interrupted."""
    # Force-install handlers regardless of inherited disposition (PR #129
    # lesson): a SIG_IGN-inheriting parent (systemd contexts, CI) would
    # otherwise make the process unkillable-gracefully and skip the
    # collector.close() zstd-footer flush.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    acquire_pid_file(PID_FILE)

    db_url = os.getenv(CFG.database_url_env)
    if not db_url:
        raise ValueError(f"{CFG.database_url_env} not found in environment variables")

    print(f"{CFG.display_name} Sidecar Collector")
    print("=" * 50)
    print(f"Trip updates:      every {CFG.tick_sec * CFG.trip_updates_every_ticks}s")
    print(f"Vehicle positions: every {CFG.tick_sec * CFG.vehicle_positions_every_ticks}s")
    print(f"Archive dir:       {CFG.archive_dir}")
    print(f"Pid file:          {PID_FILE}")
    print("Press Ctrl+C to stop")
    print("=" * 50)

    init_db(db_url=db_url)

    collector = build_collector()

    tick_idx = 0
    try:
        while True:
            start = time.monotonic()
            run_one_tick(tick_idx, collector, db_url)
            elapsed = time.monotonic() - start

            sleep_for = CFG.tick_sec - elapsed
            if sleep_for < 0:
                print(
                    f"[{now_str()}] tick={tick_idx} WARNING: tick took "
                    f"{elapsed:.1f}s (> {CFG.tick_sec}s budget)"
                )
            else:
                time.sleep(sleep_for)

            tick_idx += 1

    except KeyboardInterrupt:
        print("\n\nStopping SFMTA collection...")
    finally:
        collector.close()
        release_pid_file(PID_FILE)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_sfmta_collector_tick.py -v && uv run pytest -m smoke`
Expected: all PASS.

- [ ] **Step 5: One live smoke poll against 511 (requires a real token in .env)**

Run (stop with Ctrl+C after ~3 ticks): `SFMTA_DATABASE_URL=sqlite:////tmp/sfmta_smoke.db uv run python scripts/sfmta_collector.py`
Expected: startup banner; `trip_updates rows=N` with N in the hundreds–thousands; `vehicle_positions rows=M` with M in the hundreds; a new file under `archive/sfmta_raw_snapshots/`. If the user's 511 token isn't available yet, mark this step blocked and continue — Task 7's spike repeats this validation.

- [ ] **Step 6: Run gates and commit**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add scripts/sfmta_collector.py tests/test_sfmta_collector_tick.py
git commit -m "feat: SFMTA sidecar collector (511.org GTFS-RT, 120s/180s cadence)"
```

---

### Task 7: Day-1 validation spike — trip_id match rate

**Files:**
- Create: `scripts/sfmta_feed_spike.py`
- Test: `tests/test_sfmta_spike.py`

**Interfaces:**
- Consumes: `load_agency_config`, `request_kwargs` (Task 1).
- Produces: `trip_id_match_rate(rt_trip_ids: set, static_trip_ids: set) -> float` (pure function, tested) and a manual-run CLI that exits 0 when ≥ 80% of RT trip_ids appear in static GTFS `trips.txt`, else 1. This is the spec's go/no-go gate for the trip-matching fast path.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_sfmta_spike.py
"""Pure-function tests for the day-1 trip_id validation spike."""

from scripts.sfmta_feed_spike import trip_id_match_rate


def test_match_rate_full_overlap():
    assert trip_id_match_rate({"a", "b"}, {"a", "b", "c"}) == 1.0


def test_match_rate_partial():
    assert trip_id_match_rate({"a", "b", "x", "y"}, {"a", "b"}) == 0.5


def test_match_rate_empty_rt_is_zero():
    """No RT trips (outage / 3am) must read as 0.0, not a crash."""
    assert trip_id_match_rate(set(), {"a"}) == 0.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_sfmta_spike.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the spike script**

```python
# scripts/sfmta_feed_spike.py
"""Day-1 SFMTA feed validation: do RT trip_ids match static GTFS trip_ids?

The trip-matching fast path (src/trip_matching.py, ~90% of WMATA matches)
assumes GTFS-RT tripDescriptor.trip_id values exist in the static GTFS
trips.txt. This spike measures that overlap for Muni BEFORE any pipeline
work builds on it (spec 2026-07-21 §1: go/no-go gate, threshold 80%).

Manual one-shot (uses 3 of the 60/hr request budget):

    uv run python scripts/sfmta_feed_spike.py

Exit 0: match rate >= 80%. Exit 1: below threshold or fetch failure —
scope fallback matching before proceeding.
"""

import csv
import io
import os
import sys
import zipfile

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

from src.agency_config import load_agency_config, request_kwargs

load_dotenv()

MATCH_THRESHOLD = 0.80
STATIC_GTFS_URL = "https://api.511.org/transit/datafeeds"


def trip_id_match_rate(rt_trip_ids: set, static_trip_ids: set) -> float:
    """Fraction of RT trip_ids present in the static GTFS trip_id set.

    Returns 0.0 when there are no RT trips (empty feed reads as failure,
    not vacuous success).
    """
    if not rt_trip_ids:
        return 0.0
    return len(rt_trip_ids & static_trip_ids) / len(rt_trip_ids)


def fetch_rt_trip_ids(cfg, api_key) -> set:
    """Fetch one TripUpdates snapshot and return its distinct trip_ids."""
    kwargs = request_kwargs(cfg, api_key)
    resp = requests.get(cfg.trip_updates_url, timeout=30, **kwargs)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return {
        e.trip_update.trip.trip_id
        for e in feed.entity
        if e.HasField("trip_update") and e.trip_update.trip.trip_id
    }


def fetch_static_trip_ids(cfg, api_key) -> set:
    """Download the SF static GTFS zip from 511 and return trips.txt trip_ids."""
    kwargs = request_kwargs(cfg, api_key)
    kwargs.setdefault("params", {})
    kwargs["params"]["operator_id"] = cfg.extra_params.get("agency", "SF")
    resp = requests.get(STATIC_GTFS_URL, timeout=120, **kwargs)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open("trips.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            return {row["trip_id"] for row in reader if row.get("trip_id")}


def main() -> int:
    """Run the spike; print a verdict; return a process exit code."""
    cfg = load_agency_config("sfmta")
    api_key = os.getenv(cfg.api_key_env)
    if not api_key:
        print(f"ERROR: {cfg.api_key_env} not set")
        return 1

    rt_ids = fetch_rt_trip_ids(cfg, api_key)
    static_ids = fetch_static_trip_ids(cfg, api_key)
    rate = trip_id_match_rate(rt_ids, static_ids)

    print(f"RT trip_ids:      {len(rt_ids)}")
    print(f"Static trip_ids:  {len(static_ids)}")
    print(f"Match rate:       {rate:.1%}  (threshold {MATCH_THRESHOLD:.0%})")
    unmatched = sorted(rt_ids - static_ids)[:10]
    if unmatched:
        print(f"Sample unmatched RT trip_ids: {unmatched}")

    if rate >= MATCH_THRESHOLD:
        print("VERDICT: PASS — trip-matching fast path viable for Muni")
        return 0
    print("VERDICT: FAIL — scope fallback matching before pipeline work")
    return 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_sfmta_spike.py -v`
Expected: 3 passed.

- [ ] **Step 5: Live spike run (requires the 511 token; uses 2 requests + 1 static download)**

Run: `uv run python scripts/sfmta_feed_spike.py`
Expected: printed counts and a PASS/FAIL verdict. Record the actual match rate in the PR body. If FAIL, STOP after this plan (do not proceed to Plan 2) and report — that's the spike doing its job. If the token isn't available, mark blocked for the user to run.

- [ ] **Step 6: Run gates and commit**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add scripts/sfmta_feed_spike.py tests/test_sfmta_spike.py
git commit -m "feat: day-1 SFMTA feed spike — RT/static trip_id match-rate gate"
```

---

### Task 8: systemd unit, .env.example, deployment docs

**Files:**
- Create: `deployment/systemd/sfmta-collector.service`
- Modify: `.env.example` (append SFMTA block)
- Modify: `docs/DEPLOYMENT.md` (add an "SFMTA sidecar collector" subsection; follow the file's existing structure)

**Interfaces:**
- Consumes: Task 6's entrypoint.
- Produces: deployable unit + documented manual VM steps. No code.

- [ ] **Step 1: Write the unit file** (mirror `deployment/systemd/wmata-collector.service` exactly — same User/Group/WorkingDirectory/EnvironmentFile/venv-python conventions and restart policy; copy that file and change only Description, ExecStart, and SyslogIdentifier if present):

```ini
# deployment/systemd/sfmta-collector.service
[Unit]
Description=SFMTA (Muni) Sidecar Data Collector
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=simple
User=wmata
Group=wmata
WorkingDirectory=/home/wmata/wmata-dashboard
Environment="PATH=/home/wmata/.local/bin:/usr/local/bin:/usr/bin:/bin"
Environment="PYTHONUNBUFFERED=1"
EnvironmentFile=/home/wmata/wmata-dashboard/.env

# Invoke the venv interpreter directly (not `uv run`) — same rationale as
# wmata-collector.service.
ExecStart=/home/wmata/wmata-dashboard/.venv/bin/python3 scripts/sfmta_collector.py
```

Then append the `[Service]` restart-policy lines and `[Install]` section copied verbatim from `wmata-collector.service` (read that file — the plan intentionally doesn't guess its restart stanza). Confirm whether `wmata-collector.service` sets `PYTHONUNBUFFERED`; keep the sfmta unit's explicit line regardless (spec §1 ops-hygiene requirement).

- [ ] **Step 2: Append to `.env.example`**

```bash
# --- SFMTA (Muni) sidecar collector (spec 2026-07-21) ---
# Free token: https://511.org/open-data/token — default cap 60 req/hr.
SFMTA_API_KEY=your_511_org_token_here
# Separate database — never point this at the WMATA DB.
SFMTA_DATABASE_URL=postgresql://wmata:password@localhost:5432/sfmta_dashboard
# Optional healthchecks.io dead-man ping (its own check, not WMATA's).
SFMTA_COLLECTOR_HEALTHCHECK_URL=
```

- [ ] **Step 3: Add the DEPLOYMENT.md subsection** — cover, in the file's existing style: create the database (`sudo -u postgres createdb -O wmata sfmta_dashboard`); add the three env vars to the VM's `.env`; install/enable the unit **by reference to §2 of the same doc** (do not duplicate the cp/daemon-reload/restart commands); verify with `journalctl -u sfmta-collector -n 20` and `sudo -u wmata psql -d sfmta_dashboard -c "SELECT max(ts) FROM collector_heartbeats"`; note the healthchecks.io check creation and the 511 rate-increase email as operator steps.

- [ ] **Step 4: Run gates and commit**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add deployment/systemd/sfmta-collector.service .env.example docs/DEPLOYMENT.md
git commit -m "feat: sfmta-collector systemd unit + env template + deploy docs"
```

- [ ] **Step 5: Open the PR**

PR: `feature/sfmta-sidecar-collector` → `main`. Body must include: link to the spec; the spike's measured match rate (Task 7); the deploy section **citing docs/DEPLOYMENT.md §2** for unit installation; the manual operator checklist (511 token, createdb, .env, healthchecks.io check, rate-increase email). Do not run VM deployment from the implementation session — the user executes VM steps via one-shot ssh.

---

## Post-merge (user-executed, not part of the PR)

1. Register 511 token → laptop + VM `.env`.
2. Run `scripts/sfmta_feed_spike.py` from the laptop if Task 7's live run was blocked.
3. VM: createdb, env vars, install unit per DEPLOYMENT.md §2, start, verify heartbeats.
4. Create the healthchecks.io check; set `SFMTA_COLLECTOR_HEALTHCHECK_URL`; restart unit.
5. Send the 511 rate-increase email.
6. Note the collection start date — the matched comparison window begins after teething.
