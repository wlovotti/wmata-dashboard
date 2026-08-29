# CLAUDE.md

WMATA vs SFMTA (Muni) bus-performance dashboard. FastAPI + Postgres
backend, React/Vite frontend. **The laptop's local PostgreSQL 16
(`wmata_dashboard`) is the system of record**; a stateless nano
collector polls WMATA + SFMTA GTFS-RT and uploads raw JSONL to S3 (no
database on the collector box). API + frontend run locally against the
local DB. Freshness: run `bin/pull-and-derive.sh` (syncs the S3
archives, loads + replays, derives — no tunnel or VM access needed).
Topology/ops detail: `docs/DEPLOYMENT.md`. Punch list: `NOTES.md`
(index; item bodies in `notes/NOTES-N.md`).

## Commands

```bash
uv sync --extra dev                          # install (--extra viz --extra postgres for matplotlib/psycopg2 scripts)
uv run uvicorn api.main:app --reload         # API on :8000
cd frontend && npm run dev                   # frontend on :5173
bin/pull-and-derive.sh                       # freshness: sync S3 raw archives + load + replay + derive locally
uv run python pipelines/run_daily_batch.py   # derive + aggregate + rollup (--agency sfmta for Muni)
psql -d wmata_dashboard                      # ad-hoc queries (Muni sidecar DB: sfmta_dashboard)
uv run pytest -m smoke                       # fast tests
bin/test-with-pg                             # full suite vs Postgres (mirrors CI)
uv run ruff check src/ scripts/ api/ pipelines/ tests/           # lint — CI gate
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/  # format — separate CI gate
cd frontend && npm run lint && npm test && npm run build         # frontend gates (CI runs all three)
```

## Architecture standards

- **PostgreSQL 16 everywhere** (laptop system-of-record, CI, dev).
  `src/database.py` requires `DATABASE_URL`; no SQLite fallback
  (tests monkeypatch SQLite in-memory; use the `pg_session` fixture
  for pg-specific SQL). Per-agency DBs resolve via
  `src/agency_config.py` (`SFMTA_DATABASE_URL`).
  `bin/refresh-dev-db.sh` restores *scratch* copies; it refuses to
  touch the primary without `--clobber-primary`.
- **`stop_events` + `runs` are the foundation** — all per-route
  metrics (OTP, service-delivered, EWT, bunching, excess-trip-time)
  derive from them via `pipelines/run_daily_batch.py`; system rollups
  land in `system_metrics_daily`.
- **GTFS is versioned via `is_current`** — every query on
  routes/stops/trips/stop_times/calendar/calendar_dates must filter
  `is_current=True`. Refresh only via
  `scripts/reload_gtfs_complete.py` (single transaction,
  all-or-nothing rollback).
- **Datetime storage is naive UTC**; service-date semantics ("today",
  "last 7 days") are Eastern — use `src/timezones.py`. Never
  `datetime.now()` for date math, never `datetime.fromtimestamp()`
  for storage.
- **GTFS-based OTP is primary** — WMATA's `BusPositions` deviation
  field was validated unreliable; never use it for OTP.
- **Pipeline upserts go through
  `src/upsert_helpers.py:upsert_rows`** — don't hand-roll
  `pg_insert(...).on_conflict_do_update(...)` in new pipelines.
- **Migrations on the system of record follow `docs/MIGRATIONS.md`**:
  backup first, test on a restored copy, wrap in a transaction,
  `--dry-run` when available.

## Domain gotchas

- **`stop_id` is not direction-unique** — aggregate per-route stop
  metrics by `(route_id, direction_id, stop_id)`; termini and shared
  hubs silently double-count otherwise.
- **GTFS times are unpadded strings** (`9:06:00`; hours may be ≥ 24
  past midnight) — never string-min/max them; parse to seconds
  (`src/service_profile.py:_parse_gtfs_time_to_seconds`) or
  `LPAD(..., 8, '0')` before sorting.
- **`stop_events.source` is dual** — pick `proximity` for OTP and
  per-stop spatial analysis, `trip_update` for headways/EWT/bunching;
  mixing sources double-counts. `trip_update` cannot observe trip
  origins.
- **Two notions of "frequent"** — route-level WMATA designation is
  `config/frequent_routes.yaml` via `src/frequent_routes.py` (drives
  the EWT-vs-OTP headline choice). `src/ewt.py:
  FREQUENT_HEADWAY_MAX_SEC` is internal to EWT computation only —
  not a route-level check.
- **`timepoints` uses GTFS-Plus internal stop_ids** — map to public
  stops by lat/lon join (~50 m); direct stop_id joins silently return
  zero rows.
- Trip matching uses GTFS-RT `trip_id` directly (~90% fast path);
  position/time matching is fallback only (`src/trip_matching.py`).
- ~40% of arrivals are early, and a low overall vehicle↔stop match
  rate is healthy — real operational patterns, not data errors.
- WMATA API budget: 10 calls/sec, 50k/day (60 s polling ≈ 1,440/day).
- `session.execute(text(...))` does NOT autoflush — call
  `session.flush()` before raw SQL that depends on pending ORM rows.
- Long-running pipeline stdout is buffered when redirected — use
  `PYTHONUNBUFFERED=1` / `python -u` for live logs.
- `VACUUM` doesn't return disk to the OS; `VACUUM FULL` does (takes
  ACCESS EXCLUSIVE — pause writers first).

## Working agreements

- Before pushing, run `ruff check`, `ruff format --check`, and
  `cd frontend && npm run lint` — all separate CI gates (frontend
  unit tests run in CI too).
- Playwright visual regression is a blocking CI gate with
  platform-specific baselines: changing a baselined page (Overview /
  RouteList / RouteDetail-D72) requires regenerating BOTH the darwin
  and linux sets (commands in `frontend/README.md`); regen is
  user-run.
- Project Claude tooling: auto-triggering skills in
  `.claude/skills/<name>/SKILL.md`, explicit slash commands in
  `.claude/commands/<name>.md` — both checked in.
