# CLAUDE.md

WMATA bus-performance profile for local transit advocacy, with peer
agencies (SFMTA/Muni first) as shallow comparison points. FastAPI +
Postgres backend, React/Vite frontend. **The laptop's local PostgreSQL
16 (`wmata_dashboard`) is the system of record**; a stateless nano
collector polls WMATA + SFMTA GTFS-RT and uploads raw JSONL to S3 (no
database on the collector box). API + frontend run locally against the
local DB. Freshness is on demand: run `bin/pull-and-derive.sh` by hand
— one service date at a time it fetches just that date's raw files from
S3, loads VP, replays, derives, and deletes the local raw files once
verified in S3 (`pipelines/refresh_by_day.py`; S3 is the raw store — the
laptop keeps only the next couple of days' raw files plus anything the
prune refused; no tunnel or VM access needed).
The nightly launchd wrapper/plist from PR #259 (`deployment/launchd/`,
`docs/DEPLOYMENT.md` §12) still exist but are not deployed and not the
intended mode (#260 closed as not planned). Topology/ops detail: `docs/DEPLOYMENT.md`.
Punch list: GitHub Issues (`gh issue list`; labels `track:*`, `sev:*`,
`effort:*`, `blocked`).
Historical `NOTES-N` numbers in old PRs and docs refer to the retired
`NOTES.md` file; migrated issues say "Migrated from NOTES-N" in their
body, so `gh issue list --state all --search NOTES-N` finds them.

## North star

A WMATA bus-performance profile for local transit advocacy (set
2026-11-24). The main output is periodic, defensible, citable findings
— generated reports/scorecards that state their method. The dashboard
is the owner's exploration tool, not a hosted product (hosted deploy
#254/#255 closed as not planned).

- **WMATA is the only deep agency**: stop-level detail, diagnostics,
  segments.
- **Peer agencies are shallow comparison points** (SFMTA/Muni first,
  maybe more cities later): the product compares them on headline KPIs
  computed identically, with honest comparability caveats. Today SFMTA
  runs the same pipeline and tables as WMATA; trimming peers to
  aggregates is part of the storage direction below.
- **Storage direction (not yet implemented)**: raw data in S3 as the
  source of truth, stop-level tables kept for a rolling window (peers
  possibly not at all), daily aggregates kept forever for every agency,
  old periods re-derived from S3 on demand.
  Don't describe this as current behaviour.

Every open issue either serves this or is explicitly parked
(`track:deferred`).

## Commands

```bash
uv sync --extra dev                          # install (psycopg2 is core; --extra viz for matplotlib scripts)
uv run uvicorn api.main:app --reload         # API on :8000
cd frontend && npm run dev                   # frontend on :5173
bin/pull-and-derive.sh                       # freshness: per date fetch raw from S3 + load VP + replay + derive + prune local raw (--start/--end, --date, --dry-run)
uv run python scripts/prune_local_archive.py # dry run: which local raw files are S3-verified + processed (--apply deletes)
uv run python pipelines/run_daily_batch.py   # derive + aggregate + rollup (--agency sfmta for Muni)
psql -d wmata_dashboard                      # ad-hoc queries (Muni sidecar DB: sfmta_dashboard)
uv run pytest -m smoke                       # fast tests
bin/test-with-pg                             # full suite vs Postgres (mirrors CI); points DATABASE_URL/PG_TEST_DATABASE_URL/SFMTA_DATABASE_URL at a scratch DB for you
# any pytest run whose selection isn't entirely smoke-marked refuses to
# proceed against the production DB (wmata_dashboard/sfmta_dashboard);
# use bin/test-with-pg, or set DATABASE_URL, PG_TEST_DATABASE_URL, AND
# SFMTA_DATABASE_URL to a scratch DB yourself, or override with
# PYTEST_ALLOW_PROD_DB=1 (issue #247)
uv run ruff check src/ scripts/ api/ pipelines/ tests/           # lint — CI gate
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/  # format — separate CI gate
cd frontend && npm run lint && npm test && npm run build         # frontend gates (CI runs lint + test, not build)
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
  `is_current=True` — except per-service-date pipelines, which take
  `gtfs_version_filter(model, gtfs_snapshot_id)`; `run_daily_batch.py`
  passes the snapshot `src/gtfs_versioning.py:resolve_gtfs_snapshot_for_date`
  picks for each date (issue #258). Refresh only via
  `scripts/reload_gtfs_complete.py` (single transaction,
  all-or-nothing rollback; archives the zip to S3 `gtfs-archive/` first
  and skips unchanged feeds).
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
- Playwright visual regression (the `visual regression` pixel
  snapshots) is advisory, not a merge gate: a mismatch leaves CI green
  and uploads the diffs as the `playwright-visual-diffs` artifact.
  Playwright functional e2e, frontend lint and unit tests stay
  blocking. Baselines are platform-specific (darwin + linux); after a
  deliberate change to a baselined page (Overview / RouteList /
  RouteDetail-D72 / Segments), regenerate both sets when convenient
  (commands in `frontend/README.md`, user-run) — not required to merge.
- A PR that closes an issue puts `Closes #N` in its body, and the
  body explains *why* the change was scoped that way — the PR is the
  durable record of the decision, the issue of the problem. Reference
  issues as `#N` / `issue #N`, never `NOTES-N`.
- Project Claude tooling: auto-triggering skills in
  `.claude/skills/<name>/SKILL.md` (`file-issue`), explicit slash
  commands in `.claude/commands/<name>.md` (`/issue-cycle`,
  `/issue-batch`, `/collector-status`), review agent in
  `.claude/agents/pr-reviewer.md` — all checked in.
