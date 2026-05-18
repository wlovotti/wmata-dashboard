# CLAUDE.md

WMATA bus/rail performance dashboard. FastAPI + Postgres backend, React/Vite
frontend. Feature-complete but not deployed. The user has stepped back from
active development — current work is mostly cleanup. Don't add features
without asking. See `NOTES.md` for the active punch list.

## Load-bearing constraints

- **PostgreSQL only.** `src/database.py` requires `DATABASE_URL`; there is
  no SQLite fallback in production. Tests run on SQLite in-memory and set
  `DATABASE_URL=sqlite:///:memory:` via `tests/conftest.py` monkeypatch.

- **Stop_events / runs are the architectural foundation.** Per-route
  metrics (OTP, service-delivered, EWT, bunching, excess-trip-time) are
  derived from `stop_events` and `runs`, populated by the per-date
  pipelines orchestrated via `pipelines/run_daily_batch.py`. The legacy
  daily-batch pipeline (`compute_daily_metrics.py`) and its
  materialization tables (`route_metrics_daily`, `route_metrics_summary`)
  were retired in NOTES-19. System-level rollups land in
  `system_metrics_daily` via `pipelines/upsert_system_metrics_daily.py`.

- **GTFS schedule is versioned via `is_current`.** All queries against
  `routes`, `stops`, `trips`, `stop_times`, `calendar`, `calendar_dates`
  must filter `is_current=True`. `scripts/reload_gtfs_complete.py`
  refreshes GTFS in a single transaction: versioned tables get UPDATE
  (mark old `is_current=False`) + INSERT new; `agencies` is upserted by
  `agency_id` so the FK target stays stable; the remaining unversioned
  tables (feed_info / timepoints / timepoint_times / route_service_profile)
  are truncated and reinserted. Any failure rolls back to the prior
  snapshot — there is no partial-migration state. `scripts/init_database.py`
  is first-time setup only and refuses to run if snapshots already exist.

- **GTFS-based OTP is primary.** WMATA's `BusPositions` deviation field was
  validated and found unreliable (up to 7.7 min discrepancies). Don't use
  it for OTP.

- **Datetime storage is naive UTC.** Every `DateTime` column in the DB
  holds UTC. Service-date semantics ("today", "last 7 days") are an
  Eastern question — use `src/timezones.py` (`eastern_today`,
  `eastern_day_bounds_utc`, `to_eastern_sql`). Never call `datetime.now()`
  for date math; never call `datetime.fromtimestamp()` for storage. The
  only legitimate uses of naive local time are stdout log prefixes.

## Non-obvious gotchas

- **Trip matching uses GTFS-RT `trip_id` directly** (`src/trip_matching.py`,
  ~90% fast path). Position/time-based matching is the fallback only.

- **~40% of arrivals are early.** Real operational pattern, not a data
  error. Strict OTP windows will look harsh.

- **22.75% vehicle/stop match rate is healthy.** Buses spend 75-80% of
  their time between stops; top routes hit 45-50%.

- **WMATA API limits: 10 calls/sec, 50k/day.** 60 s polling = 1,440/day.
  Don't propose more aggressive polling without checking the budget.

- **Tests run on two engines** (`tests/conftest.py`): default `db_session`
  fixture is SQLite in-memory; `pg_session` fixture is real Postgres for
  integration tests that need pg-specific SQL (`pg_insert` / ON CONFLICT /
  `IS NOT DISTINCT FROM`). CI sets `PG_TEST_DATABASE_URL`; locally
  `pg_session` defaults to `postgresql:///wmata_dashboard`. SAVEPOINT
  semantics keep each test's writes rolled back from the dev DB.

- **`session.execute(text(...))` does NOT autoflush.** Autoflush fires for
  ORM `Select(<Model>)` but not arbitrary `text()` queries. When mixing
  `session.add(obj)` with raw SQL that depends on the pending row, call
  `session.flush()` explicitly — otherwise the raw SQL runs against
  server-side state without the new row.

- **`stop_id` is not direction-unique.** Most WMATA stops are split by
  direction (NB stop and SB stop are different `stop_id`s on opposite
  sides of a street), but **termini, layover bays, and some hubs serve
  both directions under one `stop_id`**. Any per-route, per-stop
  aggregation must group by `(route_id, direction_id, stop_id)` — never
  `(route_id, stop_id)` alone — or it silently double-counts at shared
  stops and produces metrics that look ~2x too tight. For "reference
  stop" selection, restrict to stops where
  `COUNT(DISTINCT direction_id) = 1` for trips on the route.

- **GTFS times are unpadded strings.** WMATA stores `arrival_time` as
  `9:06:00` (no leading zero on the hour), so SQL `MIN(arrival_time)`
  does the wrong thing — `"10:00:07" < "9:58:27"` lexicographically.
  Don't string-min/max GTFS times. Parse to integer seconds in
  application code (`src/service_profile.py:_parse_gtfs_time_to_seconds`),
  or `LPAD(arrival_time, 8, '0')` before sorting. Also: hours can be
  `≥ 24` for service that extends past midnight on the same service day.

- **`timepoints` table uses GTFS-Plus internal `stop_id`s** that do
  NOT match public GTFS `stops.stop_id`. To map a timepoint to a
  public stop you must lat/lon-join (50m haversine usually suffices;
  `timepoint_times` uses the same internal IDs). Direct stop_id joins
  silently return zero matches.

- **Two notions of "frequent" — keep them straight.** Route-level
  WMATA designation lives in `config/frequent_routes.yaml`, loaded
  via `src/frequent_routes.py:load_frequent_route_ids()`; pulled
  from WMATA's High-Frequency Metrobus Service Maps (Better Bus,
  June 2025). Drives headline-KPI choice on the UI (EWT vs OTP) and
  any "frequent route" filter in API / analysis code. The
  per-cell-hour data-driven gate `src/ewt.py:FREQUENT_HEADWAY_MAX_SEC
  = 15 min` is internal to EWT computation only — don't use it as a
  route-level "is this a frequent route?" check. The historical
  illustrative list in `src/otp_constants.py`'s docstring
  (70, 79, X2, 90, 92, 16Y, Metroway) is preserved as pre-Better-Bus
  context only — not authoritative.

- **`stop_events.source` is dual ('proximity' | 'trip_update')** with
  nearly inverse blind spots at trip endpoints (see StopEvent / Run
  docstrings). Pick `proximity` for OTP and per-stop spatial analysis;
  pick `trip_update` for headways / EWT / bunching. Mixing or
  wrong-source picks silently double-count or miss data.

- **Pipeline upserts go through `src/upsert_helpers.py:upsert_rows`**
  (`upsert_rows(db, model, rows, constraint_name, update_cols)`). All
  four per-route pipelines (`derive_stop_events*`, `aggregate_runs`,
  `compute_bunching`) use it — don't hand-roll
  `pg_insert(...).on_conflict_do_update(...)` in a new pipeline.
  Postgres-only by construction; tests still pass on SQLite because
  the helper is invoked only inside pipeline code that the smoke
  suite doesn't exercise against SQLite.

- **`VACUUM` marks pages reusable but does NOT shrink the file.**
  `VACUUM FULL` is what returns space to the OS. After bulk DELETEs,
  regular VACUUM can take ~90+ min on a multi-GB table because index
  cleanup buffers (`maintenance_work_mem`) fill repeatedly. VACUUM FULL
  on the post-DELETE table is faster AND reclaims to the filesystem;
  it takes ACCESS EXCLUSIVE so pause writers (`SIGINT` the collector)
  first.

- **Long-running pipeline stdout is buffered when redirected.** Run via
  `PYTHONUNBUFFERED=1 uv run python ...` or `python -u` so per-batch
  `print()` lines hit the log file in real time. Without that flag, the
  buffer flushes only at process exit — `tail -F | grep` monitors on the
  log will fire all at once at the end. Monitor real artifacts (file
  appearance, DB row counts) when running unbuffered isn't possible.

## Commands

```bash
uv sync --extra dev                                       # install
uv sync --extra viz --extra postgres                      # add for matplotlib / psycopg2-using scripts
uv run uvicorn api.main:app --reload                      # API on :8000
cd frontend && npm run dev                                # frontend on :5173
cd frontend && npm run lint && npm run build              # frontend verification (no tests yet)
uv run python scripts/continuous_combined_collector.py    # combined trip-update + position collector (30s/60s)
uv run python scripts/collector_status.py                 # one-shot collector health check
uv run python pipelines/run_daily_batch.py                # nightly batch (derive + aggregate + system rollup)
psql -d wmata_dashboard                                   # ad-hoc DB queries
uv run pytest -m smoke                                    # fast tests
uv run ruff check src/ scripts/ api/ pipelines/ tests/    # lint (CI requires)
```

## Working agreements

- Run `ruff check` before committing — CI will fail otherwise.
- Frontend lint is enforced in CI as of PR #126 (kept at zero errors).
  Run `cd frontend && npm run lint` before pushing.
- Frontend unit tests run in CI as of PR #126: `cd frontend && npm test`
  (Vitest). Playwright visual regression is a blocking CI gate as of
  PR #127 — baselines are platform-specific (`*-chromium-linux.png` for
  CI, `*-chromium-darwin.png` for local macOS). When you change a
  baselined page (Overview / RouteList / RouteDetail-D72), regenerate
  BOTH sets or CI will fail on stale Linux PNGs:
  - macOS (local): `cd frontend && npx playwright test --update-snapshots`
  - Linux (Docker): `cd frontend && docker run --rm -v "$(pwd):/work"
    -v /work/node_modules -w /work mcr.microsoft.com/playwright:v1.60.0-noble
    bash -c "npm ci --silent && npx playwright test --update-snapshots"`
  See `frontend/README.md` for full details.
- Project Claude tooling: auto-triggering skills go in
  `.claude/skills/<name>/SKILL.md`, explicit slash commands go in
  `.claude/commands/<name>.md`. Both are checked in.
- The user is not in build-more mode. For ambiguous requests, prefer
  cleanup / verification / "delete unused code" over new features. Ask
  before adding.
