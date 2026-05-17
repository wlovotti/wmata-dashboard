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

- **Tests use SQLite in-memory** (`tests/conftest.py`), but production code
  is Postgres-only. Don't conflate the two when reasoning about queries.

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

## Commands

```bash
uv sync --extra dev                                       # install
uv sync --extra viz --extra postgres                      # add for matplotlib / psycopg2-using scripts
uv run uvicorn api.main:app --reload                      # API on :8000
cd frontend && npm run dev                                # frontend on :5173
cd frontend && npm run lint && npm run build              # frontend verification (no tests yet)
uv run python scripts/continuous_collector.py             # 60 s collector
uv run python pipelines/run_daily_batch.py                # nightly batch (derive + aggregate + system rollup)
psql -d wmata_dashboard                                   # ad-hoc DB queries
uv run pytest -m smoke                                    # fast tests
uv run ruff check src/ scripts/ api/ pipelines/ tests/    # lint (CI requires)
```

## Working agreements

- Run `ruff check` before committing — CI will fail otherwise.
- Frontend lint has pre-existing errors on main that don't fail CI; if
  yours overlap, `git stash; cd frontend && npm run lint; cd ..;
  git stash pop` confirms which ones you actually introduced.
- Project Claude tooling: auto-triggering skills go in
  `.claude/skills/<name>/SKILL.md`, explicit slash commands go in
  `.claude/commands/<name>.md`. Both are checked in.
- The user is not in build-more mode. For ambiguous requests, prefer
  cleanup / verification / "delete unused code" over new features. Ask
  before adding.
