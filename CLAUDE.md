# CLAUDE.md

WMATA bus/rail performance dashboard. FastAPI + Postgres backend, React/Vite
frontend. Feature-complete but not deployed. The user has stepped back from
active development — current work is mostly cleanup. Don't add features
without asking. See `NOTES.md` for the active punch list.

## Load-bearing constraints

- **PostgreSQL only.** `src/database.py` requires `DATABASE_URL`; there is
  no SQLite fallback. The `os.getenv("DATABASE_URL", "sqlite:///...")`
  defaults you'll see in `src/analytics.py` and `pipelines/compute_daily_metrics.py`
  are dead — the app crashes earlier without `DATABASE_URL`.

- **Pre-computed aggregations are the architectural rule.** API endpoints
  read from `route_metrics_summary` / `route_metrics_daily`, populated by
  `pipelines/compute_daily_metrics.py`. Never compute metrics live in an API
  handler — that's the difference between 37 ms and 30 s. If a feature
  needs an expensive calculation, do it offline.

- **GTFS schedule is versioned via `is_current`.** All queries against
  `routes`, `stops`, `trips`, `stop_times`, `calendar`, `calendar_dates`
  must filter `is_current=True`. `scripts/reload_gtfs_complete.py`
  *partially* refreshes GTFS — it correctly versions
  routes/stops/trips/stop_times/calendar via UPDATE, but for
  agencies/feed_info/timepoints/timepoint_times/route_service_profile it
  attempts a plain DELETE that crashes on FK violations on a populated
  DB (NOTES.md NOTES-22). Per-table commits make a partial run durable,
  so half-migrated state is the norm. `scripts/init_database.py` is
  first-time setup only and refuses to run if snapshots already exist.

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

## Commands

```bash
uv sync --extra dev                                       # install
uv run uvicorn api.main:app --reload                      # API on :8000
cd frontend && npm run dev                                # frontend on :5173
uv run python scripts/continuous_collector.py             # 60 s collector
uv run python pipelines/compute_daily_metrics.py --recalculate              # batch (last 7 days)
uv run python pipelines/compute_daily_metrics.py --route C51 --date 2025-10-21 --recalculate   # backfill one route/day
uv run pytest -m smoke                                    # fast tests
uv run ruff check src/ scripts/ api/ pipelines/           # lint (CI requires)
```

## Working agreements

- Run `ruff check` before committing — CI will fail otherwise.
- The user is not in build-more mode. For ambiguous requests, prefer
  cleanup / verification / "delete unused code" over new features. Ask
  before adding.
