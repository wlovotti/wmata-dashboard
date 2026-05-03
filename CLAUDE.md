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
  must filter `is_current=True`. To refresh GTFS data, run
  `scripts/reload_gtfs_complete.py` — it correctly invalidates the prior
  snapshot before inserting new rows. `scripts/init_database.py` is
  first-time setup only and refuses to run if snapshots already exist.

- **GTFS-based OTP is primary.** WMATA's `BusPositions` deviation field was
  validated and found unreliable (up to 7.7 min discrepancies). Don't use
  it for OTP.

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
