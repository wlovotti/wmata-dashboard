# Code Review Notes

Findings from a code review on 2026-05-02. Each item was verified against the
code and database directly, not inferred from CLAUDE.md.

---

## 1. Duplicate GTFS snapshots — RESOLVED 2026-05-02

**Status: fixed.** Snapshot 2 invalidated and deleted, `VACUUM FULL`
reclaimed ~1.2 GB (DB went from 3.4 GB → 2.1 GB). Pipeline regression test
on D6X 2025-10-21 produced byte-identical metrics to the pre-cleanup
baseline. `scripts/init_database.py` now refuses to run if `gtfs_snapshots`
already has rows; refreshes must use `scripts/reload_gtfs_complete.py`.
Pre-cleanup pg_dump archived at `/tmp/wmata_dashboard_pre_cleanup.dump`.

Original findings preserved below for reference.

---

**Severity: medium. Existing precomputed metrics are likely correct, but the
DB is in a fragile state — any pipeline rerun or any live API endpoint that
joins through `is_current` is operating on doubled rows.**

### Evidence

`gtfs_snapshots` shows the failure pattern:

```
id=1, 2025-10-24 01:23  → 0 rows         (init_database.py crashed mid-load)
id=2, 2025-10-24 01:24  → 5,518,738 rows (re-run, full load — clean)
id=3, 2025-10-28 02:49  → 0 rows         (init_database.py crashed again)
id=4, 2025-10-28 02:54  → 5,518,738 rows (re-run — but stacked on snapshot 2)
```

All affected tables (`routes`, `stops`, `trips`, `stop_times`, `calendar`,
`calendar_dates`) have `is_current=true` for both snapshot_id=2 and
snapshot_id=4. Zero rows are marked `is_current=false` anywhere. GROUP BY on
`(trip_id, stop_id, stop_sequence)` filtering `is_current=true` returns 2
rows for every tuple — confirmed duplication, not just snapshot history.

### Root cause

`scripts/init_database.py:106-119` creates a new `GTFSSnapshot` and inserts
new versioned rows on every invocation, but **never invalidates prior
snapshots**. Compare to `scripts/reload_gtfs_complete.py:144-160`, which
correctly does:

```python
db.query(Route).filter(Route.is_current).update(
    {"valid_to": now, "is_current": False}, synchronize_session=False
)
# ... same for Stop, Trip, StopTime, Calendar, CalendarDate
```

`init_database.py` has no such block. The script header says "you only need
to run this once" but nothing enforces or warns. It was run twice (Oct 24
and Oct 28), each time crashing mid-load and being retried — the second
retry stacked snapshot 4 on top of snapshot 2 without flipping
`is_current` on the prior data.

### Actual impact (more limited than it first looked)

- **Existing precomputed metrics are likely correct.**
  `route_metrics_summary.computed_at = 2025-10-28 02:35`, which is *after*
  snapshot 2 (clean) but *before* snapshot 4 was created at 02:54. So the
  pipeline ran against unduplicated data. Sample values are in the expected
  range (e.g. D6X: 3,112 observations / 68 vehicles / 7 days), not 2×.
- **Re-running `compute_daily_metrics.py` now will corrupt all metrics.**
  Do not run the pipeline until this is cleaned up.
- **Live API endpoints that join through `is_current` are returning doubled
  data right now.** This includes `/api/routes/{id}/time-periods`,
  `/api/routes/{id}/shapes`, and any per-stop OTP queries. The scorecard
  endpoint `/api/routes` happens to dedup accidentally because
  `api/aggregations.py:81` builds a dict keyed by `route_id` — that's a
  lucky save, not deliberate.
- **Data collection is unaffected.** `vehicle_positions` has no snapshot
  system; the collector writes independently of GTFS state.
- **Storage cost: ~1.2 GB extra in stop_times and proportional bloat
  elsewhere.**

### Fix

1. Pick the snapshot to keep (most recent, snapshot_id=4).
2. Flip `is_current=false` for prior snapshot rows:
   ```sql
   UPDATE routes         SET is_current=false, valid_to=NOW() WHERE snapshot_id=2;
   UPDATE stops          SET is_current=false, valid_to=NOW() WHERE snapshot_id=2;
   UPDATE trips          SET is_current=false, valid_to=NOW() WHERE snapshot_id=2;
   UPDATE stop_times     SET is_current=false, valid_to=NOW() WHERE snapshot_id=2;
   UPDATE calendar       SET is_current=false, valid_to=NOW() WHERE snapshot_id=2;
   UPDATE calendar_dates SET is_current=false, valid_to=NOW() WHERE snapshot_id=2;
   ```
3. Optionally hard-delete the historical rows and `VACUUM FULL` to reclaim
   the ~1.2 GB. The data is byte-identical, so there's no archive value.
4. Fix `scripts/init_database.py` so it cannot silently double-load.
   Simplest: refuse to run if `gtfs_snapshots` already has rows. The script
   is meant for first-time setup; refreshes should use
   `reload_gtfs_complete.py`.
5. After cleanup, recompute `route_metrics_daily` and
   `route_metrics_summary` once against the deduplicated data — both as a
   correctness check and to make sure the precomputed values still match.

---

## 2. BusPosition code is dead in production — RESOLVED 2026-05-02

**Status: fixed.** All BusPosition code removed (model, collector methods,
analytics function, four `debug/` scripts, `docs/SESSION_SUMMARY.md`); docs
updated; `bus_positions` table dropped (was 0 rows, 96 kB). PR #23.

Original findings preserved below for reference.

---

**Severity: low (cleanup, no functional impact)**

### Evidence

Verified callers — none outside `debug/`:

- `src/wmata_collector.py:505` `get_bus_positions()` — no production caller
- `src/wmata_collector.py:555` `_save_bus_positions()` — no production caller
- `src/analytics.py:2089` `calculate_otp_from_bus_positions()` — only called
  from `debug/test_otp_bus_positions.py`
- `src/models.py:392` `BusPosition` table — only imported by the three
  dead functions above

CLAUDE.md notes the deviation field was found unreliable, which matches the
fact that nothing in production uses it.

### Fix

Delete:
- `BusPosition` class in `src/models.py`
- `get_bus_positions()` and `_save_bus_positions()` in `src/wmata_collector.py`
- `calculate_otp_from_bus_positions()` in `src/analytics.py`
- The four `debug/` files that depend on these
- Drop the `bus_positions` table

Total: ~250 lines + one table.

---

## 3. Speed segments endpoint unreachable from the UI — RESOLVED 2026-05-02

**Status: fixed (deleted).** No product reason to expose the feature, so
the endpoint and its supporting code were removed rather than wired into
the UI: `/api/routes/{id}/segments` route and `get_route_speed_segments()`
deleted from `api/main.py` / `api/aggregations.py`; `showSpeedSegments`
prop and segment fetch/render branches removed from `RouteMap.jsx`;
related tests and docs cleaned up.

Original findings preserved below for reference.

---

**Severity: low (cleanup, no functional impact)**

### Evidence

- `frontend/src/components/RouteMap.jsx:17` defaults `showSpeedSegments=false`
- `frontend/src/components/RouteDetail.jsx:226` renders `<RouteMap
  routeId={routeId} />` without passing the prop — so the default `false`
  always wins
- No grep match anywhere for `showSpeedSegments={true}` or
  `showSpeedSegments=true`

So `/api/routes/{id}/segments` is never called from the frontend, and the
146-line vectorized NumPy implementation in `api/aggregations.py:278-423` is
unreachable in the deployed app.

### Decision needed

Two reasonable paths:

1. **Wire it in**: add a UI toggle on `RouteDetail` ("Show speed segments")
   that passes `showSpeedSegments={true}` to `RouteMap`. The backend code is
   already there.
2. **Delete it**: remove the `/api/routes/{id}/segments` endpoint, the
   `get_route_speed_segments()` function in `api/aggregations.py`, and the
   segment-rendering branches in `RouteMap.jsx`.

If you don't have a strong product reason to keep it, deleting is cheaper.
The current state — keeping the code without exposing the feature — is the
worst of both options.

---

## 4. Bump Python from 3.9 to 3.11 or 3.12 — OPEN

**Severity: low (maintenance, not blocking).**

### Evidence

- `pyproject.toml:6` pins `requires-python = ">=3.9"`.
- `pyproject.toml:43` pins `target-version = "py39"` for ruff.
- `.venv` runs Python 3.9.6.
- Python 3.9 reached end of life on 2025-10-31; VS Code's Jupyter
  extension surfaces a "no longer supported" warning when loading the
  kernel. Nothing breaks today, but no further security patches upstream.

### Fix

1. Pick a target — 3.11 or 3.12 are both safe; 3.13 is fine if you want
   the latest. None of the current deps (sqlalchemy 2, pandas 2,
   fastapi, gtfs-realtime-bindings, psycopg/psycopg2, jupyter) require
   anything older.
2. Update `requires-python` in `pyproject.toml`.
3. Update `target-version` in the ruff config (`py311` / `py312`).
4. `uv sync --extra postgres --extra viz --extra dev` to rebuild the
   venv against the new interpreter (uv will fetch it if not installed).
5. `uv run pytest -m smoke` and a one-off `uv run python -c "import api.main"` to confirm imports cleanly.
6. CI: check `.github/workflows/` for any `python-version: '3.9'` pins
   and bump them to match.

Not coupled to any other work — can be done in a 5-minute PR whenever.
