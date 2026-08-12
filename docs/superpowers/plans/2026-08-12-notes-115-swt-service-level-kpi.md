# SWT + Daytime Service-Level KPI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two schedule-promise KPIs to the agency comparison page: a nightly-materialized scheduled wait time (SWT, frequent pool, beside EWT) and a live-computed all-routes daytime service-level tile (trip-weighted median scheduled headway + % ≤ 15 min).

**Architecture:** Hybrid per the approved spec (`docs/superpowers/specs/2026-08-12-swt-service-level-kpi-design.md`): `_system_ewt_and_bunching_for_date` already computes the pooled SWT and discards it — thread it through `src/system_metrics.py` into a new nullable `system_metrics_daily.swt_seconds` column. The service-level stats are a new pure module (`src/service_level.py`) fed by the module-cached `fetch_scheduled_cell_hours_for_routes(db, "weekday")`, attached per-agency by the comparison endpoint at read time.

**Tech Stack:** FastAPI + SQLAlchemy (Postgres prod / SQLite tests), React + Vitest frontend.

## Global Constraints

- Branch: all work happens on `feature/notes-115-swt-service-level` (already checked out; spec is committed on it). Never commit to main.
- Every function/class/method gets a docstring (user's global rule).
- All GTFS-table queries must filter `is_current=True` — inherited automatically here by using `fetch_scheduled_cell_hours_for_routes`; do not write new direct GTFS queries.
- Tests default to SQLite in-memory (`db_session` fixture); don't use `pg_session` — nothing in this plan needs pg-specific SQL.
- CI gates: `uv run pytest -m smoke`, `uv run ruff check src/ scripts/ api/ pipelines/ tests/`, `uv run ruff format --check src/ scripts/ api/ pipelines/ tests/` (both ruff gates MUST include `tests/`), `cd frontend && npm run lint && npm test`.
- The ALTER TABLE migration and the ~20-day backfill are **user-run** — documented in the PR body (Task 6), never executed by an implementing agent.
- Do NOT run Playwright or regenerate visual baselines — the comparison page is not baselined.

---

### Task 1: Thread SWT through the system rollup into `system_metrics_daily`

**Files:**
- Modify: `api/aggregations.py:2136-2266` (`_system_ewt_and_bunching_for_date`)
- Modify: `src/models.py:762-777` (`SystemMetricsDaily`)
- Modify: `src/system_metrics.py` (both functions)
- Test: `tests/test_system_metrics.py`

**Interfaces:**
- Consumes: existing `compute_awt(headways: list[float]) -> float | None` (already imported in `api/aggregations.py`).
- Produces: `_system_ewt_and_bunching_for_date(...) -> tuple[float | None, float | None, float | None]` returning `(ewt_seconds, swt_seconds, bunching_rate)`; `compute_system_metrics_for_date` dict gains key `"swt_seconds"`; ORM column `SystemMetricsDaily.swt_seconds` (nullable Float). Task 3 relies on the column name `swt_seconds`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_system_metrics.py` (imports of `compute_system_metrics_for_date`, `upsert_system_metrics_for_date`, `SystemMetricsDaily`, `pytest` already exist at top of file; add `from datetime import date` if not present — the file currently imports `datetime, timedelta`):

```python
def test_compute_system_metrics_includes_swt_key_empty_db(db_session):
    """Empty DB: the metrics dict carries swt_seconds=None, never a missing key."""
    metrics = compute_system_metrics_for_date(db_session, datetime(2026, 8, 10).date())
    assert "swt_seconds" in metrics
    assert metrics["swt_seconds"] is None


def test_system_swt_computed_from_schedule_pool_alone(db_session, monkeypatch):
    """SWT is schedule-side only: with zero observed stop_events it is still
    computed from the frequent scheduled pool (EWT stays None)."""
    import api.aggregations as agg

    def _fake_sched(db, day_type, route_ids=None, gtfs_snapshot_id=None):
        # One frequent cell: two 600s headways -> SWT = (600²+600²)/(2·1200) = 300.
        return {"64": {(0, "S1", 8): [600.0, 600.0]}}

    monkeypatch.setattr(agg, "fetch_scheduled_cell_hours_for_routes", _fake_sched)
    monkeypatch.setattr(agg, "get_cell_hour_gate_sec", lambda route_id: 900)

    ewt, swt, bunching = agg._system_ewt_and_bunching_for_date(
        db_session, datetime(2026, 8, 10).date(), {}
    )
    assert ewt is None
    assert swt == pytest.approx(300.0)
    assert bunching is None


def test_upsert_persists_swt_seconds(db_session, monkeypatch):
    """The upsert writes swt_seconds on both the insert and update paths."""
    from src import system_metrics as sm

    def _fake_compute(db, service_date, gtfs_snapshot_id=None, tz_name="America/New_York"):
        return {
            "otp_percentage": 70.0,
            "service_delivered_ratio": 0.9,
            "ewt_seconds": 150.0,
            "swt_seconds": 300.0,
            "bunching_rate": 0.03,
        }

    monkeypatch.setattr(sm, "compute_system_metrics_for_date", _fake_compute)
    monkeypatch.setattr(
        "src.data_completeness.coverage_pct_for_date",
        lambda db, service_date, tz_name="America/New_York": 1.0,
    )
    monkeypatch.setattr(
        "src.data_completeness.is_date_sufficiently_complete",
        lambda db, service_date, threshold=0.80, tz_name="America/New_York": True,
    )

    upsert_system_metrics_for_date(db_session, datetime(2026, 8, 10).date())
    row = db_session.query(SystemMetricsDaily).filter_by(service_date="2026-08-10").one()
    assert row.swt_seconds == 300.0

    # Update path: change the value, re-run, confirm overwrite.
    def _fake_compute_2(db, service_date, gtfs_snapshot_id=None, tz_name="America/New_York"):
        out = _fake_compute(db, service_date)
        out["swt_seconds"] = 280.0
        return out

    monkeypatch.setattr(sm, "compute_system_metrics_for_date", _fake_compute_2)
    upsert_system_metrics_for_date(db_session, datetime(2026, 8, 10).date())
    row = db_session.query(SystemMetricsDaily).filter_by(service_date="2026-08-10").one()
    assert row.swt_seconds == 280.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_system_metrics.py -k swt -v`
Expected: FAIL — `test_compute_system_metrics_includes_swt_key_empty_db` on missing key; `test_system_swt_computed_from_schedule_pool_alone` on "cannot unpack 2 values into 3"; `test_upsert_persists_swt_seconds` on `swt_seconds` not being an attribute.

- [ ] **Step 3: Implement**

3a. `src/models.py` — in `SystemMetricsDaily`, after `ewt_seconds = Column(Float)` add:

```python
    # Pooled random-incidence scheduled wait over the same frequent
    # cell-hour pool as ewt_seconds (NOTES-115): swt + ewt = pooled AWT
    # (up to EWT's clamp at 0). Nullable — NULL until the nightly batch
    # (or a backfill re-run) writes it.
    swt_seconds = Column(Float)
```

Also extend the class docstring's metric list ("OTP, service-delivered ratio, EWT, ...") to mention SWT.

3b. `api/aggregations.py` — `_system_ewt_and_bunching_for_date`:
- Change return annotation to `tuple[float | None, float | None, float | None]`.
- Update the docstring's Returns line to `(ewt_seconds, swt_seconds, bunching_rate)` and note SWT is schedule-side only (computed even when no stop_events were observed).
- The function already computes `swt = compute_awt(sched_pool)` (line ~2242). Change the final `return ewt_seconds, bunching_rate` to:

```python
    return ewt_seconds, swt, bunching_rate
```

3c. `src/system_metrics.py`:
- In `compute_system_metrics_for_date`, unpack the 3-tuple and add the key:

```python
    ewt_seconds, swt_seconds, bunching_rate = _system_ewt_and_bunching_for_date(
        db, service_date, sched_by_day_type, gtfs_snapshot_id, tz_name=tz_name
    )
```

and in the returned dict, after `"ewt_seconds": ewt_seconds,` add `"swt_seconds": swt_seconds,`. Update the docstring key list.
- In `upsert_system_metrics_for_date`: on the update path add `existing.swt_seconds = metrics["swt_seconds"]`; on the insert path add `swt_seconds=metrics["swt_seconds"],` to the `SystemMetricsDaily(...)` constructor. In the final success `print`, add `f"SWT={metrics['swt_seconds']}, "` before the EWT segment. Update this docstring too (it enumerates the four metrics).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_system_metrics.py -v`
Expected: all PASS (including the pre-existing tests — the dict gained a key, nothing was renamed).

- [ ] **Step 5: Commit**

```bash
git add src/models.py src/system_metrics.py api/aggregations.py tests/test_system_metrics.py
git commit -m "feature: persist system SWT alongside EWT in system_metrics_daily (NOTES-115)"
```

---

### Task 2: `src/service_level.py` — pure daytime service-level computation

**Files:**
- Create: `src/service_level.py`
- Test: `tests/test_service_level.py` (new)

**Interfaces:**
- Consumes: the return shape of `src/ewt.py:fetch_scheduled_cell_hours_for_routes` — `{route_id: {(direction_id, stop_id, hour): [headway_sec, ...]}}`.
- Produces: `compute_service_level_stats(sched_by_route, *, hour_start=7, hour_end=19, threshold_sec=900.0) -> dict` with keys `median_headway_seconds` (float|None), `pct_at_most_15min` (float|None, 0–1 fraction), `n_headways` (int); and `service_level_for_agency(db) -> dict` (same shape). Task 3 embeds this dict verbatim as each agency's `service_level` block.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_service_level.py`:

```python
"""Unit tests for the schedule-derived daytime service-level stats (NOTES-115).

Pure-function tests only — no database. The wrapper
`service_level_for_agency` is exercised through the comparison-endpoint
tests (tests/test_agency_comparison.py) with the schedule fetch
monkeypatched, because `fetch_scheduled_cell_hours_for_routes` caches by
db identity and in-memory SQLite sessions share one identity.
"""

import pytest

from src.service_level import compute_service_level_stats


def test_empty_schedule_returns_nulls():
    """No routes at all -> null stats, zero samples."""
    out = compute_service_level_stats({})
    assert out == {
        "median_headway_seconds": None,
        "pct_at_most_15min": None,
        "n_headways": 0,
    }


def test_daytime_hour_filter_is_half_open():
    """Hours 7..18 are in; hour 6 and hour 19 are out."""
    sched = {
        "A": {
            (0, "S1", 6): [100.0],   # before window
            (0, "S1", 7): [600.0],   # in
            (0, "S1", 18): [1200.0],  # in
            (0, "S1", 19): [100.0],  # after window
        }
    }
    out = compute_service_level_stats(sched)
    assert out["n_headways"] == 2
    assert out["median_headway_seconds"] == pytest.approx(900.0)


def test_reference_stop_is_max_samples_per_route_direction():
    """Per (route, direction) only the stop with the most daytime samples
    contributes — stop-dense routes must not be overweighted."""
    sched = {
        "A": {
            (0, "S1", 8): [600.0, 600.0, 600.0],  # reference (3 samples)
            (0, "S2", 8): [60.0, 60.0],           # decoy stop, ignored
        }
    }
    out = compute_service_level_stats(sched)
    assert out["n_headways"] == 3
    assert out["median_headway_seconds"] == pytest.approx(600.0)


def test_directions_pool_independently():
    """Each direction picks its own reference stop; both directions pool."""
    sched = {
        "A": {
            (0, "S1", 8): [600.0, 600.0],
            (1, "S9", 8): [1200.0, 1200.0, 1200.0],
        }
    }
    out = compute_service_level_stats(sched)
    assert out["n_headways"] == 5
    assert out["median_headway_seconds"] == pytest.approx(1200.0)


def test_median_and_share_trip_weighted_across_routes():
    """Routes pool sample-by-sample (trip-weighted), not route-equal."""
    sched = {
        "FREQ": {(0, "S1", 9): [600.0, 600.0, 600.0, 600.0]},  # 4 samples ≤ 15 min
        "RARE": {(0, "S2", 9): [1800.0, 1800.0]},              # 2 samples > 15 min
    }
    out = compute_service_level_stats(sched)
    assert out["n_headways"] == 6
    assert out["median_headway_seconds"] == pytest.approx(600.0)
    assert out["pct_at_most_15min"] == pytest.approx(4 / 6, abs=1e-4)


def test_share_boundary_is_inclusive_at_900s():
    """A headway of exactly 15 min counts toward the ≤ 15 min share."""
    sched = {"A": {(0, "S1", 10): [900.0, 901.0]}}
    out = compute_service_level_stats(sched)
    assert out["pct_at_most_15min"] == pytest.approx(0.5)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_service_level.py -v`
Expected: FAIL at import — `ModuleNotFoundError: No module named 'src.service_level'`.

- [ ] **Step 3: Implement `src/service_level.py`**

```python
"""
Schedule-derived daytime service-level stats for the agency comparison
page (NOTES-115).

The comparison page's other KPIs measure performance *against* the
schedule; these stats surface the schedule itself — the "promise" term.
Computed from the current GTFS weekday schedule over the daytime window,
trip-weighted via per-route-direction reference stops (see
`compute_service_level_stats`). Random-incidence SWT is not valid for
infrequent service (riders time their arrivals), so this module reports
headway distribution stats instead — see the design spec
`docs/superpowers/specs/2026-08-12-swt-service-level-kpi-design.md`.
"""

from statistics import median

from sqlalchemy.orm import Session

# Daytime window, agency-local clock hours: [start, end) — matches the
# NOTES-115 motivating measurement (weekday 7:00–19:00).
DAYTIME_HOUR_START = 7
DAYTIME_HOUR_END = 19

# Share threshold: fraction of scheduled service at ≤ 15-minute headways.
FREQUENT_SHARE_THRESHOLD_SEC = 900.0


def compute_service_level_stats(
    sched_by_route: dict,
    *,
    hour_start: int = DAYTIME_HOUR_START,
    hour_end: int = DAYTIME_HOUR_END,
    threshold_sec: float = FREQUENT_SHARE_THRESHOLD_SEC,
) -> dict:
    """Trip-weighted daytime headway stats over a scheduled-cell-hour map.

    Args:
        sched_by_route: `{route_id: {(direction_id, stop_id, hour):
            [headway_sec, ...]}}` — the return shape of
            `src.ewt.fetch_scheduled_cell_hours_for_routes`.
        hour_start / hour_end: half-open agency-local hour window
            (GTFS clock hours; the cell key's hour component).
        threshold_sec: headway cutoff for the `pct_at_most_15min` share.

    Weighting: for each (route, direction), only the *reference stop* —
    the stop with the most headway samples inside the window — is pooled,
    so a route-direction's weight is proportional to its scheduled trips
    (TfL-style frequency weighting) rather than its stop count, and
    route-equal averaging (the NYCT Wait Assessment flaw) is avoided.
    Ties on sample count keep the first-seen stop (stable, arbitrary).

    Returns:
        Dict with `median_headway_seconds` (float | None),
        `pct_at_most_15min` (0–1 fraction, float | None), and
        `n_headways` (int). Nulls / 0 when no samples survive the window.
    """
    pool: list[float] = []
    for cells in sched_by_route.values():
        # Gather daytime samples per (direction, stop).
        by_dir_stop: dict[tuple, list[float]] = {}
        for (direction_id, stop_id, hour), headways in cells.items():
            if not (hour_start <= hour < hour_end):
                continue
            by_dir_stop.setdefault((direction_id, stop_id), []).extend(headways)
        # Reference stop per direction: max daytime sample count.
        best_by_direction: dict = {}
        for (direction_id, _stop_id), samples in by_dir_stop.items():
            current = best_by_direction.get(direction_id)
            if current is None or len(samples) > len(current):
                best_by_direction[direction_id] = samples
        for samples in best_by_direction.values():
            pool.extend(samples)

    if not pool:
        return {"median_headway_seconds": None, "pct_at_most_15min": None, "n_headways": 0}

    return {
        "median_headway_seconds": round(median(pool), 1),
        "pct_at_most_15min": round(sum(1 for h in pool if h <= threshold_sec) / len(pool), 4),
        "n_headways": len(pool),
    }


def service_level_for_agency(db: Session) -> dict:
    """Service-level stats for one agency from its current weekday GTFS.

    Thin wrapper over `fetch_scheduled_cell_hours_for_routes(db,
    "weekday")` (module-cached; inherits the NOTES-106 day-type resolver,
    so SFMTA's calendar_dates-only weekday service resolves correctly).
    Raises whatever the schedule fetch raises — the comparison endpoint
    catches and degrades to a null block.
    """
    from src.ewt import fetch_scheduled_cell_hours_for_routes

    return compute_service_level_stats(fetch_scheduled_cell_hours_for_routes(db, "weekday"))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_service_level.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/service_level.py tests/test_service_level.py
git commit -m "feature: schedule-derived daytime service-level stats module (NOTES-115)"
```

---

### Task 3: Comparison endpoint — `swt` metric + `service_level` block + caveats

**Files:**
- Modify: `api/aggregations.py:2277-2282` (`_METRIC_TO_COLUMN`), `:2479` (`AGENCY_COMPARISON_METRICS`), `:2487-2511` (`AGENCY_COMPARISON_CAVEATS`), `:2580-2642` (`get_agency_comparison_data` agency loop)
- Test: `tests/test_agency_comparison.py`

**Interfaces:**
- Consumes: `SystemMetricsDaily.swt_seconds` (Task 1); `src.service_level.service_level_for_agency(db) -> dict` (Task 2).
- Produces: API payload — each agency dict gains `"service_level": {median_headway_seconds, pct_at_most_15min, n_headways}`; `metrics` gains key `"swt"` with the standard `{window_mean, wow_delta, days_included, partial_days}` shape; two new caveat strings. Task 4/5 (frontend) rely on exactly these key names.

- [ ] **Step 1: Write the failing tests**

In `tests/test_agency_comparison.py`:

1a. In `_seed_row`, add to `defaults`: `"swt_seconds": 300.0,` (keeps every existing seeded row exercising the new column).

1b. Add to `TestGetAgencyComparisonData`:

```python
    def test_swt_metric_window_mean(self):
        """swt joins the metric set and window-means like ewt."""
        db = _make_session()
        try:
            _seed_row(db, AGENCY_COMPARISON_WINDOW_START, swt_seconds=280.0)
            _seed_row(db, AGENCY_COMPARISON_WINDOW_START + timedelta(days=1), swt_seconds=320.0)
            db.commit()
            result = get_agency_comparison_data({"wmata": db})
            swt = result["agencies"][0]["metrics"]["swt"]
            assert swt["window_mean"] == pytest.approx(300.0)
            assert swt["days_included"] == 2
        finally:
            db.close()

    def test_service_level_block_attached_per_agency(self, monkeypatch):
        """Each agency carries the service_level dict from src.service_level."""
        monkeypatch.setattr(
            "src.service_level.service_level_for_agency",
            lambda db: {
                "median_headway_seconds": 720.0,
                "pct_at_most_15min": 0.6,
                "n_headways": 100,
            },
        )
        db = _make_session()
        try:
            _seed_row(db, AGENCY_COMPARISON_WINDOW_START)
            db.commit()
            result = get_agency_comparison_data({"wmata": db})
            assert result["agencies"][0]["service_level"] == {
                "median_headway_seconds": 720.0,
                "pct_at_most_15min": 0.6,
                "n_headways": 100,
            }
        finally:
            db.close()

    def test_service_level_degrades_to_nulls_on_fetch_failure(self, monkeypatch):
        """A schedule-fetch blow-up must not 500 the whole comparison."""

        def _boom(db):
            raise RuntimeError("no GTFS here")

        monkeypatch.setattr("src.service_level.service_level_for_agency", _boom)
        db = _make_session()
        try:
            _seed_row(db, AGENCY_COMPARISON_WINDOW_START)
            db.commit()
            result = get_agency_comparison_data({"wmata": db})
            assert result["agencies"][0]["service_level"] == {
                "median_headway_seconds": None,
                "pct_at_most_15min": None,
                "n_headways": 0,
            }
        finally:
            db.close()

    def test_caveats_state_service_level_weighting_and_swt_scope(self):
        """Acceptance criterion (NOTES-115): the caveats name the weighting
        choice and window for the service-level tile, and SWT's
        frequent-only scope."""
        result = get_agency_comparison_data({})
        joined = " ".join(result["caveats"])
        assert "trip-weighted" in joined
        assert "7:00" in joined and "19:00" in joined
        assert "current" in joined  # current-schedule disclosure
        assert "random" in joined  # random-arrival assumption for SWT
```

(`AGENCY_COMPARISON_WINDOW_START`, `timedelta`, `pytest` are already imported in this file — check the header and add any that are missing.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_agency_comparison.py -v`
Expected: the four new tests FAIL (`KeyError: 'swt'` / `KeyError: 'service_level'` / caveat asserts); pre-existing tests still pass (seeding an extra column is harmless once Task 1's model change is in).

- [ ] **Step 3: Implement in `api/aggregations.py`**

3a. `_METRIC_TO_COLUMN`: add `"swt": "swt_seconds",` after the `"ewt"` entry.

3b. `AGENCY_COMPARISON_METRICS = ("otp", "service_delivered", "swt", "ewt", "bunching")` — update the comment above it if it enumerates "the four" metrics.

3c. Append two strings to `AGENCY_COMPARISON_CAVEATS`:

```python
    "The daytime service-level tile is computed from each agency's "
    "current GTFS weekday schedule, 7:00-19:00 agency-local: per "
    "route-direction, the stop with the most scheduled arrivals serves "
    "as reference stop and all route-directions' headways pool "
    "together -- a trip-weighted view (frequent routes weigh "
    "proportionally more), not ridership-weighted (neither pipeline "
    "has APC data). It reflects the schedule as of today, not the "
    "historical schedule of the comparison window.",
    "Scheduled wait covers only frequent-gated cell-hours -- the same "
    "pool as excess wait, so scheduled + excess = actual wait -- and "
    "assumes riders arrive at random. Infrequent routes are excluded "
    "because riders there time their arrivals to the timetable; the "
    "service-level tile is the all-routes view of the schedule promise.",
```

3d. In `get_agency_comparison_data`, inside the `for agency_name, by_date in rows_by_agency.items():` loop, before `agencies_out.append(...)`:

```python
        # Daytime service level (NOTES-115): live from current GTFS via the
        # module-cached schedule fetch. Degrade to a null block on any
        # failure (e.g. a dev DB with no GTFS loaded) rather than failing
        # the whole comparison payload.
        try:
            from src.service_level import service_level_for_agency

            service_level = service_level_for_agency(sessions[agency_name])
        except Exception:
            service_level = {
                "median_headway_seconds": None,
                "pct_at_most_15min": None,
                "n_headways": 0,
            }
```

and add `"service_level": service_level,` to the appended agency dict. Update the function docstring's Returns section to mention the `service_level` block and the `swt` metric key.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_agency_comparison.py tests/test_aggregations.py tests/test_system_metrics.py -v`
Expected: all PASS (`test_aggregations.py` is included because `_METRIC_TO_COLUMN` is shared with the system-trend path).

- [ ] **Step 5: Commit**

```bash
git add api/aggregations.py tests/test_agency_comparison.py
git commit -m "feature: swt metric + daytime service-level block on agency comparison (NOTES-115)"
```

---

### Task 4: Frontend formatters (`agencyComparison.js`)

**Files:**
- Modify: `frontend/src/utils/agencyComparison.js`
- Test: `frontend/tests/unit/agencyComparison.test.js`

**Interfaces:**
- Consumes: API keys from Task 3 (`swt` metric; `service_level.median_headway_seconds` / `.pct_at_most_15min`).
- Produces: `'swt'` in `METRIC_ORDER` (before `'ewt'`) / `METRIC_LABELS` / `HIGHER_IS_BETTER`; `formatMetricValue` and `formatDelta` handle `'swt'` like `'ewt'`; new `formatServiceLevel(serviceLevel) -> {median: string, share: string|null} | null`. Task 5 renders exactly these.

- [ ] **Step 1: Write the failing tests**

Append to `frontend/tests/unit/agencyComparison.test.js` (it already imports from `../../src/utils/agencyComparison`; extend the import list with `formatServiceLevel`, `METRIC_ORDER`):

```js
describe('swt metric', () => {
  it('appears in METRIC_ORDER immediately before ewt', () => {
    const swtIdx = METRIC_ORDER.indexOf('swt')
    expect(swtIdx).toBeGreaterThan(-1)
    expect(METRIC_ORDER[swtIdx + 1]).toBe('ewt')
  })

  it('formats swt values as minutes like ewt', () => {
    expect(formatMetricValue('swt', 300)).toBe('5.0 min')
    expect(formatMetricValue('swt', null)).toBe('—')
  })

  it('tints a falling swt green (lower promise-wait is better)', () => {
    const delta = formatDelta('swt', -30)
    expect(delta.tint).toBe('green')
    expect(delta.text).toBe('−0.5 min vs prior week')
  })
})

describe('formatServiceLevel', () => {
  it('returns null when the block is missing or empty', () => {
    expect(formatServiceLevel(null)).toBeNull()
    expect(formatServiceLevel({ median_headway_seconds: null })).toBeNull()
  })

  it('formats median headway and the ≤15-min share', () => {
    const out = formatServiceLevel({
      median_headway_seconds: 720,
      pct_at_most_15min: 0.6,
      n_headways: 100,
    })
    expect(out.median).toBe('12.0 min')
    expect(out.share).toBe('60% of scheduled service every ≤15 min')
  })

  it('omits the share line when pct is null', () => {
    const out = formatServiceLevel({ median_headway_seconds: 720, pct_at_most_15min: null })
    expect(out.median).toBe('12.0 min')
    expect(out.share).toBeNull()
  })
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd frontend && npx vitest run tests/unit/agencyComparison.test.js`
Expected: FAIL — `formatServiceLevel` not exported; `'swt'` missing from `METRIC_ORDER`.

- [ ] **Step 3: Implement in `frontend/src/utils/agencyComparison.js`**

- `METRIC_ORDER = ['otp', 'service_delivered', 'swt', 'ewt', 'bunching']` (update the comment above it — it says "four headline KPIs").
- `METRIC_LABELS`: add `swt: 'Scheduled wait (frequent svc)',` before the `ewt` entry.
- `HIGHER_IS_BETTER`: add `swt: false,`.
- `formatMetricValue`: change the ewt branch to `if (metric === 'ewt' || metric === 'swt') return \`${(value / 60).toFixed(1)} min\``.
- `formatDelta`: change the ewt magnitude branch the same way (`metric === 'ewt' || metric === 'swt'`). Update both functions' `@param` metric type unions to include `'swt'`.
- Add:

```js
/**
 * Format the daytime service-level block for its tile. Returns null when
 * the API degraded to the null block (no schedule available), so the tile
 * can render its em-dash empty state.
 *
 * @param {{median_headway_seconds: number|null, pct_at_most_15min: number|null}|null|undefined} serviceLevel
 * @returns {{median: string, share: string|null} | null}
 */
export function formatServiceLevel(serviceLevel) {
  if (!serviceLevel || serviceLevel.median_headway_seconds == null) return null
  const median = `${(serviceLevel.median_headway_seconds / 60).toFixed(1)} min`
  const share =
    serviceLevel.pct_at_most_15min == null
      ? null
      : `${(serviceLevel.pct_at_most_15min * 100).toFixed(0)}% of scheduled service every ≤15 min`
  return { median, share }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd frontend && npm test`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/utils/agencyComparison.js frontend/tests/unit/agencyComparison.test.js
git commit -m "feature: swt + service-level formatters for the comparison page (NOTES-115)"
```

---

### Task 5: Frontend tile rendering (`AgencyComparison.jsx`)

**Files:**
- Modify: `frontend/src/components/AgencyComparison.jsx`

**Interfaces:**
- Consumes: `formatServiceLevel` from Task 4; the `agency.service_level` payload block from Task 3. The `swt` tile needs NO component change — `AgencyColumn` maps `METRIC_ORDER`, which Task 4 already extended.

- [ ] **Step 1: Implement**

In `frontend/src/components/AgencyComparison.jsx`:

- Extend the top import: `import { METRIC_ORDER, METRIC_LABELS, formatMetricValue, formatDelta, formatServiceLevel } from '../utils/agencyComparison'`.
- Add below `MetricTile`:

```jsx
/**
 * Schedule-promise tile (NOTES-115): trip-weighted median daytime
 * scheduled headway + share of service at ≤15-min headways, computed
 * from the current GTFS — no week-over-week delta by design (it only
 * changes when an agency ships a new schedule).
 */
function ServiceLevelTile({ serviceLevel }) {
  const formatted = formatServiceLevel(serviceLevel)
  return (
    <div className="agency-metric-tile">
      <div className="agency-metric-label">Daytime service level</div>
      <div className="agency-metric-value">{formatted ? formatted.median : '—'}</div>
      {formatted?.share && <div className="agency-metric-partial">{formatted.share}</div>}
      <div className="agency-metric-partial">
        Median scheduled headway · weekday 7:00–19:00 · current schedule
      </div>
    </div>
  )
}
```

- In `AgencyColumn`, inside the `agency-metric-grid` div, after the `METRIC_ORDER.map(...)` expression, add:

```jsx
        <ServiceLevelTile serviceLevel={agency.service_level} />
```

- Update `AgencyColumn`'s and `AgencyComparison`'s doc comments: "four headline KPIs" → "headline KPIs (OTP, service-delivered, scheduled wait, EWT, bunching) plus the daytime service-level tile".

- [ ] **Step 2: Verify lint + tests + build**

Run: `cd frontend && npm run lint && npm test && npm run build`
Expected: zero lint errors, all tests pass, build succeeds. (No Playwright — this page is not baselined.)

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/AgencyComparison.jsx
git commit -m "feature: render swt and daytime service-level tiles (NOTES-115)"
```

---

### Task 6: Punch-list fold, full verification, PR

**Files:**
- Delete: `notes/NOTES-115.md`
- Modify: `NOTES.md` (remove the NOTES-115 index line; its "Comparison depth (active)" section becomes empty — remove the section header too)
- Modify: any file the cross-ref sweep surfaces

**Interfaces:** none — bookkeeping + verification.

- [ ] **Step 1: Fold the punch-list edits**

```bash
git rm notes/NOTES-115.md
```

Remove the NOTES-115 line and the now-empty `## Comparison depth (active)` section header from `NOTES.md`. Then sweep:

```bash
grep -rn 'NOTES-115' --include='*.md' --include='*.py' --include='*.tsx' --include='*.ts' --include='*.jsx' .
```

Rewrite surviving references to a PR-anchored phrase, e.g. "the SWT / service-level comparison KPIs (NOTES-115, closed by PR #M)" — the spec (`docs/superpowers/specs/2026-08-12-swt-service-level-kpi-design.md`), this plan, and the new code comments citing NOTES-115 may keep the token as historical context but must not link to the deleted `notes/NOTES-115.md` file. Use a TODO for the PR number and patch it right after `gh pr create` returns.

- [ ] **Step 2: Full verification (matches CI)**

```bash
uv run pytest            # full suite — multi-surface change
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
cd frontend && npm run lint && npm test
```

Expected: all clean. Fix and re-run until they are.

- [ ] **Step 3: Commit the fold + push + open PR**

```bash
git add -A
git commit -m "feature: close NOTES-115 — punch-list fold"
git push -u origin feature/notes-115-swt-service-level
gh pr create --title "feature: SWT + daytime service-level KPIs on the agency comparison page (NOTES-115)" --body-file <(cat <<'EOF'
## Why this shape

The comparison page's four KPIs are all performance-against-promise and
normalize the promise away — WMATA's 24-min vs SFMTA's 12-min median
daytime scheduled headway was invisible. Per the approved design spec
(docs/superpowers/specs/2026-08-12-swt-service-level-kpi-design.md,
research-grounded in TCRP/TfL/NYCT-audit practice):

- **Scheduled wait (SWT)** is materialized nightly beside EWT from the
  *identical* frequent cell-hour pool (the rollup already computed and
  discarded it), so scheduled + excess = actual wait holds exactly and
  historical values stay faithful to the GTFS snapshot they were
  computed under.
- **Daytime service level** (all routes) is computed live from current
  GTFS: trip-weighted via per-route-direction reference stops (avoids
  route-equal averaging and stop-density bias), weekday 7:00–19:00,
  median headway + share ≤15 min. Random-incidence SWT is invalid for
  infrequent service, hence distribution stats instead.
- New comparability caveats state the weighting, window,
  current-schedule scope, and SWT's frequent-only pool (the NOTES-115
  acceptance criterion).

## User-run migration + backfill (do BEFORE restarting the API on this code)

1. Additive column, both laptop DBs:

```sql
-- psql -d wmata_dashboard  AND  psql -d sfmta_dashboard
ALTER TABLE system_metrics_daily ADD COLUMN swt_seconds double precision;
```

2. Backfill the matched window (~20 dates × 2 agencies; re-runs
   recompute against the current GTFS snapshot — accepted minor drift,
   --gtfs-snapshot-id pinning available if it ever matters):

```bash
d=2026-07-23
while [ "$d" != "$(date -v+1d +%F)" ]; do
  uv run python -m pipelines.upsert_system_metrics_daily --date "$d"
  uv run python -m pipelines.upsert_system_metrics_daily --date "$d" --agency sfmta
  d=$(date -j -f %F -v+1d "$d" +%F)
done
```

Until backfilled, swt_seconds is NULL and the tile shows its em-dash
null state — nothing breaks.

## Verification

Full pytest suite, both ruff gates (incl. tests/), frontend lint +
vitest + build. Comparison page is not visual-baselined — no Playwright
regen needed.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)
```

Then patch any `PR #TODO` cross-references from Step 1 with the real number and amend/commit.

- [ ] **Step 4: Return the four-field summary**

```
PR_NUMBER: <int>
PR_URL: <url>
SUMMARY: <one paragraph — what changed and what verification ran>
NEW_NOTES: none
```
