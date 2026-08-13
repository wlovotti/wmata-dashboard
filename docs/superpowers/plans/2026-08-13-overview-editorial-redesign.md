# Overview Editorial Redesign (NOTES-84) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the Overview as an editorial page (hero verdict + compare teaser → system map + movers fold → smoothed trends → demoted drags table), add a bulk `/api/shapes` endpoint, and collapse the nav to four entries with a Diagnostics index — shipped as four serial PRs.

**Architecture:** Hierarchy over new data — every payload except the bulk shapes endpoint already exists. Frontend stays fetch-in-component with one page-level fetch passed down as props (the WhatChangedPanel precedent). The map is leaflet (already a dependency) joining `/api/shapes` geometry to `/api/routes` metrics client-side.

**Tech Stack:** FastAPI + SQLAlchemy (backend), React 19 + react-router v7 + react-leaflet 5 + Recharts 3 (frontend), pytest / vitest + RTL / Playwright (tests).

**Spec:** `docs/superpowers/specs/2026-08-13-overview-editorial-redesign-design.md` — read it first; this plan argues from it.

## Global Constraints

- **Four SERIAL PRs.** Each PR merges before the next branch is cut from a fresh `main` (`git checkout main && git pull --ff-only`). Never stack — squash-merge makes stacked branches conflict.
- **Never commit to `main`.** Branch names below are given per PR.
- **Verification gates for every PR** (run all, in order, before `gh pr create`):
  `uv run pytest -m smoke` · `uv run ruff check src/ scripts/ api/ pipelines/ tests/` · `uv run ruff format --check src/ scripts/ api/ pipelines/ tests/` · `cd frontend && npm run lint && npm test && npm run build`. Both ruff gates MUST include `tests/`.
- **Baseline regeneration is USER-RUN** (PRs 2–4). The executor STOPS and asks; never runs `--update-snapshots` itself. Both platforms (darwin + linux-docker per `frontend/README.md`), and always with `--update-snapshots=all` — the default `changed` mode silently writes nothing for diffs inside the 1% tolerance (NOTES-120).
- **GTFS versioning:** `trips` filters `is_current` (via `Trip.is_current.is_(True)` — bare `== True` fails ruff E712). The `shapes` table has NO `is_current` column — current shapes are reached only via current trips' `shape_id`.
- **Docstrings on every new function/class/method** (user convention).
- **Week-over-week framing only** in all new UI copy — never "vs last month" (pre-2026-05-25 aggregates are contaminated).
- **Datetime:** never `datetime.now()`; this plan's code needs no new datetime logic — keep it that way.
- **PR bodies** must explain the scoping rationale and link the spec path. PR 4's body is the durable record of NOTES-84's closure.
- Plan/spec docs: the spec is already committed on branch `docs/notes-84-overview-redesign-spec`; this plan is committed to the same branch. Merge that docs PR to main BEFORE starting Task 1 (it touches only `docs/superpowers/`, so it can't conflict with anything).

---

# PR 1 — bulk shapes backend

**Branch:** `feature/notes-84-pr1-bulk-shapes` (from fresh main)

### Task 1: Douglas-Peucker polyline simplifier

**Files:**
- Create: `src/shape_simplify.py`
- Test: `tests/test_shape_simplify.py`

**Interfaces:**
- Consumes: nothing (pure function).
- Produces: `simplify_polyline(points: list[tuple[float, float]], tolerance_deg: float = DEFAULT_TOLERANCE_DEG) -> list[tuple[float, float]]` and constant `DEFAULT_TOLERANCE_DEG = 1e-4`. Task 2 imports both.

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for the Douglas-Peucker polyline simplifier (NOTES-84 system map)."""

import pytest

from src.shape_simplify import DEFAULT_TOLERANCE_DEG, simplify_polyline


@pytest.mark.smoke
def test_short_inputs_returned_unchanged():
    """Polylines with 0-2 points cannot be simplified — returned as-is (copy)."""
    assert simplify_polyline([]) == []
    assert simplify_polyline([(38.9, -77.0)]) == [(38.9, -77.0)]
    two = [(38.9, -77.0), (38.91, -77.01)]
    out = simplify_polyline(two)
    assert out == two
    assert out is not two  # a copy, not the caller's list


@pytest.mark.smoke
def test_collinear_points_collapse_to_endpoints():
    """Interior points on a straight line are within any positive tolerance."""
    line = [(38.90, -77.00), (38.91, -77.00), (38.92, -77.00), (38.93, -77.00)]
    assert simplify_polyline(line) == [(38.90, -77.00), (38.93, -77.00)]


@pytest.mark.smoke
def test_significant_deviation_is_preserved():
    """A point far off the chord (>> tolerance) must survive simplification."""
    dogleg = [(38.90, -77.00), (38.905, -76.99), (38.91, -77.00)]  # ~0.01 deg spike
    assert simplify_polyline(dogleg) == dogleg


@pytest.mark.smoke
def test_endpoints_always_preserved_and_count_never_grows():
    """First/last points survive; output is never longer than input."""
    wiggle = [(38.90 + i * 0.001, -77.00 + (0.00005 if i % 2 else 0.0)) for i in range(50)]
    out = simplify_polyline(wiggle)
    assert out[0] == wiggle[0]
    assert out[-1] == wiggle[-1]
    assert len(out) <= len(wiggle)
    # The 0.00005-deg wiggle is inside the 1e-4 default tolerance → big reduction.
    assert len(out) < len(wiggle) / 2


def test_tolerance_zero_keeps_everything_noncollinear():
    """tolerance 0 keeps every point that deviates at all."""
    dogleg = [(0.0, 0.0), (0.5, 0.0001), (1.0, 0.0)]
    assert simplify_polyline(dogleg, tolerance_deg=0.0) == dogleg


assert DEFAULT_TOLERANCE_DEG == pytest.approx(1e-4)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_shape_simplify.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.shape_simplify'`

- [ ] **Step 3: Write the implementation**

```python
"""Polyline simplification for map payloads (the NOTES-84 system map).

Douglas-Peucker over raw (lat, lon) degrees. Degree-space distance is
anisotropic (~1.28x lat vs lon at DC latitudes) but the tolerance here is a
display-level knob, not a geodesic measurement — 1e-4 deg is ~11 m N-S,
invisible at system-map zoom, and typically cuts GTFS shape point counts
5-10x.
"""

DEFAULT_TOLERANCE_DEG = 1e-4


def _perpendicular_distance(point, start, end):
    """Distance in degrees from `point` to the segment start→end.

    Falls back to point-to-point distance when start == end. Clamps the
    projection to the segment so hairpin shapes don't measure against the
    infinite line.
    """
    (px, py), (sx, sy), (ex, ey) = point, start, end
    dx, dy = ex - sx, ey - sy
    if dx == 0 and dy == 0:
        return ((px - sx) ** 2 + (py - sy) ** 2) ** 0.5
    t = ((px - sx) * dx + (py - sy) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    cx, cy = sx + t * dx, sy + t * dy
    return ((px - cx) ** 2 + (py - cy) ** 2) ** 0.5


def simplify_polyline(points, tolerance_deg=DEFAULT_TOLERANCE_DEG):
    """Simplify a polyline with iterative Douglas-Peucker.

    Args:
        points: ordered list of (lat, lon) tuples.
        tolerance_deg: max perpendicular deviation (in degrees) a dropped
            point may have from the simplified line. Points deviating MORE
            than this are kept.

    Returns:
        A new list of (lat, lon) tuples — always includes the first and last
        input points, never longer than the input. Inputs of length <= 2 are
        returned as a shallow copy.

    Iterative (explicit stack) rather than recursive: GTFS rail/limited
    shapes can run thousands of points and recursion depth is input-shaped.
    """
    if len(points) <= 2:
        return list(points)
    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        start, end = stack.pop()
        max_dist = 0.0
        max_idx = None
        for i in range(start + 1, end):
            d = _perpendicular_distance(points[i], points[start], points[end])
            if d > max_dist:
                max_dist, max_idx = d, i
        if max_idx is not None and max_dist > tolerance_deg:
            keep[max_idx] = True
            stack.append((start, max_idx))
            stack.append((max_idx, end))
    return [p for p, k in zip(points, keep) if k]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_shape_simplify.py -v`
Expected: PASS (all 5)

- [ ] **Step 5: Commit**

```bash
git add src/shape_simplify.py tests/test_shape_simplify.py
git commit -m "feat: Douglas-Peucker polyline simplifier for map payloads (NOTES-84)"
```

### Task 2: `get_system_shapes` aggregation

**Files:**
- Modify: `api/aggregations.py` (add near the other cache blocks; imports at top)
- Test: `tests/test_system_shapes.py` (new file)

**Interfaces:**
- Consumes: `simplify_polyline` from Task 1; existing `Shape`, `Trip` models; `time.monotonic` cache pattern already used throughout `api/aggregations.py`.
- Produces: `get_system_shapes(db: Session) -> dict` returning `{"routes": [{"route_id": str, "points": [[lat, lon], ...]}, ...]}` sorted by `route_id`, plus module-level `_shapes_cache` / `_SHAPES_TTL_SEC`. Task 3 imports `get_system_shapes`.

- [ ] **Step 1: Write the failing tests**

`api/aggregations.py` already imports `Session`, `time`, and (verify — add if missing) `Shape`, `Trip` from `src.models` and `func` from `sqlalchemy`. The test seeds via the standard `db_session` fixture from `tests/conftest.py`.

```python
"""Tests for the bulk system-shapes aggregation (NOTES-84 system map)."""

import pytest

import api.aggregations as agg
from src.models import Route, Shape, Trip


@pytest.fixture(autouse=True)
def _clear_shapes_cache():
    """The system-shapes cache is keyed by a constant, so it MUST be cleared
    between tests — otherwise one test's seeded payload leaks into the next
    test's empty-DB expectation."""
    agg._shapes_cache.clear()
    yield
    agg._shapes_cache.clear()


def _seed_route_with_shapes(db):
    """One current route with two shape variants: shape A serves 2 current
    trips, shape B serves 1 — A is representative. A retired (is_current=False)
    trip points at shape C, which must be ignored entirely."""
    db.add(Route(route_id="R1", route_short_name="R1", route_type=3, is_current=True))
    db.add_all(
        [
            Trip(trip_id="t1", route_id="R1", shape_id="A", is_current=True),
            Trip(trip_id="t2", route_id="R1", shape_id="A", is_current=True),
            Trip(trip_id="t3", route_id="R1", shape_id="B", is_current=True),
            Trip(trip_id="t4", route_id="R1", shape_id="C", is_current=False),
        ]
    )
    # Shape A: 3 collinear points → simplifier collapses to 2.
    db.add_all(
        [
            Shape(shape_id="A", shape_pt_lat=38.90, shape_pt_lon=-77.00, shape_pt_sequence=1),
            Shape(shape_id="A", shape_pt_lat=38.91, shape_pt_lon=-77.00, shape_pt_sequence=2),
            Shape(shape_id="A", shape_pt_lat=38.92, shape_pt_lon=-77.00, shape_pt_sequence=3),
            Shape(shape_id="B", shape_pt_lat=38.80, shape_pt_lon=-77.10, shape_pt_sequence=1),
            Shape(shape_id="B", shape_pt_lat=38.81, shape_pt_lon=-77.11, shape_pt_sequence=2),
            Shape(shape_id="C", shape_pt_lat=38.70, shape_pt_lon=-77.20, shape_pt_sequence=1),
            Shape(shape_id="C", shape_pt_lat=38.71, shape_pt_lon=-77.21, shape_pt_sequence=2),
        ]
    )
    db.commit()


@pytest.mark.smoke
def test_empty_database_returns_empty_routes(db_session):
    assert agg.get_system_shapes(db_session) == {"routes": []}


@pytest.mark.smoke
def test_most_trips_shape_wins_and_is_simplified(db_session):
    _seed_route_with_shapes(db_session)
    result = agg.get_system_shapes(db_session)
    assert len(result["routes"]) == 1
    entry = result["routes"][0]
    assert entry["route_id"] == "R1"
    # Shape A won (2 trips > 1), and its 3 collinear points simplified to 2.
    assert entry["points"] == [[38.90, -77.00], [38.92, -77.00]]


def test_retired_trips_do_not_elect_a_shape(db_session):
    """A route whose only trips are is_current=False contributes nothing."""
    db_session.add(Route(route_id="R2", route_short_name="R2", route_type=3, is_current=True))
    db_session.add(Trip(trip_id="t9", route_id="R2", shape_id="C", is_current=False))
    db_session.add_all(
        [
            Shape(shape_id="C", shape_pt_lat=38.70, shape_pt_lon=-77.20, shape_pt_sequence=1),
            Shape(shape_id="C", shape_pt_lat=38.71, shape_pt_lon=-77.21, shape_pt_sequence=2),
        ]
    )
    db_session.commit()
    assert agg.get_system_shapes(db_session) == {"routes": []}


def test_result_is_cached(db_session):
    _seed_route_with_shapes(db_session)
    first = agg.get_system_shapes(db_session)
    # Second call within TTL returns the identical cached object.
    assert agg.get_system_shapes(db_session) is first
```

(If `Route`'s constructor kwargs differ — e.g. `route_type` not accepted — mirror the kwargs `tests/conftest.py`'s `sample_route` fixture uses. Do not guess: open the fixture.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_system_shapes.py -v`
Expected: FAIL — `AttributeError: module 'api.aggregations' has no attribute '_shapes_cache'`

- [ ] **Step 3: Implement `get_system_shapes`**

Add to `api/aggregations.py` (top imports: ensure `from src.shape_simplify import simplify_polyline`; `Shape`/`Trip` added to the existing `src.models` import; `func` already imported from sqlalchemy — verify). Place the block near the other `_*_cache` definitions:

```python
# Bulk system-map shapes (NOTES-84). One representative simplified polyline
# per current route. Keyed by a constant because the payload only changes on
# GTFS reload; the short TTL exists to pick that reload up without a restart.
_SHAPES_TTL_SEC = 60.0
_shapes_cache: dict[str, tuple[float, dict]] = {}


def get_system_shapes(db: Session) -> dict:
    """All current routes' representative polylines for the Overview system map.

    For each route with at least one ``is_current`` trip, elects the
    ``shape_id`` serving the most current trips (ties broken by shape_id for
    determinism) and returns its points, Douglas-Peucker-simplified, as
    compact ``[lat, lon]`` pair-arrays.

    Returns:
        ``{"routes": [{"route_id": ..., "points": [[lat, lon], ...]}, ...]}``
        sorted by route_id. Routes whose elected shape has < 2 points are
        omitted (nothing drawable).

    Cached for ``_SHAPES_TTL_SEC`` under a constant key — the payload is a
    pure function of the current GTFS snapshot.
    """
    cached = _shapes_cache.get("system")
    if cached is not None and (time.monotonic() - cached[0]) < _SHAPES_TTL_SEC:
        return cached[1]

    trip_counts = (
        db.query(Trip.route_id, Trip.shape_id, func.count(Trip.id).label("n"))
        .filter(Trip.is_current.is_(True), Trip.shape_id.isnot(None))
        .group_by(Trip.route_id, Trip.shape_id)
        .all()
    )
    best: dict[str, tuple[int, str]] = {}
    for route_id, shape_id, n in trip_counts:
        candidate = (n, shape_id)
        if route_id not in best or candidate > best[route_id]:
            best[route_id] = candidate

    shape_ids = {shape_id for (_, shape_id) in best.values()}
    points_by_shape: dict[str, list[tuple[float, float]]] = {s: [] for s in shape_ids}
    if shape_ids:
        rows = (
            db.query(Shape.shape_id, Shape.shape_pt_lat, Shape.shape_pt_lon)
            .filter(Shape.shape_id.in_(shape_ids))
            .order_by(Shape.shape_id, Shape.shape_pt_sequence)
            .all()
        )
        for shape_id, lat, lon in rows:
            points_by_shape[shape_id].append((lat, lon))

    routes = []
    for route_id in sorted(best):
        _, shape_id = best[route_id]
        simplified = simplify_polyline(points_by_shape.get(shape_id, []))
        if len(simplified) >= 2:
            routes.append(
                {"route_id": route_id, "points": [[lat, lon] for lat, lon in simplified]}
            )

    result = {"routes": routes}
    _shapes_cache["system"] = (time.monotonic(), result)
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_system_shapes.py -v`
Expected: PASS (all 4)

- [ ] **Step 5: Commit**

```bash
git add api/aggregations.py tests/test_system_shapes.py
git commit -m "feat: bulk system-shapes aggregation with representative-shape election (NOTES-84)"
```

### Task 3: `/api/shapes` endpoint + startup warm + PR 1

**Files:**
- Modify: `api/main.py` (new route near `/api/routes/{route_id}/shapes` at ~line 793; import + warm hook at top)
- Test: `tests/test_api_endpoints.py` (append)

**Interfaces:**
- Consumes: `get_system_shapes` from Task 2; existing `client` fixture (`tests/conftest.py:122`).
- Produces: `GET /api/shapes` → the Task 2 payload verbatim. PR 4's frontend consumes it.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_api_endpoints.py`; it already imports `Shape`, `Trip`, `Route`)

```python
@pytest.mark.api
def test_get_system_shapes_empty(client):
    """GET /api/shapes on an empty DB returns an empty routes list, not 404."""
    import api.aggregations as agg

    agg._shapes_cache.clear()
    response = client.get("/api/shapes")
    assert response.status_code == 200
    assert response.json() == {"routes": []}


@pytest.mark.api
def test_get_system_shapes_returns_current_route_polyline(client, db_session):
    """GET /api/shapes returns one simplified polyline per current route."""
    import api.aggregations as agg

    agg._shapes_cache.clear()
    db_session.add(Trip(trip_id="s1", route_id="TESTS", shape_id="SH", is_current=True))
    db_session.add_all(
        [
            Shape(shape_id="SH", shape_pt_lat=38.90, shape_pt_lon=-77.00, shape_pt_sequence=1),
            Shape(shape_id="SH", shape_pt_lat=38.91, shape_pt_lon=-77.00, shape_pt_sequence=2),
        ]
    )
    db_session.commit()
    response = client.get("/api/shapes")
    assert response.status_code == 200
    data = response.json()
    assert data["routes"] == [
        {"route_id": "TESTS", "points": [[38.90, -77.00], [38.91, -77.00]]}
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api_endpoints.py -k system_shapes -v`
Expected: FAIL — 404 (route not registered)

- [ ] **Step 3: Implement the endpoint + warm**

In `api/main.py`: add `get_system_shapes` to the existing `from api.aggregations import (...)` block. Add the route directly ABOVE `@app.get("/api/routes/{route_id}/shapes")`:

```python
@app.get("/api/shapes")
async def get_system_shapes_endpoint():
    """All current routes' representative simplified polylines, in one payload.

    Feeds the Overview system map (NOTES-84): the frontend joins these
    polylines to `/api/routes` metrics by route_id for coloring. Cached
    server-side (60s TTL; effectively always hot — the payload only changes
    on GTFS reload).
    """
    db = get_session()
    try:
        return get_system_shapes(db)
    finally:
        db.close()
```

In `_warm_scorecard_cache_sync` (api/main.py:56), after the `get_live_metrics_for_window(db, end_date, 7)` line, add:

```python
            get_system_shapes(db)
            logger.info("System shapes cache warmed")
```

(Keep it inside the same `try`; a warm failure is already caught by the enclosing best-effort `except`. Note the early `return` when `end_date is None` skips shape warming too — acceptable: no derived data means nobody is looking at the map yet, and the first request pays a sub-second cold cost.)

- [ ] **Step 4: Run tests, then the full PR-1 gate battery**

```bash
uv run pytest tests/test_api_endpoints.py -k system_shapes -v   # PASS
uv run pytest -m smoke                                          # PASS
uv run pytest tests/test_shape_simplify.py tests/test_system_shapes.py tests/test_api_endpoints.py -v
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
cd frontend && npm run lint && npm test && npm run build && cd ..
```
Expected: all clean (frontend untouched but the gate battery is per-PR, not per-area).

- [ ] **Step 5: Commit, push, open PR 1**

```bash
git add api/main.py tests/test_api_endpoints.py
git commit -m "feat: bulk /api/shapes endpoint for the Overview system map (NOTES-84 PR 1/4)"
git push -u origin feature/notes-84-pr1-bulk-shapes
gh pr create --title "feat: bulk /api/shapes endpoint for the Overview system map (NOTES-84 PR 1/4)" --body "$(cat <<'EOF'
First of four serial PRs implementing the NOTES-84 Overview editorial
redesign (spec: docs/superpowers/specs/2026-08-13-overview-editorial-redesign-design.md).

Backend only: `GET /api/shapes` returns one representative simplified
polyline per current route — the shape_id serving the most is_current
trips, Douglas-Peucker-simplified at 1e-4 deg (~11 m). Scoped so the
frontend PRs (nav collapse, Overview rebuild, system map) each land
against a merged, tested API. No frontend or baseline changes here.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: STOP — human checkpoint.** Report the PR URL. Wait for CI green + user merge. After merge: `git checkout main && git pull --ff-only`.

---

# PR 2 — nav collapse + Diagnostics index

**Branch:** `feature/notes-84-pr2-nav` (from fresh main, after PR 1 merges)

### Task 4: DiagnosticsIndex component

**Files:**
- Create: `frontend/src/components/DiagnosticsIndex.jsx`
- Modify: `frontend/src/App.css` (append section)
- Test: `frontend/tests/unit/DiagnosticsIndex.test.jsx`

**Interfaces:**
- Consumes: `react-router-dom` `Link`; existing `.chart-container` / `.drilldown-anchor` CSS.
- Produces: default-export `DiagnosticsIndex` component rendering four `<Link>` cards to `/blocks`, `/targets`, `/schedule-audit`, `/segments`. Task 5 routes `/diagnostics` to it.

- [ ] **Step 1: Write the failing test**

```jsx
/**
 * DiagnosticsIndex (NOTES-84 nav collapse): the /diagnostics landing page
 * cards the four tools demoted from the top-level nav. Pins that all four
 * links exist with their original URLs — the collapse moves nav entries,
 * never breaks bookmarks.
 */
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import DiagnosticsIndex from '../../src/components/DiagnosticsIndex'

describe('DiagnosticsIndex', () => {
  test('renders a card link for each demoted tool at its original URL', () => {
    render(
      <MemoryRouter>
        <DiagnosticsIndex />
      </MemoryRouter>,
    )
    expect(screen.getByRole('link', { name: /blocks/i })).toHaveAttribute('href', '/blocks')
    expect(screen.getByRole('link', { name: /targets/i })).toHaveAttribute('href', '/targets')
    expect(screen.getByRole('link', { name: /schedule audit/i })).toHaveAttribute(
      'href',
      '/schedule-audit',
    )
    expect(screen.getByRole('link', { name: /segments/i })).toHaveAttribute('href', '/segments')
  })

  test('renders the page heading', () => {
    render(
      <MemoryRouter>
        <DiagnosticsIndex />
      </MemoryRouter>,
    )
    expect(screen.getByRole('heading', { name: 'Diagnostics' })).toBeVisible()
  })
})
```

- [ ] **Step 2: Run to verify failure**

Run: `cd frontend && npx vitest run tests/unit/DiagnosticsIndex.test.jsx`
Expected: FAIL — cannot resolve `../../src/components/DiagnosticsIndex`

- [ ] **Step 3: Implement the component**

```jsx
import { Link } from 'react-router-dom'

// The four tools demoted from the top-level nav by the NOTES-84 collapse.
// Existing URLs are preserved verbatim — this page is an index, not a move.
const TOOLS = [
  {
    to: '/blocks',
    title: 'Blocks',
    description: 'Live block activity — which scheduled vehicle assignments are on the road right now.',
  },
  {
    to: '/targets',
    title: 'Targets',
    description: 'Route-level performance targets, and which routes are missing them.',
  },
  {
    to: '/schedule-audit',
    title: 'Schedule audit',
    description: 'Where the printed schedule itself is the problem — infeasible scheduled run times.',
  },
  {
    to: '/segments',
    title: 'Segments',
    description: 'Cross-route corridor segments — where routes slow down on shared streets.',
  },
]

/**
 * `/diagnostics` landing page (NOTES-84 nav collapse). One card per
 * deep-dive tool that used to hold a top-level nav slot. Each card links to
 * the tool's unchanged URL and answers "what question does this page
 * answer?" in one line, so the index earns its click instead of being a
 * bare list of nouns.
 */
function DiagnosticsIndex() {
  return (
    <main>
      <div className="chart-container">
        <h2>Diagnostics</h2>
        <p className="drilldown-anchor">
          Deep-dive tools behind the Overview and Routes pages.
        </p>
        <div className="diagnostics-grid">
          {TOOLS.map((tool) => (
            <Link key={tool.to} to={tool.to} className="diagnostics-card">
              <h3>{tool.title}</h3>
              <p>{tool.description}</p>
            </Link>
          ))}
        </div>
      </div>
    </main>
  )
}

export default DiagnosticsIndex
```

Append to `frontend/src/App.css`:

```css
/* Diagnostics index (NOTES-84 nav collapse) */
.diagnostics-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 1rem;
  padding: 0.5rem 0 0.5rem;
}

.diagnostics-card {
  display: block;
  padding: 1.25rem 1.5rem;
  border: 1px solid #e2e8f0;
  border-radius: 0.75rem;
  text-decoration: none;
  color: inherit;
  transition: border-color 0.15s ease;
}

.diagnostics-card:hover {
  border-color: #94a3b8;
}

.diagnostics-card h3 {
  margin: 0 0 0.35rem;
  font-size: 1rem;
}

.diagnostics-card p {
  margin: 0;
  font-size: 0.85rem;
  color: #64748b;
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cd frontend && npx vitest run tests/unit/DiagnosticsIndex.test.jsx`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/DiagnosticsIndex.jsx frontend/src/App.css frontend/tests/unit/DiagnosticsIndex.test.jsx
git commit -m "feat: Diagnostics index landing page (NOTES-84 PR 2/4)"
```

### Task 5: Nav rewrite + PR 2

**Files:**
- Modify: `frontend/src/App.jsx` (nav block at lines ~127-146, routes block, imports)

**Interfaces:**
- Consumes: `DiagnosticsIndex` from Task 4.
- Produces: four-link nav (Overview / Routes / Compare / Diagnostics); `/diagnostics` route. All previous routes still registered.

- [ ] **Step 1: Rewrite the nav.** In `App.jsx`, add `import DiagnosticsIndex from './components/DiagnosticsIndex'` and replace the four NavLinks for Blocks / Targets / Schedule audit / Segments with:

```jsx
            <NavLink to="/compare" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
              Compare
            </NavLink>
            <NavLink to="/diagnostics" className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}>
              Diagnostics
            </NavLink>
```

(The Overview and Routes NavLinks stay exactly as they are.)

- [ ] **Step 2: Update the routes block.** Add above the `/compare` route:

```jsx
            <Route path="/diagnostics" element={<DiagnosticsIndex />} />
```

Replace the four-line "URL only for now" comment above the `/compare` route with:

```jsx
            {/* Agency comparison page (PR #198), promoted to the nav by the
                NOTES-84 nav collapse. */}
```

Every existing `<Route>` (blocks, targets, schedule-audit, segments, runs, route/:routeId, blocks/:blockId) stays registered — only nav links moved.

- [ ] **Step 3: Verify no orphaned references.**

```bash
cd frontend && npm run lint && npm test && npm run build
npx playwright test tests/e2e/overview.spec.js --grep "smoke"
```
Expected: lint/unit/build clean. The Playwright smoke ("Overview nav link visible") passes. The four visual-regression screenshots are EXPECTED to mismatch now (header changed) — do not chase them; baselines regenerate at Step 5.

- [ ] **Step 4: Run backend gates + commit + push + PR**

```bash
uv run pytest -m smoke
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add frontend/src/App.jsx
git commit -m "feat: collapse nav to Overview/Routes/Compare/Diagnostics (NOTES-84 PR 2/4)"
git push -u origin feature/notes-84-pr2-nav
gh pr create --title "feat: collapse nav to Overview/Routes/Compare/Diagnostics (NOTES-84 PR 2/4)" --body "$(cat <<'EOF'
Second of four serial NOTES-84 PRs (spec:
docs/superpowers/specs/2026-08-13-overview-editorial-redesign-design.md).

Collapses the six tool-shaped nav entries to four: Overview / Routes /
Compare / Diagnostics. `/compare` (PR #198) is promoted from URL-only to
a nav entry; Blocks, Targets, Schedule audit, and Segments move behind a
new `/diagnostics` index page — every existing URL still resolves, only
top-level links moved. Scoped ahead of the Overview rebuild so the
header/baseline churn lands once, separately from page-content churn.

All four Playwright baselines regenerate in this PR (header renders on
every baselined page). Regenerated with `--update-snapshots=all` — the
default `changed` mode silently skips diffs inside the 1% tolerance
(NOTES-120).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: STOP — human checkpoint (baselines + merge).** Ask the user to regenerate ALL FOUR baselines on both platforms and push to this branch:

```bash
cd frontend && npx playwright test --update-snapshots=all
docker run --rm -v "$PWD":/work -w /work/frontend mcr.microsoft.com/playwright:v1.x-jammy \
  bash -c "npm ci --silent && npx playwright test --update-snapshots=all"
# (exact docker image/tag per frontend/README.md — follow the README, adding =all)
git add frontend/tests/e2e/*-snapshots/ && git commit -m "test: regenerate all visual baselines after nav collapse" && git push
```

Then wait for CI green + user merge. After merge: `git checkout main && git pull --ff-only`.

---

# PR 3 — Overview rebuild

**Branch:** `feature/notes-84-pr3-overview` (from fresh main, after PR 2 merges)

### Task 6: rolling-mean smoothing util

**Files:**
- Create: `frontend/src/utils/rollingMean.js`
- Test: `frontend/tests/unit/rollingMean.test.js`

**Interfaces:**
- Consumes: nothing (pure function).
- Produces: `rollingMean(series, windowDays = 7) -> [{date, value}]` (same length/dates as input, ascending by date; `value` null where the trailing window has no clean days) and `ROLLING_WINDOW_DAYS = 7`. Tasks 8/11 consume.

- [ ] **Step 1: Write the failing tests**

```js
/**
 * rollingMean (NOTES-84 trend smoothing): 7-day trailing mean over a daily
 * series, excluding partial-quality days and nulls from window means. The
 * output keeps every input date so the smoothed line and the raw ghost dots
 * share an x-axis.
 */
import { rollingMean, ROLLING_WINDOW_DAYS } from '../../src/utils/rollingMean'

const day = (i) => `2026-08-${String(i).padStart(2, '0')}`

describe('rollingMean', () => {
  test('window constant is 7', () => {
    expect(ROLLING_WINDOW_DAYS).toBe(7)
  })

  test('flat series smooths to itself', () => {
    const series = [1, 2, 3, 4, 5, 6, 7, 8].map((i) => ({ date: day(i), value: 50 }))
    const out = rollingMean(series)
    expect(out).toHaveLength(8)
    out.forEach((row) => expect(row.value).toBe(50))
  })

  test('trailing window: early points average over fewer days', () => {
    const series = [
      { date: day(1), value: 10 },
      { date: day(2), value: 20 },
      { date: day(3), value: 30 },
    ]
    const out = rollingMean(series)
    expect(out[0].value).toBe(10)      // window = [10]
    expect(out[1].value).toBe(15)      // window = [10, 20]
    expect(out[2].value).toBe(20)      // window = [10, 20, 30]
  })

  test('partial days and nulls are excluded from the mean but keep their date', () => {
    const series = [
      { date: day(1), value: 10 },
      { date: day(2), value: 99, data_quality: 'partial' },
      { date: day(3), value: null },
      { date: day(4), value: 20 },
    ]
    const out = rollingMean(series)
    expect(out.map((r) => r.date)).toEqual([day(1), day(2), day(3), day(4)])
    expect(out[3].value).toBe(15) // mean of clean 10 and 20 only
  })

  test('all-partial window yields null, and input order does not matter', () => {
    const series = [
      { date: day(2), value: 99, data_quality: 'partial' },
      { date: day(1), value: 88, data_quality: 'partial' },
    ]
    const out = rollingMean(series)
    expect(out[0].date).toBe(day(1)) // sorted ascending
    expect(out[0].value).toBeNull()
    expect(out[1].value).toBeNull()
  })

  test('non-array input returns empty array', () => {
    expect(rollingMean(null)).toEqual([])
  })
})
```

- [ ] **Step 2: Run to verify failure** — `cd frontend && npx vitest run tests/unit/rollingMean.test.js` → FAIL (module not found).

- [ ] **Step 3: Implement**

```js
// 7-day trailing rolling mean for the Overview trend cards (NOTES-84).
// Window length matches the week-over-week framing used everywhere else in
// the app; do not widen it past the post-cutover clean-data window semantics.
export const ROLLING_WINDOW_DAYS = 7

/**
 * Smooth a daily metric series with a trailing rolling mean.
 *
 * @param {Array<{date: string, value: number|null, data_quality?: string}>} series
 *   Daily rows in any order; dates are ISO strings (lexicographic ==
 *   chronological). Rows with `data_quality === 'partial'` or a null value
 *   are excluded from window means but keep their date slot in the output.
 * @param {number} [windowDays] - trailing window length, default 7.
 * @returns {Array<{date: string, value: number|null}>} one row per input
 *   date, ascending; `value` is null when the trailing window contains no
 *   clean day.
 */
export function rollingMean(series, windowDays = ROLLING_WINDOW_DAYS) {
  if (!Array.isArray(series)) return []
  const sorted = [...series].sort((a, b) =>
    a.date < b.date ? -1 : a.date > b.date ? 1 : 0,
  )
  return sorted.map((row, i) => {
    const window = sorted.slice(Math.max(0, i - windowDays + 1), i + 1)
    const clean = window.filter((r) => r.value != null && r.data_quality !== 'partial')
    if (clean.length === 0) return { date: row.date, value: null }
    const mean = clean.reduce((acc, r) => acc + r.value, 0) / clean.length
    return { date: row.date, value: mean }
  })
}
```

- [ ] **Step 4: Run to verify pass** — same command → PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/utils/rollingMean.js frontend/tests/unit/rollingMean.test.js
git commit -m "feat: trailing rolling-mean util for trend smoothing (NOTES-84 PR 3/4)"
```

### Task 7: extract hero summary math from HealthPulse

**Files:**
- Create: `frontend/src/utils/heroSummary.js`
- Test: `frontend/tests/unit/heroSummary.test.js`

**Interfaces:**
- Consumes: logic currently inline in `frontend/src/components/Overview.jsx` (`latestNonNull` lines ~100-107, `gapFraction` lines ~119-123, the routes-below-target loop lines ~162-197). This task only CREATES the util — Overview.jsx keeps its inline copies until Task 12 deletes them wholesale.
- Produces:
  - `latestNonNull(series, key) -> number|null`
  - `gapFraction({current, target, higherIsBetter}) -> number|null`
  - `worstMetric(systemMetrics) -> {key, label, current, target, gap}|null` (largest gapFraction; null when none computable)
  - `countRoutesBelowTarget(routes) -> {below, evaluated}`

- [ ] **Step 1: Write the failing tests**

```js
/**
 * heroSummary (NOTES-84): the worst-of-four + routes-below-target math
 * extracted from the retired HealthPulse banner, now driving the
 * OverviewHero verdict. Semantics are pinned to the HealthPulse originals.
 */
import {
  latestNonNull,
  gapFraction,
  worstMetric,
  countRoutesBelowTarget,
} from '../../src/utils/heroSummary'

describe('latestNonNull', () => {
  test('walks backward to the most recent non-null value', () => {
    const series = [
      { date: 'a', otp_percentage: 70 },
      { date: 'b', otp_percentage: 75 },
      { date: 'c', otp_percentage: null },
    ]
    expect(latestNonNull(series, 'otp_percentage')).toBe(75)
  })

  test('returns null for empty or non-array input', () => {
    expect(latestNonNull([], 'x')).toBeNull()
    expect(latestNonNull(null, 'x')).toBeNull()
  })
})

describe('gapFraction', () => {
  test('positive when worse than target, normalized by target', () => {
    expect(gapFraction({ current: 45, target: 50, higherIsBetter: true })).toBeCloseTo(0.1)
    expect(gapFraction({ current: 110, target: 100, higherIsBetter: false })).toBeCloseTo(0.1)
  })

  test('negative when beating target; null on missing sides', () => {
    expect(gapFraction({ current: 55, target: 50, higherIsBetter: true })).toBeCloseTo(-0.1)
    expect(gapFraction({ current: null, target: 50, higherIsBetter: true })).toBeNull()
    expect(gapFraction({ current: 55, target: 0, higherIsBetter: true })).toBeNull()
  })
})

describe('worstMetric', () => {
  const metrics = [
    { key: 'otp', label: 'OTP', higherIsBetter: true, current: 74, target: 75 },
    { key: 'bunching', label: 'Bunching', higherIsBetter: false, current: 14, target: 10 },
  ]

  test('picks the largest normalized gap and reports it', () => {
    const worst = worstMetric(metrics)
    expect(worst.key).toBe('bunching') // 0.4 gap beats OTP's ~0.013
    expect(worst.gap).toBeCloseTo(0.4)
  })

  test('null when no metric has both sides', () => {
    expect(worstMetric([{ key: 'otp', current: null, target: 75, higherIsBetter: true }])).toBeNull()
  })
})

describe('countRoutesBelowTarget', () => {
  test('counts routes missing any of their four targets; unmeasured routes excluded', () => {
    const routes = [
      // Below on OTP.
      { otp_all_pct: 60, targets: { otp: 75 } },
      // Meets OTP, no other targets.
      { otp_all_pct: 80, targets: { otp: 75 } },
      // No live data at all → excluded from `evaluated`.
      { otp_all_pct: null, targets: { otp: 75 } },
      // Below on bunching (fractions on both sides, NOTES-47 units).
      { bunching_rate: 0.2, targets: { bunching: 0.1 } },
    ]
    expect(countRoutesBelowTarget(routes)).toEqual({ below: 2, evaluated: 3 })
  })

  test('empty/missing input counts nothing', () => {
    expect(countRoutesBelowTarget(null)).toEqual({ below: 0, evaluated: 0 })
  })
})
```

- [ ] **Step 2: Run to verify failure** — `cd frontend && npx vitest run tests/unit/heroSummary.test.js` → FAIL.

- [ ] **Step 3: Implement** — port the three code blocks from `Overview.jsx` verbatim into exported functions (they are already correct; this is a move, not a rewrite), then add the two small wrappers:

```js
// Hero verdict math (NOTES-84). Extracted verbatim from the retired
// HealthPulse banner in Overview.jsx so the OverviewHero component and its
// tests can share it. Unit conventions follow NOTES-47: OTP percent,
// service_delivered / bunching fractions scaled x100 by CALLERS before
// comparison where noted.

/**
 * Most recent non-null value of `key` in a trend_data series, or null.
 * Falls back through earlier days so early-morning hits (before the daily
 * pipeline runs) read yesterday instead of nothing.
 */
export function latestNonNull(series, key) {
  if (!Array.isArray(series)) return null
  for (let i = series.length - 1; i >= 0; i--) {
    const value = series[i]?.[key]
    if (value != null) return value
  }
  return null
}

/**
 * Normalized gap to target: positive = wrong side of target, negative =
 * beating it, null when either side is missing or target is 0. Magnitude is
 * gap/|target| so a 10% miss reads identically across metrics.
 */
export function gapFraction({ current, target, higherIsBetter }) {
  if (current == null || target == null || target === 0) return null
  const rawGap = higherIsBetter ? target - current : current - target
  return rawGap / Math.abs(target)
}

/**
 * The metric with the largest normalized gap — the hero's "sore spot".
 *
 * @param {Array<{key, label, current, target, higherIsBetter}>} systemMetrics
 * @returns {{key, label, current, target, gap}|null} null when no metric has
 *   both a current value and a target.
 */
export function worstMetric(systemMetrics) {
  let worst = null
  for (const m of systemMetrics || []) {
    const gap = gapFraction({
      current: m.current,
      target: m.target,
      higherIsBetter: m.higherIsBetter,
    })
    if (gap == null) continue
    if (worst == null || gap > worst.gap) {
      worst = { key: m.key, label: m.label, current: m.current, target: m.target, gap }
    }
  }
  return worst
}

/**
 * Count scorecard routes on the wrong side of any of their four targets.
 * A route is "evaluated" only if at least one metric has both a current
 * value and a target — unmeasured routes are not "below target", they're
 * unmeasured. Mirrors the retired HealthPulse loop exactly (including the
 * x100 scaling of the fraction-unit metrics before comparison).
 *
 * @returns {{below: number, evaluated: number}}
 */
export function countRoutesBelowTarget(routes) {
  let below = 0
  let evaluated = 0
  for (const r of routes || []) {
    const targets = r.targets || {}
    const checks = [
      { current: r.otp_all_pct, target: targets.otp, higherIsBetter: true },
      {
        current: r.service_delivered_ratio != null ? r.service_delivered_ratio * 100 : null,
        target: targets.service_delivered != null ? targets.service_delivered * 100 : null,
        higherIsBetter: true,
      },
      { current: r.ewt_seconds, target: targets.ewt, higherIsBetter: false },
      {
        current: r.bunching_rate != null ? r.bunching_rate * 100 : null,
        target: targets.bunching != null ? targets.bunching * 100 : null,
        higherIsBetter: false,
      },
    ]
    let hasAnyMeasurement = false
    let isBelow = false
    for (const c of checks) {
      if (c.current == null || c.target == null) continue
      hasAnyMeasurement = true
      const gap = c.higherIsBetter ? c.target - c.current : c.current - c.target
      if (gap > 0) {
        isBelow = true
        break
      }
    }
    if (hasAnyMeasurement) {
      evaluated += 1
      if (isBelow) below += 1
    }
  }
  return { below, evaluated }
}
```

- [ ] **Step 4: Run to verify pass**, then commit:

```bash
git add frontend/src/utils/heroSummary.js frontend/tests/unit/heroSummary.test.js
git commit -m "feat: extract hero verdict math from HealthPulse (NOTES-84 PR 3/4)"
```

### Task 8: Sparkline ghost-dot support

**Files:**
- Modify: `frontend/src/components/RouteTrend.jsx` (`Sparkline`, lines ~164-291)
- Test: `frontend/tests/unit/Sparkline.test.jsx` (append)

**Interfaces:**
- Consumes: existing `Sparkline({data, color, valueFormat, height})`.
- Produces: new optional prop `ghostData: Array<{date, value, data_quality?, coverage_pct?}>` — rendered as small low-opacity dots on the same x-axis (partial days keep their grey treatment). Existing callers (RouteTrend, WhatChangedPanel-era code) pass nothing and render identically.

- [ ] **Step 1: Write the failing test** (append to `frontend/tests/unit/Sparkline.test.jsx`, matching its existing render/query style — read the file's existing tests first and mirror how they mount and assert; Recharts renders SVG, so assert on `circle` elements):

```jsx
describe('Sparkline ghostData (NOTES-84 smoothing)', () => {
  test('renders a ghost dot per non-null ghost row without altering the line data', () => {
    const data = [
      { date: '2026-08-01', value: 50 },
      { date: '2026-08-02', value: 52 },
      { date: '2026-08-03', value: 54 },
    ]
    const ghostData = [
      { date: '2026-08-01', value: 48 },
      { date: '2026-08-02', value: 55 },
      { date: '2026-08-03', value: null }, // null ghost → no dot
    ]
    const { container } = render(
      <Sparkline data={data} ghostData={ghostData} color="#002F6C" valueFormat={(v) => `${v}%`} />,
    )
    const ghosts = container.querySelectorAll('circle.sparkline-ghost-dot')
    expect(ghosts).toHaveLength(2)
  })

  test('omitting ghostData renders no ghost dots (back-compat)', () => {
    const data = [
      { date: '2026-08-01', value: 50 },
      { date: '2026-08-02', value: 52 },
    ]
    const { container } = render(
      <Sparkline data={data} color="#002F6C" valueFormat={(v) => `${v}%`} />,
    )
    expect(container.querySelectorAll('circle.sparkline-ghost-dot')).toHaveLength(0)
  })
})
```

NOTE: Recharts `ResponsiveContainer` has zero size in jsdom and may render nothing. Check how the existing `Sparkline.test.jsx` tests already cope (they exist, so a working pattern is in that file — reuse it, e.g. mocking ResponsiveContainer or asserting on structure). If the existing tests only assert on the empty/fallback states, wrap these ghost assertions the same way the file handles rendered-chart assertions; if the file has no rendered-chart precedent, test via a fixed-size wrapper: replace `ResponsiveContainer` assertions with rendering `<LineChart>` internals is NOT worth fighting — in that case assert through Recharts' accessible structure or fall back to testing that `Sparkline` forwards `ghostData` by extracting the ghost-filtering into a tiny exported helper `visibleGhostRows(ghostData)` and unit-testing that instead. Prefer the helper route if jsdom fights back; it keeps the test honest without mocking Recharts.

- [ ] **Step 2: Run to verify failure** — `cd frontend && npx vitest run tests/unit/Sparkline.test.jsx` → new tests FAIL.

- [ ] **Step 3: Implement.** In `Sparkline`:

1. Signature: `function Sparkline({ data, color, valueFormat, height = 60, ghostData = null })`.
2. Add an exported helper above the component:

```jsx
/**
 * Ghost rows that should render as dots: non-null values only (NOTES-84 —
 * the raw daily points ghosted under the smoothed 7-day line).
 */
function visibleGhostRows(ghostData) {
  return (ghostData || []).filter((row) => row.value != null)
}
```

3. Include ghost values in the y-domain computation (extend `allValues`):

```jsx
  const ghostRows = visibleGhostRows(ghostData)
  const ghostValues = ghostRows.map((row) => row.value)
  const allValues = [...completeValues, ...partialValues, ...ghostValues]
```

4. After the existing `partialDots.map(...)` ReferenceDots, add:

```jsx
        {ghostRows.map((row) => (
          <ReferenceDot
            key={`ghost-${row.date}`}
            x={row.date}
            y={row.value}
            r={1.75}
            fill={row.data_quality === 'partial' ? '#94a3b8' : color}
            fillOpacity={0.35}
            stroke="none"
            className="sparkline-ghost-dot"
            ifOverflow="extendDomain"
          />
        ))}
```

5. Export `visibleGhostRows` alongside the existing named exports (`export { DeltaIndicator, Sparkline, TargetIndicator, visibleGhostRows }`).

CAVEAT: `ReferenceDot` needs the ghost `x` (date) to exist in the chart's x-axis domain. Ghost dates equal the data dates in the Task 11 usage, so this holds; the docstring on `ghostData` must state it: "ghost rows must use dates present in `data`".

- [ ] **Step 4: Run to verify pass** — the whole file: `cd frontend && npx vitest run tests/unit/Sparkline.test.jsx` (existing tests must stay green).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/RouteTrend.jsx frontend/tests/unit/Sparkline.test.jsx
git commit -m "feat: Sparkline ghost-dot layer for smoothed trends (NOTES-84 PR 3/4)"
```

### Task 9: OverviewHero + CompareStrip

**Files:**
- Create: `frontend/src/components/OverviewHero.jsx`, `frontend/src/components/CompareStrip.jsx`
- Modify: `frontend/src/App.css` (append hero styles + `:root` tokens)
- Test: `frontend/tests/unit/OverviewHero.test.jsx`, `frontend/tests/unit/CompareStrip.test.jsx`

**Interfaces:**
- Consumes: `worstMetric`, `countRoutesBelowTarget` (Task 7); `computeWindowDelta` from `frontend/src/utils/computeWindowDelta.js` (returns `{delta, recentMean, priorMean, recentN, priorN}` or null); `formatContribMetricValue` from `frontend/src/utils/formatters.js`; `formatMetricValue`, `formatDelta` from `frontend/src/utils/agencyComparison.js` (`formatDelta` returns `{text, tint}` or null); `Link` from react-router.
- Produces:
  - `OverviewHero({ systemMetrics, scorecardRoutes, otpSeries })` — `systemMetrics` is the same 4-entry array Overview already builds (key/label/higherIsBetter/current/target); `otpSeries` is `[{date, value, data_quality}]` in percent.
  - `CompareStrip()` — self-fetching, renders null on any failure.

- [ ] **Step 1: Write the failing tests**

`OverviewHero.test.jsx`:

```jsx
/**
 * OverviewHero (NOTES-84): the big-number verdict that replaced the
 * HealthPulse banner. Pins the plain-language framing rules:
 *   - headline = 7-day OTP mean with an up/down/steady clause
 *   - subline = routes-below-target count
 *   - a "sore spot" sentence appears only when the worst metric isn't OTP
 */
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import OverviewHero from '../../src/components/OverviewHero'

const day = (i) => `2026-08-${String(i).padStart(2, '0')}`

// 14 clean days: prior week at 77, recent week at 75 → delta −2.0 pp.
const otpSeries = [
  ...[1, 2, 3, 4, 5, 6, 7].map((i) => ({ date: day(i), value: 77 })),
  ...[8, 9, 10, 11, 12, 13, 14].map((i) => ({ date: day(i), value: 75 })),
]

const systemMetrics = (bunchingCurrent) => [
  { key: 'otp', label: 'OTP', higherIsBetter: true, current: 75, target: 75 },
  { key: 'bunching', label: 'Bunching', higherIsBetter: false, current: bunchingCurrent, target: 10 },
]

const routes = [
  { otp_all_pct: 60, targets: { otp: 75 } },
  { otp_all_pct: 80, targets: { otp: 75 } },
]

function renderHero(props) {
  return render(
    <MemoryRouter>
      <OverviewHero
        systemMetrics={systemMetrics(9)}
        scorecardRoutes={routes}
        otpSeries={otpSeries}
        {...props}
      />
    </MemoryRouter>,
  )
}

describe('OverviewHero', () => {
  test('headline: weekly OTP with a signed week-over-week clause', () => {
    renderHero()
    expect(screen.getByText(/75% on time this week/i)).toBeVisible()
    expect(screen.getByText(/down 2\.0 pts/i)).toBeVisible()
  })

  test('subline counts routes below target', () => {
    renderHero()
    expect(screen.getByText(/1 of 2 routes below target/i)).toBeVisible()
  })

  test('sore-spot sentence appears only when the worst metric is not OTP', () => {
    renderHero({ systemMetrics: systemMetrics(14) }) // bunching 14 vs target 10 → worst
    expect(screen.getByText(/Bunching is the sore spot/i)).toBeVisible()
  })

  test('no sore-spot sentence when OTP itself is worst or nothing is off-target', () => {
    renderHero() // bunching 9 beats its target of 10
    expect(screen.queryByText(/sore spot/i)).not.toBeInTheDocument()
  })

  test('degrades to a neutral message when the OTP series is too thin', () => {
    renderHero({ otpSeries: [{ date: day(1), value: 75 }] })
    expect(screen.getByText(/not enough history yet/i)).toBeVisible()
  })
})
```

`CompareStrip.test.jsx`:

```jsx
/**
 * CompareStrip (NOTES-84): one-row WMATA-vs-Muni OTP teaser inside the hero,
 * linking to /compare. Never load-bearing: any fetch problem renders nothing.
 */
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'
import CompareStrip from '../../src/components/CompareStrip'

const payload = {
  agencies: [
    {
      agency: 'wmata',
      display_name: 'WMATA',
      metrics: { otp: { window_mean: 75.2, wow_delta: -2.1 } },
    },
    {
      agency: 'sfmta',
      display_name: 'SFMTA (Muni)',
      metrics: { otp: { window_mean: 71.0, wow_delta: 0.8 } },
    },
  ],
}

function mockFetch(impl) {
  vi.stubGlobal('fetch', vi.fn(impl))
}

afterEach(() => vi.unstubAllGlobals())

describe('CompareStrip', () => {
  test('renders both agencies OTP and links to /compare', async () => {
    mockFetch(() => Promise.resolve({ ok: true, json: () => Promise.resolve(payload) }))
    render(
      <MemoryRouter>
        <CompareStrip />
      </MemoryRouter>,
    )
    await waitFor(() => expect(screen.getByText(/WMATA/)).toBeVisible())
    expect(screen.getByText(/SFMTA \(Muni\)/)).toBeVisible()
    expect(screen.getByRole('link', { name: /full comparison/i })).toHaveAttribute(
      'href',
      '/compare',
    )
  })

  test('renders nothing on fetch failure', async () => {
    mockFetch(() => Promise.reject(new Error('down')))
    const { container } = render(
      <MemoryRouter>
        <CompareStrip />
      </MemoryRouter>,
    )
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })

  test('renders nothing when fewer than two agencies report', async () => {
    mockFetch(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ agencies: [payload.agencies[0]] }),
      }),
    )
    const { container } = render(
      <MemoryRouter>
        <CompareStrip />
      </MemoryRouter>,
    )
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container).toBeEmptyDOMElement()
  })
})
```

- [ ] **Step 2: Run to verify failure** — both files FAIL (modules not found).

- [ ] **Step 3: Implement `OverviewHero.jsx`**

```jsx
import { countRoutesBelowTarget, worstMetric } from '../utils/heroSummary'
import { computeWindowDelta } from '../utils/computeWindowDelta'
import { formatContribMetricValue } from '../utils/formatters'
import CompareStrip from './CompareStrip'

// Deltas inside ±HERO_FLAT_PP read as "steady" — mirrors DeltaIndicator's
// 0.5 flat threshold so the hero never contradicts the trend cards below.
const HERO_FLAT_PP = 0.5

/**
 * Build the hero's plain-language week-over-week clause from a
 * computeWindowDelta result. Exposed for the component's tests via render
 * assertions only — not exported.
 */
function weekClause(delta) {
  if (delta == null) return null
  if (Math.abs(delta.delta) <= HERO_FLAT_PP) return 'steady vs last week'
  const direction = delta.delta > 0 ? 'up' : 'down'
  return `${direction} ${Math.abs(delta.delta).toFixed(1)} pts`
}

/**
 * The Overview's big-number verdict (NOTES-84) — replaces the HealthPulse
 * banner. Headline: 7-day mean OTP with a plain-language week-over-week
 * clause (strictly week-over-week — the pre-2026-05-25 window is
 * contaminated, so no longer-horizon framing). Subline: routes below
 * target. When the worst-of-four metric is not OTP, one extra sentence
 * names it. Tint reuses the HealthPulse thresholds (worst normalized gap:
 * <=0 green, <=0.1 yellow, >0.1 red) via the same gap math in heroSummary.
 *
 * Props:
 *   systemMetrics   – the 4-entry {key,label,higherIsBetter,current,target}
 *                     array Overview builds from the trend payloads.
 *   scorecardRoutes – `routes` array from /api/routes, or null while loading.
 *   otpSeries       – [{date, value, data_quality}] daily OTP percent rows.
 */
function OverviewHero({ systemMetrics, scorecardRoutes, otpSeries }) {
  const cleanOtp = (otpSeries || []).filter((r) => r.data_quality !== 'partial')
  const weekDelta = computeWindowDelta(cleanOtp)
  const worst = worstMetric(systemMetrics || [])
  const { below, evaluated } = countRoutesBelowTarget(scorecardRoutes)

  let tint = 'overview-hero-green'
  if (worst == null) tint = 'overview-hero-neutral'
  else if (worst.gap > 0.1) tint = 'overview-hero-red'
  else if (worst.gap > 0) tint = 'overview-hero-yellow'

  return (
    <div className={`overview-hero ${tint}`} role="status">
      {weekDelta == null ? (
        <p className="overview-hero-headline">
          System verdict unavailable — not enough history yet this week.
        </p>
      ) : (
        <p className="overview-hero-headline">
          <span className="overview-hero-number">
            {Math.round(weekDelta.recentMean)}% on time this week
          </span>
          <span className="overview-hero-delta"> — {weekClause(weekDelta)}</span>
        </p>
      )}
      {evaluated > 0 && (
        <p className="overview-hero-subline">
          {below} of {evaluated} routes below target
        </p>
      )}
      {worst != null && worst.key !== 'otp' && worst.gap > 0 && (
        <p className="overview-hero-subline">
          {worst.label} is the sore spot:{' '}
          {formatContribMetricValue(
            worst.key,
            // formatters expect fractions for the x100-scaled metrics —
            // systemMetrics carries them pre-scaled to percent, so undo it.
            worst.key === 'service_delivered' || worst.key === 'bunching'
              ? worst.current / 100
              : worst.current,
          )}{' '}
          vs target{' '}
          {formatContribMetricValue(
            worst.key,
            worst.key === 'service_delivered' || worst.key === 'bunching'
              ? worst.target / 100
              : worst.target,
          )}
        </p>
      )}
      <CompareStrip />
    </div>
  )
}

export default OverviewHero
```

Implement `CompareStrip.jsx`:

```jsx
import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { formatMetricValue, formatDelta } from '../utils/agencyComparison'

/**
 * One-row WMATA-vs-Muni OTP teaser inside the Overview hero (NOTES-84),
 * linking to the full /compare page. Deliberately never load-bearing:
 * fetch failure, missing endpoint, or a single-agency payload all render
 * nothing rather than an error — the hero must not degrade because the
 * sidecar DB is unreachable.
 */
function CompareStrip() {
  const [agencies, setAgencies] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch('/api/agency-comparison')
      .then((res) => (res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`))))
      .then((json) => {
        if (!cancelled) setAgencies(json?.agencies ?? null)
      })
      .catch(() => {
        // Teaser only — swallow and render nothing.
      })
    return () => {
      cancelled = true
    }
  }, [])

  if (!Array.isArray(agencies) || agencies.length < 2) return null

  return (
    <p className="compare-strip">
      {agencies.map((agency) => {
        const otp = agency.metrics?.otp
        const delta = otp ? formatDelta('otp', otp.wow_delta) : null
        return (
          <span key={agency.agency} className="compare-strip-agency">
            {agency.display_name} {formatMetricValue('otp', otp?.window_mean)}
            {delta && (
              <span className={`compare-strip-delta compare-strip-delta-${delta.tint}`}>
                {' '}
                {delta.text}
              </span>
            )}
          </span>
        )
      })}
      <Link to="/compare" className="compare-strip-link">
        Full comparison →
      </Link>
    </p>
  )
}

export default CompareStrip
```

Append to `frontend/src/App.css` — the tokens FIRST (top of file, before existing rules), the hero styles at the end:

```css
/* Design tokens (NOTES-84 — seeded from the existing hardcoded palette;
   NOTES-85 migrates the rest of this file onto them). New rules only —
   do not rewrite existing rules to tokens in this PR. */
:root {
  --color-good: #0E8A6F;
  --color-warn: #D97706;
  --color-bad: #C8102E;
  --color-brand: #002F6C;
  --color-muted: #64748b;
  --color-neutral: #94a3b8;
}
```

```css
/* Overview hero (NOTES-84 editorial redesign) */
.overview-hero {
  border-radius: 0.75rem;
  padding: 1.75rem 2rem;
  margin-bottom: 1.5rem;
  border-left: 6px solid var(--color-neutral);
  background: #f8fafc;
}

.overview-hero-green { border-left-color: var(--color-good); }
.overview-hero-yellow { border-left-color: var(--color-warn); }
.overview-hero-red { border-left-color: var(--color-bad); }
.overview-hero-neutral { border-left-color: var(--color-neutral); }

.overview-hero-headline {
  margin: 0;
  font-size: 1.75rem;
  font-weight: 700;
  color: #0f172a;
}

.overview-hero-delta {
  font-size: 1.1rem;
  font-weight: 500;
  color: var(--color-muted);
}

.overview-hero-subline {
  margin: 0.4rem 0 0;
  font-size: 0.95rem;
  color: var(--color-muted);
}

.compare-strip {
  margin: 0.9rem 0 0;
  font-size: 0.85rem;
  color: var(--color-muted);
  display: flex;
  gap: 1.25rem;
  flex-wrap: wrap;
}

.compare-strip-delta-good { color: var(--color-good); }
.compare-strip-delta-bad { color: var(--color-bad); }
.compare-strip-delta-flat { color: var(--color-muted); }

.compare-strip-link {
  color: var(--color-brand);
  text-decoration: none;
  font-weight: 600;
}
```

(Check `formatDelta`'s actual `tint` values in `frontend/src/utils/agencyComparison.js` — the CSS class suffixes above must match them exactly; adjust the CSS suffixes if they are e.g. `up`/`down` instead of `good`/`bad`.)

- [ ] **Step 4: Run to verify pass** — `cd frontend && npx vitest run tests/unit/OverviewHero.test.jsx tests/unit/CompareStrip.test.jsx`

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/OverviewHero.jsx frontend/src/components/CompareStrip.jsx frontend/src/App.css frontend/tests/unit/OverviewHero.test.jsx frontend/tests/unit/CompareStrip.test.jsx
git commit -m "feat: OverviewHero verdict + CompareStrip teaser (NOTES-84 PR 3/4)"
```

### Task 10: MoversPanel (WhatChangedPanel successor)

**Files:**
- Create: `frontend/src/components/MoversPanel.jsx`
- Test: `frontend/tests/unit/MoversPanel.test.jsx`
- (WhatChangedPanel.jsx is DELETED in Task 12, not here — Overview still imports it until then.)

**Interfaces:**
- Consumes: `deltas` blocks on `/api/routes` rows (`{value, valid, current_n, prior_n}` per metric); `DeltaIndicator` from `./RouteTrend`; `badgeColor` from `../frequencyClass`; `formatContribMetricValue` from `../utils/formatters`.
- Produces: `MoversPanel({ routes })` — single ranked list, "Getting worse" by default with a "Getting better" toggle, top 7, only `valid: true` deltas, and a minimum-information rule: fewer than 3 valid movers in the selected direction → message instead of a pseudo-ranking.

- [ ] **Step 1: Write the failing tests**

```jsx
/**
 * MoversPanel (NOTES-84): WhatChangedPanel's degradations list promoted to
 * the top fold. Pins the honesty rules: only valid deltas rank, and fewer
 * than MIN_VALID_MOVERS rows renders a message, not a pseudo-ranking
 * (the NOTES-44 information-content lesson).
 */
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import userEvent from '@testing-library/user-event'
import MoversPanel from '../../src/components/MoversPanel'

const route = (id, otpDelta, valid = true) => ({
  route_id: id,
  route_name: id,
  route_long_name: `Route ${id}`,
  otp_all_pct: 70,
  deltas: { otp: { value: otpDelta, valid, current_n: 7, prior_n: 7 } },
})

function renderPanel(routes) {
  return render(
    <MemoryRouter>
      <MoversPanel routes={routes} />
    </MemoryRouter>,
  )
}

describe('MoversPanel', () => {
  test('ranks worsening routes by |delta| descending, worst first', () => {
    renderPanel([route('A', -1.2), route('B', -6.1), route('C', -3.9), route('D', 2.0)])
    const rows = screen.getAllByRole('row').slice(1) // drop header row
    expect(rows[0]).toHaveTextContent('B')
    expect(rows[1]).toHaveTextContent('C')
    expect(rows[2]).toHaveTextContent('A')
    // D improved — not in the worse list.
    expect(screen.queryByText('Route D')).not.toBeInTheDocument()
  })

  test('invalid deltas never rank', () => {
    renderPanel([route('A', -9.9, false), route('B', -1.0), route('C', -2.0), route('D', -3.0)])
    expect(screen.queryByText('Route A')).not.toBeInTheDocument()
  })

  test('fewer than 3 valid movers renders the not-enough-history message', () => {
    renderPanel([route('A', -1.0), route('B', -2.0)])
    expect(screen.getByText(/not enough history this week/i)).toBeVisible()
    expect(screen.queryByRole('table')).not.toBeInTheDocument()
  })

  test('toggle switches to improving routes', async () => {
    const user = userEvent.setup()
    renderPanel([
      route('A', 1.0),
      route('B', 2.0),
      route('C', 3.0),
      route('D', -1.0),
      route('E', -2.0),
      route('F', -3.0),
    ])
    await user.click(screen.getByRole('button', { name: /getting better/i }))
    const rows = screen.getAllByRole('row').slice(1)
    expect(rows[0]).toHaveTextContent('C')
    expect(screen.queryByText('Route F')).not.toBeInTheDocument()
  })
})
```

(If `@testing-library/user-event` is not in `frontend/package.json` devDependencies, use `fireEvent.click` from `@testing-library/react` instead — check before adding any dependency; do NOT add packages for a test convenience.)

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement**

```jsx
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { badgeColor } from '../frequencyClass'
import { DeltaIndicator } from './RouteTrend'
import { formatContribMetricValue } from '../utils/formatters'

// Metric options — same 4-entry list as Overview/RouteList (kept inline per
// the existing convention comment in those files).
const MOVER_METRICS = [
  { key: 'otp', label: 'On-Time %' },
  { key: 'service_delivered', label: 'Service Delivered' },
  { key: 'ewt', label: 'EWT' },
  { key: 'bunching', label: 'Bunching' },
]

const MOVERS_TOP_N = 7

// Below this many valid movers in the selected direction, a "ranking" is
// noise wearing a table costume — render a message instead (NOTES-44).
const MIN_VALID_MOVERS = 3

/** True when a larger raw delta is operationally good for `metric`. */
function isHigherBetter(metric) {
  return metric === 'otp' || metric === 'service_delivered'
}

/** Delta display formatter per metric, magnitudes only (sign is the arrow's job). */
function deltaFormatter(metric) {
  if (metric === 'otp') return (d) => `${Math.abs(d).toFixed(1)} pp`
  if (metric === 'service_delivered') return (d) => `${(Math.abs(d) * 100).toFixed(1)} pp`
  if (metric === 'ewt') return (d) => `${Math.round(Math.abs(d))}s`
  if (metric === 'bunching') return (d) => `${(Math.abs(d) * 100).toFixed(1)} pp`
  return (d) => String(Math.abs(d))
}

/** Current metric value from a scorecard row, in formatter-native units. */
function getCurrentValue(metric, row) {
  if (!row) return null
  if (metric === 'otp') return row.otp_all_pct ?? null
  if (metric === 'service_delivered') return row.service_delivered_ratio ?? null
  if (metric === 'ewt') return row.ewt_seconds ?? null
  if (metric === 'bunching') return row.bunching_rate ?? null
  return null
}

/**
 * "Getting worse" movers panel (NOTES-84) — WhatChangedPanel's degradations
 * list promoted to the top fold, with a "Getting better" toggle preserving
 * the improvements half. Ranks by |week-over-week delta| descending using
 * the `deltas` block on /api/routes rows; only `valid: true` deltas rank.
 *
 * Props:
 *   routes – the `routes` array from /api/routes, or null while loading.
 */
function MoversPanel({ routes }) {
  const navigate = useNavigate()
  const [metric, setMetric] = useState('otp')
  const [direction, setDirection] = useState('worse')

  const movers = (() => {
    if (!routes) return []
    const higherBetter = isHigherBetter(metric)
    const wantImproving = direction === 'better'
    const rows = []
    for (const r of routes) {
      const delta = r.deltas?.[metric]
      if (!delta || !delta.valid || delta.value == null) continue
      const isImprovement = higherBetter ? delta.value > 0 : delta.value < 0
      if (isImprovement !== wantImproving) continue
      rows.push({
        routeId: r.route_id,
        routeShortName: r.route_name,
        routeLongName: r.route_long_name,
        currentValue: getCurrentValue(metric, r),
        deltaValue: delta.value,
        absDelta: Math.abs(delta.value),
        currentN: delta.current_n,
        priorN: delta.prior_n,
      })
    }
    rows.sort((a, b) => b.absDelta - a.absDelta)
    return rows.slice(0, MOVERS_TOP_N)
  })()

  const metricLabel = MOVER_METRICS.find((m) => m.key === metric)?.label ?? metric
  const fmt = deltaFormatter(metric)
  const lowerIsBetter = !isHigherBetter(metric)

  return (
    <div className="table-container movers-panel">
      <div className="movers-panel-header">
        <h2>{direction === 'worse' ? 'Getting worse' : 'Getting better'}</h2>
        <button
          type="button"
          className="movers-panel-toggle"
          onClick={() => setDirection((d) => (d === 'worse' ? 'better' : 'worse'))}
        >
          {direction === 'worse' ? 'Getting better →' : '← Getting worse'}
        </button>
      </div>
      <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
        Routes whose {metricLabel} moved most vs the prior 7-day window.
      </p>
      <div className="filters" style={{ marginBottom: '0.75rem' }}>
        <div>
          <label htmlFor="movers-metric" style={{ marginRight: '0.5rem' }}>
            Metric:
          </label>
          <select id="movers-metric" value={metric} onChange={(e) => setMetric(e.target.value)}>
            {MOVER_METRICS.map((m) => (
              <option key={m.key} value={m.key}>
                {m.label}
              </option>
            ))}
          </select>
        </div>
      </div>

      {routes == null ? null : movers.length < MIN_VALID_MOVERS ? (
        <p style={{ color: 'var(--color-muted)', padding: '0 1.5rem 1.5rem' }}>
          Not enough history this week to rank {direction === 'worse' ? 'worsening' : 'improving'}{' '}
          routes on {metricLabel} — fewer than {MIN_VALID_MOVERS} routes have valid
          week-over-week deltas.
        </p>
      ) : (
        <table className="routes-table">
          <thead>
            <tr>
              <th>Route</th>
              <th>Name</th>
              <th>{metricLabel}</th>
              <th>Change</th>
            </tr>
          </thead>
          <tbody>
            {movers.map((r) => (
              <tr
                key={r.routeId}
                onClick={() => navigate(`/route/${r.routeId}`)}
                style={{ cursor: 'pointer' }}
              >
                <td className="route-id">
                  <span className="route-badge" style={{ backgroundColor: badgeColor(null, true) }}>
                    {r.routeShortName || r.routeId}
                  </span>
                </td>
                <td className="route-name">{r.routeLongName || 'N/A'}</td>
                <td className="metric">{formatContribMetricValue(metric, r.currentValue)}</td>
                <td className="metric">
                  <DeltaIndicator
                    delta={r.deltaValue}
                    format={fmt}
                    lowerIsBetter={lowerIsBetter}
                    title={`Last 7 days vs prior 7 days (${r.currentN}/${r.priorN} valid days)`}
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

export default MoversPanel
```

Append to `frontend/src/App.css`:

```css
/* Movers panel (NOTES-84) */
.movers-panel-header {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 1rem;
}

.movers-panel-toggle {
  background: none;
  border: none;
  color: var(--color-brand);
  font-size: 0.85rem;
  font-weight: 600;
  cursor: pointer;
  padding: 0;
}
```

- [ ] **Step 4: Run to verify pass**, then commit:

```bash
git add frontend/src/components/MoversPanel.jsx frontend/src/App.css frontend/tests/unit/MoversPanel.test.jsx
git commit -m "feat: MoversPanel with getting-worse default and validity floor (NOTES-84 PR 3/4)"
```

### Task 11: SystemTrend — props + smoothing

**Files:**
- Modify: `frontend/src/components/SystemTrend.jsx`

**Interfaces:**
- Consumes: `rollingMean` (Task 6); `Sparkline` `ghostData` prop (Task 8).
- Produces: `SystemTrend({ trendData })` — takes the `{otp, service_delivered, ewt, bunching}` payload object as a PROP (Overview owns the fetch after Task 12); renders the smoothed line with daily ghost dots. `trendData === null` renders the loading state; `trendData === 'error'` is NOT a thing — Overview passes `error` string as a second prop `error`.

- [ ] **Step 1: Rework the component.**

1. Signature: `function SystemTrend({ trendData, loading, error })`. Delete the `useMultiFetch` import/call and the `SYSTEM_TREND_URLS` constant (Overview owns the URLs after Task 12). `loading` / `error` render the existing two early-return blocks unchanged.
2. Keep the four `*Series` mappings exactly as they are (they already carry `data_quality`), reading from `const data = trendData ?? { otp: null, service_delivered: null, ewt: null, bunching: null }`.
3. For each card, compute the smoothed series and pass raw as ghosts. OTP card example (repeat the pattern for the other three):

```jsx
  const otpSmoothed = rollingMean(otpSeries)
```

```jsx
          <Sparkline
            data={otpSmoothed}
            ghostData={otpSeries}
            color={OTP_LINE_COLOR}
            valueFormat={(v) => `${v.toFixed(1)}%`}
          />
```

4. Rename the heading from `30-Day System Trend` to `System trend — 7-day rolling, daily points ghosted` (the honest label for what's now drawn).
5. Deltas/targets/meta lines: unchanged — they already compute from the RAW series (`computeSystemDelta(otpSeries, ...)`), which stays correct; smoothing is presentation-only.
6. Update the component docstring: fetching moved to Overview (props down), line = 7-day trailing mean via `rollingMean`, ghosts = raw daily incl. partial grey dots.

- [ ] **Step 2: Verify.**

```bash
cd frontend && npm run lint && npx vitest run
```
Expected: clean. (SystemTrend has no dedicated unit test; its behavior is covered by the Overview e2e in Task 13 and the rollingMean/Sparkline units. NOTE: Overview.jsx still renders `<SystemTrend />` bare at this commit — that renders the loading state harmlessly; Task 12 wires the props in the same PR, and the e2e suite is only consulted at Task 13.)

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/SystemTrend.jsx
git commit -m "feat: SystemTrend smoothed line + ghost dots, fetch lifted to Overview (NOTES-84 PR 3/4)"
```

### Task 12: Overview rebuild + off-target re-home

**Files:**
- Rewrite: `frontend/src/components/Overview.jsx`
- Modify: `frontend/src/components/Targets.jsx` (gains the Off-target section)
- Delete: `frontend/src/components/WhatChangedPanel.jsx`
- Modify: `frontend/tests/unit/OffTargetEmptyState.test.jsx` (import path only)

**Interfaces:**
- Consumes: OverviewHero (Task 9), MoversPanel (Task 10), SystemTrend (Task 11), heroSummary utils (Task 7).
- Produces: the new Overview page assembly; `OffTargetEmptyState` now exported from `Targets.jsx`. Task 15 later inserts `SystemMap` into the `.overview-fold` grid — keep that div's structure exactly as written here.

- [ ] **Step 1: Rewrite `Overview.jsx`.** The rebuilt file keeps: the scorecard fetch, the targets fetch is REMOVED (moves to Targets.jsx), the contributors fetch + table (retitled), the trend fan-out via `useMultiFetch`. It drops: HealthPulse, the off-target panel + `OffTargetEmptyState` export + `currentForMetric`/`formatGap` helpers (all move to Targets.jsx), the WhatChangedPanel import. Full replacement:

```jsx
import { useEffect, useState } from 'react'
import useMultiFetch from '../hooks/useMultiFetch'
import { useNavigate, Link } from 'react-router-dom'
import { badgeColor } from '../frequencyClass'
import { formatContribMetricValue } from '../utils/formatters'
import { latestNonNull } from '../utils/heroSummary'
import OverviewHero from './OverviewHero'
import MoversPanel from './MoversPanel'
import SystemTrend from './SystemTrend'

// Metric options for the "Biggest drags" table. Same 4-entry list as
// RouteList — kept inline per the existing convention.
const CONTRIB_METRICS = [
  { key: 'otp', label: 'On-Time %' },
  { key: 'service_delivered', label: 'Service Delivered' },
  { key: 'ewt', label: 'EWT' },
  { key: 'bunching', label: 'Bunching' },
]

const CONTRIB_TOP_N = 5

// One page-level fan-out for the four trend payloads; SystemTrend and the
// hero both read from this single fetch (props down — NOTES-84 data flow).
const OVERVIEW_TREND_URLS = [
  '/api/system/trend?metric=otp&days=30',
  '/api/system/trend?metric=service_delivered&days=30',
  '/api/system/trend?metric=ewt&days=30',
  '/api/system/trend?metric=bunching&days=30',
]

/**
 * Overview landing page, rebuilt as an editorial stack (NOTES-84):
 *
 *   1. OverviewHero  — big-number verdict + compare teaser (absorbs the
 *                      retired HealthPulse banner)
 *   2. Fold          — MoversPanel ("getting worse" promoted to the top
 *                      fold; the system map joins this grid in the next PR)
 *   3. SystemTrend   — 7-day-smoothed trend cards, daily points ghosted
 *   4. Biggest drags — the contributors table, demoted to the bottom
 *
 * One page-level fetch each for /api/routes and the trend fan-out, passed
 * down as props; the off-target panel moved to the Targets page.
 */
function Overview() {
  const navigate = useNavigate()
  const [scorecard, setScorecard] = useState(null)
  const [contribMetric, setContribMetric] = useState('otp')
  const [contribData, setContribData] = useState(null)
  const [contribLoading, setContribLoading] = useState(false)
  const [contribError, setContribError] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch('/api/routes')
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) setScorecard(json)
      })
      .catch(() => {
        // Hero and movers degrade gracefully without the scorecard.
      })
    return () => {
      cancelled = true
    }
  }, [])

  const {
    data: rawSystemTrendData,
    loading: trendLoading,
    error: trendError,
  } = useMultiFetch(OVERVIEW_TREND_URLS, ([otp, sd, ewt, bun]) => ({
    otp,
    service_delivered: sd,
    ewt,
    bunching: bun,
  }))
  const systemTrendData = rawSystemTrendData ?? null

  useEffect(() => {
    let cancelled = false
    setContribLoading(true)
    setContribError(null)
    fetch(`/api/routes/contributors?metric=${contribMetric}&days=30`)
      .then((res) => (res.ok ? res.json() : Promise.reject(`HTTP ${res.status}`)))
      .then((json) => {
        if (!cancelled) {
          setContribData(json)
          setContribLoading(false)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setContribError(err.message || String(err))
          setContribLoading(false)
        }
      })
    return () => {
      cancelled = true
    }
  }, [contribMetric])

  // The 4-entry worst-of-four input for the hero — same construction the
  // retired HealthPulse used (percent-scaled fractions, trend targets).
  const systemMetrics = [
    {
      key: 'otp',
      label: 'OTP',
      higherIsBetter: true,
      current: latestNonNull(systemTrendData?.otp?.trend_data, 'otp_percentage'),
      target: systemTrendData?.otp?.target_value ?? null,
    },
    {
      key: 'service_delivered',
      label: 'Service Delivered',
      higherIsBetter: true,
      current: (() => {
        const v = latestNonNull(
          systemTrendData?.service_delivered?.trend_data,
          'service_delivered_ratio',
        )
        return v != null ? v * 100 : null
      })(),
      target:
        systemTrendData?.service_delivered?.target_value != null
          ? systemTrendData.service_delivered.target_value * 100
          : null,
    },
    {
      key: 'ewt',
      label: 'EWT',
      higherIsBetter: false,
      current: latestNonNull(systemTrendData?.ewt?.trend_data, 'ewt_seconds'),
      target: systemTrendData?.ewt?.target_value ?? null,
    },
    {
      key: 'bunching',
      label: 'Bunching',
      higherIsBetter: false,
      current: (() => {
        const v = latestNonNull(systemTrendData?.bunching?.trend_data, 'bunching_rate')
        return v != null ? v * 100 : null
      })(),
      target:
        systemTrendData?.bunching?.target_value != null
          ? systemTrendData.bunching.target_value * 100
          : null,
    },
  ]

  // Daily OTP series for the hero's week-over-week math.
  const otpSeries = (systemTrendData?.otp?.trend_data || []).map((row) => ({
    date: row.date,
    value: row.otp_percentage,
    data_quality: row.data_quality,
  }))

  const visibleContributors = (contribData?.contributors ?? []).slice(0, CONTRIB_TOP_N)

  return (
    <main>
      <OverviewHero
        systemMetrics={systemMetrics}
        scorecardRoutes={scorecard?.routes ?? null}
        otpSeries={otpSeries}
      />

      {/* "Where is it going badly" fold. The system map (NOTES-84 PR 4)
          joins this grid as the left column; until then the movers panel
          spans it alone. */}
      <div className="overview-fold">
        <MoversPanel routes={scorecard?.routes ?? null} />
      </div>

      <SystemTrend trendData={systemTrendData} loading={trendLoading} error={trendError} />

      <div className="table-container">
        <h2>Biggest drags</h2>
        <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
          Top {CONTRIB_TOP_N} routes ranked by their contribution to system
          underperformance — the routes whose attention would move the
          system the most.
        </p>
        <div className="filters" style={{ marginBottom: '0.75rem' }}>
          <div>
            <label htmlFor="overview-contrib-metric" style={{ marginRight: '0.5rem' }}>
              Metric:
            </label>
            <select
              id="overview-contrib-metric"
              value={contribMetric}
              onChange={(e) => setContribMetric(e.target.value)}
            >
              {CONTRIB_METRICS.map((m) => (
                <option key={m.key} value={m.key}>
                  {m.label}
                </option>
              ))}
            </select>
          </div>
        </div>

        {contribError && (
          <p style={{ color: 'var(--color-muted)', padding: '0 1.5rem 1rem' }}>
            Unable to load contributors: {contribError}
          </p>
        )}

        {contribLoading ? (
          <div className="loading-spinner">
            <div className="spinner"></div>
            <p>Loading contributors...</p>
          </div>
        ) : contribData == null ? null : visibleContributors.length === 0 ? (
          <p style={{ padding: '0 1.5rem 1rem' }}>
            No routes have enough data to score contribution for this metric yet.
          </p>
        ) : (
          <table className="routes-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Route</th>
                <th>Name</th>
                <th>Route value</th>
                <th title="Per-route target if configured, otherwise system 30-day baseline">
                  Reference
                </th>
              </tr>
            </thead>
            <tbody>
              {visibleContributors.map((c, idx) => (
                <tr
                  key={c.route_id}
                  onClick={() => navigate(`/route/${c.route_id}`)}
                  style={{ cursor: 'pointer' }}
                >
                  <td>{idx + 1}</td>
                  <td className="route-id">
                    <span
                      className="route-badge"
                      style={{ backgroundColor: badgeColor(null, true) }}
                    >
                      {c.route_short_name || c.route_id}
                    </span>
                  </td>
                  <td className="route-name">{c.route_long_name || 'N/A'}</td>
                  <td className="metric">
                    {formatContribMetricValue(contribMetric, c.route_value)}
                  </td>
                  <td className="metric">
                    {formatContribMetricValue(contribMetric, c.reference_value ?? c.baseline_value)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        <div style={{ padding: '1rem 1.5rem 1.5rem' }}>
          <Link to="/routes" className="see-all-link">
            See all routes →
          </Link>
        </div>
      </div>
    </main>
  )
}

export default Overview
```

Append to `App.css`:

```css
/* Overview map+movers fold (NOTES-84). Single column until the system map
   lands (PR 4 makes this a 3fr/2fr split at desktop widths). */
.overview-fold {
  display: grid;
  grid-template-columns: 1fr;
  gap: 1.5rem;
  margin-bottom: 1.5rem;
}
```

- [ ] **Step 2: Re-home the off-target panel into `Targets.jsx`.** Move — verbatim, as a move not a rewrite — from the OLD Overview.jsx (use `git show HEAD:frontend/src/components/Overview.jsx` as the source): `OFF_TARGET_TOP_N`, `currentForMetric`, `formatGap`, and the exported `OffTargetEmptyState` component. Then extend `Targets()`:

1. Add state + fetch for the scorecard (same pattern as its existing targets fetch): `const [scorecard, setScorecard] = useState(null)` + a `useEffect` fetching `/api/routes` with the same cancelled-flag/catch-silently shape.
2. Add metric-select state: `const [offTargetMetric, setOffTargetMetric] = useState('otp')`.
3. Port the `offTargetRows` IIFE from old Overview verbatim, substituting `offTargetMetric` for `contribMetric` and `data` (the Targets page's `/api/targets` payload) for `targetsData`. NOTE the payload key difference: Targets.jsx's endpoint payload uses `data.routes` for overrides just like Overview did — verify against the running shape (`routes` block); `hasAnyOverrides` ports identically.
4. Render a new `<h3>Off target</h3>` section between "System defaults" and "Per-route overrides", containing: the metric `<select>` (reuse the page's `METRIC_ORDER`/`METRIC_LABELS` for options), and the exact off-target `<table>`/`OffTargetEmptyState` JSX from old Overview (row click → `/route/:id` needs `useNavigate` imported).
5. Export `OffTargetEmptyState` from Targets.jsx: `export { OffTargetEmptyState }`.

- [ ] **Step 3: Update the test import + delete WhatChangedPanel.**

In `frontend/tests/unit/OffTargetEmptyState.test.jsx`, change the import to `import { OffTargetEmptyState } from '../../src/components/Targets'` (and its doc comment's "(Overview.jsx)" to "(Targets.jsx)").

```bash
git rm frontend/src/components/WhatChangedPanel.jsx
```

- [ ] **Step 4: Full frontend verification**

```bash
cd frontend && npm run lint && npx vitest run && npm run build
```
Expected: all clean — no dangling WhatChangedPanel imports (only Overview imported it), OffTargetEmptyState tests pass from the new home.

- [ ] **Step 5: Commit**

```bash
git add -A frontend/src frontend/tests/unit
git commit -m "feat: rebuild Overview as editorial stack; re-home off-target panel to Targets (NOTES-84 PR 3/4)"
```

### Task 13: e2e update + PR 3

**Files:**
- Modify: `frontend/tests/e2e/overview.spec.js`
- Create: `frontend/tests/fixtures/agency_comparison.json`

**Interfaces:**
- Consumes: the rebuilt page's headings ("Getting worse", "Biggest drags", hero role=status).
- Produces: green e2e suite modulo the Overview screenshot (regenerated by the user).

- [ ] **Step 1: Add the fixture** `frontend/tests/fixtures/agency_comparison.json` (shape mirrors `/api/agency-comparison`; values arbitrary but stable):

```json
{
  "window_start": "2026-07-23",
  "window_end": "2026-08-12",
  "agencies": [
    {
      "agency": "wmata",
      "display_name": "WMATA",
      "metrics": { "otp": { "window_mean": 75.2, "wow_delta": -2.1, "partial_days": 0, "days_included": 7 } }
    },
    {
      "agency": "sfmta",
      "display_name": "SFMTA (Muni)",
      "metrics": { "otp": { "window_mean": 71.0, "wow_delta": 0.8, "partial_days": 0, "days_included": 7 } }
    }
  ],
  "caveats": []
}
```

(Before committing, sanity-check the real payload key spelling against `api/main.py`'s `get_agency_comparison_data` return — `agencies` must be a list here because `AgencyComparison.jsx` maps over it.)

- [ ] **Step 2: Update `overview.spec.js`.** In the `page.route` handler add, BEFORE the generic `/api/routes` check:

```js
    if (url.includes('/api/agency-comparison')) {
      return route.fulfill({ json: fixture('agency_comparison.json') })
    }
```

Replace the three heading tests + screenshot test's waits with the new page's anatomy:

```js
test('Overview: smoke — nav link visible', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('link', { name: 'Overview' })).toBeVisible()
})

test('Overview: hero verdict renders', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText(/on time this week/i)).toBeVisible()
})

test('Overview: movers panel renders', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('heading', { name: 'Getting worse' })).toBeVisible()
})

test('Overview: biggest drags renders', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('heading', { name: 'Biggest drags' })).toBeVisible()
})

test('Overview: visual regression', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText(/on time this week/i)).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Getting worse' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Biggest drags' })).toBeVisible()
  await page.waitForTimeout(500)
  await expect(page).toHaveScreenshot('overview.png', { fullPage: true })
})
```

Update the file's header comment fixture list to include `agency_comparison.json`.

- [ ] **Step 3: Run e2e non-visual tests**

```bash
cd frontend && npx playwright test tests/e2e/overview.spec.js --grep-invert "visual regression"
```
Expected: PASS. (The screenshot test fails against the stale baseline by design — user regenerates.)

- [ ] **Step 4: Full gates + PR**

```bash
uv run pytest -m smoke
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
cd frontend && npm run lint && npm test && npm run build && cd ..
git add frontend/tests
git commit -m "test: Overview e2e for the editorial layout + agency-comparison fixture (NOTES-84 PR 3/4)"
git push -u origin feature/notes-84-pr3-overview
gh pr create --title "feat: rebuild Overview as an editorial page (NOTES-84 PR 3/4)" --body "$(cat <<'EOF'
Third of four serial NOTES-84 PRs (spec:
docs/superpowers/specs/2026-08-13-overview-editorial-redesign-design.md).

Rebuilds the Overview as an editorial stack: OverviewHero big-number
verdict (7-day OTP, week-over-week clause, routes-below-target subline,
sore-spot sentence, WMATA-vs-Muni CompareStrip teaser), MoversPanel
("getting worse" promoted to the top fold, valid-deltas-only with a
3-mover information floor per the NOTES-44 lesson), SystemTrend smoothed
to a 7-day rolling line with daily ghost dots, and the contributors
table demoted to "Biggest drags". The off-target panel re-homed to the
Targets page; HealthPulse and WhatChangedPanel retired (their math lives
on in utils/heroSummary.js and MoversPanel). New :root design tokens
seed only the new styles — the app-wide migration stays in NOTES-85.

Only the Overview baseline regenerates here (header untouched since PR 2).
Regenerated with --update-snapshots=all (NOTES-120).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: STOP — human checkpoint (baseline + merge).** User regenerates the OVERVIEW baseline on both platforms (`--update-snapshots=all`, scoped: `npx playwright test tests/e2e/overview.spec.js --update-snapshots=all` locally + the docker equivalent), pushes, CI green, merge. Then `git checkout main && git pull --ff-only`.

---

# PR 4 — system map + NOTES-84 closure

**Branch:** `feature/notes-84-pr4-system-map` (from fresh main, after PR 3 merges)

### Task 14: route line-color helper

**Files:**
- Create: `frontend/src/utils/mapColors.js`
- Test: `frontend/tests/unit/mapColors.test.js`

**Interfaces:**
- Consumes: `computeSpectrumBar`, `COLOR_NEUTRAL` from `frontend/src/utils/spectrumBar.js` (`computeSpectrumBar({current, target, higherIsBetter}) -> {color, fillPct}|null`).
- Produces: `routeLineColor(scorecardRow) -> string` — hex color for a route polyline (OTP vs the row's collapsed target; neutral when either side missing).

- [ ] **Step 1: Write the failing tests**

```js
/**
 * routeLineColor (NOTES-84 system map): OTP-vs-target banding for route
 * polylines, delegating to computeSpectrumBar so map colors match the
 * scorecard's spectrum bars exactly. Missing row/value/target → neutral.
 */
import { routeLineColor } from '../../src/utils/mapColors'
import { COLOR_NEUTRAL } from '../../src/utils/spectrumBar'

describe('routeLineColor', () => {
  test('at/above target is green', () => {
    expect(routeLineColor({ otp_all_pct: 80, targets: { otp: 75 } })).toBe('#0E8A6F')
  })

  test('within the 10% band below target is yellow', () => {
    expect(routeLineColor({ otp_all_pct: 70, targets: { otp: 75 } })).toBe('#D97706')
  })

  test('past the band is red', () => {
    expect(routeLineColor({ otp_all_pct: 50, targets: { otp: 75 } })).toBe('#C8102E')
  })

  test('missing row, value, or target is neutral', () => {
    expect(routeLineColor(null)).toBe(COLOR_NEUTRAL)
    expect(routeLineColor({ otp_all_pct: null, targets: { otp: 75 } })).toBe(COLOR_NEUTRAL)
    expect(routeLineColor({ otp_all_pct: 70, targets: {} })).toBe(COLOR_NEUTRAL)
    expect(routeLineColor({ otp_all_pct: 70 })).toBe(COLOR_NEUTRAL)
  })
})
```

- [ ] **Step 2: Run to verify failure**, then implement:

```js
import { computeSpectrumBar, COLOR_NEUTRAL } from './spectrumBar'

/**
 * Line color for a route polyline on the Overview system map (NOTES-84).
 *
 * Colors by OTP vs the route's target using the same banding as the
 * scorecard spectrum bars (computeSpectrumBar), so "yellow on the map"
 * and "yellow in the table" always agree. The `targets` block on a
 * /api/routes row already collapses per-route override vs system default.
 *
 * @param {object|null} scorecardRow - a route row from /api/routes, or
 *   null/undefined when the route has no scorecard entry.
 * @returns {string} hex color; COLOR_NEUTRAL for unmeasured routes.
 */
export function routeLineColor(scorecardRow) {
  if (!scorecardRow) return COLOR_NEUTRAL
  const bar = computeSpectrumBar({
    current: scorecardRow.otp_all_pct,
    target: scorecardRow.targets?.otp ?? null,
    higherIsBetter: true,
  })
  return bar ? bar.color : COLOR_NEUTRAL
}
```

- [ ] **Step 3: Run to verify pass**, commit:

```bash
git add frontend/src/utils/mapColors.js frontend/tests/unit/mapColors.test.js
git commit -m "feat: route line-color helper matching spectrum-bar bands (NOTES-84 PR 4/4)"
```

### Task 15: SystemMap component + fold integration

**Files:**
- Create: `frontend/src/components/SystemMap.jsx`
- Modify: `frontend/src/components/Overview.jsx` (fold grid), `frontend/src/App.css` (fold columns)

**Interfaces:**
- Consumes: `GET /api/shapes` (PR 1 payload: `{routes: [{route_id, points: [[lat, lon], ...]}]}`); `routeLineColor` (Task 14); `MapContainer`/`TileLayer`/`Polyline`/`useMap` from react-leaflet (the `RouteMap.jsx` pattern, including its `FitBounds` helper shape).
- Produces: `SystemMap({ scorecardRoutes })`; the Overview fold becomes map + movers side-by-side, degrading to movers-only on shapes failure.

- [ ] **Step 1: Implement `SystemMap.jsx`** (no unit test for the leaflet assembly itself — jsdom can't lay out leaflet; the color logic was unit-tested in Task 14 and the rendered result is pinned by the e2e screenshot in Task 16):

```jsx
import { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { MapContainer, TileLayer, Polyline, useMap } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { routeLineColor } from '../utils/mapColors'

/** Fit the map viewport to the full network extent once bounds are known. */
function FitBounds({ bounds }) {
  const map = useMap()

  useEffect(() => {
    if (bounds && bounds.length > 0) {
      map.fitBounds(bounds, { padding: [20, 20] })
    }
  }, [bounds, map])

  return null
}

/**
 * Overview system map (NOTES-84): every current route's representative
 * polyline from /api/shapes, colored by OTP vs target via routeLineColor —
 * the most direct answer to "where is it going badly". Routes without a
 * scorecard row render neutral grey (the network stays visible; unmeasured
 * is not hidden). Click a route to open its detail page.
 *
 * Failure posture: the map is an enhancement, not load-bearing — a shapes
 * fetch failure renders a quiet inline note and the fold's movers panel
 * carries the answer alone (Overview's CSS lets this cell collapse).
 *
 * Props:
 *   scorecardRoutes – `routes` array from /api/routes, or null while loading.
 */
function SystemMap({ scorecardRoutes }) {
  const navigate = useNavigate()
  const [shapes, setShapes] = useState(null)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false
    fetch('/api/shapes')
      .then((res) => (res.ok ? res.json() : Promise.reject(new Error(`HTTP ${res.status}`))))
      .then((json) => {
        if (!cancelled) setShapes(json?.routes ?? [])
      })
      .catch((err) => {
        if (!cancelled) setError(err.message || String(err))
      })
    return () => {
      cancelled = true
    }
  }, [])

  const byRouteId = useMemo(
    () => new Map((scorecardRoutes || []).map((r) => [r.route_id, r])),
    [scorecardRoutes],
  )

  if (error) {
    return (
      <div className="chart-container system-map-error">
        <h2>System map</h2>
        <p style={{ color: 'var(--color-muted)', fontSize: '0.85rem' }}>
          Map unavailable right now ({error}) — the movers list still tells
          you where to look.
        </p>
      </div>
    )
  }

  if (shapes == null) {
    return (
      <div className="chart-container">
        <h2>System map</h2>
        <div className="loading-spinner">
          <div className="spinner"></div>
          <p>Loading map...</p>
        </div>
      </div>
    )
  }

  const bounds = shapes.flatMap((route) => route.points)
  const defaultCenter = [38.9072, -77.0369]

  return (
    <div className="chart-container system-map">
      <h2>System map</h2>
      <p className="drilldown-anchor" style={{ marginBottom: '0.75rem' }}>
        Routes colored by on-time performance vs target — grey routes have
        no measured data this week. Click a route for detail.
      </p>
      <MapContainer
        center={defaultCenter}
        zoom={11}
        style={{ height: '420px', width: '100%', borderRadius: '0.75rem' }}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {shapes.map((route) => (
          <Polyline
            key={route.route_id}
            positions={route.points}
            pathOptions={{
              color: routeLineColor(byRouteId.get(route.route_id)),
              weight: 3,
              opacity: 0.85,
            }}
            eventHandlers={{ click: () => navigate(`/route/${route.route_id}`) }}
          />
        ))}
        {bounds.length > 0 && <FitBounds bounds={bounds} />}
      </MapContainer>
    </div>
  )
}

export default SystemMap
```

- [ ] **Step 2: Wire into the Overview fold.** In `Overview.jsx`: add `import SystemMap from './SystemMap'` and change the fold div to:

```jsx
      <div className="overview-fold overview-fold-with-map">
        <SystemMap scorecardRoutes={scorecard?.routes ?? null} />
        <MoversPanel routes={scorecard?.routes ?? null} />
      </div>
```

Update the fold comment (drop "the system map ... joins this grid in the next PR"). In `App.css`, extend the fold section:

```css
.overview-fold-with-map {
  grid-template-columns: 3fr 2fr;
  align-items: start;
}

@media (max-width: 900px) {
  .overview-fold-with-map {
    grid-template-columns: 1fr;
  }
}

/* When the map errors, let the quiet note shrink and the movers dominate. */
.overview-fold-with-map .system-map-error {
  align-self: stretch;
}
```

- [ ] **Step 3: Verify** — `cd frontend && npm run lint && npx vitest run && npm run build` → clean.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/SystemMap.jsx frontend/src/components/Overview.jsx frontend/src/App.css
git commit -m "feat: system map joins the Overview fold (NOTES-84 PR 4/4)"
```

### Task 16: e2e determinism + punch-list closure + PR 4

**Files:**
- Create: `frontend/tests/fixtures/api_shapes.json`, `frontend/tests/e2e/helpers/tileStub.js`
- Modify: `frontend/tests/e2e/overview.spec.js`
- Delete: `notes/NOTES-84.md`; Modify: `NOTES.md`, `notes/NOTES-85.md`, `notes/NOTES-86.md`, `notes/NOTES-88.md`

**Interfaces:**
- Consumes: everything above.
- Produces: deterministic Overview e2e (no live tiles); NOTES-84 closed.

- [ ] **Step 1: Shapes fixture** `frontend/tests/fixtures/api_shapes.json` — small but visibly plural:

```json
{
  "routes": [
    { "route_id": "D72", "points": [[38.895, -77.07], [38.905, -77.03], [38.912, -76.99]] },
    { "route_id": "C51", "points": [[38.88, -77.05], [38.89, -77.02], [38.90, -76.98]] },
    { "route_id": "A99", "points": [[38.86, -77.10], [38.87, -77.06], [38.885, -77.02]] }
  ]
}
```

(`D72`/`C51` should exist in `routes_scorecard.json` so the map shows colored lines; `A99` deliberately doesn't → one neutral-grey line in the baseline. Check the fixture's actual route_ids and adjust these three to match: two present, one absent.)

- [ ] **Step 2: Tile stub** `frontend/tests/e2e/helpers/tileStub.js`:

```js
// Deterministic tile stub for map screenshots (NOTES-84 system map).
//
// OSM raster tiles are a live network dependency with nondeterministic
// content — fatal for visual baselines. Every tile request is fulfilled
// with the same solid light-grey 256x256 PNG so polylines render over a
// flat, stable background.
//
// PNG bytes: a 1x1 #eef0f2 PNG (browsers scale it to the 256x256 slot).
const TILE_PNG_BASE64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=='

/**
 * Register a route intercept that answers all OSM tile requests with the
 * stub PNG. Call in beforeEach BEFORE page.goto.
 */
export async function stubMapTiles(page) {
  await page.route('**/*.tile.openstreetmap.org/**', (route) =>
    route.fulfill({
      contentType: 'image/png',
      body: Buffer.from(TILE_PNG_BASE64, 'base64'),
    }),
  )
}
```

(That base64 is a placeholder-colored 1x1 PNG; if the decoded color isn't the intended light grey, regenerate one — `python3 -c "import base64,zlib,struct; ..."` is overkill: just render any solid 1x1 PNG and re-encode. The REQUIREMENT is: valid PNG, solid light color, committed as a constant.)

- [ ] **Step 3: Update `overview.spec.js`.** Import `{ stubMapTiles }` and add to `beforeEach` before the API route registration: `await stubMapTiles(page)`. In the API handler add (before the generic `/api/routes` check):

```js
    if (url.includes('/api/shapes')) {
      return route.fulfill({ json: fixture('api_shapes.json') })
    }
```

Add a rendered-map wait to the visual test and a new smoke test:

```js
test('Overview: system map renders polylines', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByRole('heading', { name: 'System map' })).toBeVisible()
  // Leaflet renders polylines as SVG paths inside the map pane.
  await expect(page.locator('.leaflet-overlay-pane path').first()).toBeVisible()
})
```

and in the visual-regression test, before the screenshot: `await expect(page.locator('.leaflet-overlay-pane path').first()).toBeVisible()`.

- [ ] **Step 4: Punch-list closure (ride-on edits, no separate PR).**

```bash
git rm notes/NOTES-84.md
```

1. `NOTES.md`: delete the NOTES-84 index line (under "WMATA depth & UX").
2. Sweep and rewrite survivors — run exactly:

```bash
grep -rn 'NOTES-84' --include='*.md' --include='*.py' --include='*.tsx' --include='*.ts' --include='*.jsx' --include='*.js' . | grep -v node_modules | grep -v docs/superpowers
```

Expected survivors and their rewrites (verify against the live grep — files drift):
- `notes/NOTES-85.md` ("after NOTES-84"): → "after the Overview editorial redesign (PR #<PR3>/#<PR4>)".
- `notes/NOTES-86.md` ("coordinate placement with NOTES-84"): → "coordinate placement with the editorial Overview layout (PR #<PR3>)".
- `notes/NOTES-88.md`: rewrite its NOTES-84 mention to "the Overview editorial redesign (PR #<PR3>)".
- `NOTES.md` index lines for 85/86 ("after NOTES-84"): same rewrite, one line each.
- `frontend/src/App.jsx` / `AgencyComparison.jsx` comments: PR 2/3 already rewrote or removed these — if the grep still hits them, rewrite to "the NOTES-84 nav collapse" → "the Overview editorial redesign (PR #<PR2>)".
- `docs/superpowers/**` (spec + this plan) are dated historical records — leave untouched (already excluded from the grep above).

Substitute the real PR numbers (PR 2/3 are merged by now; PR 4's number is known after `gh pr create` — do this step's cross-ref rewrite with a placeholder, then amend right after the PR opens, or open the PR first and then push this commit; either order is fine as long as the final push contains real numbers).

- [ ] **Step 5: Full gates**

```bash
uv run pytest -m smoke
uv run pytest            # full suite — this PR closes a multi-surface item
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
cd frontend && npm run lint && npm test && npm run build
npx playwright test tests/e2e/overview.spec.js --grep-invert "visual regression"
cd ..
```

- [ ] **Step 6: Commit, push, open PR 4**

```bash
git add -A
git commit -m "feat: deterministic map e2e + close NOTES-84 (PR 4/4)"
git push -u origin feature/notes-84-pr4-system-map
gh pr create --title "feat: Overview system map + close NOTES-84 (PR 4/4)" --body "$(cat <<'EOF'
Final NOTES-84 PR (spec:
docs/superpowers/specs/2026-08-13-overview-editorial-redesign-design.md).

Adds the SystemMap to the Overview fold: every current route's
representative polyline from /api/shapes (PR 1), colored by OTP vs
target with the same banding as the scorecard spectrum bars, neutral
grey for unmeasured routes, click-through to route detail. The map is
an enhancement, not load-bearing — fetch failure degrades to a quiet
note beside a full-width movers panel.

E2E determinism: OSM tile requests are stubbed with a solid PNG and
/api/shapes served from a fixture, so the Overview baseline contains no
live-network pixels.

Closes NOTES-84: item file deleted, index line removed, surviving
cross-references rewritten to PR anchors. The four-PR sequence
(shapes API #<PR1> → nav collapse #<PR2> → editorial rebuild #<PR3> →
this) is the durable record of the redesign.

Overview baseline regenerates once more (--update-snapshots=all).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

(Fill `#<PR1>`–`#<PR3>` with the real numbers before submitting.)

- [ ] **Step 7: STOP — human checkpoint (baseline + merge).** User regenerates the Overview baseline (both platforms, `--update-snapshots=all`), pushes, CI green, merge. Then `git checkout main && git pull --ff-only` and confirm `grep -rn 'NOTES-84' NOTES.md notes/` returns nothing.

---

## Self-review notes (already applied)

- Spec coverage: hero (T9), compare strip (T9), map (T14-16), movers (T10), smoothing (T6/T8/T11), nav+diagnostics (T4/T5), off-target re-home (T12), bulk endpoint (T1-3), tokens (T9), tile stubbing + shapes fixture (T16), punch-list closure (T16), four serial PRs with user-run regen gates — all mapped.
- Known judgment calls the executor should NOT re-litigate: smoothing is presentation-only (deltas still compute from raw series); `SystemTrend` heading copy change is intentional; `Shape` has no `is_current` (reach current shapes via current trips); MoversPanel keeps the metric list inline per existing convention.
- Verify-don't-trust points called out inline: `Route`/`Trip` constructor kwargs (conftest), `formatDelta` tint strings, `routes_scorecard.json` route_ids, Sparkline jsdom render pattern, tile-PNG color.
