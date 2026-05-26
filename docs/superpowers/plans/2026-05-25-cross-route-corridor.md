# Cross-route corridor diagnostic V2 — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a directional corridor rollup over PR #140's stop-pair segment diagnostic so a planner sees "Wisconsin Ave SB: Friendship Heights → Foggy Bottom" as one investment-target row rather than N adjacent stop-pairs. Surfaced via a toggle on the existing `/segments` page with a static map preview per corridor.

**Architecture:** Two-table identity (`corridors` + `corridor_route_membership`) refreshed only on GTFS reload from `shapes` via Python-side proximity-plus-bearing matching. One slip rollup table (`corridor_slip_rollup`) refreshed nightly by joining the membership table against the existing `route_diagnostic_segment` per-route per-stop-pair source. Static PNG previews rendered once per corridor at GTFS-reload time and served via FastAPI's StaticFiles mount. Frontend reuses `SegmentDiagnostic.jsx` with a level toggle.

**Tech Stack:** Python 3.12 + SQLAlchemy + Postgres (Python-side shape matching, no PostGIS); FastAPI + Pydantic; React + Vite (JSX, not TSX); `staticmap` library for PNG rendering.

**Source spec:** `docs/superpowers/specs/2026-05-25-cross-route-corridor-design.md`

**Verified facts** (Section 9 of the spec):
- Per-route source: `RouteDiagnosticSegment` in `src/models.py:977`, table `route_diagnostic_segment`. Columns: `route_id`, `direction_id`, `period`, `from_seq`, `from_stop_id`, `to_seq`, `to_stop_id`, `mean_slip_sec`, `cum_slip_sec`, `n_observations`, `is_timepoint`, `computed_at`.
- Frontend page: `frontend/src/components/SegmentDiagnostic.jsx`, routed at `/segments` in `frontend/src/App.jsx:75`.
- Migrations: `scripts/migrate_create_<table>.py` + orchestrator `scripts/migrate_all.py`.
- Periods: `ALL_PERIODS` in `src/route_diagnostics.py:78`, re-exported as `DIAGNOSTIC_PERIODS` in `api/main.py:40`.
- Upsert helper: `src/upsert_helpers.py:upsert_rows`.
- Existing API endpoint: `api/main.py:1086` (`get_segments`) → `api/aggregations.py:4493` (`get_cross_route_segments`).

---

## Phase 1 — Corridor identity foundation

Build the data model and the pure-Python matching algorithm. No DB writes yet beyond migrations and ORM. End of phase 1: `corridors` and `corridor_route_membership` tables exist; matching algorithm is unit-tested.

---

### Task 1: ORM models for `Corridor` and `CorridorRouteMembership`

**Files:**
- Modify: `src/models.py` — append two new ORM classes after `CrossRouteSegmentRollup` (around line 1259).

- [ ] **Step 1: Write the failing test**

Create `tests/test_corridor_models.py`:

```python
"""Smoke tests for Corridor + CorridorRouteMembership ORM models."""
from src.models import Corridor, CorridorRouteMembership


def test_corridor_tablename():
    """Corridor maps to the corridors table."""
    assert Corridor.__tablename__ == "corridors"


def test_corridor_route_membership_tablename():
    """CorridorRouteMembership maps to the corridor_route_membership table."""
    assert CorridorRouteMembership.__tablename__ == "corridor_route_membership"


def test_corridor_required_columns():
    """Corridor declares every column from the spec."""
    columns = {c.name for c in Corridor.__table__.columns}
    expected = {
        "corridor_id", "direction_bearing_deg", "direction_cardinal",
        "start_stop_id", "end_stop_id", "length_m", "n_routes",
        "route_set", "display_name", "geometry_wkt", "gtfs_snapshot_id",
        "created_at",
    }
    assert expected.issubset(columns), f"Missing: {expected - columns}"


def test_corridor_route_membership_required_columns():
    """CorridorRouteMembership declares every column from the spec."""
    columns = {c.name for c in CorridorRouteMembership.__table__.columns}
    expected = {
        "corridor_id", "route_id", "direction_id", "canonical_shape_id",
        "start_stop_sequence", "end_stop_sequence",
    }
    assert expected.issubset(columns), f"Missing: {expected - columns}"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_corridor_models.py -v
```

Expected: `ImportError: cannot import name 'Corridor' from 'src.models'`.

- [ ] **Step 3: Add the ORM classes**

Append to `src/models.py` after `CrossRouteSegmentRollup` (end of the class, around line 1259):

```python
# ---------------------------------------------------------------------------
# Cross-route corridor diagnostic V2 (NOTES-62)
# ---------------------------------------------------------------------------
#
# `corridors` + `corridor_route_membership` are pure-shape-derived. They
# rebuild atomically on GTFS reload via `pipelines/refresh_corridors.py`.
# `corridor_slip_rollup` is refreshed nightly from `route_diagnostic_segment`
# by `pipelines/refresh_corridor_slip.py`.
#
# Spec: docs/superpowers/specs/2026-05-25-cross-route-corridor-design.md


class Corridor(Base):
    """
    A directional cross-route corridor: a contiguous stretch of street
    where >=2 routes' canonical shapes run within 15m of each other and
    within 30 degrees of bearing. Identified from `shapes` alone — no
    OSM dependency. Refreshed only on GTFS reload.

    `route_set` is the sorted JSONB array of contributing route_ids
    (denormalized for API convenience). `corridor_route_membership` is
    the authoritative per-route membership table with stop_sequence
    ranges for slip aggregation.

    `direction_cardinal` is derived from `direction_bearing_deg` at
    pipeline time (NB: 337.5-22.5, NE: 22.5-67.5, ..., NW: 292.5-337.5).
    Stored denormalized for API filtering.

    `display_name` is the stop-anchored label, e.g.
    "Wisconsin Ave SB: Friendship Heights -> Foggy Bottom". Generated
    at pipeline time from `start_stop_id` + `end_stop_id` + cardinal.

    NOTES-62.
    """

    __tablename__ = "corridors"

    corridor_id = Column(Integer, primary_key=True, autoincrement=True)
    direction_bearing_deg = Column(Float, nullable=False)
    direction_cardinal = Column(String, nullable=False)
    start_stop_id = Column(String, nullable=False)
    end_stop_id = Column(String, nullable=False)
    length_m = Column(Float, nullable=False)
    n_routes = Column(Integer, nullable=False)
    route_set = Column(JSON, nullable=False)  # JSONB on Postgres; sorted array
    display_name = Column(String, nullable=False)
    geometry_wkt = Column(Text, nullable=False)  # LINESTRING(lon lat, lon lat, ...)
    gtfs_snapshot_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=utcnow_naive)

    __table_args__ = (
        UniqueConstraint(
            "direction_cardinal",
            "start_stop_id",
            "end_stop_id",
            "route_set",
            name="uq_corridor_identity",
        ),
        Index("idx_corridor_cardinal", "direction_cardinal"),
        Index("idx_corridor_n_routes", "n_routes"),
    )


class CorridorRouteMembership(Base):
    """
    Per-(corridor, route) join table. Encodes which route_id contributes
    to a corridor, which GTFS direction_id participates, and the
    stop_sequence range of that route's canonical trip that falls inside
    the corridor's stop bounds.

    Used by `pipelines/refresh_corridor_slip.py` as the authoritative
    join target against `route_diagnostic_segment`.

    NOTES-62.
    """

    __tablename__ = "corridor_route_membership"

    corridor_id = Column(
        Integer,
        ForeignKey("corridors.corridor_id", ondelete="CASCADE"),
        primary_key=True,
    )
    route_id = Column(String, primary_key=True)
    direction_id = Column(Integer, nullable=False)
    canonical_shape_id = Column(String, nullable=False)
    start_stop_sequence = Column(Integer, nullable=False)
    end_stop_sequence = Column(Integer, nullable=False)

    __table_args__ = (
        Index("idx_corridor_route_membership_route", "route_id"),
    )
```

Confirm the imports at the top of `src/models.py` include `JSON`, `Text`, `ForeignKey`, `Index`, `UniqueConstraint`. They do — already used by other classes.

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_corridor_models.py -v
```

Expected: 4 passing.

- [ ] **Step 5: Run smoke + drift check**

```bash
uv run pytest -m smoke
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
```

Expected: all green. (No migration yet, so `check_schema_drift.py` will not yet detect the new tables; that's wired in Task 2.)

- [ ] **Step 6: Commit**

```bash
git add src/models.py tests/test_corridor_models.py
git commit -m "feat(models): add Corridor + CorridorRouteMembership ORM (NOTES-62)"
```

---

### Task 2: Migration script for `corridors` + `corridor_route_membership`

**Files:**
- Create: `scripts/migrate_create_corridors.py`
- Modify: `scripts/migrate_all.py` (append call to new migration)
- Modify: `scripts/check_schema_drift.py` (if it has a manual table list — verify)

- [ ] **Step 1: Read an existing migration script for the convention**

```bash
cat scripts/migrate_create_cross_route_segments.py | head -80
```

Note the convention: idempotent (check if table exists, skip if so), uses `sqlalchemy.text` for raw DDL, logs to stdout.

- [ ] **Step 2: Write the migration**

Create `scripts/migrate_create_corridors.py`:

```python
"""
One-shot migration: create `corridors` and `corridor_route_membership`
tables for NOTES-62.

Idempotent: skips if tables already exist. Run via
`scripts/migrate_all.py` or directly:

    uv run python scripts/migrate_create_corridors.py
"""
from __future__ import annotations

import sys

from sqlalchemy import inspect, text

from src.database import get_engine


def migrate() -> bool:
    """Create both corridor tables if they don't exist. Return True if anything changed."""
    engine = get_engine()
    inspector = inspect(engine)
    changed = False

    with engine.begin() as conn:
        if "corridors" in inspector.get_table_names():
            print("[migrate_corridors] corridors already exists; skipping")
        else:
            print("[migrate_corridors] creating corridors")
            conn.execute(
                text(
                    """
                    CREATE TABLE corridors (
                        corridor_id          SERIAL PRIMARY KEY,
                        direction_bearing_deg REAL NOT NULL,
                        direction_cardinal   TEXT NOT NULL,
                        start_stop_id        TEXT NOT NULL,
                        end_stop_id          TEXT NOT NULL,
                        length_m             REAL NOT NULL,
                        n_routes             INTEGER NOT NULL,
                        route_set            JSONB NOT NULL,
                        display_name         TEXT NOT NULL,
                        geometry_wkt         TEXT NOT NULL,
                        gtfs_snapshot_id     INTEGER NOT NULL,
                        created_at           TIMESTAMP NOT NULL DEFAULT NOW(),
                        CONSTRAINT uq_corridor_identity
                            UNIQUE (direction_cardinal, start_stop_id, end_stop_id, route_set)
                    );
                    CREATE INDEX idx_corridor_cardinal ON corridors (direction_cardinal);
                    CREATE INDEX idx_corridor_n_routes ON corridors (n_routes);
                    """
                )
            )
            changed = True

        if "corridor_route_membership" in inspector.get_table_names():
            print(
                "[migrate_corridors] corridor_route_membership already exists; skipping"
            )
        else:
            print("[migrate_corridors] creating corridor_route_membership")
            conn.execute(
                text(
                    """
                    CREATE TABLE corridor_route_membership (
                        corridor_id            INTEGER NOT NULL
                            REFERENCES corridors(corridor_id) ON DELETE CASCADE,
                        route_id               TEXT NOT NULL,
                        direction_id           INTEGER NOT NULL,
                        canonical_shape_id     TEXT NOT NULL,
                        start_stop_sequence    INTEGER NOT NULL,
                        end_stop_sequence      INTEGER NOT NULL,
                        PRIMARY KEY (corridor_id, route_id)
                    );
                    CREATE INDEX idx_corridor_route_membership_route
                        ON corridor_route_membership (route_id);
                    """
                )
            )
            changed = True

    return changed


if __name__ == "__main__":
    changed = migrate()
    sys.exit(0 if changed else 0)  # always exit 0; idempotent
```

- [ ] **Step 3: Wire into the orchestrator**

Read `scripts/migrate_all.py`:

```bash
grep -n "migrate_create" scripts/migrate_all.py
```

Append a call after the most recent migration (likely `migrate_create_cross_route_segments`). Add the import and the call following the existing pattern. If the orchestrator uses a simple sequential list, add the new migration as the last entry.

- [ ] **Step 4: Run the migration against the dev DB**

```bash
uv run python scripts/migrate_create_corridors.py
```

Expected: two `creating ...` lines, then exit. Verify:

```bash
psql -d wmata_dashboard -c "\d corridors"
psql -d wmata_dashboard -c "\d corridor_route_membership"
```

Both should show the columns + indexes + FK constraint.

- [ ] **Step 5: Verify idempotence**

```bash
uv run python scripts/migrate_create_corridors.py
```

Expected: two `... already exists; skipping` lines.

- [ ] **Step 6: Run drift check + smoke**

```bash
uv run python scripts/check_schema_drift.py
uv run pytest -m smoke
uv run ruff check scripts/
uv run ruff format --check scripts/
```

Expected: drift check clean (ORM matches DB), smoke green, ruff green.

- [ ] **Step 7: Commit**

```bash
git add scripts/migrate_create_corridors.py scripts/migrate_all.py
git commit -m "feat(db): migration for corridors + corridor_route_membership (NOTES-62)"
```

---

### Task 3: Bearing helper module

**Files:**
- Create: `src/corridor_identity.py`
- Create: `tests/test_corridor_identity.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_corridor_identity.py`:

```python
"""Unit tests for corridor identity helpers."""
import math

import pytest

from src.corridor_identity import (
    bearing_degrees,
    bearing_circular_distance,
    haversine_meters,
)


# Known reference points around D.C.
LINCOLN_MEMORIAL = (38.8893, -77.0502)
WASHINGTON_MONUMENT = (38.8895, -77.0353)  # ~1.3 km east
WHITE_HOUSE = (38.8977, -77.0365)  # ~1 km north of Wash Monument


def test_haversine_zero_distance():
    """Same point has zero distance."""
    assert haversine_meters(*LINCOLN_MEMORIAL, *LINCOLN_MEMORIAL) == pytest.approx(0.0, abs=0.01)


def test_haversine_known_distance():
    """Lincoln Memorial to Washington Monument is ~1.3 km."""
    dist = haversine_meters(*LINCOLN_MEMORIAL, *WASHINGTON_MONUMENT)
    assert dist == pytest.approx(1290, abs=30)


def test_bearing_due_east():
    """Bearing from Lincoln Memorial to Washington Monument is ~90 degrees (due east)."""
    b = bearing_degrees(*LINCOLN_MEMORIAL, *WASHINGTON_MONUMENT)
    assert b == pytest.approx(90.0, abs=2.0)


def test_bearing_due_north():
    """Bearing from Wash Monument to White House is ~0 degrees (due north)."""
    b = bearing_degrees(*WASHINGTON_MONUMENT, *WHITE_HOUSE)
    assert b == pytest.approx(0.0, abs=5.0)


def test_bearing_due_south():
    """Bearing from White House to Wash Monument is ~180 degrees (due south)."""
    b = bearing_degrees(*WHITE_HOUSE, *WASHINGTON_MONUMENT)
    assert b == pytest.approx(180.0, abs=5.0)


def test_bearing_circular_distance_simple():
    """Bearings 10 and 350 are 20 degrees apart, not 340."""
    assert bearing_circular_distance(10, 350) == pytest.approx(20.0)


def test_bearing_circular_distance_opposite():
    """Bearings 0 and 180 are 180 degrees apart."""
    assert bearing_circular_distance(0, 180) == pytest.approx(180.0)


def test_bearing_circular_distance_same():
    """Same bearing has zero distance."""
    assert bearing_circular_distance(45, 45) == 0.0


def test_bearing_circular_distance_wraps():
    """Bearings 359 and 1 are 2 degrees apart."""
    assert bearing_circular_distance(359, 1) == pytest.approx(2.0)
```

- [ ] **Step 2: Run tests to verify failure**

```bash
uv run pytest tests/test_corridor_identity.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement the helpers**

Create `src/corridor_identity.py`:

```python
"""
Corridor identity algorithm — NOTES-62.

Pure-Python helpers for the shape-matching pipeline. Bearing computation,
haversine distance, and proximity-plus-bearing match logic. No DB calls
here; the orchestrating pipeline pulls shape data, calls these helpers,
and writes results.

Spec: docs/superpowers/specs/2026-05-25-cross-route-corridor-design.md
"""
from __future__ import annotations

import math

# Calibration knobs (Section 5 of the spec).
SHAPE_PROXIMITY_THRESHOLD_M = 15.0
BEARING_AGREEMENT_THRESHOLD_DEG = 30.0
MIN_CORRIDOR_LENGTH_M = 500.0
MIN_RUN_POINTS = 5
ENDPOINT_STOP_SNAP_M = 100.0

EARTH_RADIUS_M = 6_371_000.0


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters between two (lat, lon) points."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def bearing_degrees(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing in degrees (0-360) from point 1 to point 2."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    y = math.sin(dlambda) * math.cos(phi2)
    x = (
        math.cos(phi1) * math.sin(phi2)
        - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    )
    theta = math.atan2(y, x)
    return (math.degrees(theta) + 360.0) % 360.0


def bearing_circular_distance(b1: float, b2: float) -> float:
    """Smallest angular distance between two bearings in degrees (0-180)."""
    diff = abs(b1 - b2) % 360.0
    return min(diff, 360.0 - diff)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_corridor_identity.py -v
```

Expected: 9 passing.

- [ ] **Step 5: Commit**

```bash
git add src/corridor_identity.py tests/test_corridor_identity.py
git commit -m "feat(corridor): haversine + bearing helpers (NOTES-62)"
```

---

### Task 4: Canonical shape selection + bearing-augmented shape rows

**Files:**
- Modify: `src/corridor_identity.py` (add functions)
- Modify: `tests/test_corridor_identity.py` (add tests)

- [ ] **Step 1: Add failing tests**

Append to `tests/test_corridor_identity.py`:

```python
from src.corridor_identity import (
    augment_shape_with_bearings,
    pick_canonical_shapes,
)


def test_augment_shape_with_bearings_basic():
    """Bearings computed along a simple eastward shape; last point uses look-back."""
    points = [
        (38.89, -77.05, 1),
        (38.89, -77.04, 2),  # ~870m east
        (38.89, -77.03, 3),  # ~870m east again
    ]
    augmented = augment_shape_with_bearings(points)

    assert len(augmented) == 3
    # All three points heading east; bearings ~90.
    for lat, lon, seq, bearing in augmented:
        assert bearing == pytest.approx(90.0, abs=2.0)


def test_augment_shape_with_bearings_lookback_at_end():
    """Last point's bearing falls back to i-1 -> i."""
    points = [
        (38.89, -77.05, 1),
        (38.89, -77.04, 2),  # heading east
    ]
    augmented = augment_shape_with_bearings(points)
    # Last point's bearing should also be ~90 (look-back uses point 0 -> point 1).
    assert augmented[-1][3] == pytest.approx(90.0, abs=2.0)


def test_pick_canonical_shapes_highest_trip_count_per_direction():
    """For each (route_id, direction_id), pick the highest-trip-count shape."""
    # Synthetic input: route X has two shapes per direction, route Y has one shape.
    trip_shape_counts = [
        # (route_id, direction_id, shape_id, n_trips)
        ("X", 0, "X:01", 100),
        ("X", 0, "X:02", 200),  # winner for X dir 0
        ("X", 1, "X:51", 150),  # winner for X dir 1 (only one)
        ("Y", 0, "Y:01", 50),   # winner for Y dir 0 (only one)
    ]

    canonical = pick_canonical_shapes(trip_shape_counts)

    assert canonical == {
        ("X", 0): "X:02",
        ("X", 1): "X:51",
        ("Y", 0): "Y:01",
    }
```

- [ ] **Step 2: Run tests to verify failure**

```bash
uv run pytest tests/test_corridor_identity.py -v -k "canonical or bearings"
```

Expected: `ImportError`.

- [ ] **Step 3: Implement the functions**

Append to `src/corridor_identity.py`:

```python
def augment_shape_with_bearings(
    points: list[tuple[float, float, int]],
) -> list[tuple[float, float, int, float]]:
    """
    Compute the local bearing at every shape point.

    Input rows: (lat, lon, shape_pt_sequence) sorted by sequence.
    Output rows: (lat, lon, shape_pt_sequence, bearing_deg).

    Bearing at point i is the compass angle from i to i+1. At the last
    point, look back: bearing from i-1 to i. Single-point shapes are
    invalid input and will raise.
    """
    if len(points) < 2:
        raise ValueError("Shape must have at least 2 points to compute bearings")

    result: list[tuple[float, float, int, float]] = []
    for i, (lat, lon, seq) in enumerate(points):
        if i < len(points) - 1:
            next_lat, next_lon, _ = points[i + 1]
            bearing = bearing_degrees(lat, lon, next_lat, next_lon)
        else:
            prev_lat, prev_lon, _ = points[i - 1]
            bearing = bearing_degrees(prev_lat, prev_lon, lat, lon)
        result.append((lat, lon, seq, bearing))
    return result


def pick_canonical_shapes(
    trip_shape_counts: list[tuple[str, int, str, int]],
) -> dict[tuple[str, int], str]:
    """
    For each (route_id, direction_id), pick the shape_id with the
    highest trip count as the canonical representative.

    Input rows: (route_id, direction_id, shape_id, n_trips).
    Output: mapping (route_id, direction_id) -> canonical shape_id.

    Ties broken by lexicographic shape_id (deterministic).
    """
    # Group by (route_id, direction_id), keep max trips with shape_id tiebreaker.
    best: dict[tuple[str, int], tuple[int, str]] = {}
    for route_id, direction_id, shape_id, n_trips in trip_shape_counts:
        key = (route_id, direction_id)
        candidate = (n_trips, -ord(shape_id[0]) if shape_id else 0, shape_id)
        # Simpler: take max by n_trips, then min shape_id for stability.
        if key not in best:
            best[key] = (n_trips, shape_id)
        else:
            current_trips, current_shape = best[key]
            if n_trips > current_trips or (
                n_trips == current_trips and shape_id < current_shape
            ):
                best[key] = (n_trips, shape_id)
    return {key: shape_id for key, (_, shape_id) in best.items()}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_corridor_identity.py -v
```

Expected: 12 passing (9 from Task 3 + 3 new).

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/corridor_identity.py tests/test_corridor_identity.py
uv run ruff format --check src/corridor_identity.py tests/test_corridor_identity.py
git add src/corridor_identity.py tests/test_corridor_identity.py
git commit -m "feat(corridor): bearings + canonical shape selection (NOTES-62)"
```

---

### Task 5: Proximity-plus-bearing colocation matcher

**Files:**
- Modify: `src/corridor_identity.py`
- Modify: `tests/test_corridor_identity.py`

- [ ] **Step 1: Add failing tests**

Append to `tests/test_corridor_identity.py`:

```python
from src.corridor_identity import compute_colocated_route_sets


def test_colocated_routes_two_routes_same_street_same_direction():
    """Two shapes running side-by-side in the same direction colocate everywhere."""
    # Route A and Route B both head east along the same coords, sub-meter apart.
    shape_a = [(38.89, -77.05, 1, 90.0), (38.89, -77.04, 2, 90.0)]
    shape_b = [(38.890001, -77.05, 1, 90.0), (38.890001, -77.04, 2, 90.0)]

    result = compute_colocated_route_sets(
        canonical_shapes={
            ("A", 0): ("A:01", shape_a),
            ("B", 0): ("B:01", shape_b),
        }
    )

    # Each (route, dir, seq) maps to the set of OTHER colocated (route, dir) pairs.
    assert ("B", 0) in result[("A", 0, 1)]
    assert ("B", 0) in result[("A", 0, 2)]
    assert ("A", 0) in result[("B", 0, 1)]


def test_colocated_routes_opposite_directions_dont_match():
    """Two shapes on the same street in opposite directions do NOT colocate."""
    # Route A goes east; Route B goes west along the same coords.
    shape_a = [(38.89, -77.05, 1, 90.0), (38.89, -77.04, 2, 90.0)]
    shape_b = [(38.89, -77.04, 1, 270.0), (38.89, -77.05, 2, 270.0)]

    result = compute_colocated_route_sets(
        canonical_shapes={
            ("A", 0): ("A:01", shape_a),
            ("B", 0): ("B:01", shape_b),
        }
    )

    # Bearings differ by 180 degrees > 30; no matches.
    assert result[("A", 0, 1)] == set()
    assert result[("A", 0, 2)] == set()
    assert result[("B", 0, 1)] == set()
    assert result[("B", 0, 2)] == set()


def test_colocated_routes_parallel_streets_dont_match():
    """Two parallel streets (~80m apart) do not colocate."""
    # Route A on lat 38.89, Route B on lat 38.8908 (~85m north). Same bearing.
    shape_a = [(38.89, -77.05, 1, 90.0), (38.89, -77.04, 2, 90.0)]
    shape_b = [(38.8908, -77.05, 1, 90.0), (38.8908, -77.04, 2, 90.0)]

    result = compute_colocated_route_sets(
        canonical_shapes={
            ("A", 0): ("A:01", shape_a),
            ("B", 0): ("B:01", shape_b),
        }
    )

    # 85m > 15m; no matches.
    assert result[("A", 0, 1)] == set()
    assert result[("B", 0, 1)] == set()
```

- [ ] **Step 2: Run tests to verify failure**

```bash
uv run pytest tests/test_corridor_identity.py -v -k "colocated"
```

Expected: `ImportError`.

- [ ] **Step 3: Implement the matcher**

Append to `src/corridor_identity.py`:

```python
# Type alias for clarity.
ShapeKey = tuple[str, int]  # (route_id, direction_id)
PointKey = tuple[str, int, int]  # (route_id, direction_id, shape_pt_sequence)


def compute_colocated_route_sets(
    canonical_shapes: dict[ShapeKey, tuple[str, list[tuple[float, float, int, float]]]],
) -> dict[PointKey, set[ShapeKey]]:
    """
    For each canonical shape point, return the set of OTHER (route_id,
    direction_id) shapes that pass within SHAPE_PROXIMITY_THRESHOLD_M
    AND within BEARING_AGREEMENT_THRESHOLD_DEG of bearing.

    Input: mapping (route_id, direction_id) -> (canonical_shape_id, bearing-augmented points).
    Output: mapping (route_id, direction_id, shape_pt_sequence) -> set of (route_id, direction_id) keys.

    Algorithm: bucket each point into a ~30m x 30m grid cell; for each
    point, scan the 9 neighbor cells (self + 8 surrounding) for
    candidate matches; apply exact haversine + bearing test.

    O(N) expected where N = total point count, given uniform spatial
    distribution across the grid. For ~503k points and ~30m bucket
    size, the per-cell load is small.
    """
    # 30m grid: at D.C.'s latitude, 1 degree latitude ~= 111 km.
    # 30m / 111000 m/deg ~= 0.00027 deg.
    grid_size_deg = 0.00027

    # Build grid: cell_key -> list of (route_id, direction_id, seq, lat, lon, bearing)
    grid: dict[tuple[int, int], list[tuple[str, int, int, float, float, float]]] = {}
    for (route_id, direction_id), (_shape_id, points) in canonical_shapes.items():
        for lat, lon, seq, bearing in points:
            cell = (int(lat / grid_size_deg), int(lon / grid_size_deg))
            grid.setdefault(cell, []).append(
                (route_id, direction_id, seq, lat, lon, bearing)
            )

    result: dict[PointKey, set[ShapeKey]] = {}

    for (route_id, direction_id), (_shape_id, points) in canonical_shapes.items():
        for lat, lon, seq, bearing in points:
            point_key: PointKey = (route_id, direction_id, seq)
            colocated: set[ShapeKey] = set()

            cell = (int(lat / grid_size_deg), int(lon / grid_size_deg))
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    neighbors = grid.get((cell[0] + dx, cell[1] + dy), [])
                    for (
                        other_route_id,
                        other_direction_id,
                        _other_seq,
                        other_lat,
                        other_lon,
                        other_bearing,
                    ) in neighbors:
                        if (other_route_id, other_direction_id) == (
                            route_id,
                            direction_id,
                        ):
                            continue
                        if (
                            haversine_meters(lat, lon, other_lat, other_lon)
                            >= SHAPE_PROXIMITY_THRESHOLD_M
                        ):
                            continue
                        if (
                            bearing_circular_distance(bearing, other_bearing)
                            >= BEARING_AGREEMENT_THRESHOLD_DEG
                        ):
                            continue
                        colocated.add((other_route_id, other_direction_id))

            result[point_key] = colocated

    return result
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_corridor_identity.py -v
```

Expected: 15 passing.

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/corridor_identity.py tests/test_corridor_identity.py
uv run ruff format --check src/corridor_identity.py tests/test_corridor_identity.py
git add src/corridor_identity.py tests/test_corridor_identity.py
git commit -m "feat(corridor): proximity+bearing colocation matcher (NOTES-62)"
```

---

### Task 6: Run-length encoding + corridor finalization

**Files:**
- Modify: `src/corridor_identity.py`
- Modify: `tests/test_corridor_identity.py`

- [ ] **Step 1: Add failing tests**

Append to `tests/test_corridor_identity.py`:

```python
from src.corridor_identity import (
    extract_runs,
    bearing_to_cardinal,
    Run,
)


def test_extract_runs_single_run():
    """A continuous stretch with the same route_set produces one run."""
    # Route A, direction 0, sequence 1..5; colocates with B throughout.
    point_keys = [("A", 0, i) for i in range(1, 6)]
    points = [(38.89, -77.05 + 0.001 * i, i, 90.0) for i in range(1, 6)]
    colocated = {pk: {("B", 0)} for pk in point_keys}

    runs = extract_runs(
        route_id="A",
        direction_id=0,
        canonical_shape_id="A:01",
        points=points,
        colocated=colocated,
    )

    assert len(runs) == 1
    assert runs[0].route_set == frozenset({("A", 0), ("B", 0)})
    assert len(runs[0].points) == 5


def test_extract_runs_break_on_routeset_change():
    """A change in colocated route set breaks the run."""
    points = [(38.89, -77.05 + 0.001 * i, i, 90.0) for i in range(1, 11)]
    point_keys = [("A", 0, i) for i in range(1, 11)]

    # Seq 1-5: colocates with B. Seq 6-10: colocates with B AND C.
    colocated = {}
    for i in range(1, 6):
        colocated[("A", 0, i)] = {("B", 0)}
    for i in range(6, 11):
        colocated[("A", 0, i)] = {("B", 0), ("C", 0)}

    runs = extract_runs(
        route_id="A",
        direction_id=0,
        canonical_shape_id="A:01",
        points=points,
        colocated=colocated,
    )

    assert len(runs) == 2
    assert runs[0].route_set == frozenset({("A", 0), ("B", 0)})
    assert runs[1].route_set == frozenset({("A", 0), ("B", 0), ("C", 0)})


def test_extract_runs_discard_single_route():
    """Runs where the route is alone (|set| < 2 including self) are dropped."""
    points = [(38.89, -77.05 + 0.001 * i, i, 90.0) for i in range(1, 6)]
    colocated = {("A", 0, i): set() for i in range(1, 6)}  # nobody else colocates

    runs = extract_runs(
        route_id="A",
        direction_id=0,
        canonical_shape_id="A:01",
        points=points,
        colocated=colocated,
    )

    assert runs == []


def test_extract_runs_discard_too_short():
    """Runs with fewer than MIN_RUN_POINTS (5) are dropped."""
    points = [(38.89, -77.05 + 0.001 * i, i, 90.0) for i in range(1, 5)]  # 4 points
    colocated = {("A", 0, i): {("B", 0)} for i in range(1, 5)}

    runs = extract_runs(
        route_id="A",
        direction_id=0,
        canonical_shape_id="A:01",
        points=points,
        colocated=colocated,
    )

    assert runs == []


def test_bearing_to_cardinal_eight_directions():
    """Every cardinal/intercardinal sector maps correctly."""
    cases = [
        (0, "N"), (22, "N"),
        (45, "NE"), (67, "NE"),
        (90, "E"), (112, "E"),
        (135, "SE"), (157, "SE"),
        (180, "S"), (202, "S"),
        (225, "SW"), (247, "SW"),
        (270, "W"), (292, "W"),
        (315, "NW"), (337, "NW"),
        (350, "N"),  # wraps back to N
    ]
    for bearing, expected in cases:
        assert bearing_to_cardinal(bearing) == expected, f"{bearing} -> got {bearing_to_cardinal(bearing)}, expected {expected}"
```

- [ ] **Step 2: Run tests to verify failure**

```bash
uv run pytest tests/test_corridor_identity.py -v -k "extract_runs or cardinal"
```

Expected: `ImportError`.

- [ ] **Step 3: Implement runs + cardinal**

Append to `src/corridor_identity.py`:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class Run:
    """A continuous stretch of one route's canonical shape that colocates with the same route_set."""

    route_id: str
    direction_id: int
    canonical_shape_id: str
    # frozenset of (route_id, direction_id) including this run's own route
    route_set: frozenset[tuple[str, int]]
    # (lat, lon, shape_pt_sequence, bearing_deg)
    points: tuple[tuple[float, float, int, float], ...]

    @property
    def mean_bearing_deg(self) -> float:
        """Arithmetic mean of point bearings; OK at sub-corridor scales."""
        return sum(p[3] for p in self.points) / len(self.points)

    @property
    def length_m(self) -> float:
        """Sum of consecutive haversine distances along the run."""
        total = 0.0
        for i in range(len(self.points) - 1):
            lat1, lon1, _, _ = self.points[i]
            lat2, lon2, _, _ = self.points[i + 1]
            total += haversine_meters(lat1, lon1, lat2, lon2)
        return total


def extract_runs(
    route_id: str,
    direction_id: int,
    canonical_shape_id: str,
    points: list[tuple[float, float, int, float]],
    colocated: dict[PointKey, set[ShapeKey]],
) -> list[Run]:
    """
    Walk a canonical shape in sequence; emit runs at every change in the
    colocated route set. Drop runs where |route_set| < 2 (route is alone)
    or |points| < MIN_RUN_POINTS (too short / grazing match).
    """
    if not points:
        return []

    runs: list[Run] = []
    own_key: ShapeKey = (route_id, direction_id)

    current_set: frozenset[ShapeKey] | None = None
    current_points: list[tuple[float, float, int, float]] = []

    def flush() -> None:
        if (
            current_set is not None
            and len(current_set) >= 2
            and len(current_points) >= MIN_RUN_POINTS
        ):
            runs.append(
                Run(
                    route_id=route_id,
                    direction_id=direction_id,
                    canonical_shape_id=canonical_shape_id,
                    route_set=current_set,
                    points=tuple(current_points),
                )
            )

    for point in points:
        _lat, _lon, seq, _bearing = point
        point_key: PointKey = (route_id, direction_id, seq)
        others = colocated.get(point_key, set())
        full_set = frozenset(others | {own_key})

        if full_set != current_set:
            flush()
            current_set = full_set
            current_points = [point]
        else:
            current_points.append(point)

    flush()
    return runs


_CARDINAL_BINS = [
    (22.5, "N"),
    (67.5, "NE"),
    (112.5, "E"),
    (157.5, "SE"),
    (202.5, "S"),
    (247.5, "SW"),
    (292.5, "W"),
    (337.5, "NW"),
    (360.0, "N"),  # wrap-around
]


def bearing_to_cardinal(bearing_deg: float) -> str:
    """Map a bearing in [0, 360) to one of 8 cardinal/intercardinal labels."""
    b = bearing_deg % 360.0
    for upper, label in _CARDINAL_BINS:
        if b < upper:
            return label
    return "N"  # defensive; shouldn't be reachable since 360.0 is the last bin
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_corridor_identity.py -v
```

Expected: 20 passing.

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/corridor_identity.py tests/test_corridor_identity.py
uv run ruff format --check src/corridor_identity.py tests/test_corridor_identity.py
git add src/corridor_identity.py tests/test_corridor_identity.py
git commit -m "feat(corridor): run extraction + cardinal mapping (NOTES-62)"
```

---

## Phase 2 — Identity pipeline

Wire the algorithm to the database. End of phase 2: `corridors` + `corridor_route_membership` are populated from real WMATA shapes; pipeline runs in the GTFS reload transaction.

---

### Task 7: Stop snapping + corridor finalization

**Files:**
- Modify: `src/corridor_identity.py`
- Modify: `tests/test_corridor_identity.py`

- [ ] **Step 1: Add failing tests**

Append to `tests/test_corridor_identity.py`:

```python
from src.corridor_identity import (
    snap_run_to_stops,
    StopInfo,
    build_display_name,
    StopRef,
)


def test_snap_run_to_stops_picks_nearest_shared_stop():
    """Endpoints snap to the nearest stop served by every route in the run's route_set."""
    # Run from west to east along a fictional Wisconsin Ave segment.
    points = [(38.94, -77.07 + 0.001 * i, i, 90.0) for i in range(10)]

    # Stops: stop_S near point 0 (served by A and B), stop_E near point 9 (served by A and B).
    # Plus extra stops that aren't on the shared route set.
    stops = {
        "stop_S": StopInfo(stop_id="stop_S", stop_name="Friendship Heights", lat=38.94, lon=-77.07),
        "stop_E": StopInfo(stop_id="stop_E", stop_name="Foggy Bottom", lat=38.94, lon=-77.061),
        "stop_other": StopInfo(stop_id="stop_other", stop_name="Some other", lat=38.93, lon=-77.07),
    }

    # Each route serves which stops? A: stop_S, stop_E. B: stop_S, stop_E. Plus an irrelevant route C with different stops.
    route_stops = {
        ("A", 0): [("stop_S", 1), ("stop_E", 2)],  # (stop_id, stop_sequence)
        ("B", 0): [("stop_S", 1), ("stop_E", 2)],
    }

    result = snap_run_to_stops(
        route_set=frozenset({("A", 0), ("B", 0)}),
        run_points=points,
        stops=stops,
        route_stops=route_stops,
    )

    assert result is not None
    start, end, per_route = result
    assert start.stop_id == "stop_S"
    assert end.stop_id == "stop_E"
    # Each route reports its own start/end stop_sequence.
    assert per_route[("A", 0)] == (1, 2)
    assert per_route[("B", 0)] == (1, 2)


def test_snap_run_to_stops_returns_none_when_no_shared_stop_in_range():
    """If no stop within 100m of the run endpoints is shared by all routes, return None."""
    points = [(38.94, -77.07 + 0.001 * i, i, 90.0) for i in range(10)]

    # Only stop_lonely exists, far from both endpoints.
    stops = {
        "stop_lonely": StopInfo(stop_id="stop_lonely", stop_name="Far Away", lat=38.50, lon=-77.0),
    }

    route_stops = {
        ("A", 0): [("stop_lonely", 1)],
        ("B", 0): [("stop_lonely", 1)],
    }

    result = snap_run_to_stops(
        route_set=frozenset({("A", 0), ("B", 0)}),
        run_points=points,
        stops=stops,
        route_stops=route_stops,
    )

    assert result is None


def test_build_display_name_format():
    """Display name renders the corridor in 'Street SB: A -> B' shape."""
    name = build_display_name(
        cardinal="SB",
        start_stop_name="Friendship Heights",
        end_stop_name="Foggy Bottom",
        # Optional 'street hint' could be appended; spec doesn't require it,
        # so just embed stops.
    )
    assert name == "SB: Friendship Heights -> Foggy Bottom"
```

- [ ] **Step 2: Run tests to verify failure**

```bash
uv run pytest tests/test_corridor_identity.py -v -k "snap or display_name"
```

Expected: `ImportError`.

- [ ] **Step 3: Implement snapping + display name**

Append to `src/corridor_identity.py`:

```python
@dataclass(frozen=True)
class StopInfo:
    """One stop's identity + position for snapping."""

    stop_id: str
    stop_name: str
    lat: float
    lon: float


@dataclass(frozen=True)
class StopRef:
    """A stop selected as a corridor endpoint."""

    stop_id: str
    stop_name: str


def _nearest_stop_within(
    target_lat: float,
    target_lon: float,
    candidate_stop_ids: set[str],
    stops: dict[str, StopInfo],
    max_distance_m: float,
) -> str | None:
    """Return the stop_id of the closest candidate within max_distance_m, or None."""
    best_stop_id: str | None = None
    best_distance = max_distance_m
    for stop_id in candidate_stop_ids:
        stop = stops.get(stop_id)
        if stop is None:
            continue
        dist = haversine_meters(target_lat, target_lon, stop.lat, stop.lon)
        if dist < best_distance:
            best_distance = dist
            best_stop_id = stop_id
    return best_stop_id


def snap_run_to_stops(
    route_set: frozenset[ShapeKey],
    run_points: list[tuple[float, float, int, float]],
    stops: dict[str, StopInfo],
    route_stops: dict[ShapeKey, list[tuple[str, int]]],
) -> tuple[StopRef, StopRef, dict[ShapeKey, tuple[int, int]]] | None:
    """
    Snap a run's endpoints to stops that are served by every route in
    route_set, within ENDPOINT_STOP_SNAP_M of the run's endpoint shape
    points. Return (start, end, per_route_sequence_range) or None if
    no valid shared stop is found at one of the endpoints.

    per_route_sequence_range maps each (route_id, direction_id) to
    (start_stop_sequence, end_stop_sequence) along the route's canonical
    trip stop ordering.
    """
    if not run_points:
        return None

    # Intersect stops across the route_set: only stops served by EVERY route in the set.
    shared_stop_ids: set[str] | None = None
    for key in route_set:
        stop_ids_for_route = {stop_id for stop_id, _seq in route_stops.get(key, [])}
        if shared_stop_ids is None:
            shared_stop_ids = stop_ids_for_route
        else:
            shared_stop_ids = shared_stop_ids & stop_ids_for_route
    if not shared_stop_ids:
        return None

    start_lat, start_lon, _, _ = run_points[0]
    end_lat, end_lon, _, _ = run_points[-1]

    start_stop_id = _nearest_stop_within(
        start_lat, start_lon, shared_stop_ids, stops, ENDPOINT_STOP_SNAP_M
    )
    end_stop_id = _nearest_stop_within(
        end_lat, end_lon, shared_stop_ids, stops, ENDPOINT_STOP_SNAP_M
    )
    if start_stop_id is None or end_stop_id is None or start_stop_id == end_stop_id:
        return None

    # Compute per-route stop_sequence ranges using the picked start/end stops.
    per_route: dict[ShapeKey, tuple[int, int]] = {}
    for key in route_set:
        sequence_map = {stop_id: seq for stop_id, seq in route_stops.get(key, [])}
        if start_stop_id not in sequence_map or end_stop_id not in sequence_map:
            return None
        s_seq = sequence_map[start_stop_id]
        e_seq = sequence_map[end_stop_id]
        if s_seq > e_seq:
            # Direction reversed for this route; swap to a sane (low, high) range.
            s_seq, e_seq = e_seq, s_seq
        per_route[key] = (s_seq, e_seq)

    return (
        StopRef(stop_id=start_stop_id, stop_name=stops[start_stop_id].stop_name),
        StopRef(stop_id=end_stop_id, stop_name=stops[end_stop_id].stop_name),
        per_route,
    )


def build_display_name(
    cardinal: str,
    start_stop_name: str,
    end_stop_name: str,
) -> str:
    """Render a corridor's display name."""
    return f"{cardinal}: {start_stop_name} -> {end_stop_name}"
```

Also add the import at the top of the file:

```python
from dataclasses import dataclass
```

(May already be there from Task 6.)

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_corridor_identity.py -v
```

Expected: 23 passing.

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/corridor_identity.py tests/test_corridor_identity.py
uv run ruff format --check src/corridor_identity.py tests/test_corridor_identity.py
git add src/corridor_identity.py tests/test_corridor_identity.py
git commit -m "feat(corridor): stop snapping + display name (NOTES-62)"
```

---

### Task 8: Pipeline orchestrator — `refresh_corridors`

**Files:**
- Create: `pipelines/refresh_corridors.py`
- Create: `tests/test_refresh_corridors.py`

- [ ] **Step 1: Sketch the orchestrator's contract**

The pipeline:
1. Reads `is_current=TRUE` shapes + trips + stops + stop_times from Postgres.
2. Picks canonical shapes per (route_id, direction_id).
3. Augments shape points with bearings.
4. Computes colocated route sets via the grid matcher.
5. Extracts runs per canonical shape.
6. Dedupes cross-route (same physical corridor appears once per contributing route).
7. Snaps endpoints to stops; computes lengths; filters by MIN_CORRIDOR_LENGTH_M.
8. Writes `corridors` + `corridor_route_membership` in one transaction.
9. Returns counts for logging.

- [ ] **Step 2: Write the failing test**

Create `tests/test_refresh_corridors.py`:

```python
"""Integration test for refresh_corridors pipeline against a fixture GTFS."""
import pytest

from pipelines.refresh_corridors import refresh_corridors
from src.models import Corridor, CorridorRouteMembership


pytestmark = pytest.mark.pg  # Postgres only; uses JSONB


def test_refresh_corridors_two_routes_same_corridor(pg_session, populate_fixture_gtfs):
    """
    Fixture: two routes (FX1, FX2) sharing a 1km stretch along synthetic
    'East St' in both directions; one perpendicular route (FX3); no
    variant-only corridors.

    Expected: 2 corridors (East St EB and East St WB), each with route_set
    = ['FX1', 'FX2']. FX3 has no overlap and produces no corridor membership.
    """
    populate_fixture_gtfs(pg_session, scenario="two_routes_one_corridor")

    counts = refresh_corridors(session=pg_session, gtfs_snapshot_id=1)

    assert counts["corridors_inserted"] == 2

    corridors = pg_session.query(Corridor).all()
    assert len(corridors) == 2
    cardinals = sorted(c.direction_cardinal for c in corridors)
    # Eastbound and westbound — the cardinals depend on the fixture's geometry.
    assert cardinals == sorted(["E", "W"]) or cardinals == sorted(["EB", "WB"])

    memberships = pg_session.query(CorridorRouteMembership).all()
    # Each corridor has 2 routes (FX1, FX2). 2 corridors x 2 routes = 4 rows.
    assert len(memberships) == 4
    route_ids = {m.route_id for m in memberships}
    assert route_ids == {"FX1", "FX2"}
```

Note: the `populate_fixture_gtfs` fixture is a test helper that this task introduces. It seeds `routes`, `trips`, `stops`, `stop_times`, `shapes` rows representing a small synthetic GTFS scenario. Implemented in the next step alongside the pipeline.

- [ ] **Step 3: Add the fixture helper**

Create or modify `tests/conftest.py` to add a `populate_fixture_gtfs` fixture. Read existing `tests/conftest.py` first to follow conventions:

```bash
head -100 tests/conftest.py
```

Add (after existing fixtures):

```python
@pytest.fixture
def populate_fixture_gtfs():
    """
    Seed a Postgres session with a small synthetic GTFS.

    Scenarios:
      - "two_routes_one_corridor": FX1 + FX2 along synthetic East St
        (both directions); FX3 perpendicular. Stops named systematically.
    """
    from src.models import Route, Trip, Stop, StopTime, Shape

    def _seed(session, scenario: str):
        if scenario != "two_routes_one_corridor":
            raise ValueError(f"Unknown fixture scenario: {scenario}")

        # 3 routes.
        for route_id in ("FX1", "FX2", "FX3"):
            session.add(
                Route(
                    route_id=route_id,
                    agency_id="WMATA",
                    route_short_name=route_id,
                    route_long_name=f"{route_id} fixture",
                    route_type=3,
                    snapshot_id=1,
                    is_current=True,
                )
            )

        # 10 stops along East St (lon -77.07 + 0.001 * i, lat 38.94), plus
        # a few perpendicular stops for FX3.
        for i in range(10):
            session.add(
                Stop(
                    stop_id=f"east_{i}",
                    stop_name=f"East St & {i}th",
                    stop_lat=38.94,
                    stop_lon=-77.07 + 0.0010 * i,
                    snapshot_id=1,
                    is_current=True,
                )
            )
        # FX3 perpendicular stops, far north.
        for i in range(3):
            session.add(
                Stop(
                    stop_id=f"north_{i}",
                    stop_name=f"North St & {i}",
                    stop_lat=38.95 + 0.001 * i,
                    stop_lon=-77.08,
                    snapshot_id=1,
                    is_current=True,
                )
            )

        # Shape FX1:51 (eastbound), FX1:03 (westbound) — same points reversed.
        # Same for FX2. FX3 north-south.
        for shape_id_prefix in ("FX1", "FX2"):
            for direction, suffix in ((0, "51"), (1, "03")):
                shape_id = f"{shape_id_prefix}:{suffix}"
                pts = list(range(10))
                if direction == 1:
                    pts = list(reversed(pts))
                for seq, i in enumerate(pts, start=1):
                    session.add(
                        Shape(
                            shape_id=shape_id,
                            shape_pt_lat=38.94,
                            shape_pt_lon=-77.07 + 0.0010 * i,
                            shape_pt_sequence=seq,
                            snapshot_id=1,
                        )
                    )

        # FX3 perpendicular shape (north-south).
        for seq, i in enumerate(range(3), start=1):
            session.add(
                Shape(
                    shape_id="FX3:01",
                    shape_pt_lat=38.95 + 0.001 * i,
                    shape_pt_lon=-77.08,
                    shape_pt_sequence=seq,
                    snapshot_id=1,
                )
            )

        # Trips: one per (route, direction) for FX1/FX2; one for FX3.
        for route_id in ("FX1", "FX2"):
            for direction in (0, 1):
                shape_id = f"{route_id}:{('51' if direction == 0 else '03')}"
                trip_id = f"{route_id}_dir{direction}_T1"
                session.add(
                    Trip(
                        trip_id=trip_id,
                        route_id=route_id,
                        direction_id=direction,
                        shape_id=shape_id,
                        service_id="WEEKDAY",
                        snapshot_id=1,
                        is_current=True,
                    )
                )
                # Stop times along east_0..east_9 (or reversed for direction 1).
                stop_seq = list(range(10))
                if direction == 1:
                    stop_seq = list(reversed(stop_seq))
                for stop_sequence, i in enumerate(stop_seq, start=1):
                    session.add(
                        StopTime(
                            trip_id=trip_id,
                            stop_id=f"east_{i}",
                            stop_sequence=stop_sequence,
                            arrival_time=f"6:{stop_sequence:02d}:00",
                            departure_time=f"6:{stop_sequence:02d}:00",
                            snapshot_id=1,
                            is_current=True,
                        )
                    )

        session.add(
            Trip(
                trip_id="FX3_T1",
                route_id="FX3",
                direction_id=0,
                shape_id="FX3:01",
                service_id="WEEKDAY",
                snapshot_id=1,
                is_current=True,
            )
        )
        for i in range(3):
            session.add(
                StopTime(
                    trip_id="FX3_T1",
                    stop_id=f"north_{i}",
                    stop_sequence=i + 1,
                    arrival_time=f"6:{i:02d}:00",
                    departure_time=f"6:{i:02d}:00",
                    snapshot_id=1,
                    is_current=True,
                )
            )

        session.flush()

    return _seed
```

(Adjust ORM field names to match `src/models.py` — verify by reading the `Trip`, `StopTime`, `Shape`, etc. classes if any field above doesn't exist. The `is_current`, `snapshot_id` pattern is used across versioned tables per CLAUDE.md.)

- [ ] **Step 4: Run the test to verify failure**

```bash
PG_TEST_DATABASE_URL=postgresql:///wmata_dashboard uv run pytest tests/test_refresh_corridors.py -v
```

Expected: `ImportError: cannot import name 'refresh_corridors' from 'pipelines.refresh_corridors'`.

- [ ] **Step 5: Implement the pipeline**

Create `pipelines/refresh_corridors.py`:

```python
"""
Pipeline: rebuild `corridors` + `corridor_route_membership` from current
GTFS shapes. Called from `scripts/reload_gtfs_complete.py`.

Algorithm overview (see spec Section 1):
  1. Pick canonical shape per (route_id, direction_id) by trip count.
  2. Augment each canonical shape's points with local bearings.
  3. Grid-bucket all canonical points; for each point, find colocated
     route/direction pairs (within 15m AND 30deg bearing).
  4. Walk each canonical shape; emit runs at every change in the
     colocated route set. Discard alone runs (|set| < 2) and short
     runs (|points| < 5).
  5. Dedupe across contributing routes — each physical corridor appears
     once per contributing route's runs; pick one canonical representation.
  6. Snap endpoints to stops shared by all contributing routes (within 100m).
  7. Drop corridors below MIN_CORRIDOR_LENGTH_M (500m).
  8. Persist in a single transaction (truncate + insert).
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import delete, select, text
from sqlalchemy.orm import Session

from src.corridor_identity import (
    MIN_CORRIDOR_LENGTH_M,
    Run,
    StopInfo,
    augment_shape_with_bearings,
    bearing_to_cardinal,
    build_display_name,
    compute_colocated_route_sets,
    extract_runs,
    haversine_meters,
    pick_canonical_shapes,
    snap_run_to_stops,
)
from src.models import (
    Corridor,
    CorridorRouteMembership,
    Shape,
    Stop,
    StopTime,
    Trip,
)


@dataclass
class RefreshCounts:
    """Pipeline result counters for logging."""

    canonical_shapes_picked: int = 0
    points_examined: int = 0
    runs_extracted: int = 0
    corridors_inserted: int = 0
    memberships_inserted: int = 0


def _load_trip_shape_counts(session: Session) -> list[tuple[str, int, str, int]]:
    """Return (route_id, direction_id, shape_id, n_trips) for current trips."""
    rows = session.execute(
        text(
            """
            SELECT t.route_id, t.direction_id, t.shape_id, COUNT(*) AS n_trips
            FROM trips t
            WHERE t.is_current = TRUE
              AND t.shape_id IS NOT NULL
            GROUP BY t.route_id, t.direction_id, t.shape_id
            """
        )
    ).all()
    return [(r.route_id, r.direction_id, r.shape_id, r.n_trips) for r in rows]


def _load_canonical_shape_points(
    session: Session,
    canonical: dict[tuple[str, int], str],
) -> dict[tuple[str, int], tuple[str, list[tuple[float, float, int, float]]]]:
    """For each canonical (route, direction) -> (shape_id, [(lat, lon, seq, bearing), ...])."""
    if not canonical:
        return {}

    shape_ids = list({sid for sid in canonical.values()})
    rows = session.execute(
        text(
            """
            SELECT shape_id, shape_pt_lat AS lat, shape_pt_lon AS lon,
                   shape_pt_sequence AS seq
            FROM shapes
            WHERE shape_id = ANY(:shape_ids)
            ORDER BY shape_id, shape_pt_sequence
            """
        ),
        {"shape_ids": shape_ids},
    ).all()

    points_by_shape: dict[str, list[tuple[float, float, int]]] = defaultdict(list)
    for row in rows:
        points_by_shape[row.shape_id].append((row.lat, row.lon, row.seq))

    out: dict[tuple[str, int], tuple[str, list[tuple[float, float, int, float]]]] = {}
    for key, shape_id in canonical.items():
        pts = points_by_shape.get(shape_id, [])
        if len(pts) < 2:
            continue
        augmented = augment_shape_with_bearings(pts)
        out[key] = (shape_id, augmented)
    return out


def _load_stops(session: Session) -> dict[str, StopInfo]:
    """Load all current stops into a stop_id -> StopInfo map."""
    rows = session.execute(
        text(
            """
            SELECT stop_id, stop_name, stop_lat AS lat, stop_lon AS lon
            FROM stops
            WHERE is_current = TRUE
            """
        )
    ).all()
    return {
        r.stop_id: StopInfo(stop_id=r.stop_id, stop_name=r.stop_name, lat=r.lat, lon=r.lon)
        for r in rows
    }


def _load_route_stops(
    session: Session,
) -> dict[tuple[str, int], list[tuple[str, int]]]:
    """
    For each (route_id, direction_id), return one trip's [(stop_id, stop_sequence), ...]
    (the canonical trip's stop pattern).

    Picks the highest-trip-count shape's first trip as the stop pattern source.
    Different variants may have different stop sets, but per the spec the
    membership table records the canonical-trip stop ranges.
    """
    # One representative trip per (route, direction, shape) — picks any trip_id.
    # We then need the stop_times for that trip_id.
    rows = session.execute(
        text(
            """
            WITH ranked AS (
                SELECT t.route_id, t.direction_id, t.shape_id, t.trip_id,
                       ROW_NUMBER() OVER (PARTITION BY t.route_id, t.direction_id, t.shape_id
                                          ORDER BY t.trip_id) AS rn
                FROM trips t
                WHERE t.is_current = TRUE
                  AND t.shape_id IS NOT NULL
            ),
            picked AS (
                SELECT route_id, direction_id, shape_id, trip_id
                FROM ranked
                WHERE rn = 1
            )
            SELECT p.route_id, p.direction_id, p.shape_id,
                   st.stop_id, st.stop_sequence
            FROM picked p
            JOIN stop_times st ON st.trip_id = p.trip_id AND st.is_current = TRUE
            ORDER BY p.route_id, p.direction_id, p.shape_id, st.stop_sequence
            """
        )
    ).all()

    by_route_dir_shape: dict[tuple[str, int, str], list[tuple[str, int]]] = defaultdict(list)
    for row in rows:
        by_route_dir_shape[(row.route_id, row.direction_id, row.shape_id)].append(
            (row.stop_id, row.stop_sequence)
        )
    return by_route_dir_shape  # type: ignore[return-value]


def _run_length_m(points: tuple[tuple[float, float, int, float], ...]) -> float:
    """Sum of haversine distances along a run's shape points."""
    total = 0.0
    for i in range(len(points) - 1):
        lat1, lon1, _, _ = points[i]
        lat2, lon2, _, _ = points[i + 1]
        total += haversine_meters(lat1, lon1, lat2, lon2)
    return total


def _build_geometry_wkt(points: tuple[tuple[float, float, int, float], ...]) -> str:
    """Render a list of points as a WKT LINESTRING (lon lat ordering)."""
    coords = ", ".join(f"{lon} {lat}" for lat, lon, _, _ in points)
    return f"LINESTRING({coords})"


def refresh_corridors(session: Session, gtfs_snapshot_id: int) -> dict[str, int]:
    """Rebuild `corridors` + `corridor_route_membership` from current GTFS."""
    counts = RefreshCounts()

    # Step 1: pick canonical shapes per (route_id, direction_id).
    trip_shape_counts = _load_trip_shape_counts(session)
    canonical = pick_canonical_shapes(trip_shape_counts)
    counts.canonical_shapes_picked = len(canonical)

    if not canonical:
        # No data — wipe both tables and return.
        session.execute(delete(CorridorRouteMembership))
        session.execute(delete(Corridor))
        return _counts_to_dict(counts)

    # Step 2: load + augment canonical shape points.
    augmented_shapes = _load_canonical_shape_points(session, canonical)
    counts.points_examined = sum(
        len(points) for _shape_id, points in augmented_shapes.values()
    )

    # Step 3: compute colocation.
    colocated = compute_colocated_route_sets(augmented_shapes)

    # Step 4: extract runs per canonical shape.
    all_runs: list[Run] = []
    for (route_id, direction_id), (shape_id, points) in augmented_shapes.items():
        all_runs.extend(
            extract_runs(
                route_id=route_id,
                direction_id=direction_id,
                canonical_shape_id=shape_id,
                points=points,
                colocated=colocated,
            )
        )
    counts.runs_extracted = len(all_runs)

    # Step 5: dedupe across contributing routes. A physical corridor appears
    # once per route in the route_set. Dedup key: (route_set, cardinal, rounded endpoint coords).
    seen: dict[tuple, Run] = {}
    for run in all_runs:
        cardinal = bearing_to_cardinal(run.mean_bearing_deg)
        # Rough endpoint key: round endpoint lat/lon to ~10m.
        sl, slon, _, _ = run.points[0]
        el, elon, _, _ = run.points[-1]
        dedup_key = (
            run.route_set,
            cardinal,
            round(sl, 4),
            round(slon, 4),
            round(el, 4),
            round(elon, 4),
        )
        # Keep the longest run for each dedup key.
        prior = seen.get(dedup_key)
        if prior is None or run.length_m > prior.length_m:
            seen[dedup_key] = run
    deduped_runs = list(seen.values())

    # Step 6: load stops + route_stops for snapping.
    stops = _load_stops(session)
    route_stops_by_shape = _load_route_stops(session)
    # Flatten: route_stops keyed by (route, direction) using the canonical shape's stops.
    route_stops: dict[tuple[str, int], list[tuple[str, int]]] = {}
    for key, shape_id in canonical.items():
        route_stops[key] = route_stops_by_shape.get(
            (key[0], key[1], shape_id), []
        )

    # Step 7: snap + filter by length; build corridor records.
    corridor_rows: list[dict] = []
    membership_rows: list[dict] = []

    for run in deduped_runs:
        length_m = run.length_m
        if length_m < MIN_CORRIDOR_LENGTH_M:
            continue

        snap = snap_run_to_stops(
            route_set=run.route_set,
            run_points=list(run.points),
            stops=stops,
            route_stops=route_stops,
        )
        if snap is None:
            continue
        start_ref, end_ref, per_route_range = snap
        cardinal = bearing_to_cardinal(run.mean_bearing_deg)
        display_name = build_display_name(
            cardinal=cardinal,
            start_stop_name=start_ref.stop_name,
            end_stop_name=end_ref.stop_name,
        )
        # Sorted list of route_ids (route_set is frozenset of (route_id, direction_id)
        # but the JSONB column expects a sorted list of route_ids).
        route_ids = sorted({rid for (rid, _) in run.route_set})

        corridor_row = {
            "direction_bearing_deg": run.mean_bearing_deg,
            "direction_cardinal": cardinal,
            "start_stop_id": start_ref.stop_id,
            "end_stop_id": end_ref.stop_id,
            "length_m": length_m,
            "n_routes": len(route_ids),
            "route_set": route_ids,
            "display_name": display_name,
            "geometry_wkt": _build_geometry_wkt(run.points),
            "gtfs_snapshot_id": gtfs_snapshot_id,
        }
        corridor_rows.append(corridor_row)

        # Tag membership rows with a placeholder index; will fix corridor_id after insert.
        for (route_id, direction_id), (s_seq, e_seq) in per_route_range.items():
            membership_rows.append(
                {
                    "_corridor_index": len(corridor_rows) - 1,
                    "route_id": route_id,
                    "direction_id": direction_id,
                    "canonical_shape_id": canonical[(route_id, direction_id)],
                    "start_stop_sequence": s_seq,
                    "end_stop_sequence": e_seq,
                }
            )

    # Step 8: persist in a single transaction.
    session.execute(delete(CorridorRouteMembership))
    session.execute(delete(Corridor))

    inserted_corridor_ids: list[int] = []
    for row in corridor_rows:
        c = Corridor(**row)
        session.add(c)
        session.flush()  # materialize corridor_id
        inserted_corridor_ids.append(c.corridor_id)

    for mrow in membership_rows:
        idx = mrow.pop("_corridor_index")
        mrow["corridor_id"] = inserted_corridor_ids[idx]
        session.add(CorridorRouteMembership(**mrow))

    counts.corridors_inserted = len(corridor_rows)
    counts.memberships_inserted = len(membership_rows)

    session.flush()
    return _counts_to_dict(counts)


def _counts_to_dict(counts: RefreshCounts) -> dict[str, int]:
    return {
        "canonical_shapes_picked": counts.canonical_shapes_picked,
        "points_examined": counts.points_examined,
        "runs_extracted": counts.runs_extracted,
        "corridors_inserted": counts.corridors_inserted,
        "memberships_inserted": counts.memberships_inserted,
    }


if __name__ == "__main__":
    from src.database import get_session

    session = get_session()
    try:
        # Pull the latest snapshot_id from a versioned table.
        snap_id = session.execute(
            text("SELECT MAX(snapshot_id) FROM routes WHERE is_current = TRUE")
        ).scalar_one()
        counts = refresh_corridors(session=session, gtfs_snapshot_id=snap_id)
        session.commit()
        print(f"[refresh_corridors] {counts}")
    finally:
        session.close()
```

- [ ] **Step 6: Run the integration test**

```bash
PG_TEST_DATABASE_URL=postgresql:///wmata_dashboard uv run pytest tests/test_refresh_corridors.py -v
```

Expected: PASS with 2 corridors + 4 memberships.

If the fixture's directions produce cardinals other than E/W (because `38.94` latitude → near-perpendicular bearing), adjust the fixture's stop spacing to be more directly east-west, or relax the assertion to "2 distinct cardinals, opposite of each other".

- [ ] **Step 7: Run against real WMATA data (smoke)**

```bash
uv run python pipelines/refresh_corridors.py
```

Expected: counts printed; corridors_inserted in the 20-40 range per the empirical expectation. Spot-check a Wisconsin Ave corridor:

```bash
psql -d wmata_dashboard -c "SELECT corridor_id, direction_cardinal, display_name, length_m, route_set FROM corridors WHERE display_name ILIKE '%Wisconsin%' OR route_set::text LIKE '%D80%' LIMIT 10;"
```

If counts are far off the expected 20-40 (say, 5 or 200), revisit calibration knobs in `src/corridor_identity.py`. Document any adjustments in the commit message.

- [ ] **Step 8: Lint + commit**

```bash
uv run ruff check pipelines/refresh_corridors.py tests/test_refresh_corridors.py tests/conftest.py
uv run ruff format --check pipelines/refresh_corridors.py tests/test_refresh_corridors.py tests/conftest.py
git add pipelines/refresh_corridors.py tests/test_refresh_corridors.py tests/conftest.py
git commit -m "feat(pipeline): refresh_corridors orchestrator + fixture (NOTES-62)"
```

---

### Task 9: Wire `refresh_corridors` into `reload_gtfs_complete`

**Files:**
- Modify: `scripts/reload_gtfs_complete.py`

- [ ] **Step 1: Read the existing reload script's structure**

```bash
grep -n "with engine.begin\|commit\|snapshot" scripts/reload_gtfs_complete.py | head -20
```

Identify the existing transaction boundary. The corridor refresh must run INSIDE that transaction so atomicity holds (GTFS reload either fully succeeds, including corridors, or fully rolls back).

- [ ] **Step 2: Add the call**

After the last GTFS table is written (likely after `route_service_profile` reinsert), and BEFORE the transaction commits, call `refresh_corridors`:

```python
# At the top of the file:
from pipelines.refresh_corridors import refresh_corridors

# Inside the transaction, after GTFS tables are populated:
print("[reload_gtfs] rebuilding corridors (NOTES-62)")
corridor_counts = refresh_corridors(session=session, gtfs_snapshot_id=new_snapshot_id)
print(f"[reload_gtfs] {corridor_counts}")
```

Verify against the actual variable names used in the script (`session`, `engine.begin()`, etc.).

- [ ] **Step 3: Dry-run the reload script**

If there's a dry-run or staging mode, use it. Otherwise, on a backup of the DB or a sandbox copy, run:

```bash
uv run python scripts/reload_gtfs_complete.py --dry-run  # if supported
```

Expected: the script reports corridor counts in its output. If `--dry-run` isn't supported, skip this step and rely on the integration test from Task 8.

- [ ] **Step 4: Run smoke**

```bash
uv run pytest -m smoke
uv run ruff check scripts/
uv run ruff format --check scripts/
```

- [ ] **Step 5: Commit**

```bash
git add scripts/reload_gtfs_complete.py
git commit -m "feat(gtfs): refresh corridors inside GTFS reload txn (NOTES-62)"
```

---

## Phase 3 — Slip rollup

End of phase 3: nightly batches populate `corridor_slip_rollup`; ranked-list queries work in psql.

---

### Task 10: ORM model for `CorridorSlipRollup`

**Files:**
- Modify: `src/models.py`
- Modify: `tests/test_corridor_models.py`

- [ ] **Step 1: Add failing test**

Append to `tests/test_corridor_models.py`:

```python
from src.models import CorridorSlipRollup


def test_corridor_slip_rollup_tablename():
    assert CorridorSlipRollup.__tablename__ == "corridor_slip_rollup"


def test_corridor_slip_rollup_required_columns():
    columns = {c.name for c in CorridorSlipRollup.__table__.columns}
    expected = {
        "corridor_id", "period",
        "n_route_directions", "n_observed_segments", "n_total_observations",
        "total_weighted_slip_sec", "mean_slip_per_segment_sec",
        "mean_slip_per_observation_sec", "peak_period", "computed_at",
    }
    assert expected.issubset(columns), f"Missing: {expected - columns}"
```

- [ ] **Step 2: Run test to verify failure**

```bash
uv run pytest tests/test_corridor_models.py -v -k "corridor_slip"
```

Expected: `ImportError`.

- [ ] **Step 3: Add the ORM class**

Append to `src/models.py` after `CorridorRouteMembership`:

```python
class CorridorSlipRollup(Base):
    """
    Per-(corridor_id, period) aggregated slip across all routes in the
    corridor's route_set. Materialized nightly by
    `pipelines/refresh_corridor_slip.py` from `route_diagnostic_segment`.

    `total_weighted_slip_sec` = SUM(mean_slip_sec * n_observations)
    over the corridor's contributing route_diagnostic_segment rows,
    where rows are filtered by:
      seg.route_id = membership.route_id
      AND seg.direction_id = membership.direction_id
      AND seg.from_seq >= membership.start_stop_sequence
      AND seg.to_seq <= membership.end_stop_sequence

    `peak_period` (only set on period='all' rows) is the named period
    with the highest total_weighted_slip_sec for the corridor.

    Source window: `route_diagnostic_segment` is itself a 30-day
    rolling aggregate (see RouteDiagnosticSegment docstring); this table
    inherits that window.

    NOTES-62.
    """

    __tablename__ = "corridor_slip_rollup"

    corridor_id = Column(
        Integer,
        ForeignKey("corridors.corridor_id", ondelete="CASCADE"),
        primary_key=True,
    )
    period = Column(String, primary_key=True)

    n_route_directions = Column(Integer, nullable=False)
    n_observed_segments = Column(Integer, nullable=False)
    n_total_observations = Column(Integer, nullable=False)
    total_weighted_slip_sec = Column(Float, nullable=False)
    mean_slip_per_segment_sec = Column(Float, nullable=True)
    mean_slip_per_observation_sec = Column(Float, nullable=True)
    peak_period = Column(String, nullable=True)
    computed_at = Column(DateTime, nullable=False, default=utcnow_naive)

    __table_args__ = (
        Index(
            "idx_corridor_slip_rollup_period",
            "period",
            "total_weighted_slip_sec",
        ),
    )
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_corridor_models.py -v
```

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/models.py tests/test_corridor_models.py
git commit -m "feat(models): add CorridorSlipRollup ORM (NOTES-62)"
```

---

### Task 11: Migration for `corridor_slip_rollup`

**Files:**
- Create: `scripts/migrate_create_corridor_slip_rollup.py`
- Modify: `scripts/migrate_all.py`

- [ ] **Step 1: Write the migration**

Create `scripts/migrate_create_corridor_slip_rollup.py`:

```python
"""
One-shot migration: create `corridor_slip_rollup` table (NOTES-62).

Idempotent. Depends on `corridors` already existing.
"""
from __future__ import annotations

import sys

from sqlalchemy import inspect, text

from src.database import get_engine


def migrate() -> bool:
    engine = get_engine()
    inspector = inspect(engine)

    if "corridor_slip_rollup" in inspector.get_table_names():
        print("[migrate_corridor_slip_rollup] already exists; skipping")
        return False

    with engine.begin() as conn:
        print("[migrate_corridor_slip_rollup] creating corridor_slip_rollup")
        conn.execute(
            text(
                """
                CREATE TABLE corridor_slip_rollup (
                    corridor_id              INTEGER NOT NULL
                        REFERENCES corridors(corridor_id) ON DELETE CASCADE,
                    period                   TEXT NOT NULL,
                    n_route_directions       INTEGER NOT NULL,
                    n_observed_segments      INTEGER NOT NULL,
                    n_total_observations     INTEGER NOT NULL,
                    total_weighted_slip_sec  REAL NOT NULL,
                    mean_slip_per_segment_sec REAL,
                    mean_slip_per_observation_sec REAL,
                    peak_period              TEXT,
                    computed_at              TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (corridor_id, period)
                );
                CREATE INDEX idx_corridor_slip_rollup_period
                    ON corridor_slip_rollup (period, total_weighted_slip_sec);
                """
            )
        )
    return True


if __name__ == "__main__":
    migrate()
    sys.exit(0)
```

- [ ] **Step 2: Wire into `migrate_all.py`**

Append the call following the same pattern as `migrate_create_corridors`.

- [ ] **Step 3: Run the migration**

```bash
uv run python scripts/migrate_create_corridor_slip_rollup.py
psql -d wmata_dashboard -c "\d corridor_slip_rollup"
```

Expected: table created with FK and index.

- [ ] **Step 4: Drift check + smoke**

```bash
uv run python scripts/check_schema_drift.py
uv run pytest -m smoke
uv run ruff check scripts/
uv run ruff format --check scripts/
```

- [ ] **Step 5: Commit**

```bash
git add scripts/migrate_create_corridor_slip_rollup.py scripts/migrate_all.py
git commit -m "feat(db): migration for corridor_slip_rollup (NOTES-62)"
```

---

### Task 12: Slip aggregation pipeline

**Files:**
- Create: `pipelines/refresh_corridor_slip.py`
- Create: `tests/test_refresh_corridor_slip.py`

- [ ] **Step 1: Write the failing integration test**

Create `tests/test_refresh_corridor_slip.py`:

```python
"""Integration test for refresh_corridor_slip against fixture data."""
import pytest

from pipelines.refresh_corridor_slip import refresh_corridor_slip
from pipelines.refresh_corridors import refresh_corridors
from src.models import CorridorSlipRollup, RouteDiagnosticSegment


pytestmark = pytest.mark.pg


def test_refresh_corridor_slip_aggregates_from_per_route_segments(
    pg_session, populate_fixture_gtfs
):
    """
    Setup:
      - 2 routes (FX1, FX2) on East St; 2 corridors (E, W).
      - Insert RouteDiagnosticSegment rows for FX1 and FX2 in both directions
        with known mean_slip_sec * n_observations.
    Expect:
      - corridor_slip_rollup has one row per (corridor_id, period).
      - total_weighted_slip_sec matches the sum of contributing FX1+FX2 segments.
    """
    populate_fixture_gtfs(pg_session, scenario="two_routes_one_corridor")
    refresh_corridors(session=pg_session, gtfs_snapshot_id=1)

    # Seed route_diagnostic_segment rows.
    # FX1 dir 0 east_0->east_1 (seq 1->2), period 'all', slip=60s, n_obs=10.
    # FX1 dir 0 east_1->east_2 (seq 2->3), period 'all', slip=30s, n_obs=10.
    # FX2 same.
    for route_id in ("FX1", "FX2"):
        for from_seq in range(1, 10):
            pg_session.add(
                RouteDiagnosticSegment(
                    route_id=route_id,
                    direction_id=0,
                    period="all",
                    from_seq=from_seq,
                    from_stop_id=f"east_{from_seq - 1}",
                    to_seq=from_seq + 1,
                    to_stop_id=f"east_{from_seq}",
                    mean_slip_sec=60.0,
                    cum_slip_sec=60.0 * from_seq,
                    n_observations=10,
                    is_timepoint=False,
                )
            )
    pg_session.flush()

    counts = refresh_corridor_slip(session=pg_session)
    assert counts["rollups_inserted"] >= 1

    # Verify aggregate math for the eastbound corridor.
    rollup_rows = (
        pg_session.query(CorridorSlipRollup).filter_by(period="all").all()
    )
    # At least one corridor (eastbound) should have data;
    # total_weighted = SUM(mean_slip_sec * n_observations)
    # = 9 segments * 60 * 10 per route * 2 routes = 10800
    eastbound = [r for r in rollup_rows if r.total_weighted_slip_sec > 0]
    assert eastbound, "Expected at least one corridor with non-zero slip"
    assert any(
        abs(r.total_weighted_slip_sec - 10800.0) < 1.0 for r in eastbound
    ), f"Got slip totals: {[r.total_weighted_slip_sec for r in eastbound]}"
```

- [ ] **Step 2: Run the test to verify failure**

```bash
PG_TEST_DATABASE_URL=postgresql:///wmata_dashboard uv run pytest tests/test_refresh_corridor_slip.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement the pipeline**

Create `pipelines/refresh_corridor_slip.py`:

```python
"""
Pipeline: rebuild `corridor_slip_rollup` from `route_diagnostic_segment`
by joining through `corridor_route_membership`. Called nightly from
`pipelines/run_daily_batch.py`.

Source window inherited: route_diagnostic_segment is a 30-day rolling
aggregate (see src/models.py:RouteDiagnosticSegment docstring), so this
rollup follows the same window automatically.
"""
from __future__ import annotations

from sqlalchemy import delete, text
from sqlalchemy.orm import Session

from src.models import CorridorSlipRollup


def refresh_corridor_slip(session: Session) -> dict[str, int]:
    """
    Rebuild corridor_slip_rollup from route_diagnostic_segment.

    Two passes:
      1. Primary aggregation per (corridor_id, period).
      2. Backfill peak_period on period='all' rows.

    Row-count guard (NOTES-72 lesson): fail loudly if input has rows but
    output is zero.
    """
    # Sanity: input has data?
    input_count = session.execute(
        text("SELECT COUNT(*) FROM route_diagnostic_segment")
    ).scalar_one()

    # Wipe existing rollup (cascade also wipes on corridor delete).
    session.execute(delete(CorridorSlipRollup))

    # Primary aggregation.
    session.execute(
        text(
            """
            INSERT INTO corridor_slip_rollup (
                corridor_id,
                period,
                n_route_directions,
                n_observed_segments,
                n_total_observations,
                total_weighted_slip_sec,
                mean_slip_per_segment_sec,
                mean_slip_per_observation_sec
            )
            SELECT
                crm.corridor_id,
                rds.period,
                COUNT(DISTINCT (rds.route_id, rds.direction_id)) AS n_route_directions,
                COUNT(*) AS n_observed_segments,
                SUM(rds.n_observations) AS n_total_observations,
                SUM(rds.mean_slip_sec * rds.n_observations) AS total_weighted_slip_sec,
                CASE
                    WHEN COUNT(*) > 0
                    THEN SUM(rds.mean_slip_sec * rds.n_observations) / COUNT(*)
                    ELSE NULL
                END AS mean_slip_per_segment_sec,
                CASE
                    WHEN SUM(rds.n_observations) > 0
                    THEN SUM(rds.mean_slip_sec * rds.n_observations) / SUM(rds.n_observations)
                    ELSE NULL
                END AS mean_slip_per_observation_sec
            FROM corridor_route_membership crm
            JOIN route_diagnostic_segment rds
              ON rds.route_id = crm.route_id
             AND rds.direction_id = crm.direction_id
             AND rds.from_seq >= crm.start_stop_sequence
             AND rds.to_seq   <= crm.end_stop_sequence
            GROUP BY crm.corridor_id, rds.period
            """
        )
    )

    inserted = session.execute(
        text("SELECT COUNT(*) FROM corridor_slip_rollup")
    ).scalar_one()

    # Row-count guard.
    if input_count > 0 and inserted == 0:
        raise RuntimeError(
            f"refresh_corridor_slip produced 0 rows from {input_count} input segments. "
            f"Check whether corridor_route_membership is populated and route_diagnostic_segment "
            f"has matching route_id/direction_id."
        )

    # Second pass: peak_period on period='all' rows.
    session.execute(
        text(
            """
            UPDATE corridor_slip_rollup csr_all
            SET peak_period = sub.peak_period
            FROM (
                SELECT corridor_id,
                       (ARRAY_AGG(period ORDER BY total_weighted_slip_sec DESC))[1]
                           AS peak_period
                FROM corridor_slip_rollup
                WHERE period != 'all'
                GROUP BY corridor_id
            ) sub
            WHERE csr_all.corridor_id = sub.corridor_id
              AND csr_all.period = 'all'
            """
        )
    )

    session.flush()
    return {
        "input_segments": input_count,
        "rollups_inserted": inserted,
    }


if __name__ == "__main__":
    from src.database import get_session

    session = get_session()
    try:
        counts = refresh_corridor_slip(session=session)
        session.commit()
        print(f"[refresh_corridor_slip] {counts}")
    finally:
        session.close()
```

- [ ] **Step 4: Run the integration test**

```bash
PG_TEST_DATABASE_URL=postgresql:///wmata_dashboard uv run pytest tests/test_refresh_corridor_slip.py -v
```

Expected: PASS.

- [ ] **Step 5: Run against real data**

```bash
uv run python pipelines/refresh_corridor_slip.py
psql -d wmata_dashboard -c "SELECT corridor_id, period, n_route_directions, total_weighted_slip_sec, peak_period FROM corridor_slip_rollup WHERE period='all' ORDER BY total_weighted_slip_sec DESC LIMIT 10;"
```

Expected: top-10 corridors with non-zero slip; `peak_period` populated on `period='all'` rows.

- [ ] **Step 6: Lint + commit**

```bash
uv run ruff check pipelines/refresh_corridor_slip.py tests/test_refresh_corridor_slip.py
uv run ruff format --check pipelines/refresh_corridor_slip.py tests/test_refresh_corridor_slip.py
git add pipelines/refresh_corridor_slip.py tests/test_refresh_corridor_slip.py
git commit -m "feat(pipeline): refresh_corridor_slip (NOTES-62)"
```

---

### Task 13: Wire `refresh_corridor_slip` into nightly batch

**Files:**
- Modify: `pipelines/run_daily_batch.py`

- [ ] **Step 1: Locate the place to insert**

```bash
grep -n "refresh_cross_route_segments\|cross_route_segment" pipelines/run_daily_batch.py
```

Insert `refresh_corridor_slip` AFTER `refresh_cross_route_segments` runs (because both read from `route_diagnostic_segment`; order matters only for clarity, not correctness).

- [ ] **Step 2: Add the call**

Follow the existing pattern in `run_daily_batch.py` — likely a `_run_step(...)` helper or a series of `try/except` blocks with `OK`/`SKIP` logging. Add:

```python
from pipelines.refresh_corridor_slip import refresh_corridor_slip

# After refresh_cross_route_segments:
def _refresh_corridor_slip_step(session):
    return refresh_corridor_slip(session=session)

_run_step("refresh_corridor_slip", _refresh_corridor_slip_step, session=session)
```

Match the exact convention used in `run_daily_batch.py` (verify by reading a few existing steps).

- [ ] **Step 3: Dry-run the batch**

```bash
PYTHONUNBUFFERED=1 uv run python pipelines/run_daily_batch.py --service-date 2026-05-24 --dry-run 2>&1 | tail -30
```

(If `--dry-run` isn't supported, skip; the integration test from Task 12 covers the function-level call.)

- [ ] **Step 4: Smoke + lint**

```bash
uv run pytest -m smoke
uv run ruff check pipelines/
uv run ruff format --check pipelines/
```

- [ ] **Step 5: Commit**

```bash
git add pipelines/run_daily_batch.py
git commit -m "feat(batch): wire refresh_corridor_slip into nightly batch (NOTES-62)"
```

---

## Phase 4 — Static map preview

End of phase 4: each corridor has a PNG preview at `static/corridors/{id}.png`, served via FastAPI.

---

### Task 14: Add `staticmap` dependency + render function

**Files:**
- Modify: `pyproject.toml`
- Create: `src/corridor_preview.py`
- Create: `tests/test_corridor_preview.py`

- [ ] **Step 1: Add the dependency**

Edit `pyproject.toml` to add `staticmap` to dependencies:

```toml
[project]
dependencies = [
    # ... existing ...
    "staticmap>=0.5.7",
]
```

Then:

```bash
uv sync
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_corridor_preview.py`:

```python
"""Unit tests for corridor PNG preview rendering."""
import io

import pytest
from PIL import Image

from src.corridor_preview import render_corridor_preview


def test_render_corridor_preview_returns_png_bytes(monkeypatch):
    """A simple LINESTRING renders to a non-empty PNG of expected dimensions."""
    # Mock tile fetching so the test doesn't require network access.
    # If staticmap allows offline mode by setting url_template to None, use that.
    # Otherwise, monkeypatch the tile-fetch internal.
    geometry_wkt = "LINESTRING(-77.05 38.89, -77.04 38.89, -77.03 38.89)"

    png_bytes = render_corridor_preview(geometry_wkt, offline=True)

    assert png_bytes.startswith(b"\x89PNG")
    img = Image.open(io.BytesIO(png_bytes))
    assert img.width == 640
    assert img.height == 320
```

- [ ] **Step 3: Run test to verify failure**

```bash
uv run pytest tests/test_corridor_preview.py -v
```

Expected: `ImportError`.

- [ ] **Step 4: Implement the renderer**

Create `src/corridor_preview.py`:

```python
"""
Render a static PNG preview for a corridor's geometry. Used by
`pipelines/refresh_corridors.py` to populate static/corridors/{id}.png
once per GTFS reload.

Spec: docs/superpowers/specs/2026-05-25-cross-route-corridor-design.md
Section 4.
"""
from __future__ import annotations

import io
import re
from pathlib import Path

from PIL import Image
from staticmap import Line, StaticMap

PREVIEW_WIDTH_PX = 640
PREVIEW_HEIGHT_PX = 320
PREVIEW_LINE_COLOR = "#d62728"
PREVIEW_LINE_WIDTH_PX = 4
POSITRON_TILE_URL = (
    "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"
)
OSM_FALLBACK_URL = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"

_WKT_LINESTRING_RE = re.compile(r"LINESTRING\((.+)\)")


def _parse_wkt_linestring(wkt: str) -> list[tuple[float, float]]:
    """Parse 'LINESTRING(lon lat, lon lat, ...)' into [(lon, lat), ...]."""
    match = _WKT_LINESTRING_RE.search(wkt.strip())
    if not match:
        raise ValueError(f"Not a valid LINESTRING WKT: {wkt[:80]}")
    return [
        tuple(float(c) for c in pair.strip().split())
        for pair in match.group(1).split(",")
    ]


def _render_with_basemap(points: list[tuple[float, float]], tile_url: str | None) -> bytes:
    """Render points onto a basemap (or blank if tile_url is None)."""
    m = StaticMap(
        width=PREVIEW_WIDTH_PX,
        height=PREVIEW_HEIGHT_PX,
        url_template=tile_url or "",
    )
    m.add_line(Line(points, PREVIEW_LINE_COLOR, PREVIEW_LINE_WIDTH_PX))
    img = m.render()
    if img.mode != "RGB":
        img = img.convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def render_corridor_preview(
    geometry_wkt: str,
    *,
    offline: bool = False,
) -> bytes:
    """
    Render a corridor's geometry as a 640x320 PNG.

    Best-effort tile fetching: try Positron, fall back to OSM, fall back
    to a blank white background with the polyline overlay only. The
    `offline=True` flag forces the blank-background path (used in tests
    to avoid network).
    """
    points = _parse_wkt_linestring(geometry_wkt)
    if not points:
        raise ValueError("Empty LINESTRING")

    if offline:
        # Draw the polyline on a plain white background via Pillow directly.
        return _render_blank(points)

    # Primary: Positron.
    try:
        return _render_with_basemap(points, POSITRON_TILE_URL)
    except Exception:
        pass

    # Fallback: OSM.
    try:
        return _render_with_basemap(points, OSM_FALLBACK_URL)
    except Exception:
        pass

    # Last resort: blank background.
    return _render_blank(points)


def _render_blank(points: list[tuple[float, float]]) -> bytes:
    """Render the polyline on a plain white background (no basemap)."""
    from PIL import ImageDraw

    img = Image.new("RGB", (PREVIEW_WIDTH_PX, PREVIEW_HEIGHT_PX), "white")
    draw = ImageDraw.Draw(img)

    # Compute bbox + 10% padding.
    lons = [p[0] for p in points]
    lats = [p[1] for p in points]
    lon_min, lon_max = min(lons), max(lons)
    lat_min, lat_max = min(lats), max(lats)
    lon_pad = (lon_max - lon_min) * 0.10 or 0.001
    lat_pad = (lat_max - lat_min) * 0.10 or 0.001
    lon_min -= lon_pad
    lon_max += lon_pad
    lat_min -= lat_pad
    lat_max += lat_pad

    def project(lon: float, lat: float) -> tuple[int, int]:
        x = int((lon - lon_min) / (lon_max - lon_min) * (PREVIEW_WIDTH_PX - 1))
        y = int((lat_max - lat) / (lat_max - lat_min) * (PREVIEW_HEIGHT_PX - 1))
        return (x, y)

    pixel_pts = [project(*p) for p in points]
    draw.line(
        pixel_pts,
        fill=PREVIEW_LINE_COLOR,
        width=PREVIEW_LINE_WIDTH_PX,
    )

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def write_preview_to_disk(
    geometry_wkt: str,
    output_path: Path,
    *,
    offline: bool = False,
) -> None:
    """Render and write to disk. Creates parent directory if needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(render_corridor_preview(geometry_wkt, offline=offline))
```

- [ ] **Step 5: Run test to verify it passes**

```bash
uv run pytest tests/test_corridor_preview.py -v
```

Expected: PASS.

- [ ] **Step 6: Lint + commit**

```bash
uv run ruff check src/corridor_preview.py tests/test_corridor_preview.py
uv run ruff format --check src/corridor_preview.py tests/test_corridor_preview.py
git add pyproject.toml uv.lock src/corridor_preview.py tests/test_corridor_preview.py
git commit -m "feat(corridor): static PNG preview renderer (NOTES-62)"
```

---

### Task 15: Wire preview generation into `refresh_corridors`

**Files:**
- Modify: `pipelines/refresh_corridors.py`

- [ ] **Step 1: Sketch the contract**

After each corridor is inserted (and its `corridor_id` known), call `write_preview_to_disk` with the target path `static/corridors/{corridor_id}.png`. On any failure, log and continue — the corridor row is still valid without a preview.

Orphan cleanup: BEFORE writing new previews, list existing PNGs in `static/corridors/` and delete any whose `corridor_id` will no longer exist. Since we wipe + reinsert, easiest approach: delete ALL files in `static/corridors/` before rendering.

- [ ] **Step 2: Modify the pipeline**

In `pipelines/refresh_corridors.py`, after the corridor inserts and BEFORE returning, add preview generation:

```python
# Near the top of the file:
import logging
from pathlib import Path
from src.corridor_preview import write_preview_to_disk

LOG = logging.getLogger(__name__)

PREVIEW_DIR = Path("static/corridors")


def _wipe_preview_directory() -> None:
    """Delete all existing preview PNGs so orphans don't accumulate."""
    if not PREVIEW_DIR.exists():
        return
    for png in PREVIEW_DIR.glob("*.png"):
        try:
            png.unlink()
        except OSError as exc:
            LOG.warning(f"Could not delete stale preview {png}: {exc}")


def _render_previews(corridors: list[Corridor]) -> int:
    """Render PNG previews for each corridor. Returns count rendered successfully."""
    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    rendered = 0
    for c in corridors:
        try:
            write_preview_to_disk(
                geometry_wkt=c.geometry_wkt,
                output_path=PREVIEW_DIR / f"{c.corridor_id}.png",
            )
            rendered += 1
        except Exception as exc:
            LOG.warning(
                f"Preview render failed for corridor {c.corridor_id}: {exc}"
            )
    return rendered
```

Add the call inside `refresh_corridors` after the inserts complete:

```python
# After session.flush() but before _counts_to_dict:
_wipe_preview_directory()
inserted_corridors = session.query(Corridor).all()
counts_dict = _counts_to_dict(counts)
counts_dict["previews_rendered"] = _render_previews(inserted_corridors)
return counts_dict
```

- [ ] **Step 3: Run the integration test**

```bash
PG_TEST_DATABASE_URL=postgresql:///wmata_dashboard uv run pytest tests/test_refresh_corridors.py -v
```

The fixture test should now also produce PNGs. Add an assertion to `test_refresh_corridors_two_routes_same_corridor`:

```python
from pathlib import Path

def test_refresh_corridors_writes_previews(pg_session, populate_fixture_gtfs, tmp_path, monkeypatch):
    """After refresh, every corridor has a PNG preview on disk."""
    # Redirect PREVIEW_DIR to a temp location for the test.
    import pipelines.refresh_corridors as rc
    monkeypatch.setattr(rc, "PREVIEW_DIR", tmp_path)

    populate_fixture_gtfs(pg_session, scenario="two_routes_one_corridor")
    counts = refresh_corridors(session=pg_session, gtfs_snapshot_id=1)

    pngs = list(tmp_path.glob("*.png"))
    assert len(pngs) == counts["corridors_inserted"]
    assert counts["previews_rendered"] == counts["corridors_inserted"]
```

- [ ] **Step 4: Run against real data**

```bash
uv run python pipelines/refresh_corridors.py
ls -la static/corridors/ | head
```

Expected: ~20-40 PNG files. Open one in a browser or Preview to confirm it renders the corridor on a basemap.

- [ ] **Step 5: Add `static/corridors/` to `.gitignore`**

```bash
echo "static/corridors/*.png" >> .gitignore
git diff .gitignore
```

(PNGs are generated artifacts; the directory exists but its contents shouldn't be checked in.)

- [ ] **Step 6: Lint + commit**

```bash
uv run ruff check pipelines/
uv run ruff format --check pipelines/
git add pipelines/refresh_corridors.py tests/test_refresh_corridors.py .gitignore
git commit -m "feat(corridor): render PNG previews during refresh (NOTES-62)"
```

---

### Task 16: FastAPI static-files mount for `/static/corridors/`

**Files:**
- Modify: `api/main.py`

- [ ] **Step 1: Add the mount**

Read existing `api/main.py` near the FastAPI app initialization:

```bash
grep -n "FastAPI\|app =\|StaticFiles" api/main.py | head
```

Add at app initialization:

```python
from fastapi.staticfiles import StaticFiles
from pathlib import Path

# After app = FastAPI(...):
STATIC_DIR = Path(__file__).parent.parent / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)
(STATIC_DIR / "corridors").mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
```

- [ ] **Step 2: Manually verify**

Start the API:

```bash
uv run uvicorn api.main:app --reload &
```

Hit a known preview URL:

```bash
curl -I http://localhost:8000/static/corridors/1.png
```

Expected: `HTTP/1.1 200 OK` + `Content-Type: image/png`. Stop the dev server.

- [ ] **Step 3: Smoke + lint**

```bash
uv run pytest -m smoke
uv run ruff check api/
uv run ruff format --check api/
```

- [ ] **Step 4: Commit**

```bash
git add api/main.py
git commit -m "feat(api): mount /static for corridor PNG previews (NOTES-62)"
```

---

## Phase 5 — API

End of phase 5: `/api/segments?level=corridor` and `/api/corridors/{id}/segments` work and are tested.

---

### Task 17: Add `level` param + corridor query function

**Files:**
- Modify: `api/main.py`
- Modify: `api/aggregations.py`
- Create: `tests/test_api_corridors.py`

- [ ] **Step 1: Write the failing API test**

Create `tests/test_api_corridors.py`:

```python
"""Tests for /api/segments?level=corridor and /api/corridors/{id}/segments."""
import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture
def client():
    return TestClient(app)


def test_get_segments_level_corridor_returns_expected_shape(client):
    """level=corridor returns the corridor-mode response shape."""
    response = client.get("/api/segments?level=corridor")
    assert response.status_code == 200
    body = response.json()
    assert body["level"] == "corridor"
    assert "period" in body
    assert "n_rows" in body
    assert "corridors" in body
    if body["n_rows"] > 0:
        first = body["corridors"][0]
        # Required fields per spec Section 3.
        assert "corridor_id" in first
        assert "display_name" in first
        assert "direction_cardinal" in first
        assert "start_stop_id" in first
        assert "end_stop_id" in first
        assert "length_m" in first
        assert "route_set" in first
        assert "total_weighted_slip_sec" in first
        assert "preview_url" in first
        assert first["preview_url"].startswith("/static/corridors/")


def test_get_segments_level_segment_back_compat(client):
    """Default and level=segment behave identically; PR #140 contract unchanged."""
    default = client.get("/api/segments")
    explicit = client.get("/api/segments?level=segment")
    assert default.status_code == 200
    assert explicit.status_code == 200
    # Same row shape — at minimum, both have a 'segments' key (PR #140).
    assert "segments" in default.json()
    assert "segments" in explicit.json()


def test_get_segments_invalid_level(client):
    """level must be 'segment' or 'corridor'."""
    response = client.get("/api/segments?level=corridors")  # typo
    assert response.status_code == 400


def test_get_corridor_drilldown_returns_segments_within_range(client):
    """/api/corridors/{id}/segments returns PR #140-shape rows filtered to the corridor."""
    # First fetch a corridor to get an id.
    corridor_resp = client.get("/api/segments?level=corridor&limit=1")
    corridors = corridor_resp.json()["corridors"]
    if not corridors:
        pytest.skip("No corridors in dev DB yet")
    cid = corridors[0]["corridor_id"]

    drill_resp = client.get(f"/api/corridors/{cid}/segments")
    assert drill_resp.status_code == 200
    body = drill_resp.json()
    assert "segments" in body
    # Drill-down rows share PR #140's segment row shape.
    if body["segments"]:
        first = body["segments"][0]
        assert "from_stop_id" in first
        assert "to_stop_id" in first
```

- [ ] **Step 2: Run the test to verify failure**

```bash
uv run pytest tests/test_api_corridors.py -v
```

Expected: 400 errors / shape mismatches.

- [ ] **Step 3: Add the corridor query function**

Append to `api/aggregations.py`:

```python
from src.models import Corridor, CorridorRouteMembership, CorridorSlipRollup


def get_corridor_rollup(db, *, period: str, limit: int) -> dict:
    """
    Return ranked corridor rollup rows for the given period, joined with
    Corridor metadata and CorridorRouteMembership per-route contributions.
    """
    rows = (
        db.query(CorridorSlipRollup, Corridor)
        .join(Corridor, Corridor.corridor_id == CorridorSlipRollup.corridor_id)
        .filter(CorridorSlipRollup.period == period)
        .order_by(CorridorSlipRollup.total_weighted_slip_sec.desc())
        .limit(limit)
        .all()
    )

    corridor_ids = [c.corridor_id for (_, c) in rows]
    # Bulk-load memberships for these corridors.
    memberships = (
        db.query(CorridorRouteMembership)
        .filter(CorridorRouteMembership.corridor_id.in_(corridor_ids))
        .all()
        if corridor_ids
        else []
    )
    by_corridor: dict[int, list[CorridorRouteMembership]] = {}
    for m in memberships:
        by_corridor.setdefault(m.corridor_id, []).append(m)

    # Bulk-load stop names for the start/end stops.
    from src.models import Stop

    stop_ids = {c.start_stop_id for (_, c) in rows} | {c.end_stop_id for (_, c) in rows}
    stop_name_map: dict[str, str] = {}
    if stop_ids:
        stop_rows = (
            db.query(Stop.stop_id, Stop.stop_name)
            .filter(Stop.stop_id.in_(stop_ids), Stop.is_current.is_(True))
            .all()
        )
        stop_name_map = {sid: sname for sid, sname in stop_rows}

    corridors_payload = []
    for slip, corridor in rows:
        ms = by_corridor.get(corridor.corridor_id, [])
        contributing = [
            {
                "route_id": m.route_id,
                "direction_id": m.direction_id,
                "canonical_shape_id": m.canonical_shape_id,
            }
            for m in ms
        ]
        corridors_payload.append(
            {
                "corridor_id": corridor.corridor_id,
                "display_name": corridor.display_name,
                "direction_cardinal": corridor.direction_cardinal,
                "start_stop_id": corridor.start_stop_id,
                "start_stop_name": stop_name_map.get(corridor.start_stop_id, ""),
                "end_stop_id": corridor.end_stop_id,
                "end_stop_name": stop_name_map.get(corridor.end_stop_id, ""),
                "length_m": corridor.length_m,
                "n_routes": corridor.n_routes,
                "route_set": corridor.route_set,
                "n_route_directions": slip.n_route_directions,
                "n_observed_segments": slip.n_observed_segments,
                "n_total_observations": slip.n_total_observations,
                "total_weighted_slip_sec": slip.total_weighted_slip_sec,
                "mean_slip_per_segment_sec": slip.mean_slip_per_segment_sec,
                "mean_slip_per_observation_sec": slip.mean_slip_per_observation_sec,
                "peak_period": slip.peak_period,
                "preview_url": f"/static/corridors/{corridor.corridor_id}.png",
                "contributing_routes": contributing,
            }
        )

    return {
        "level": "corridor",
        "period": period,
        "n_rows": len(corridors_payload),
        "corridors": corridors_payload,
    }
```

- [ ] **Step 4: Wire `level` into the endpoint**

Modify `api/main.py` `get_segments` (line ~1086):

```python
@app.get("/api/segments")
async def get_segments(
    level: str = "segment",
    period: str = "all",
    limit: int = 100,
):
    """
    [...existing docstring...]
    
    Args:
        level: "segment" (default; PR #140 stop-pair view) or "corridor"
            (NOTES-62 corridor rollup).
        period: One of ``all``, ``am_peak``, ``midday``, ``pm_peak``,
            ``evening``, ``late``.
        limit: Max rows to return (default 100, max 500).
    """
    if level not in ("segment", "corridor"):
        raise HTTPException(
            status_code=400,
            detail="level must be 'segment' or 'corridor'",
        )
    if period not in DIAGNOSTIC_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(DIAGNOSTIC_PERIODS)}",
        )
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    db = get_session()
    try:
        if level == "corridor":
            from api.aggregations import get_corridor_rollup
            return get_corridor_rollup(db, period=period, limit=limit)
        return get_cross_route_segments(db, period=period, limit=limit)
    finally:
        db.close()
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/test_api_corridors.py -v
```

Expected: all PASS (or `pytest.skip` on the drill-down if dev DB is empty; ignore for now).

- [ ] **Step 6: Lint + commit**

```bash
uv run ruff check api/ tests/
uv run ruff format --check api/ tests/
git add api/main.py api/aggregations.py tests/test_api_corridors.py
git commit -m "feat(api): level=corridor on /api/segments + get_corridor_rollup (NOTES-62)"
```

---

### Task 18: Drill-down endpoint `/api/corridors/{id}/segments`

**Files:**
- Modify: `api/main.py`
- Modify: `api/aggregations.py`

- [ ] **Step 1: Add the query function**

Append to `api/aggregations.py`:

```python
def get_corridor_constituent_segments(db, *, corridor_id: int, period: str) -> dict:
    """
    Return the PR #140 stop-pair segment rows that fall within a given
    corridor (filtered to its route_set and stop_sequence range per route).
    """
    membership = (
        db.query(CorridorRouteMembership)
        .filter(CorridorRouteMembership.corridor_id == corridor_id)
        .all()
    )
    if not membership:
        return {"corridor_id": corridor_id, "period": period, "segments": []}

    # Build a (from_stop_id, to_stop_id) candidate set from each route's stop range.
    # Easier: query RouteDiagnosticSegment with OR conditions for each (route, dir, seq range).
    from sqlalchemy import and_, or_

    from src.models import RouteDiagnosticSegment

    route_conditions = []
    for m in membership:
        route_conditions.append(
            and_(
                RouteDiagnosticSegment.route_id == m.route_id,
                RouteDiagnosticSegment.direction_id == m.direction_id,
                RouteDiagnosticSegment.from_seq >= m.start_stop_sequence,
                RouteDiagnosticSegment.to_seq <= m.end_stop_sequence,
            )
        )

    rows = (
        db.query(RouteDiagnosticSegment)
        .filter(
            RouteDiagnosticSegment.period == period,
            or_(*route_conditions),
        )
        .order_by(RouteDiagnosticSegment.mean_slip_sec.desc())
        .all()
    )

    segments_payload = [
        {
            "route_id": r.route_id,
            "direction_id": r.direction_id,
            "from_stop_id": r.from_stop_id,
            "from_seq": r.from_seq,
            "to_stop_id": r.to_stop_id,
            "to_seq": r.to_seq,
            "mean_slip_sec": r.mean_slip_sec,
            "n_observations": r.n_observations,
        }
        for r in rows
    ]
    return {
        "corridor_id": corridor_id,
        "period": period,
        "segments": segments_payload,
    }
```

- [ ] **Step 2: Add the endpoint**

In `api/main.py`, after `get_segments`:

```python
@app.get("/api/corridors/{corridor_id}/segments")
async def get_corridor_segments(corridor_id: int, period: str = "all"):
    """
    Drill-down: stop-pair segment rows inside a given corridor (NOTES-62).
    """
    if period not in DIAGNOSTIC_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period. Must be one of: {', '.join(DIAGNOSTIC_PERIODS)}",
        )

    db = get_session()
    try:
        from api.aggregations import get_corridor_constituent_segments

        result = get_corridor_constituent_segments(
            db, corridor_id=corridor_id, period=period
        )
        # If corridor doesn't exist, segments list will be empty; differentiate from 404.
        from src.models import Corridor

        if not db.query(Corridor).filter_by(corridor_id=corridor_id).first():
            raise HTTPException(status_code=404, detail="Corridor not found")
        return result
    finally:
        db.close()
```

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/test_api_corridors.py -v
```

Expected: all PASS (the drill-down test from Task 17 now resolves).

- [ ] **Step 4: Lint + commit**

```bash
uv run ruff check api/
uv run ruff format --check api/
git add api/main.py api/aggregations.py
git commit -m "feat(api): /api/corridors/{id}/segments drill-down (NOTES-62)"
```

---

## Phase 6 — Frontend

End of phase 6: toggle on `/segments` page works; corridor rows show map previews and expand to constituent segments. Lint and Playwright baselines updated.

---

### Task 19: Toggle button + URL state in `SegmentDiagnostic.jsx`

**Files:**
- Modify: `frontend/src/components/SegmentDiagnostic.jsx`

- [ ] **Step 1: Read the existing page structure**

```bash
head -80 frontend/src/components/SegmentDiagnostic.jsx
```

Identify: the data-fetching hook (probably `useEffect` + `fetch('/api/segments...')`), the period selector, and the table render code.

- [ ] **Step 2: Add the level state + toggle**

Modify the imports and top-level state:

```jsx
import { useState, useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'

function SegmentDiagnostic() {
  const [searchParams, setSearchParams] = useSearchParams()
  const level = searchParams.get('level') || 'segment'
  const period = searchParams.get('period') || 'all'

  const setLevel = (newLevel) => {
    const next = new URLSearchParams(searchParams)
    next.set('level', newLevel)
    setSearchParams(next)
  }

  // ... existing state ...

  return (
    <div className="segment-diagnostic">
      <div className="segment-diagnostic-header">
        <h1>Segment Diagnostic</h1>
        <div className="level-toggle">
          <button
            className={level === 'segment' ? 'active' : ''}
            onClick={() => setLevel('segment')}
          >
            Segments
          </button>
          <button
            className={level === 'corridor' ? 'active' : ''}
            onClick={() => setLevel('corridor')}
          >
            Corridors
          </button>
        </div>
      </div>
      {/* ...rest of existing layout... */}
    </div>
  )
}
```

If the page already uses URL state for period, mirror that pattern; if not, this introduces URL-state for both.

- [ ] **Step 3: Modify the fetch to use `level`**

Update the existing `useEffect` that calls `/api/segments`:

```jsx
useEffect(() => {
  setLoading(true)
  fetch(`/api/segments?level=${level}&period=${period}&limit=100`)
    .then((res) => res.json())
    .then((data) => {
      setData(data)
      setLoading(false)
    })
    .catch((err) => {
      setError(err.message)
      setLoading(false)
    })
}, [level, period])
```

- [ ] **Step 4: Add CSS for the toggle**

Append to `frontend/src/App.css` (or wherever the component's styles live):

```css
.level-toggle {
  display: inline-flex;
  border: 1px solid var(--color-border, #ccc);
  border-radius: 4px;
  overflow: hidden;
}

.level-toggle button {
  padding: 0.5rem 1rem;
  border: 0;
  background: transparent;
  cursor: pointer;
}

.level-toggle button.active {
  background: var(--color-accent, #2563eb);
  color: white;
}
```

- [ ] **Step 5: Run lint + visual check in browser**

```bash
cd frontend && npm run lint
cd frontend && npm run dev
```

Visit `http://localhost:5173/segments` and verify the toggle appears and switches the URL. Switch to `/segments?level=corridor` — page will likely error on rendering because columns are still segment-shape. Next task fixes the columns.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/SegmentDiagnostic.jsx frontend/src/App.css
git commit -m "feat(frontend): segment/corridor toggle + URL state (NOTES-62)"
```

---

### Task 20: Conditional columns for corridor mode

**Files:**
- Modify: `frontend/src/components/SegmentDiagnostic.jsx`

- [ ] **Step 1: Branch the table renderer by level**

Inside the table component within `SegmentDiagnostic.jsx`, conditionally render columns:

```jsx
{level === 'corridor' ? (
  <table className="segment-table corridor-table">
    <thead>
      <tr>
        <th>Corridor</th>
        <th>Routes</th>
        <th>Dir</th>
        <th>Length (m)</th>
        <th>Slip/obs (s)</th>
        <th>Total slip (s)</th>
        <th>Peak</th>
      </tr>
    </thead>
    <tbody>
      {data.corridors.map((c) => (
        <CorridorRow key={c.corridor_id} corridor={c} period={period} />
      ))}
    </tbody>
  </table>
) : (
  /* existing segment table */
)}
```

`CorridorRow` is a new component defined in the same file (or split out if it gets long). For now, render a minimal row with display_name + key fields and an expand toggle:

```jsx
function CorridorRow({ corridor, period }) {
  const [expanded, setExpanded] = useState(false)
  return (
    <>
      <tr onClick={() => setExpanded(!expanded)} className="corridor-row">
        <td>{corridor.display_name}</td>
        <td>{corridor.route_set.join(', ')}</td>
        <td>{corridor.direction_cardinal}</td>
        <td>{Math.round(corridor.length_m)}</td>
        <td>{(corridor.mean_slip_per_observation_sec ?? 0).toFixed(1)}</td>
        <td>{Math.round(corridor.total_weighted_slip_sec)}</td>
        <td>{corridor.peak_period ?? '—'}</td>
      </tr>
      {expanded && (
        <tr className="corridor-expansion">
          <td colSpan={7}>
            {/* Two-pane expansion lands in Task 21. */}
            <div>Expansion placeholder for corridor {corridor.corridor_id}.</div>
          </td>
        </tr>
      )}
    </>
  )
}
```

- [ ] **Step 2: Verify in browser**

```bash
cd frontend && npm run dev
```

Visit `/segments?level=corridor` — the table now renders corridor rows. Click a row to see the expansion placeholder.

- [ ] **Step 3: Lint + commit**

```bash
cd frontend && npm run lint
git add frontend/src/components/SegmentDiagnostic.jsx
git commit -m "feat(frontend): corridor table mode with conditional columns (NOTES-62)"
```

---

### Task 21: Two-pane row expansion with preview + per-route table

**Files:**
- Modify: `frontend/src/components/SegmentDiagnostic.jsx`

- [ ] **Step 1: Flesh out `CorridorRow` expansion**

Replace the placeholder expansion content with the two-pane layout: top is the static map preview; middle is the per-route contribution table; bottom is the constituent segments (Task 22).

```jsx
function CorridorRow({ corridor, period }) {
  const [expanded, setExpanded] = useState(false)
  const [segments, setSegments] = useState(null)

  useEffect(() => {
    if (!expanded || segments !== null) return
    fetch(`/api/corridors/${corridor.corridor_id}/segments?period=${period}`)
      .then((res) => res.json())
      .then((data) => setSegments(data.segments))
      .catch(() => setSegments([]))
  }, [expanded, corridor.corridor_id, period, segments])

  return (
    <>
      <tr onClick={() => setExpanded(!expanded)} className="corridor-row">
        {/* row cells from Task 20 */}
      </tr>
      {expanded && (
        <tr className="corridor-expansion">
          <td colSpan={7}>
            <div className="corridor-preview">
              <img
                src={corridor.preview_url}
                alt={`Map of ${corridor.display_name}`}
                width="640"
                height="320"
                loading="lazy"
              />
            </div>
            <div className="corridor-contributing-routes">
              <h4>Contributing routes</h4>
              <table>
                <thead>
                  <tr>
                    <th>Route</th>
                    <th>Direction</th>
                    <th>Canonical shape</th>
                  </tr>
                </thead>
                <tbody>
                  {corridor.contributing_routes.map((r) => (
                    <tr key={`${r.route_id}-${r.direction_id}`}>
                      <td>{r.route_id}</td>
                      <td>{r.direction_id}</td>
                      <td>{r.canonical_shape_id}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {/* Constituent segments table lands in Task 22. */}
          </td>
        </tr>
      )}
    </>
  )
}
```

- [ ] **Step 2: CSS for the two-pane layout**

Append to `frontend/src/App.css`:

```css
.corridor-expansion {
  background: var(--color-bg-soft, #f9fafb);
}

.corridor-preview img {
  border: 1px solid var(--color-border, #ddd);
  border-radius: 4px;
}

.corridor-contributing-routes {
  margin-top: 1rem;
}

.corridor-contributing-routes table {
  border-collapse: collapse;
  font-size: 0.9rem;
}

.corridor-contributing-routes th,
.corridor-contributing-routes td {
  border: 1px solid var(--color-border, #ddd);
  padding: 0.25rem 0.5rem;
}
```

- [ ] **Step 3: Verify in browser**

Start the dev server and the API; expand a corridor row; confirm the preview image loads and the contributing routes table renders.

- [ ] **Step 4: Lint + commit**

```bash
cd frontend && npm run lint
git add frontend/src/components/SegmentDiagnostic.jsx frontend/src/App.css
git commit -m "feat(frontend): corridor row two-pane expansion with preview (NOTES-62)"
```

---

### Task 22: Constituent segments nested table + filters

**Files:**
- Modify: `frontend/src/components/SegmentDiagnostic.jsx`
- Modify: `frontend/src/App.css`

- [ ] **Step 1: Add the constituent-segments table to the expansion**

Append to the `<td colSpan>` block in `CorridorRow`:

```jsx
<div className="corridor-constituent-segments">
  <h4>Constituent segments (stop-pairs)</h4>
  {segments === null ? (
    <div>Loading…</div>
  ) : segments.length === 0 ? (
    <div>No segment data for this period.</div>
  ) : (
    <table>
      <thead>
        <tr>
          <th>Route</th>
          <th>Dir</th>
          <th>From</th>
          <th>To</th>
          <th>Slip (s)</th>
          <th>Observations</th>
        </tr>
      </thead>
      <tbody>
        {segments.map((s) => (
          <tr key={`${s.route_id}-${s.direction_id}-${s.from_seq}-${s.to_seq}`}>
            <td>{s.route_id}</td>
            <td>{s.direction_id}</td>
            <td>{s.from_stop_id}</td>
            <td>{s.to_stop_id}</td>
            <td>{s.mean_slip_sec.toFixed(1)}</td>
            <td>{s.n_observations}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )}
</div>
```

- [ ] **Step 2: Add a min_length filter for corridor mode**

In the page's top filter bar (above the table), add a slider when `level === 'corridor'`:

```jsx
{level === 'corridor' && (
  <label>
    Min length (m):
    <input
      type="range"
      min="0"
      max="3000"
      step="100"
      value={minLength}
      onChange={(e) => setMinLength(Number(e.target.value))}
    />
    <span>{minLength}</span>
  </label>
)}
```

`minLength` is a local state. Apply it to filter data client-side:

```jsx
const displayedCorridors = (data?.corridors ?? []).filter(
  (c) => c.length_m >= minLength
)
```

- [ ] **Step 3: CSS for the nested table**

Append:

```css
.corridor-constituent-segments {
  margin-top: 1rem;
}

.corridor-constituent-segments table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}

.corridor-constituent-segments th,
.corridor-constituent-segments td {
  border: 1px solid var(--color-border, #ddd);
  padding: 0.25rem 0.5rem;
}
```

- [ ] **Step 4: Run frontend tests + lint**

```bash
cd frontend && npm run lint
cd frontend && npm test
```

Expected: lint clean, Vitest passing. If there are existing component tests for `SegmentDiagnostic`, ensure they still pass; add a smoke render test if none exist.

- [ ] **Step 5: Regenerate Playwright baselines**

The toggle and new corridor mode change the page's visual appearance, which will break existing Playwright snapshots on `/segments`. Regenerate both macOS and Linux baselines per CLAUDE.md:

```bash
cd frontend && npx playwright test --update-snapshots
```

Then the Linux baseline via Docker:

```bash
cd frontend && docker run --rm -v "$(pwd):/work" -v /work/node_modules -w /work mcr.microsoft.com/playwright:v1.60.0-noble bash -c "npm ci --silent && npx playwright test --update-snapshots"
```

Confirm both `*-chromium-darwin.png` and `*-chromium-linux.png` baselines updated.

- [ ] **Step 6: Build verification**

```bash
cd frontend && npm run build
```

Expected: clean build, no warnings beyond the existing baseline.

- [ ] **Step 7: Commit**

```bash
git add frontend/
git commit -m "feat(frontend): corridor drill-down + min-length filter + new baselines (NOTES-62)"
```

---

## Phase 7 — NOTES.md fold-in + PR

End of phase 7: PR is open with all changes; NOTES-62 entries removed from NOTES.md; cross-refs rewritten.

---

### Task 23: NOTES.md edits + PR open

**Files:**
- Modify: `NOTES.md`

- [ ] **Step 1: Remove the NOTES-62 entries**

NOTES.md has two NOTES-62 sites (from the previous reads):
- The bullet under "Diagnostic outputs (route-level + system-wide)" (around line 115).
- The full `## NOTES-62. Cross-route corridor diagnostic (V2, geometric rollup)` section (around line 376).

Remove both. Rewrite any surviving cross-references in the repo:

```bash
grep -rn 'NOTES-62' --include='*.md' --include='*.py' --include='*.tsx' --include='*.ts' --include='*.jsx'
```

Replace each occurrence with a descriptive PR-anchored phrase like "the corridor V2 rollout (PR #N)". Use the in-flight PR number once known; leave a `TODO(PR-N)` placeholder if not yet open.

- [ ] **Step 2: Update the "Last edited" line**

Change the top of NOTES.md from "Last edited YYYY-MM-DD" to today's date.

- [ ] **Step 3: Verify ruff + smoke**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
uv run pytest -m smoke
```

- [ ] **Step 4: Final commit and open PR**

```bash
git add NOTES.md
# Any other files touched by cross-ref rewrites:
# git add <files>
git commit -m "docs: remove NOTES-62 (closed by corridor V2 rollout)"
git push -u origin <feature-branch>
gh pr create --title "feat: cross-route corridor diagnostic V2 (NOTES-62)" --body "$(cat <<'EOF'
## Summary
- Cross-route corridor rollup built atop PR #140's stop-pair segment diagnostic.
- Three new tables: `corridors`, `corridor_route_membership`, `corridor_slip_rollup`.
- Pure-Python shape-matching (15m proximity + 30° bearing) identifies corridors from `shapes` alone; no OSM dependency.
- `/api/segments?level=corridor` extends PR #140's endpoint; `/api/corridors/{id}/segments` drill-down.
- `SegmentDiagnostic.jsx` gains a Segment | Corridor toggle with a two-pane row expansion (static PNG preview + per-route + constituent stop-pairs).
- Static PNG previews rendered server-side at GTFS reload via `staticmap`; served at `/static/corridors/{id}.png`.

## Why this scope
Spec: `docs/superpowers/specs/2026-05-25-cross-route-corridor-design.md`. Empirical
finding during brainstorming (median 12m shape spacing, co-coded across routes
that share a street) collapsed the originally-feared OSM ingestion into a
SQL+Python pipeline. Stop-anchored display names sidestep a reverse-geocoding
dependency. Interactive map view deferred to V3 (the static PNG is the bridge).

## Test plan
- [ ] `uv run pytest tests/test_corridor_identity.py tests/test_corridor_preview.py tests/test_corridor_models.py -v`
- [ ] `PG_TEST_DATABASE_URL=postgresql:///wmata_dashboard uv run pytest tests/test_refresh_corridors.py tests/test_refresh_corridor_slip.py tests/test_api_corridors.py -v`
- [ ] `uv run python scripts/migrate_create_corridors.py && uv run python scripts/migrate_create_corridor_slip_rollup.py`
- [ ] `uv run python pipelines/refresh_corridors.py` — verify 20-40 corridors on real WMATA shapes
- [ ] `uv run python pipelines/refresh_corridor_slip.py` — verify non-zero slip totals
- [ ] Spot-check Wisconsin Ave SB corridor exists with route_set including D80 and D82
- [ ] `cd frontend && npm run lint && npm test && npm run build`
- [ ] Visual: open `/segments?level=corridor`, expand a row, confirm preview image and constituent segments render

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-review

After writing this plan, I checked it against the spec:

**1. Spec coverage:**
- Section 1 (algorithm) → Tasks 3, 4, 5, 6, 7
- Section 2 (data model) → Tasks 1, 2, 10, 11
- Section 3 (API + UI) → Tasks 17, 18, 19, 20, 21, 22
- Section 4 (static map preview) → Tasks 14, 15, 16
- Section 5 (calibration knobs) → embedded in Task 3 (`src/corridor_identity.py` constants)
- Section 6 (edge cases) → covered in fixture tests (Tasks 7, 8, 12)
- Section 7 (test strategy) → unit + integration in each task
- Section 8 (implementation surfaces) → one task per surface
- Section 9 (verified facts) → resolved during plan writing
- Section 10 (out of scope) → deferred consistently

**2. Placeholder scan:** No TBD/TODO outside of the deliberate `TODO(PR-N)` placeholder in Task 23 (cross-ref rewrite). No "implement later" or vague references.

**3. Type consistency:** `Corridor`, `CorridorRouteMembership`, `CorridorSlipRollup` ORM names used consistently. `corridor_id` column type INTEGER everywhere. `route_set` is `JSONB` in migration, `JSON` in ORM (SQLAlchemy maps JSON to JSONB on Postgres automatically). `direction_cardinal` is TEXT/String everywhere.

**4. Ambiguity check:** None spotted.
