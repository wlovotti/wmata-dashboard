# Cross-route corridor diagnostic V2 — design

**Item:** NOTES-62
**Date:** 2026-05-25
**Status:** Approved design (this doc); implementation plan to follow via writing-plans.

## Goal

Roll the cross-route segment diagnostic (PR #140) up from stop-pairs to
directional corridors, so a planner reading the ranked-list sees
"Wisconsin Ave SB: Friendship Heights → Foggy Bottom" as one investment
target rather than N adjacent stop-pairs.

Output ranks corridors by trip-volume-weighted system-wide slip — the
unit at which TSP / queue-jump / bus-lane decisions are actually scoped.
Stop-pair granularity (PR #140) is preserved as a drill-down child.

## Key empirical finding

WMATA's GTFS shapes have **median 12 m point spacing** (mean 12 m, p95
20 m) and are **co-coded across routes that share a street**: D80's
shape on Wisconsin Ave and D82's shape on Wisconsin Ave have a median
nearest-neighbor distance of 0.0 m on the shared section, with 67% of
points within 10 m.

This makes corridor identification feasible from `shapes` alone — no
OpenStreetMap road-network ingestion, no Fréchet matching, no street
graph. The original NOTES-62 framing assumed OSM was required; it
isn't.

## Non-goals (deliberate, V2 only)

- OSM ingestion or any external road-network dependency.
- Corridor naming via reverse geocoding. Stop-anchored labels are
  deterministic and rebuild-friendly.
- Interactive map view (Leaflet / MapLibre). V2 ships a per-row
  static PNG preview only.
- Per-day slip trend at the corridor level. Single rolling-window
  aggregate, matching PR #140.
- Interpretation of GTFS `direction_id` semantics. We use computed
  bearing exclusively.
- Corridor identity for variant-only shapes. Canonical-per-direction
  only.
- Cost-of-intervention estimates.

---

## 1. Corridor identity algorithm

### Inputs

- All `(shape_id, shape_pt_sequence, shape_pt_lat, shape_pt_lon)` rows
  from `shapes`.
- `trips.shape_id → route_id, direction_id` mapping, restricted to
  `is_current=TRUE`.
- `routes` and `stops` for naming + endpoint snapping.

### Step 1 — Canonical shape selection

For each `(route_id, direction_id)`, pick the highest-trip-count
`shape_id` as the canonical. Other variants (short-turns, school
trips, weekend re-routes) are ignored for **identity**. Their trips
still contribute to **slip** downstream (see Section 3).

This is an explicit asymmetry: canonical-only for identity,
all-variants for slip. Documented at the implementation site.

### Step 2 — Local bearing per shape point

For each canonical shape, compute the local bearing at every point:

- Bearing at point *i* = compass angle from point *i* to point *i+1*.
- At the last point, fall back to bearing from *i-1* to *i*.

Bearing is the actual direction of travel, computed via the standard
spherical formula:

```
bearing = atan2(
  sin(Δλ) * cos(φ2),
  cos(φ1)*sin(φ2) - sin(φ1)*cos(φ2)*cos(Δλ)
)
```

Stored as a CTE column `bearing_deg` for the matching step;
optionally materialized if pipeline timing demands it.

### Step 3 — Direction-aware proximity match

Self-join the canonical shapes. Two shape points colocate iff:

1. **Proximity**: haversine distance < 15 m. (Median shape spacing
   is 12 m; p95 is 20 m. The 15 m threshold absorbs encoder noise
   without bridging parallel streets.)
2. **Bearing agreement**: circular distance of bearings < 30°, i.e.
   `min(|b1 - b2|, 360 - |b1 - b2|) < 30°`.

For each shape point, collect the set of OTHER canonical routes whose
shapes pass within 15 m at compatible bearing → `colocated_route_set`.

This is what makes corridor identity directional from the start: a
NB shape point and a SB shape point at the same lat/lon do NOT
colocate. Wisconsin Ave NB and Wisconsin Ave SB become independent
corridors.

### Step 4 — Run-length encoding

Walk each shape in `shape_pt_sequence` order. Break into runs at
every change in `colocated_route_set`. Discard runs where
`|colocated_route_set| < 2` (route is alone). Discard runs with
fewer than 5 points (single-point grazing matches).

Each run is labeled by its (sorted) route set and the canonical
shape's local mean bearing.

### Step 5 — Cross-route deduplication

A given corridor appears in every contributing route's run sequence.
Deduplicate by `(direction_cardinal, route_set, geographic_span)`:
keep one canonical representation per unique key.

`direction_cardinal` is derived from the run's mean bearing
(N: 337.5–22.5°, NE: 22.5–67.5°, …).

### Step 6 — Stop endpoint snapping

For each retained run, snap the run's endpoints to the nearest stops
that are served by ALL contributing routes (intersection of each
route's stop set, filtered to stops within 100 m of the run's
endpoint shape points).

This produces stable, joinable stop_id endpoints — used both for the
display name ("Wisconsin Ave SB: Friendship Heights → Foggy Bottom")
and for slip aggregation joins.

If no shared stop is found within 100 m, the run is dropped.

### Step 7 — Minimum length filter

Drop corridors with `length_m < 500`. Below ~500 m a corridor is
sub-actionable for infrastructure investment (it's at most one
intersection); these are better surfaced via PR #140 stop-pair
view.

`length_m` is the summed haversine distance along the corridor's
shape points.

### Cost note

The self-join is the only expensive operation. With ~503 k shape
points and a lat/lon bounding-box index (~0.00014° at D.C.'s
latitude), the join is roughly 503 k × ~20 candidates per box =
~10 M comparisons. Single-digit seconds in psql.

Runs only on GTFS reload (~weekly). Cost is irrelevant operationally.

---

## 2. Data model

Three tables. Two refresh cadences: corridor identity rebuilds on
GTFS reload; slip rollup refreshes nightly with PR #140's pipeline.

### `corridors` (refreshed on GTFS reload only)

```sql
corridor_id            SERIAL PRIMARY KEY
direction_bearing_deg  REAL NOT NULL          -- 0-360
direction_cardinal     TEXT NOT NULL          -- 'NB'|'SB'|'EB'|'WB'|'NE'|'NW'|'SE'|'SW'
start_stop_id          TEXT NOT NULL          -- first stop in direction of travel
end_stop_id            TEXT NOT NULL          -- last stop in direction of travel
length_m               REAL NOT NULL
n_routes               INT NOT NULL           -- len(route_set), for filter UI
route_set              JSONB NOT NULL         -- sorted array of route_ids
display_name           TEXT NOT NULL          -- "Wisconsin Ave SB: ... → ..."
geometry_wkt           TEXT NOT NULL          -- LINESTRING of matched shape points
gtfs_snapshot_id       INT NOT NULL
created_at             TIMESTAMP NOT NULL DEFAULT NOW()
UNIQUE (direction_cardinal, start_stop_id, end_stop_id, route_set)
```

JSONB-in-unique-constraint is supported in Postgres; we accept the
slight oddness for the simpler shape (one table, natural-key
uniqueness inline).

### `corridor_route_membership`

```sql
corridor_id            INT NOT NULL REFERENCES corridors(corridor_id) ON DELETE CASCADE
route_id               TEXT NOT NULL
direction_id           SMALLINT NOT NULL      -- this route's GTFS direction_id
canonical_shape_id     TEXT NOT NULL          -- which variant was canonical
start_stop_sequence    INT NOT NULL           -- on this route's canonical trip
end_stop_sequence      INT NOT NULL
PRIMARY KEY (corridor_id, route_id)
INDEX (route_id)
```

`route_set` in `corridors` is denormalized for API convenience.
This table is the authoritative join target for slip aggregation,
because slip aggregation needs per-route stop_sequence ranges that
can't be expressed in JSON.

### `corridor_slip_rollup` (refreshed nightly with PR #140)

```sql
corridor_id              INT NOT NULL REFERENCES corridors(corridor_id) ON DELETE CASCADE
period                   TEXT NOT NULL        -- 'all'|'am_peak'|'midday'|'pm_peak'|'evening'|'late'
n_route_directions       INT NOT NULL         -- distinct (route_id, direction_id) contributing
n_observed_segments      INT NOT NULL         -- count of route_diagnostic_segment rows aggregated
n_total_observations     INT NOT NULL         -- SUM(n_observations) across contributing rows
total_weighted_slip_sec  REAL NOT NULL        -- SUM(mean_slip_sec * n_observations)
mean_slip_per_segment_sec REAL                -- total_weighted / n_observed_segments
mean_slip_per_observation_sec REAL            -- total_weighted / n_total_observations
peak_period              TEXT                 -- populated only on period='all' rows
computed_at              TIMESTAMP NOT NULL DEFAULT NOW()
PRIMARY KEY (corridor_id, period)
INDEX (period, total_weighted_slip_sec DESC)  -- ranked-list query
```

**Source window**: `route_diagnostic_segment` is itself materialized as
a 30-day rolling aggregate (see model docstring). The corridor rollup
inherits that window — no `lookback_days` or `service_date` column
needed; the window changes only if the source materialization changes.

**Period summability**: most fields are NOT summable across periods
(distinct counts, weighted means, categoricals). The `'all'` row is
computed from the underlying `route_diagnostic_segment` rows with
`period='all'` (which `RouteDiagnosticSegment` already stores as a
separately-computed row per its docstring), not derived from the
per-period corridor rows.

**Data quality**: `route_diagnostic_segment`'s nightly materialization
already refuses to compute on partial-collection days (per PR #143).
The corridor rollup inherits this implicit guarantee — no
`data_quality` or `coverage_pct` column needed at the corridor level.
The per-day concept doesn't translate to a 30-day rolling window
anyway.

### Aggregation SQL (the nightly join)

Source: `route_diagnostic_segment` (the **per-route** input that PR #140
itself reads from before its own cross-route grouping). Confirmed in
`src/models.py:977` — keyed by `(route_id, direction_id, period,
from_seq, to_seq)` with `mean_slip_sec`, `n_observations`. No
`service_date` column because the source table is already a 30-day
rolling aggregate.

```sql
INSERT INTO corridor_slip_rollup (...)
SELECT
  crm.corridor_id,
  rds.period,
  COUNT(DISTINCT (rds.route_id, rds.direction_id)) AS n_route_directions,
  COUNT(*) AS n_observed_segments,
  SUM(rds.n_observations) AS n_total_observations,
  SUM(rds.mean_slip_sec * rds.n_observations) AS total_weighted_slip_sec,
  SUM(rds.mean_slip_sec * rds.n_observations) / NULLIF(COUNT(*), 0)
    AS mean_slip_per_segment_sec,
  SUM(rds.mean_slip_sec * rds.n_observations) / NULLIF(SUM(rds.n_observations), 0)
    AS mean_slip_per_observation_sec
FROM corridor_route_membership crm
JOIN route_diagnostic_segment rds
  ON rds.route_id = crm.route_id
 AND rds.direction_id = crm.direction_id
 AND rds.from_seq >= crm.start_stop_sequence
 AND rds.to_seq   <= crm.end_stop_sequence
GROUP BY crm.corridor_id, rds.period
ON CONFLICT (corridor_id, period) DO UPDATE SET ...;
```

`peak_period` (only set on `period='all'` rows) is a second pass after
the primary insert — for each corridor, find the named period with
the highest `total_weighted_slip_sec` and write it back. Mirrors
PR #140's `refresh_cross_route_segments.py` pattern.

**Why this source, not `cross_route_segment_rollup`**: PR #140's
output table aggregates *across* routes (grouping by `from_stop_id,
to_stop_id` only). We need per-route per-segment slip so we can
attribute contributions to a corridor's `route_set`. The per-route
input table is the right join target. As a side effect, all trips on
a route (canonical + variant) automatically contribute via
`rds.route_id = crm.route_id`, matching the "canonical-only for
identity, all-variants for slip" asymmetry from Section 1.

### Refresh cadence + atomicity

- `corridors` + `corridor_route_membership` rebuild in a single
  transaction, triggered from `scripts/reload_gtfs_complete.py`. Same
  all-or-nothing semantics as the rest of GTFS reload.
- `corridor_slip_rollup` appends in the nightly batch alongside
  PR #140. Cascade `ON DELETE` drops stale slip rows for dropped
  corridors; next batch backfills new corridors.
- **One-time backfill**: `pipelines/backfill_corridor_slip.py`
  re-aggregates slip for the retained window after corridors first
  lands.

### Pipeline upserts

Use `src/upsert_helpers.py:upsert_rows` per CLAUDE.md convention.
Don't hand-roll `pg_insert(...).on_conflict_do_update(...)` in new
pipelines.

---

## 3. API + UI surface

### API: extend `/api/segments`

PR #140's endpoint remains the source of truth. Add a `level`
query parameter:

```
GET /api/segments?level=segment   # PR #140 behavior (default; back-compat)
GET /api/segments?level=corridor  # new
GET /api/segments?level=corridor&period=pm_peak&limit=50
```

Periods reuse `DIAGNOSTIC_PERIODS` (`all`, `am_peak`, `midday`,
`pm_peak`, `evening`, `late`).

Response when `level=corridor`:

```json
{
  "level": "corridor",
  "period": "all",
  "lookback_days": 7,
  "n_rows": 32,
  "corridors": [
    {
      "corridor_id": 12,
      "display_name": "Wisconsin Ave SB: Friendship Heights → Foggy Bottom",
      "direction_cardinal": "SB",
      "start_stop_id": "1003456",
      "start_stop_name": "Wisconsin Ave & Western Ave NW",
      "end_stop_id": "1003812",
      "end_stop_name": "Pennsylvania Ave & 23rd St NW",
      "length_m": 4823.0,
      "n_routes": 2,
      "route_set": ["D80", "D82"],
      "n_observed_runs": 1422,
      "total_weighted_slip_sec": 87440.0,
      "slip_min_per_trip": 1.02,
      "mean_slip_per_segment_sec": 4.62,
      "peak_period": "pm_peak",
      "data_quality": "complete",
      "preview_url": "/static/corridors/12.png",
      "contributing_routes": [
        {"route_id": "D80", "trip_count": 980, "weighted_slip_sec": 60100.0},
        {"route_id": "D82", "trip_count": 442, "weighted_slip_sec": 27340.0}
      ]
    }
  ]
}
```

Wire format mirrors PR #140 exactly on shared columns. Corridor-only
additions: `direction_cardinal`, `start_/end_stop_name`, `length_m`,
`display_name`, `preview_url`.

### Drill-down endpoint

```
GET /api/corridors/{corridor_id}/segments?period=all
```

Returns segments from PR #140's table filtered to those falling
within the corridor's stop range. Same row shape as
`/api/segments?level=segment` — frontend reuses the segment-row
component verbatim.

### UI: toggle on the segments page

In the existing PR #140 page (`frontend/src/pages/...` — exact
path confirmed at impl):

1. **`<ToggleButtonGroup>` at the top**: `Segments | Corridors`.
   Persists to URL state (`?level=…`). Default `segment` preserves
   PR #140 bookmark URLs.

2. **Same table component for both modes**. Conditional columns:
   - Segment: `from_stop_name → to_stop_name`
   - Corridor: `display_name`, plus `length_m` and `direction_cardinal`
     as badges
   - Both: `n_routes`, `slip_min_per_trip`,
     `total_weighted_slip_sec` (sortable), `peak_period`,
     contributing-routes pill list

3. **Row expansion**:
   - Segment row → per-route contribution table (PR #140 unchanged).
   - Corridor row → **two-pane expansion**:
     - **Top pane**: static map preview (`<img src={preview_url}>`)
       sized 640×320.
     - **Middle pane**: per-route contribution table (same component
       as segment mode).
     - **Bottom pane**: nested table of constituent stop-pair
       segments inside the corridor, ranked by their own slip;
       clicking a constituent opens its own PR #140-style expansion
       (segment → per-route).

4. **Filters reused.** Route, period, min-routes (PR #140's
   existing filters) apply identically. New filter for corridor
   mode: `min_length_m` slider, default 500 m.

### Out of scope for V2

Live interactive map (Leaflet/MapLibre). Per-day trend at the
corridor level. Variant-only corridor identity.

---

## 4. Static map preview pipeline

One PNG per corridor, generated server-side at GTFS-reload time,
served as a static asset, embedded as a plain `<img>` in the row
expansion. No interactive map dependency on the frontend.

### Generation

```python
from staticmap import StaticMap, Line

def render_corridor_preview(corridor) -> bytes:
    """Render corridor geometry onto a Positron basemap; return PNG bytes."""
    points = parse_wkt_linestring(corridor.geometry_wkt)  # [(lon, lat), ...]
    m = StaticMap(
        width=PREVIEW_WIDTH_PX,
        height=PREVIEW_HEIGHT_PX,
        url_template=POSITRON_TILE_URL,
    )
    m.add_line(Line(points, PREVIEW_LINE_COLOR, PREVIEW_LINE_WIDTH_PX))
    return m.render(zoom=auto).convert("RGB").tobytes_as_png()
```

- **Library**: `staticmap` (pure Python, MIT). Pillow under the
  hood. No headless browser, no Node.
- **Tile source**: CartoDB Positron (minimal cartography,
  high-contrast polyline). Fallback to OSM tiles if Positron
  rate-limits.
- **Image size**: 640 × 320, fits the row-expansion pane.
- **Zoom**: auto-computed from bounding box with 10% padding.
- **Tile-fetch volume**: ~30 corridors × ~10 tiles each = ~300
  tile fetches per GTFS reload (weekly). Well within fair use.

### Storage

PNGs land at `static/corridors/{corridor_id}.png`. Served by
FastAPI's `StaticFiles` mount at `/static/corridors/...`.

- **Size budget**: ~30 corridors × ~40 KB = ~1.2 MB. Trivial.
- **Cache invalidation**: corridor-identity transaction deletes
  PNGs for orphaned corridor_ids, writes new ones, overwrites
  changed-geometry corridors.

### Failure modes

- Tile fetch fails: retry once → fall back to OSM tiles → fall
  back to no-basemap (white bg + polyline). PNG generation is
  **best-effort**; corridor identity pipeline does NOT fail if
  preview rendering fails.
- PNG missing at request time (race window after reload):
  UI shows "map preview rendering…" placeholder. Next page load
  picks it up.

### Calibration knobs

```python
PREVIEW_WIDTH_PX = 640
PREVIEW_HEIGHT_PX = 320
PREVIEW_LINE_COLOR = "#d62728"   # WMATA-red-adjacent; high contrast
PREVIEW_LINE_WIDTH_PX = 4
PREVIEW_BBOX_PADDING = 0.10
POSITRON_TILE_URL = "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"
OSM_FALLBACK_URL = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
```

---

## 5. Calibration knobs (identity)

```python
# src/corridor_identity.py
SHAPE_PROXIMITY_THRESHOLD_M = 15.0   # median spacing 12m, p95 20m; absorbs encoder noise
BEARING_AGREEMENT_THRESHOLD_DEG = 30.0  # tolerates ordinary street curves
MIN_CORRIDOR_LENGTH_M = 500.0  # ~2 stops; below this is sub-actionable
MIN_RUN_POINTS = 5             # filter single-point grazing matches
LOOKBACK_DAYS = 7              # mirror PR #140 (verify on impl)
ENDPOINT_STOP_SNAP_M = 100.0   # stop must be within this of run endpoint
```

V2 ships with these defaults. Tuneable after empirical run.

---

## 6. Edge cases

| Case | Handling |
|---|---|
| Mid-batch GTFS reload | Identity transaction is atomic; slip refresh blocks on row lock; next slip refresh picks up new corridor_ids. Cascade `ON DELETE` cleans orphans. |
| Empty corridor (no slip in window) | Row exists with `n_observed_runs=0` and `total_weighted_slip_sec=0`. UI suppresses zero-row corridors from the default view but shows them when filtered explicitly. |
| Bearing at shape endpoint | Look-back fallback (i-1 → i). |
| Single-point spurious match | `MIN_RUN_POINTS = 5` filter drops these before length filter. |
| Stop snap to same start and end | Caught by `MIN_CORRIDOR_LENGTH_M`. |
| SQLite test compatibility | Pipeline uses JSONB + Postgres upsert helpers; pipeline tests run on `pg_session` only. Bearing helper + run-length encoder unit-test on SQLite. |
| One-way streets / couplets | Algorithm produces a single directional corridor. Correct framing, no special case. |
| Routes sharing a corridor at different stops | `route_set` membership is shape-based; slip contribution requires shared stops. Route shows as contributor with low trip_count. Self-explaining in the drill-down. |
| Variant-only corridor | Not surfaced in V2. If later needed, include variant shapes with ≥N trips/day in canonical pool. |
| Variant trip slip on a corridor it skips | Variant trip's stop-pairs fall outside corridor stop range → naturally excluded. Documented limit of stop-pair-anchored measurement. |

---

## 7. Test strategy

### Unit (SQLite OK)

- Bearing computation: known lat/lon → known bearing.
- Run-length encoding: synthetic colocation flag sequence → expected
  run boundaries.
- Length filter: 250 m run dropped, 750 m run kept.
- `MIN_RUN_POINTS`: 4-point run dropped, 5-point run kept.

### Pipeline integration (`pg_session`, real Postgres)

- Build corridors from a 4-route fixture GTFS:
  - 2 routes both directions on Wisconsin → expect 2 corridors
    (NB and SB).
  - 1 perpendicular route → expect 0 corridors involving it.
  - 1 route with short-turn variant → expect canonical-only in
    `route_set`; variant ignored for identity but contributes to
    slip.
- Slip aggregation: fixture `cross_route_segment_rollup` rows →
  verify rollup math matches hand-computed sum.
- **Row-count guard (NOTES-72 lesson)**: both pipelines fail fast
  if they produce 0 rows on non-empty input. Silent-zero is the
  failure mode we already paid for once.

### Static map preview

- Unit: `render_corridor_preview` produces non-empty PNG for fixture
  geometry; auto-zoom picks reasonable level for 500 m and 5 km
  corridors.
- Integration: full pipeline writes expected number of PNGs to
  `static/corridors/`.
- No visual-regression baseline for PNG content (tile basemaps drift
  upstream). Assert presence + dimensions only.

### Empirical validation (one-time during impl)

- Pin actual corridor count on real D.C. data. Expect 20–40 per the
  granularity choice. If 5 or 200, thresholds are wrong → adjust.
- Confirm D80/D82 produces a Wisconsin SB and Wisconsin NB row.
- Spot-check 3 corridors visually (matplotlib + LineString from
  `geometry_wkt`) against a map.

---

## 8. Implementation surfaces

- **New**: `src/corridor_identity.py` — bearing helper, matching SQL,
  run-length encoding, stop snapping.
- **New**: `pipelines/refresh_corridors.py` — orchestrates the
  identity rebuild; called from `scripts/reload_gtfs_complete.py`.
- **New**: `pipelines/refresh_corridor_slip.py` — nightly slip
  aggregation; wired into `pipelines/run_daily_batch.py`.
- **New**: `pipelines/backfill_corridor_slip.py` — one-time
  backfill for retained service-date window after first deploy.
- **New**: `src/corridor_preview.py` — `render_corridor_preview` +
  storage/cleanup helpers.
- **Extend**: `api/main.py` — add `level` query param to
  `/api/segments`; add `/api/corridors/{id}/segments` drill-down.
- **Extend**: PR #140's frontend page — toggle, conditional columns,
  two-pane row expansion with `<img>` preview.
- **Extend**: `scripts/reload_gtfs_complete.py` — call
  `refresh_corridors` inside its existing transaction.
- **Extend**: `pipelines/run_daily_batch.py` — call
  `refresh_corridor_slip` after PR #140's segment refresh.

---

## 9. Implementation-time facts (verified during plan writing)

These were "open items" in the draft spec; they were resolved by
reading the codebase before writing the implementation plan.

1. **Per-route segment table** — `route_diagnostic_segment`
   (`src/models.py:977`, ORM class `RouteDiagnosticSegment`). Columns:
   `route_id`, `direction_id`, `period`, `from_seq`, `from_stop_id`,
   `to_seq`, `to_stop_id`, `mean_slip_sec`, `cum_slip_sec`,
   `n_observations`, `is_timepoint`, `computed_at`. No `service_date`
   column — the table is a 30-day rolling aggregate (docstring at
   `src/models.py:981`).
2. **`'all'` rows in `route_diagnostic_segment`** are present as
   separately-materialized rows (per docstring at `src/models.py:988`).
   Corridor slip aggregator joins them directly; no `UNION ALL` pass
   needed.
3. **Source window** is 30 days (inherited from
   `route_diagnostic_segment`'s materialization). The corridor rollup
   has no `lookback_days` column — it follows the source.
4. **Frontend page** — `frontend/src/components/SegmentDiagnostic.jsx`
   (`.jsx`, not `.tsx`). Routed at `/segments` in `App.jsx:75`. 367
   lines as of plan writing.
5. **Period enum** — `src/route_diagnostics.py:78`,
   `ALL_PERIODS = ("all", "am_peak", "midday", "pm_peak", "evening",
   "late")`. Imported as `DIAGNOSTIC_PERIODS` in `api/main.py:40`.
6. **Migration convention** — `scripts/migrate_create_<table>.py`,
   wired through `scripts/migrate_all.py`. `check_schema_drift.py`
   validates SQL against the ORM model (per CLAUDE.md NOTES-72
   addendum).
7. **PR #140 API endpoint** at `api/main.py:1086` calls
   `api/aggregations.py:get_cross_route_segments` (`:4493`).
8. **Pipeline upsert helper** — `src/upsert_helpers.py:upsert_rows`
   (per CLAUDE.md). Don't hand-roll `pg_insert(...).on_conflict_do_update`.

---

## 10. Out-of-scope items captured for future work

- **V3 map view**: interactive Leaflet/MapLibre overlay of corridors
  with click-to-rank. Out of V2; the static PNG preview is the
  bridge.
- **Per-day corridor trend lines**: would mirror the route-detail
  trend strip from PR #146. Out of V2.
- **Variant-only corridors**: small operationally minor corridors
  served exclusively by school/short-turn variants. Out of V2;
  flagged in algorithm step 1.
- **Reverse-geocoded street names**: human-readable corridor names
  like "M St NW from Wisconsin to 14th" via Nominatim/Mapbox. Out
  of V2; stop-anchored names are sufficient.
- **Cost-of-intervention overlay**: pairing benefit ranking with
  WMATA/DDOT planning cost inputs. Out of V2; benefit-only ranking
  is the V2 contribution.
