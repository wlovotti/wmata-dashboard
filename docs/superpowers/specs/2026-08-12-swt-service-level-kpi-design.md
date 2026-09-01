# Scheduled wait + daytime service level KPIs on the agency comparison page

**Date:** 2026-08-12
**Item:** NOTES-115
**Status:** Approved (brainstorm session 2026-08-12)

## Problem

Every KPI on the agency comparison page (PR #198) — OTP, EWT, service
delivered, bunching — measures performance *against* the schedule, which
normalizes away the schedule itself. Measured from current GTFS for
Thursday 2026-08-06 daytime (7:00–19:00), WMATA's median scheduled
headway per route-direction is 24 min (27% of route-directions ≤ 15 min)
vs SFMTA's 12 min (60% ≤ 15 min). Total rider wait = SWT + EWT; the page
shows only the small term. "Muni just runs more service" is real and
currently invisible.

## Decision summary

Two new KPI surfaces, one per framing (decided in the brainstorm):

1. **Scheduled wait (SWT, frequent service)** — decomposes the wait
   story next to EWT, from the identical pool, so tiles read
   "scheduled wait X + excess wait Y".
2. **Daytime service level (all routes, schedule-only)** — shows the
   promise across the whole network, where the random-incidence SWT
   formula is not valid (riders on infrequent routes time their
   arrivals — TCRP/TfL both switch metrics below the frequency
   threshold).

Architecture: **hybrid** — SWT is materialized nightly (snapshot-faithful,
pairs with the persisted EWT); service level is computed at read time
from current GTFS (it is a statement about the current promise, and only
changes on GTFS reloads).

### Research grounding (agent-researched 2026-08-12)

- TfL/TCRP: EWT = AWT − SWT with expected wait ∝ E[h²]/E[h]
  (Osuna–Newell); valid for frequent service only. TCRP 165 keeps
  "service frequency" (the promise) and "reliability" (the kept promise)
  as separate LOS dimensions — exactly this two-tile split.
- Weighting: route-equal averaging is a documented flaw (NY State
  Comptroller audits of NYCT Wait Assessment). TfL weights rollups by
  route frequency. Chosen here: **trip-weighted** via per-route-direction
  reference stops (below).
- CTA and King County Metro publish the promise as headway standards /
  "service family" shares — precedent for the median + "% ≤ 15 min"
  tile pair.

## Metric definitions

### Tile 1 — Scheduled wait (frequent service)

- Formula: pooled random-incidence SWT = Σh² / (2·Σh) over the union of
  every route's frequent (direction, stop, hour) cell-hours — the
  **identical pool** `_system_ewt_and_bunching_for_date` already uses
  for system EWT (per-route 15/20-min gate via
  `get_cell_hour_gate_sec`, day_type-resolved service_ids, agency-local
  hours). The function computes this value today and discards it.
- Persisted per (agency, service_date) in `system_metrics_daily`;
  comparison page shows the matched-window mean, week-over-week delta,
  and partial-day count with the same mechanics as EWT.
- Invariant: because the pool is identical, SWT + EWT = AWT exactly
  (up to EWT's clamp-at-0).

### Tile 2 — Daytime service level (all routes)

- Source: **current** GTFS (`is_current`), weekday day_type via the
  shared NOTES-106 resolver (inherited by using
  `fetch_scheduled_cell_hours_for_routes`, which also makes SFMTA's
  calendar_dates-only weekday service resolve correctly — the same
  fix that closed the `route_service_profile` weekday-rows gap,
  PR #227), hours 7:00–19:00
  agency-local (GTFS clock time; hour key = earlier arrival's hour).
- Pooling: filter cells to hours 7–18 first; then for each
  (route_id, direction_id), pick the **reference stop** with the most
  headway samples inside that daytime window; use that stop's
  daytime consecutive-headway series. Pool all route-directions'
  series together. This is trip-weighted (a route-direction's weight ∝
  its scheduled trips — TfL-style frequency weighting) and avoids both
  the stop-density bias of pooling every stop and the route-equal
  anti-pattern.
- Big number: median of pooled headway samples. Subline: % of samples
  ≤ 900 s. No week-over-week delta — the tile is labeled as derived
  from the current schedule.

### New caveat lines (acceptance criterion)

Appended to `AGENCY_COMPARISON_CAVEATS`:

1. Weighting + window statement for the service-level tile:
   trip-weighted via per-route-direction reference stops, weekday
   7:00–19:00, computed from the current schedule (not the historical
   schedule of the comparison window).
2. SWT scope statement: scheduled wait covers frequent-gated service
   only and assumes random rider arrival; the service-level tile exists
   because that model is invalid for infrequent routes.

## Implementation

### Backend

- `api/aggregations.py::_system_ewt_and_bunching_for_date` returns
  `(ewt_seconds, swt_seconds, bunching_rate)`; docstring updated.
- `src/system_metrics.py` threads `swt_seconds` into the upsert.
- `src/models.py::SystemMetricsDaily` gains nullable
  `swt_seconds = Column(Float)`.
- Migration: single additive
  `ALTER TABLE system_metrics_daily ADD COLUMN swt_seconds double precision`
  on both laptop DBs (wmata_dashboard, sfmta_dashboard). **User-run**
  per docs/MIGRATIONS.md and the standing ALTER-permission rule;
  documented in the PR body.
- Comparison endpoint: add `"swt"` to `AGENCY_COMPARISON_METRICS` and
  `_METRIC_TO_COLUMN` (window-mean / wow-delta / partial machinery
  generalizes); add a pure function computing the service-level stats
  from `fetch_scheduled_cell_hours_for_routes(db, "weekday")` output,
  attached per agency as
  `service_level: {median_headway_seconds, pct_at_most_15min, n_headways}`
  (all null when the schedule pool is empty).

### Frontend (`frontend/src/components/AgencyComparison.jsx`)

- SWT tile inserted before EWT: label ~"Scheduled wait · frequent svc",
  mm:ss format, delta pill green-when-down (same semantics as EWT).
- Service-level tile per agency column, no delta pill: median headway
  as the big number; subline carries the % ≤ 15 min and the
  "weekday 7:00–19:00 · current schedule" qualifier.
- The comparison page is not Playwright-baselined (baselined pages:
  Overview, RouteList, RouteDetail-D72) — **no baseline regen needed**.
  Vitest covers new formatters.

### Testing (TDD — failing test first)

- 3-tuple plumbing: SWT lands in `system_metrics_daily`; null-safe on
  empty pools.
- Service-level computation as a pure function: reference-stop
  selection, 7:00–19:00 filter, median + share math, empty schedule →
  nulls.
- Endpoint response shape: `swt` metric block and `service_level` block
  present per agency.
- Gates: `uv run pytest -m smoke`, both ruff gates (incl. `tests/`),
  `cd frontend && npm run lint && npm test`.

### Backfill (user-run)

Re-run `pipelines/upsert_system_metrics_daily.py` for each date
2026-07-23 → today, for both agencies (~20 dates × 2). Known property:
re-runs recompute the schedule side against the current GTFS snapshot,
so historical EWT may drift slightly where the schedule changed —
accepted for a 3-week window; `--gtfs-snapshot-id` pinning remains
available if drift ever matters. Until backfilled, `swt_seconds` is
NULL and the tile renders its null state; nothing breaks.

## Out of scope

- Ridership/boarding weighting (no APC data in either pipeline) — the
  caveat names this as the known honesty gap, per MBTA ETT / NYCT CJTP
  direction of travel.
- Corridor-combined ("effective") headways for branch/trunk overlaps —
  the KPI is per-route-direction promise; the caveat framing keeps the
  unit of analysis consistent with EWT.
- Materializing service-level stats daily (rejected as Approach C —
  near-constant schedule-derived values).
- Recomputing SWT at read time (rejected as Approach B — historical
  SWT would silently disagree with the persisted EWT beside it after
  GTFS reloads).
