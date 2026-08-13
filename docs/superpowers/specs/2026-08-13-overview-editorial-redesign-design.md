# Overview editorial redesign (NOTES-84) — design spec

**Date:** 2026-08-13
**Status:** approved design, pending implementation plan
**Closes:** NOTES-84 (punch-list edits ride on the final PR of the
sequence)

## Problem

The 2026-06-10 product review found the Overview has the right
ingredients (HealthPulse, 30-day trends, contributors panel, What
changed) but renders them as a thin banner, four noisy
daily-granularity sparklines, and three equal-weight tables — nothing
is a headline, and the user must do the analyst's synthesis
themselves. The core question — "how is the network doing / what's
getting worse" — is answered only implicitly.

The redesign's core move is **hierarchy, not new data**: every payload
the editorial page needs already exists. The only new backend surface
is a bulk shapes endpoint for the system map.

## Decisions (with rationale)

1. **Page identity: WMATA-anchored hero + compare teaser.** The top
   fold answers "how is WMATA doing this week." A compact
   WMATA-vs-Muni strip inside the hero links to `/compare`, which is
   promoted to the nav. (Considered: comparison-led Overview —
   rejected as a larger IA change than NOTES-84 scopes; the compare
   page already exists per PR #198.)
2. **System map via a new bulk shapes endpoint** (`GET /api/shapes`),
   one representative simplified polyline per route. (Considered:
   movers-only map from N per-route calls — rejected as a curated
   illustration, not a system map; deferring the map — rejected,
   churns the Overview baseline twice for no scope savings.)
3. **Nav collapses to four entries** — Overview / Routes / Compare /
   Diagnostics — with a `DiagnosticsIndex` landing page carding the
   four demoted tools (Blocks, Targets, Schedule audit, Segments).
   All existing URLs keep working; only top-level links move.
   (Considered: header dropdown — rejected: new menu interaction +
   CSS on a baselined header, mobile behavior to solve.)
4. **Visual scope: IA + hero styling only.** New components get
   purposeful styling and a minimal `:root` token block; existing
   tables/panels keep their current look. NOTES-85 later migrates the
   rest of App.css onto the tokens. (Considered: app-wide tokens now —
   rejected as scope growth on an already-high-effort item.)
5. **Layout: editorial fold (Approach A).** Hero verdict full-width →
   map + movers side-by-side ("where is it going badly") → smoothed
   trends → single demoted contributors table. One idea per fold;
   strong hierarchy.

## Page composition

Top-to-bottom stack replacing the flat ~600-line `Overview.jsx`:

```
┌────────────────────────────────────────────┐
│  75% on time this week — down 2 pts        │  OverviewHero
│  12 of 40 routes below target              │
│  [WMATA 75% ▾2 · Muni 71% ▴1  → /compare]  │  CompareStrip
├───────────────────────┬────────────────────┤
│    SYSTEM MAP         │  GETTING WORSE     │  SystemMap +
│  (routes colored by   │  top-7 movers by   │  MoversPanel
│   OTP vs target)      │  deltas            │
├───────────────────────┴────────────────────┤
│  TRENDS (4 cards, 7-day line, ghost dots)  │  SystemTrend
├────────────────────────────────────────────┤
│  Biggest drags (contributors, demoted)     │  existing table
└────────────────────────────────────────────┘
```

### Components

- **`OverviewHero`** (new) — the big-number verdict. Headline: 7-day
  OTP with plain-language framing and a delta vs the prior 7 days.
  Subline: "N of M routes below target" (the `/api/routes` +
  `/api/targets` join HealthPulse does today). HealthPulse's
  worst-of-four severity math is extracted to a util and drives the
  hero's background tint; the banner component is deleted. If the
  worst metric is not OTP, the hero adds one sentence naming it
  ("Bunching is the sore spot: 14%, up 3 pts").
- **`CompareStrip`** (new, rendered inside the hero card) — one row
  from `/api/agency-comparison`: WMATA vs Muni OTP with
  week-over-week arrows, linking to `/compare`. Never load-bearing:
  renders nothing on fetch failure.
- **`SystemMap`** (new) — leaflet map of `/api/shapes` polylines,
  colored by OTP vs target (see Data flow). Click a route →
  `/route/:id`. v1 colors by OTP only; a shared metric selector for
  map + movers is an explicit non-goal for v1.
- **`MoversPanel`** (adapted from `WhatChangedPanel`, which is
  deleted) — top-7 worsening routes by the `deltas` block, with a
  small "getting better" toggle preserving the improvements half.
- **`SystemTrend`** (modified in place) — same four cards; a 7-day
  trailing-mean line with raw daily values as ghost dots. Smoothing
  is client-side over the existing `/api/system/trend` payload.
- **Contributors table** (kept, demoted) — retitled "Biggest drags",
  single table at the bottom, metric selector intact.
- **Removed from Overview:** the off-target table (and
  `OffTargetEmptyState`) — re-homed to the Targets page under
  Diagnostics.

## Backend: `GET /api/shapes`

- Route in `api/main.py`, logic in `api/aggregations.py` alongside
  its peers. Docstrings per repo convention.
- **Semantics:** for each `is_current` route, the representative
  `shape_id` = the one serving the most trips (direction variants
  overlap almost entirely at system-map zoom, so one polyline per
  route is honest).
- **Response:** `{routes: [{route_id, points: [[lat, lon], …]}]}` —
  compact pair-arrays to keep the payload lean.
- **Simplification:** Douglas-Peucker (small pure function in
  `src/`, no new dependency), ~10 m tolerance, applied before
  serialization. Typically cuts points 5–10× with no visible change
  at system-map zoom.
- **Caching:** the same 60 s server-cache helper `/api/routes` uses,
  warmed at startup.
- **Tests (TDD):** most-trips shape wins; `is_current` filtering;
  simplification preserves endpoints and reduces point count;
  empty GTFS returns `{routes: []}`.

## Nav & Diagnostics index

- `App.jsx` nav: Overview `/`, Routes `/routes`, Compare `/compare`,
  Diagnostics `/diagnostics`. The `/compare` route loses its
  "URL-only pending NOTES-84" comment.
- **`DiagnosticsIndex`** (new, small) at `/diagnostics`: one card per
  tool (Blocks, Targets, Schedule audit, Segments) with a one-line
  description of the question each answers. Existing URLs unchanged —
  bookmarks and in-app links keep working; pages only lose their
  top-level nav links.
- Header chrome (`RefreshButton`, `GtfsExpiryBanner`) unchanged.

## Data flow & honesty rules

- **One page-level fetch, props down.** Overview fetches
  `/api/routes`, `/api/targets`, and the four trend URLs once (via
  `useMultiFetch`) and passes props — removing today's duplicate
  trend fetch in `SystemTrend`. `SystemMap` fetches `/api/shapes`
  itself; `CompareStrip` fetches `/api/agency-comparison` itself;
  contributors keeps its own fetch (re-queries on metric change).
- **Hero math:** headline delta = current-7d vs prior-7d via the
  existing `computeWindowDelta` util; partial-quality days excluded
  from window means. Framing is strictly week-over-week — never "vs
  last month" — respecting the clean-window constraint
  (pre-2026-05-25 partial-day aggregates are contaminated; collection
  starts 2026-05-02).
- **Smoothing:** 7-day trailing mean over non-partial days, drawn as
  the line; raw daily values become ghost dots (partial days already
  grey-dot via `Sparkline`). One small pure util, unit-tested, shared
  by all four cards.
- **Movers honesty:** only `valid: true` deltas rank. Fewer than 3
  valid movers → "not enough history this week" instead of a
  pseudo-ranking (the NOTES-44 information-content lesson).
- **Map coloring:** OTP vs the route's target where one exists, else
  the system default; bands via the existing `spectrumBar`
  thresholds. Shapes rows with no scorecard row render neutral gray;
  scorecard routes with no shape are simply absent.
- **Failure posture:** hero and trends are load-bearing (skeletons →
  error states per existing skeleton CSS). Compare strip and map are
  enhancements: strip fails silent; map failure degrades the fold to
  movers full-width with a quiet inline note.
- **Tokens:** new components' colors land in a small `:root` block
  (`--color-good/-warn/-bad/-muted/-brand`) seeded from the existing
  hardcoded hexes.

## Testing, baselines, PR sequence

Four **serial** PRs — each merged before the next starts (several
touch the same files; squash-merge makes stacked PRs conflict):

1. **Backend:** `/api/shapes` + Douglas-Peucker util + pytest. No
   frontend, no baselines.
2. **Nav collapse:** four-link nav, `DiagnosticsIndex`, `/compare`
   promoted. All four visual baselines regenerate (user-run, both
   platforms, `--update-snapshots=all` — a nav-link diff is exactly
   the copy-sized change the 1% tolerance swallows, per NOTES-120).
3. **Overview rebuild:** hero, compare strip, movers, smoothed
   trends, demoted drags table; off-target table re-homed to
   Targets. Vitest units for smoothing, movers filtering, and hero
   framing text; new fixture for `/api/agency-comparison` in the
   Overview spec. Overview baseline regenerates.
4. **System map:** `SystemMap` joins the fold. Playwright adds an
   `/api/shapes` fixture (the `**/api/**` stub would otherwise 404
   the new endpoint) and stubs tile requests with a fixed blank tile
   (same `page.route` mechanism) so baselines show deterministic
   polylines, never live OSM tiles. Overview baseline regenerates
   once more.
   **NOTES-84 punch-list edits ride here.**

Every PR runs the standard gates: `pytest -m smoke`, both ruff gates
(including `tests/`), `npm run lint && npm test && npm run build`.
Baseline regeneration is user-run at PRs 2–4.

## Non-goals

- App-wide design-token migration (NOTES-85, sequenced after).
- Metric selector for the map/movers fold (post-v1 candidate).
- Any backend metric changes — the redesign consumes existing
  payloads only, plus the new shapes endpoint.
- Weekly narrative prose (NOTES-86; placement coordinates with this
  layout but ships separately).

## Acceptance

- Landing on `/` answers "how is the network doing, what's getting
  worse" without scrolling: verdict + delta + map/movers fold.
- Nav shows four entries; all six previous tool URLs still resolve.
- `/api/shapes` returns every current route's simplified polyline in
  one cacheable payload.
- Trends read as weekly direction, not daily noise; partial days
  visibly excluded.
- All Playwright baselines regenerated and green on both platforms.
