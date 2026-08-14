# NOTES-122. Client-side caching for page navigation (Overview refetches everything on every visit)

**Severity: low-medium** *(user-felt slowness on the primary landing
page; audience is personal, so no SLA — but "the tool feels slow" was
the first reaction in manual testing of the editorial Overview)*.
**Effort: medium** *(cross-cutting frontend change — a fetch-cache
layer or library adoption, touching every fetch-in-component surface)*.

Surfaced 2026-08-13 during manual testing of the editorial Overview
(PR #210): initial page load takes noticeably long, and navigating
Overview → RouteDetail → back re-runs the full fetch set from scratch.
The frontend has no client-side caching at all — every component
fetches on mount (`fetch` in `useEffect` / `useMultiFetch`), and the
`refreshKey` remount pattern in `App.jsx` means back-navigation
remounts pages cold. One Overview visit pulls `/api/routes`, four
`/api/system/trend` payloads, `/api/routes/contributors`,
`/api/agency-comparison`, and ~200 KB of `/api/shapes`, then renders
as a waterfall of per-panel spinners. Server-side caches (60 s TTLs in
`api/aggregations.py`) mask recompute cost but not
re-download/re-render cost, and expire between casual visits.

Likely shape of the fix: stale-while-revalidate on the client — either
a small module-level cache keyed by URL (serve last response
instantly, refetch in background) or adopting a library (TanStack
Query / SWR) since the fetch surface is now ~10 endpoints across
Overview, RouteList, RouteDetail, Targets, Compare, Segments. The
explicit Refresh button already exists as the manual invalidation
path, which fits SWR semantics well. `/api/shapes` is the best first
candidate (biggest payload, changes only on GTFS reload).

Acceptance: returning to the Overview from a route page renders the
last-known content immediately (no spinner wall), with data refreshing
in the background; initial cold load may still take its current time.

## Dependencies

None hard. Related but distinct: the `/api/routes` N+1 latency cliff
(NOTES-88) is server-side cost over the tunnel and blocks public
deploy only — fixing either helps the other but neither subsumes it.
Coordinate with NOTES-85 (design-system pass) only insofar as loading
states get restyled there.
