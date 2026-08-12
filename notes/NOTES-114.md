# NOTES-114. `src/analytics.py`'s `_route_stops_cache` has no db-identity or snapshot component and caches live ORM instances

**Severity: low-medium** *(same bug class as the agency-aware cache-key
fix in `src/ewt.py` (PR #195), plus two hazards that fix didn't have:
cached ORM objects and no snapshot invalidation).*
**Effort: low** *(re-key + detach or re-shape the cached value).*

Found during PR #195's review — after that PR, this is the last
remaining module-level cache with an agency-blind key. `_route_stops_cache`
in `src/analytics.py` (`get_route_stops`) is keyed on `route_id` alone:

- **No db-identity component**: a process holding sessions against both
  `wmata_dashboard` and `sfmta_dashboard` would serve one agency's stop
  list for the other whenever route_ids coincide.
- **No snapshot component and never invalidated**: entries survive a
  GTFS reload (`scripts/reload_gtfs_complete.py`), so a long-lived
  process (the API) serves pre-reload stop lists indefinitely.
- **Caches live ORM `Stop` instances** bound to the first caller's
  session — later callers on a different session risk
  `DetachedInstanceError` or stale attribute state, independent of the
  key-collision issue.

Fix sketch: key by `(db_identity, snapshot_id, route_id)` reusing
`src/ewt.py:_db_identity` (don't invent a second disambiguation
scheme), evict same-db entries on snapshot bump like the ewt caches do,
and cache plain data (stop_id/lat/lon/name tuples) or expunged copies
rather than session-bound ORM objects.

Acceptance: cache cannot collide across databases or survive a snapshot
bump; regression test mirroring `tests/test_ewt.py`'s
`TestScheduleCacheAgencyIsolation` pattern.

## Dependencies

None (unblocked). Same urgency profile as the ewt fix was: latent while
every process is single-agency, real once the API or a comparison
endpoint (NOTES-99) holds both agencies' sessions.
