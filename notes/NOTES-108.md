# NOTES-108. `src/ewt.py`'s module-level schedule caches have no agency component — cross-agency key collision risk

**Severity: medium** *(silent cross-agency data corruption in-process — a process holding both WMATA and SFMTA sessions could serve one agency's schedule to the other; today mitigated only because pipelines run per-agency processes, not by anything in either cache itself)*.
**Effort: low** *(add agency/db identity to both cache keys)*.

Found during NOTES-106's review (calendar_dates resolution fix). `src/ewt.py`
has **two** module-level caches, both keyed `(day_type, gtfs_snapshot_id)`
with no agency/database component:

```python
_schedule_cache: dict[tuple[str, int], dict[str, dict[CellHour, list[float]]]] = {}
```
— backs `fetch_scheduled_cell_hours_for_routes` (the vectorized,
all-routes path), present since the original NOTES-106 fix.

```python
_service_id_resolution_cache: dict[tuple[str, int], frozenset[str]] = {}
```
— backs `_resolve_service_ids_for_day_type` (the modal-resolution
service_id lookup), **added later in the same PR (#191, commit
`d730fa4`)** as a performance fix for the per-route path
(`_scheduled_headways_by_cell_hour`, called once per route per pass from
both `src/ewt.py` and `src/bunching.py`) — deliberately mirroring
`_schedule_cache`'s exact key shape and eviction semantics, which means
it inherited the identical agency-blind-spot this item already tracked.

`_service_id_resolution_cache` is arguably the MORE dangerous of the two:
`_schedule_cache` only covers the vectorized `route_ids=None` dashboard
path, but `_service_id_resolution_cache` covers the per-route path that
`_schedule_cache` never did — so it's exercised far more often (every
route, every pass, both EWT and bunching) and a collision there silently
serves the wrong agency's *service_id set* (not just the derived
headways), which then flows into every downstream scheduled-headway
computation for that call.

`gtfs_snapshot_id` is resolved via `MAX(GTFSSnapshot.snapshot_id)`
**within whichever `db` session is passed in** — it's a per-database
sequence, not a globally-unique identifier. As of 2026-08,
`wmata_dashboard`'s snapshot_id is at 15 and `sfmta_dashboard`'s is at 1.
If a single process ever holds sessions against both databases (e.g. a
future comparison-page endpoint that queries WMATA and SFMTA schedule
data in the same request, or any script/test that imports `src.ewt` and
touches both DBs without a process restart in between), the cache keys
in EITHER cache can collide: `("weekday", 1)` from SFMTA would satisfy a
later `("weekday", 1)` lookup meant for a hypothetical low-numbered
WMATA snapshot (less likely today given WMATA is at 15, but any two
databases whose snapshot_ids happen to coincide hit this — and there's
nothing structural preventing it). The stale/wrong agency's schedule (or
service_id set) would be served silently — no error, just wrong
SWT/EWT/bunching numbers.

Today this is masked because every pipeline invocation
(`pipelines/run_daily_batch.py`, `pipelines/compute_bunching.py`, etc.)
runs as a separate process against one agency's database via
`--agency`/`resolve_agency_db_url` — a fresh process means fresh (empty)
caches, so no cross-agency reuse actually happens in production today.
But this is incidental, not enforced, and the risk grows as multi-agency
comparison features (NOTES-99) start querying both databases more
directly (e.g. from the API process, which is long-lived and could
plausibly hold both agencies' sessions for a comparison-page request).

Fix: include something identifying the agency/database in the cache key
for **both** `_schedule_cache` and `_service_id_resolution_cache` — e.g.
`db.bind.url` (or a lighter proxy already threaded through, like the
`agency` string most call sites already have via `agency_config`)
alongside `(day_type, snapshot_id)`. Since both caches were deliberately
built to share one key shape and one eviction rule, keep them sharing
whatever the fixed shape becomes — don't fix one and leave the other on
the old shape, and don't invent two different agency-disambiguation
schemes for the two caches.

Acceptance: neither cache's key can collide across two different
databases even when their `snapshot_id` sequences happen to overlap; add
a regression test **covering both caches** that populates each from one
agency-shaped session then asserts a different agency-shaped session
with a colliding `(day_type, snapshot_id)` does NOT reuse the first
agency's cached result (service_id set for
`_service_id_resolution_cache`, derived headways for `_schedule_cache`).

## Dependencies

None (unblocked). Not urgent while pipelines stay one-process-per-agency,
but should land before any long-lived process (API, a future comparison
endpoint) queries both `wmata_dashboard` and `sfmta_dashboard` schedule
data in the same process lifetime.
