# NOTES-108. `_schedule_cache` in src/ewt.py has no agency component — cross-agency key collision risk

**Severity: medium** *(silent cross-agency data corruption in-process — a process holding both WMATA and SFMTA sessions could serve one agency's schedule to the other; today mitigated only because pipelines run per-agency processes, not by anything in the cache itself)*.
**Effort: low** *(add agency/db identity to the cache key)*.

Found during NOTES-106's review (calendar_dates resolution fix). The
module-level schedule cache in `src/ewt.py`
(`fetch_scheduled_cell_hours_for_routes`) is keyed
`(day_type, gtfs_snapshot_id)`:

```python
_schedule_cache: dict[tuple[str, int], dict[str, dict[CellHour, list[float]]]] = {}
```

`gtfs_snapshot_id` is resolved via `MAX(GTFSSnapshot.snapshot_id)` **within
whichever `db` session is passed in** — it's a per-database sequence, not a
globally-unique identifier. As of 2026-08, `wmata_dashboard`'s snapshot_id
is at 15 and `sfmta_dashboard`'s is at 1. If a single process ever holds
sessions against both databases (e.g. a future comparison-page endpoint
that queries WMATA and SFMTA schedule data in the same request, or any
script/test that imports `src.ewt` and touches both DBs without a process
restart in between), the cache keys can collide: `("weekday", 1)` from
SFMTA would satisfy a later `("weekday", 1)` lookup meant for a
hypothetical low-numbered WMATA snapshot (less likely today given WMATA is
at 15, but any two databases whose snapshot_ids happen to coincide hit
this — and there's nothing structural preventing it). The stale/wrong
agency's schedule would be served silently — no error, just wrong SWT/EWT
numbers.

Today this is masked because every pipeline invocation
(`pipelines/run_daily_batch.py`, `pipelines/compute_bunching.py`, etc.)
runs as a separate process against one agency's database via
`--agency`/`resolve_agency_db_url` — a fresh process means a fresh (empty)
`_schedule_cache`, so no cross-agency reuse actually happens in production
today. But this is incidental, not enforced, and the risk grows as
multi-agency comparison features (NOTES-99) start querying both databases
more directly (e.g. from the API process, which is long-lived and could
plausibly hold both agencies' sessions for a comparison-page request).

Fix: include something identifying the agency/database in the cache key —
e.g. `db.bind.url` (or a lighter proxy already threaded through, like the
`agency` string most call sites already have via `agency_config`) alongside
`(day_type, snapshot_id)`.

Acceptance: the cache key can't collide across two different databases
even when their `snapshot_id` sequences happen to overlap; add a
regression test that populates the cache from one agency-shaped session
then asserts a different agency-shaped session with a colliding
`(day_type, snapshot_id)` does NOT reuse the first agency's cached result.

## Dependencies

None (unblocked). Not urgent while pipelines stay one-process-per-agency,
but should land before any long-lived process (API, a future comparison
endpoint) queries both `wmata_dashboard` and `sfmta_dashboard` schedule
data in the same process lifetime.
