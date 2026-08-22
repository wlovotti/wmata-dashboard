# NOTES-129. Investigate dead composite indexes on vehicle_positions

**Severity: low** *(disk cost only — no correctness or availability
impact; the table already has ample redundant coverage per NOTES-82)*.
**Effort: low** *(mostly re-measurement + a read-path grep; the actual
drop, if warranted, is a one-line addition to a migration script)*.

Surfaced during review of PR #220 (NOTES-82). That PR's measurement
(2026-08-22, ~34-day `pg_stat_user_indexes` accumulation window since
`pg_postmaster_start_time`) showed, on **both** the primary
(`wmata_dashboard`) and the SFMTA sidecar (`sfmta_dashboard`):

```
idx_trip_timestamp      1771 MB   idx_scan = 0
idx_vehicle_timestamp   1614 MB   idx_scan = 0
```

Combined, that's **3.4 GB of composite index with zero reads** over the
window — larger than the 2.3 GB PR #220 reclaims from the three
single-column indexes it drops.

**`idx_route_timestamp` is explicitly NOT part of this item.** The old
NOTES-82 item body (deleted by PR #220 on close) recorded a 2026-07
incident note that derive pipelines are bound to scan
`idx_route_timestamp` — do NOT add a `(route_id, trip_start_date)`
index, it would deepen write amplification. PR #220's own measurement
reconfirms `idx_route_timestamp` is in active use (14,853 scans in the
same window) and deliberately declines to touch it.

Before dropping `idx_trip_timestamp` / `idx_vehicle_timestamp`:

- Re-confirm the zero-scan finding against a fresh `pg_stat_user_indexes`
  read — stats windows must be re-checked, not assumed stable, since
  `stats_reset` is unset and a server restart or manual reset would
  silently invalidate the earlier measurement.
- Grep the codebase for any read path keyed on `vehicle_id` or `trip_id`
  in combination with `timestamp` (e.g. `WHERE vehicle_id = ... ORDER BY
  timestamp`) that a composite would serve but a bare column index
  would not — `scripts/collector_status.py` and the trip-matching
  fallback path (`src/trip_matching.py`) are candidates to check first.
- Investigate whether either composite backs a low-frequency or
  seasonal/recovery-only path (e.g. VM-recovery backfill, an ad-hoc
  diagnostic query, or something exercised only during an incident
  response) that wouldn't show activity in a normal 34-day window but
  would still need the index if invoked.

Acceptance: a written conclusion (in the closing PR body) on whether
`idx_trip_timestamp` and/or `idx_vehicle_timestamp` are safe to drop,
with the causes investigated above addressed — not just a repeat of
the zero-scan measurement.
