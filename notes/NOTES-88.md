# NOTES-88. `/api/routes` latency cliff over the SSH tunnel

**Severity: medium (blocks remote use only — Overview and RouteList both
depend on `/api/routes`; surfaced the moment the DB moved off the local
socket. Fix before public deploy; dev itself is unaffected now that it runs
on a local socket).**
**Effort: medium (likely a query-shape fix in `api/aggregations.py`; unknown
until the round-trip count is profiled).**

Discovered 2026-06-13 while verifying the SSH tunnel
(`bin/db-tunnel.sh`). With the API pointed at the VM through the
tunnel, `/api/routes` times out (>90s, no response), while light
single-query endpoints are instant (`/api/gtfs/freshness` returned in
0.29s). Measured tunnel round-trip latency is only ~9ms and single
aggregate queries are fast (count over `route_metrics_daily_overlay`
782ms; a 30-day windowed query 126ms), so this is not a slow VM or a
slow link — it's almost certainly a per-route **N+1 query pattern**
(iterating ~126 routes, ×metrics, ×days, plus the server-side `deltas`
block from PR #125) that was free on the old sub-millisecond local
Unix socket and explodes at ~9ms × thousands of round-trips over the
network. The "warm path ~37ms" figure in NOTES-49 was measured against
the local socket and silently stopped holding at cutover.

Recontextualized 2026-06-14: dev now runs on a local socket (no tunnel),
so this no longer blocks dev or NOTES-84. It becomes a
co-locate-API+DB task for the NOTES-50 public deploy.

Work:
1. Profile the endpoint's query count (SQLAlchemy echo / `pg_stat_statements`
   on the VM) to confirm the N+1 and find the loop in `api/aggregations.py`.
2. Collapse the per-route loop into one (or a few) set-based queries —
   `GROUP BY route_id` over the window rather than a query per route.
3. Re-verify over the tunnel: target a cold `/api/routes` well under a few
   seconds. The server-side 60s cache only helps the second caller; the
   first (and the cache-miss after TTL) must be fast on its own.

## Dependencies

Independent; required before (or as part of) the NOTES-50 public
deploy. Not on the personal-audience critical path.
