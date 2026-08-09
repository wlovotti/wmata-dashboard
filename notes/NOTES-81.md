# NOTES-81. Phantom vehicle-reported timestamps in vehicle_positions

**Severity: low (data hygiene; the rows are preserved in S3).**

The 2026-06-10 first run of the tier-3 retention job surfaced six UTC
dates from **October 2025** in `vehicle_positions` — 2025-10-12, -16,
-18, -19, -20, -21, totaling ~2.26M rows (2025-10-20 alone had 1.53M
rows, more than any real collection day) — despite collection starting
2026-05-02. The `timestamp` column stores the GTFS-RT vehicle-reported
GPS-fix time, which is unvalidated: stale AVL clocks produce timestamps
months in the past. All six dates were archived to
`s3://wmata-dashboard-backups/wmata-vp-archive/2025-10-*.parquet` and
deleted from Postgres by the retention job, so the live table is clean
*today* — but nothing stops new phantom rows from accumulating.

Work:
1. **Collector-side sanity guard** — reject (or store with a flag) any
   vehicle timestamp more than a few hours away from collection time
   (`collected_at` exists for exactly this comparison). Log a counter so
   feed-quality regressions are visible in `collector_status.py`.
2. **Check downstream contamination** — per-date pipelines only process
   recent service dates, so the phantom dates were almost certainly
   never derived into `stop_events`; verify with a quick query against
   `stop_events`/`runs` for those dates and note the result here.
3. Optional forensics: the archived parquet files preserve the rows if
   the "which vehicles / which collection days" question ever matters.

## Dependencies

Independent — but if the NOTES-95 collector rewrite starts first, fold
the sanity guard into it rather than patching the old collector.
