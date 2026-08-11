# NOTES-113. Top-up SFMTA 2026-08-09/8-10 after next archive rsync

**Severity: low** *(the comparison window already excludes 8/8–8/9 per
the NOTES-99 window decision, so this is completeness hygiene, not a
comparison blocker.)*
**Effort: low** *(a few manual pipeline invocations once the raw data
is local.)*

During the NOTES-105/110 repave batch (2026-08-11), `run_daily_batch
--agency sfmta` derived 2026-08-09 from the raw data available locally
at the time: `trip_update_state` ended at 8/8 and `vehicle_positions`
at 8/10 01:57 UTC, so 8/9 came out proximity-only — 16,182 stop_events,
7.1% ingest coverage, flagged partial. The trap: **8/9 now has `runs`
rows, and `determine_target_dates` only auto-revisits dates with zero
`runs` rows** — so the normal daily batch will never top it up, even
after the missing raw data arrives.

After the next `bin/pull-and-derive.sh` rsync brings the 8/9–8/10
archive over, force the top-up manually:

1. `pipelines/replay_archive_to_state.py --date <D> --agency sfmta`
   for 2026-08-09 and 2026-08-10 (their `trip_update_state` rows don't
   exist locally; the JSONL archive is the source).
2. Re-run both derive pipelines for those dates
   (`--all-routes --date <D> --agency sfmta`).
3. Re-run the aggregate chain (`aggregate_runs` → `compute_bunching` →
   overlay → `upsert_system_metrics_daily`) for the same dates —
   or simply delete those dates' `runs` rows first and let
   `run_daily_batch --agency sfmta` re-target them.

## Dependencies

Blocked on the next SFMTA archive rsync (`bin/pull-and-derive.sh`).
