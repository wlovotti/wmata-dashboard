# NOTES-100. SFMTA derivation end-to-end (Plan 2 of the comparison arc)

**Severity: high (blocks NOTES-99; the matched raw data has been
accumulating since 2026-07-22 but none of it is derivable yet).**
**Effort: medium-high (thread `--agency` through the pipeline chain;
validate metric parity on real data).**

Take the raw SFMTA JSONL archive (sidecar collector live since
2026-07-22, snapshots in `archive/sfmta_raw_snapshots/`, triple-homed
per the 2026-08-08 ops check) through the full derivation chain:
replay → `trip_update_state` → `stop_events` → `runs` → per-route
metrics → agency-level rollups, per the SFMTA comparison design spec
(`docs/superpowers/specs/2026-07-21-sfmta-comparison-design.md`,
Plan 2). Agency configs already exist (`config/agencies/{wmata,sfmta}.yaml`).

Work:
1. Thread an `--agency` parameter through `pipelines/run_daily_batch.py`
   and the per-route pipelines it orchestrates (default `wmata` so
   existing invocations are unchanged).
2. Service-date and timezone correctness: SFMTA service days are
   Pacific — builds on the NOTES-96 replay changes; audit any other
   `eastern_*` call sites the SFMTA path would hit (`src/timezones.py`).
3. SFMTA GTFS schedule load (versioned via `is_current`, same as WMATA)
   if not already present — the OTP/EWT/delivered metrics all join
   against the schedule.
4. Backfill: replay + derive the archive from 2026-07-22 forward.
5. Parity spot-validation: distributions sane vs known WMATA patterns
   (e.g. the ~40% early-arrival rate has a Muni analogue to find, match
   rates in the healthy 45–50% band for top routes), and a day of Muni
   headways eyeballed against the published schedule.

**Subagent note:** the code is subagent-suitable; the backfill run is
heavy and user-run (document the commands in the PR body — same
convention as baseline regens).

## Dependencies

NOTES-96 first (same file: `pipelines/replay_archive_to_state.py` —
don't stack PRs). Blocks NOTES-99.
