# NOTES-145. Replay re-folds the full lookback window every freshness run

**Severity: medium (the 2026-09-07 `bin/pull-and-derive.sh` run took ~9 h
wall-clock; the manual trigger feels expensive because the run is).**
**Effort: medium (manifest table + replay skip logic + tests; mirrors the
VP loader's `vp_archive_loaded_files` pattern from PR #235).**

`pipelines/replay_archive_to_state.py` is idempotent only in the *result*
sense: re-running a date re-reads and re-folds every archive file for it.
`bin/pull-and-derive.sh` replays `LOOKBACK_DAYS` (default 14) dates per
run, so each run re-folds ~100M WMATA + ~30M SFMTA snapshot rows per day
× 14 days (~1.8B rows) to advance state by the 1–6 days that are actually
new. Derive (`determine_target_dates`) then ignores every date that
already has `runs` rows, so the re-fold of already-derived dates is
discarded work — no metric reads `trip_update_state` at request time,
and window metrics (7d/30d/deltas/ETT) read the persisted per-date
derived tables.

The VP loader already solved this shape: `vp_archive_loaded_files`
records each immutable S3-synced file once, in the same transaction as
its rows, and skips it on later runs.

Work: add a `tu_archive_replayed_files` manifest keyed by
`(filename, target_service_date)` — the key must include the target date
because the UTC-next-day supplement file for date D is the same physical
file as D+1's first primary file and is legitimately folded once per
target. Fold all not-yet-manifested files for a date in one in-memory
pass (the existing fold sorts by snapshot_ts; the DB upsert is
always-overwrite for `final_snapshot_ts`, so a stale file must never be
folded *after* a newer one on its own). Guard the out-of-order case
(an older per-process file arriving after a newer sibling was already
manifested — collector-restart overlap racing the S3 sync) explicitly.
Migration script + `migrate_all.py` pickup; first run after deploy
re-folds the window once to seed the manifest.

## Dependencies

None. [NOTES-146](NOTES-146.md) (scheduled freshness) should land after
this — a nightly 9 h job competing with interactive use is worse than the
manual trigger.
