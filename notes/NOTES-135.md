# NOTES-135. `bin/pull-and-derive.sh` never replays or derives SFMTA — it stays fully manual

**Severity: low-medium** *(discovered while closing NOTES-134 — the
GTFS reload gate now covers SFMTA, but the pipeline stage it protects
still doesn't run for SFMTA automatically)*.
**Effort: medium**.

`bin/pull-and-derive.sh`'s replay loop only replays
`$LOCAL_ARCHIVE` (the WMATA TU archive root) into `trip_update_state`
— there is no equivalent loop over `$LOCAL_ARCHIVE_SFMTA`. Its derive
step calls `pipelines/run_daily_batch.py --lookback-days N` with no
`--agency` flag, so it only ever drives the WMATA pipeline chain. VP
archives ARE synced and loaded for both agencies (the `aws s3 sync`
and `load_vp_archive.py` steps already cover
`LOCAL_ARCHIVE_SFMTA`/`LOCAL_ARCHIVE_SFMTA_VP`), so SFMTA's
`vehicle_positions` table stays fresh through this script, but
`trip_update_state`, `stop_events`, `runs`, and the aggregate chain
for SFMTA only advance when someone runs
`pipelines/replay_archive_to_state.py --agency sfmta` and
`pipelines/run_daily_batch.py --agency sfmta` by hand (as documented
in NOTES-113's recovery steps, and as happened again manually on
2026-08-30/31 while diagnosing NOTES-134's Muni service-change
incident).

Practical effect: SFMTA metrics silently go stale between manual
runs, with no automated signal (no error, just an unadvanced dataset)
— the same "silent staleness" failure shape NOTES-23 and NOTES-134
both closed for their respective pieces, just one layer up the
pipeline.

Fix shape: extend `bin/pull-and-derive.sh`'s replay loop to also
replay `$LOCAL_ARCHIVE_SFMTA` (with `--agency sfmta`), and add a
second `run_daily_batch.py --agency sfmta --lookback-days N`
invocation after the WMATA one — mirroring the per-agency pattern this
same script now uses for the GTFS reload gate (NOTES-134) and already
uses for the VP loaders.

## Dependencies

None — unblocked. Natural follow-on to NOTES-134 (PR that closed it
added the per-agency GTFS reload gate + trip_id match-rate canary,
but left SFMTA replay/derive automation out of scope).
