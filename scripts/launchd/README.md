# launchd jobs (retired location)

This directory held the laptop's per-user launchd plists before issue #246
consolidated the scheduled-job plists under `deployment/launchd/` (mirroring
`deployment/systemd/` for the nano collector). **The live plist is now
`deployment/launchd/com.wmata-dashboard.pull-and-derive.plist`** — see
`docs/DEPLOYMENT.md` §12 for the full install/verify/uninstall runbook.

## com.wmata-dashboard.daily-batch.plist — retired

Retired by PR #259 (issue #246): installed-but-never-loaded in production
(`launchctl list | grep wmata` showed nothing as of 2026-08-11), and its
job — `pipelines/run_daily_batch.py` — is now one step inside
`bin/pull-and-derive.sh`, which the new
`com.wmata-dashboard.pull-and-derive` job runs nightly. If a copy is still
sitting in `~/Library/LaunchAgents/` from an earlier install attempt,
`launchctl bootout gui/$UID/com.wmata-dashboard.daily-batch` it (or
`launchctl unload -w ~/Library/LaunchAgents/com.wmata-dashboard.daily-batch.plist`
on an older launchd) and delete the file.

## com.wmata-dashboard.retain-trip-update-state.plist — retired

Retired by PR #259 (issue #246): also installed-but-never-loaded. Its job —
pruning `trip_update_state` so the table stays bounded even if the nightly
batch is paused — is superseded now that the batch itself (via
`pipelines/cleanup_trip_update_state.py`, both agencies) runs nightly
through `bin/pull-and-derive.sh` rather than being an occasionally-run
manual step: WMATA's cleanup happens inside `run_daily_batch.py`'s
housekeeping loop, and `bin/pull-and-derive.sh` calls
`cleanup_trip_update_state.py --agency sfmta` directly after a successful
SFMTA derive. `pipelines/retain_trip_update_state.py` itself (the
standalone 14-day safety-net script) is left in the repo as a manual
escape hatch, just no longer scheduled. Uninstall the same way as
daily-batch above if a stray copy is still loaded.

## com.wmata-dashboard.gtfs-reload.plist — retired

Retired by the stateless-collector cutover PR (closing NOTES-95/94/81): the weekly
GTFS reload is now step 1 of `bin/pull-and-derive.sh`
(`scripts/run_gtfs_reload.py --max-age-days 7`, gating on the loaded
snapshot's age) rather than its own schedule. This job was never loaded
in production (`launchctl list | grep wmata` never showed it), so
retiring it has no live-topology impact. The plist itself
(`com.wmata-dashboard.gtfs-reload.plist`) was removed from the repo in
the same PR — if it's still sitting in `~/Library/LaunchAgents/` on this
laptop from an earlier install attempt, `launchctl unload -w` it and
delete it manually.
