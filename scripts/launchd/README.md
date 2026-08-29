# launchd jobs

Per-user launchd plists for scheduled background work on the developer's
Mac. Single-user repo, so paths are concrete (no templating).

## com.wmata-dashboard.daily-batch.plist

Runs `pipelines/run_daily_batch.py` daily at 03:00 local time. The
wrapper covers yesterday's service date plus any service date in the
prior week with zero rows in `runs` (catch-up after launchd outages or
suspended laptops). Closes NOTES-28.

### Install

```sh
cp scripts/launchd/com.wmata-dashboard.daily-batch.plist \
   ~/Library/LaunchAgents/com.wmata-dashboard.daily-batch.plist
launchctl load -w ~/Library/LaunchAgents/com.wmata-dashboard.daily-batch.plist
```

`-w` flips the `Disabled` bit so the job actually runs at the next
calendar fire. `RunAtLoad` is `false`, so loading does not trigger an
immediate run — kick it manually with `launchctl start
com.wmata-dashboard.daily-batch` if you want to validate end-to-end.

### Status / logs

```sh
launchctl list | grep wmata-dashboard
tail -f logs/daily_batch_$(date +%Y-%m-%d).log    # structured per-day log
tail -f logs/launchd_daily_batch.{out,err}.log    # launchd's capture (early-failure net)
```

### Uninstall

```sh
launchctl unload -w ~/Library/LaunchAgents/com.wmata-dashboard.daily-batch.plist
rm ~/Library/LaunchAgents/com.wmata-dashboard.daily-batch.plist
```

### Updating

After editing the plist in this repo, copy it over again and reload:

```sh
launchctl unload -w ~/Library/LaunchAgents/com.wmata-dashboard.daily-batch.plist
cp scripts/launchd/com.wmata-dashboard.daily-batch.plist \
   ~/Library/LaunchAgents/com.wmata-dashboard.daily-batch.plist
launchctl load -w ~/Library/LaunchAgents/com.wmata-dashboard.daily-batch.plist
```

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
