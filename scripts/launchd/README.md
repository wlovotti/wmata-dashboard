# launchd jobs

Per-user launchd plists for scheduled background work on the developer's
Mac. Single-user repo, so paths are concrete (no templating).

The two jobs are deliberately offset (03:00 and 04:00 Sunday) so they
never contend for DB / network / WMATA API budget at the same instant.

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

## com.wmata-dashboard.gtfs-reload.plist

Runs `scripts/run_gtfs_reload.py` weekly, Sunday at 04:00 local time.
The wrapper invokes `scripts/reload_gtfs_complete.py` (transactional,
FK-safe since PR #48), captures its output to a per-run log, and fires
a macOS desktop notification on failure. Closes NOTES-23.

Weekly is the right cadence because WMATA revises GTFS roughly
quarterly; daily would just be noise. The failure mode this job exists
to prevent is silent staleness — before PR #48 + this schedule, the
GTFS snapshot went 6 months stale before someone noticed.

### Install

```sh
cp scripts/launchd/com.wmata-dashboard.gtfs-reload.plist \
   ~/Library/LaunchAgents/com.wmata-dashboard.gtfs-reload.plist
launchctl load -w ~/Library/LaunchAgents/com.wmata-dashboard.gtfs-reload.plist
```

`RunAtLoad` is `false`, so loading does not trigger an immediate
reload. Validate end-to-end with `launchctl start
com.wmata-dashboard.gtfs-reload` (this WILL hit the WMATA API and
rewrite GTFS tables — only run when you mean to).

### Status / logs

```sh
launchctl list | grep wmata-dashboard
tail -f logs/gtfs_reload_$(date +%Y-%m-%d).log    # structured per-run log
tail -f logs/launchd_gtfs_reload.{out,err}.log    # launchd's capture
ls logs/gtfs_reload_LAST_FAILURE.json             # marker file dropped on failure (absent on success)
```

### Uninstall

```sh
launchctl unload -w ~/Library/LaunchAgents/com.wmata-dashboard.gtfs-reload.plist
rm ~/Library/LaunchAgents/com.wmata-dashboard.gtfs-reload.plist
```

### Updating

```sh
launchctl unload -w ~/Library/LaunchAgents/com.wmata-dashboard.gtfs-reload.plist
cp scripts/launchd/com.wmata-dashboard.gtfs-reload.plist \
   ~/Library/LaunchAgents/com.wmata-dashboard.gtfs-reload.plist
launchctl load -w ~/Library/LaunchAgents/com.wmata-dashboard.gtfs-reload.plist
```

### Failure alerting

On non-zero exit from `reload_gtfs_complete.py`, the wrapper:

1. Fires a macOS desktop notification (`osascript -e 'display
   notification ...'`) so the next time the user looks at the laptop
   they actually see the failure.
2. Writes `logs/gtfs_reload_LAST_FAILURE.json` with timestamp and exit
   code. Cleared on the next successful run. Available for the
   dashboard to surface (NOTES-24) if a "last reload failed" badge is
   ever wanted.
3. Exits non-zero so launchd's own log capture
   (`logs/launchd_gtfs_reload.err.log`) records the failure too.

The DB stays consistent regardless — the reload script's transactional
contract (PR #48) guarantees either-fully-applied-or-rolled-back.
