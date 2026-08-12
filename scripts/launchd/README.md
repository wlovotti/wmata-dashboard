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

**Install runbook landed 2026-08 (Path 2a) — NOT YET LOADED.** This job
was durably disabled 2026-06-13 during laptop retirement, and the
2026-06/07 incident's RC2 (feed S1000249 expired 6/20, unnoticed until
manually fixed 7/12 — see `docs/POSTMORTEM_2026-07.md`) was originally
slated to be closed by scheduling the reload on the cloud VM instead.
That plan didn't survive contact with the topology: Path 2a
(2026-07-18) made the **laptop's local PostgreSQL 16 the system of
record** — derivation (`bin/pull-and-derive.sh` →
`pipelines/run_daily_batch.py`) reads GTFS `trips`/`routes`/`stops`
from the laptop DB, and nothing on the VM reads GTFS at all (the
collector only writes raw feed data). So re-enabling this laptop
launchd job *is* the fix, not a VM systemd unit — PR #196 fixed the
plist/wrapper/docs and confirmed nothing is bitrotted, but did not
load the job (a live GTFS reload against the system-of-record DB is a
human-run step, not something to trigger unattended). **As of
2026-08-11 the job is still unloaded** — `launchctl list | grep wmata`
returns nothing and the plist sits in `~/Library/LaunchAgents/`
un-activated. The laptop's `gtfs_snapshots.created_at` is still stuck
at 2026-07-12 (30+ days stale and counting) until someone runs the
Install + First-run verification steps below. Do not treat this
section as "done" just because the runbook exists — verify with
`launchctl list | grep wmata` before assuming the job is live.

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

### First-run verification (do this once, after install)

Confirm the job is actually wired up end-to-end against the real
system-of-record DB — don't just trust that `launchctl load` succeeded
silently.

```sh
# 1. Confirm the job is loaded and not disabled
launchctl list | grep com.wmata-dashboard.gtfs-reload

# 2. Kick a real run (hits the WMATA API + rewrites GTFS tables in the
#    laptop's system-of-record DB — intentional, not a drill)
launchctl start com.wmata-dashboard.gtfs-reload

# 3. Watch the structured per-run log until it reaches OK or FAILED
tail -f logs/gtfs_reload_$(date +%Y-%m-%d).log

# 4. No failure marker should exist after a successful run
ls logs/gtfs_reload_LAST_FAILURE.json 2>/dev/null && echo "FAILED — see marker" || echo "OK — no failure marker"

# 5. Confirm the API-visible freshness endpoint now reports a current
#    snapshot (needs `uv run uvicorn api.main:app --reload` running
#    locally against the same DATABASE_URL)
curl -s localhost:8000/api/gtfs/freshness | python3 -m json.tool

# 6. Or check the DB directly without the API
psql -d wmata_dashboard -Atc \
  "SELECT snapshot_date, created_at, feed_version FROM gtfs_snapshots ORDER BY created_at DESC LIMIT 1;"
```

Expect steps 5/6 to show `snapshot_date`/`created_at` as today's date
and `status: "ok"` (not `"expired"` or `"expiring_soon"`).

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
