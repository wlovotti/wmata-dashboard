# NOTES-146. Freshness pull is manually triggered

**Severity: low (personal audience; staleness is an annoyance, not a
correctness problem — the dashboard is simply days behind until someone
runs `bin/pull-and-derive.sh`).**
**Effort: low-medium once [NOTES-145](NOTES-145.md) lands (launchd wrapper +
healthchecks ping + API cache invalidation); not worth doing before it.**

Since the stateless-collector cutover (Path 2a, PR #222–#225) the laptop
is the system of record and freshness is a manual `bin/pull-and-derive.sh`
run. `docs/DEPLOYMENT.md` §12 documents two laptop launchd jobs
(`com.wmata-dashboard.daily-batch`, `…retain-trip-update-state`) that are
installed but unloaded and predate the pull-and-derive flow (they only
derive; they don't sync S3 or replay).

Work: replace the unloaded daily-batch plist with a launchd job that runs
`bin/pull-and-derive.sh` nightly (`StartCalendarInterval` fires missed
runs on next wake, so a sleeping laptop yields "fresh within the hour of
opening it"), with a healthchecks.io dead-man ping on success like the
collector's. Handle the API's 1 h window caches (a restart hook or a
shorter TTL — the 9/7 run needed a manual API restart before new dates
showed). Retire or fold in the two stale plists and update §12.

## Dependencies

Blocked on [NOTES-145](NOTES-145.md): with the full-window re-fold a
nightly job is a multi-hour DB writer competing with interactive use.
