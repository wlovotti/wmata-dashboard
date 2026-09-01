# NOTES-136. SFMTA trip_update_state has no retention schedule — automating the SFMTA replay makes it a standing gap

**Severity: low-medium.**
**Effort: low.**

Origin: PR #228 review (bin/pull-and-derive.sh SFMTA replay+derive
automation, NOTES-135).

`pipelines/run_daily_batch.py`'s `run_housekeeping_pipeline` loop
skips housekeeping wholesale for any non-default agency
(`pipelines/run_daily_batch.py:554-564`) — three of the four
housekeeping pipelines aren't agency-aware, but the fourth,
`cleanup_trip_update_state.py`, IS agency-aware (`--agency`) and is
still skipped for `agency=sfmta`, with only a log line telling an
operator to run it on its own schedule. No schedule anywhere actually
runs `cleanup_trip_update_state.py --agency sfmta` — it was never
wired into a launchd job or cron, and PR #227's own history shows the
WMATA-only retention timer.

Before PR #228, this was a latent gap: SFMTA's `trip_update_state`
only grew when someone manually ran the SFMTA replay. PR #228 made
`bin/pull-and-derive.sh` replay SFMTA on every freshness run (its
normal manual cadence, potentially frequent), which advances
`trip_update_state` for SFMTA regularly — turning the missing
retention into a standing, unbounded-growth problem rather than an
occasional one.

**Fix shape:** either (a) make `cleanup_trip_update_state.py --agency
sfmta` part of `bin/pull-and-derive.sh` itself (append it as a
per-agency step near the other housekeeping-adjacent calls), or (b)
give it its own scheduled job the way WMATA's retention timer works
(see `docs/DEPLOYMENT.md` §12/§13's LaunchAgents table), or (c) make
`run_housekeeping_pipeline`'s loop agency-aware for
`cleanup_trip_update_state.py` specifically instead of skipping the
whole loop for non-default agencies. (a) is the smallest change and
keeps the behavior next to the other SFMTA automation PR #228 already
added.

## Dependencies

None — self-contained; can be picked up independently of any other
open NOTES item.
