# NOTES-134. GTFS reload gate: SFMTA not covered, and the age proxy misses service-change days

**Severity: medium** *(bit for real on 2026-08-30 — Muni's fall service
change produced a near-zero derive day until manually diagnosed)*.
**Effort: medium**.

`bin/pull-and-derive.sh` step 1 runs `scripts/run_gtfs_reload.py
--max-age-days 7`, which only reloads **WMATA** static GTFS. SFMTA's
static GTFS has no gate at all — it is reloaded only when someone
remembers to run `scripts/reload_gtfs_complete.py --agency sfmta`. On
2026-08-30 Muni's fall service change took effect with a new trip_id
space: feed trip_ids matched 0 of 3,657 against `is_current` trips, so
both derive paths (`derive_stop_events_from_state` gates on
`vp_trip_ids & trip_direction.keys()`; the proximity matcher likewise)
produced ~zero stop_events for the date while exiting 0 — the failure
was silent until a per-date count sweep caught it.

Two gaps to close:

1. **Agency coverage** — the reload gate should run per agency (loop
   the configs, or add `--agency` to `run_gtfs_reload.py` and call it
   twice from `bin/pull-and-derive.sh`).
2. **Trigger quality** — the 7-day age gate is a proxy (already
   documented as such in `reload_due`'s docstring) and misses exactly
   the day that matters: a service change landing when the local copy
   is younger than the threshold. Better triggers, roughly in order of
   strength: compare the published feed's `feed_info`/ETag against the
   loaded snapshot before deciding; or a post-replay canary that
   checks the feed-trip_id ∩ current-GTFS-trip_id match rate for the
   latest replayed date and fails loudly (or auto-reloads + re-derives)
   when it collapses. The 8/30 signature — match rate dropping from
   ~normal to ~0 day-over-day — is unambiguous and cheap to compute.

Also applies to WMATA: its fall service change will hit the same window
if the local GTFS is <7 days old on changeover day. Until this lands,
the manual tell is a derive that completes instantly with near-zero
rows (e.g. `derive_stop_events_from_state` finishing in <1s for a date).

## Dependencies

None — but a fix should re-derive any dates that already derived
against the stale snapshot (delete their `runs` rows first; NOTES-113's
failure shape, since near-zero derives can still write `runs` rows).
