# NOTES-109. Full per-date scheduled-pool resolution for EWT/bunching (deferred third option from NOTES-106)

**Severity: low-medium** *(current modal-per-day_type resolution, shipped in PR #191, is a legitimate representative-schedule approximation — this is a precision upgrade, not a bug fix)*.
**Effort: medium** *(needs a cache rework keyed by date instead of day_type, plus a fresh WMATA output re-validation since the change is a deliberate, not accidental, output shift)*.

During NOTES-106's second review round (PR #191), three designs were on
the table for resolving "which service_ids does the scheduled-headway
pool for EWT/bunching draw from":

1. Feed-wide pooling of matching-weekday `calendar_dates` exceptions —
   rejected (self-reverting on any single exception; blended disjoint
   schedule-revision eras together).
2. A single deterministic "most recent evidence" anchor date — rejected
   (targets the feed's terminal edge, exactly where agencies stack
   schedule-transition exceptions; verified on real `wmata_dashboard` data
   to silently corrupt weekday/Saturday resolution).
3. **Modal resolution** — sample every representative-weekday date
   (Tuesday for `weekday`, Saturday for `saturday`, Sunday for `sunday`)
   in the feed's validity window, resolve each independently via
   `src.gtfs_calendar.scheduled_service_ids_for_date`, take the most
   common (modal) result. **This is what shipped** — verified against
   real data to reproduce WMATA's pre-PR output exactly
   (weekday={11}, saturday={14}, sunday={13}) and to resolve SFMTA to the
   majority schedule-revision era (weekday={78968}).

A fourth option was raised and explicitly declined for this PR: drop the
representative-day_type abstraction entirely and resolve EVERY
`service_date` the EWT/bunching pipelines actually process against its
own **exact** date via `scheduled_service_ids_for_date` directly — no
day_type sampling, no modal vote, no "representative Tuesday" fiction at
all. This is the most GTFS-correct option (Fridays get Friday's real
schedule, federal holidays get the holiday's real schedule, and every
schedule-revision era is resolved on its own exact dates with no
majority/minority ambiguity), but it was deferred rather than shipped in
PR #191 because it needs:

- **A cache rework.** `fetch_scheduled_cell_hours_for_routes`'s
  module-level `_schedule_cache` is keyed `(day_type, snapshot_id)` today
  — 3 entries per snapshot, shared across every observed date of that
  day_type. Per-date resolution needs a cache keyed by
  `(service_date, snapshot_id)` (or similar) instead, changing the
  cache's hit-rate characteristics for the windowed multi-date dashboard
  queries (`compute_ewt_headline_for_routes_multi_date` and friends,
  which currently fetch the schedule ONCE per day_type for an entire
  multi-week window).
- **A fresh WMATA re-validation.** Unlike NOTES-106's fix (which was
  scoped specifically to NOT change WMATA's output — see PR #191's
  acceptance criteria), per-date resolution WOULD deliberately change
  WMATA's output: Fridays would stop being silently excluded from the
  "weekday" schedule (WMATA's Friday service_id doesn't have
  `tuesday=1`, so it's invisible to both the old code and the
  modal-resolution fix that shipped), and federal holidays would use
  their real substituted schedule instead of "whatever the modal Tuesday
  looks like." That's a genuine improvement, but it needs its own
  validation pass and sign-off as an intentional metric change, not a
  side effect of a calendar_dates bug fix.

Acceptance (when this is picked up): EWT/bunching scheduled pools are
computed per exact `service_date` via `scheduled_service_ids_for_date`
directly (no day_type/modal layer); the schedule cache is keyed by date;
a WMATA before/after comparison is run and the output difference (Fridays,
holidays) is reviewed and accepted as intentional before merge.

One more reason full per-date resolution is the eventual right answer:
SFMTA's modal margin is thin — `78968` wins weekday 3 Tuesdays to
`82660`'s 2 out of the 7/23-8/28 window sampled at PR #191's merge time.
One more week of feed (a single additional Tuesday landing in either
era) flips the mode, so a historical SFMTA re-derive done today and the
same re-derive done after the next GTFS reload are not guaranteed to
agree — modal resolution is reload-order-sensitive in a way exact
per-date resolution structurally isn't (each date always resolves the
same way regardless of what the rest of the window looks like).

## Dependencies

None (unblocked) — but not urgent. Modal resolution (PR #191) is a
reasonable interim: it matches WMATA's pre-existing output exactly and
resolves SFMTA to a real, majority-representative schedule. This item is
a precision upgrade for the Friday/holiday edge cases, not a fix for a
known-wrong number.
