# NOTES-126. Add an observed-departure timestamp to stop_events for a principled slip-origin exclusion

**Severity: low** *(the layover-contamination guard shipped in the
segment-slip origin guard PR closes the immediate measurement-artifact
problem well enough for the schedule-audit page; this is a
follow-on precision improvement, not a correctness gap)*.
**Effort: medium-high** *(derivation-schema change — a new
`stop_events` column plus new logic in the proximity matcher to detect
"last ping before pull-out" rather than "first ping within radius")*.

Deferred from work item 4 of the segment-slip origin guard item
(formerly NOTES-125, closed without this): that item's guard excludes
any from-stop that is spatially near the route's origin AND
behaviorally grossly-early (median deviation), which correctly
suppresses multi-bay-terminal layover contamination but is still a
proxy — it works by recognizing the *symptom* (early arrival at a
near-origin stop) rather than measuring the thing we actually want
(when the bus left).

The principled fix: derive an `observed_departure_ts` for each
`stop_events` row — the timestamp of the *last* proximity ping within
radius before the vehicle moves away, rather than the first. For a
layover bay this is very close to true pull-out time; for an ordinary
street stop it's very close to the existing arrival timestamp (dwell
time is short), so the change should be low-risk for non-terminal
segments. With `observed_departure_ts` available, segment slip could
be computed as `observed_departure_ts` (at the from-stop) → 
`observed_arrival_ts` (at the to-stop) instead of arrival-to-arrival,
which would make the origin-exclusion guards in
`compute_layover_stop_ids` / `_assemble_segment_slip_output`
unnecessary for genuinely-observed segments (though upstream schedule
mismatches would still need the canonical-pattern filtering that
already exists).

Scope, if pursued:
1. Add `observed_departure_ts` (nullable) to `stop_events`.
2. Update the proximity matcher (`src/analytics.py`) to record it —
   requires tracking the last in-radius ping per (trip, stop) rather
   than stopping at the first.
3. Re-derive historical `stop_events` for the column to backfill
   (or leave NULL pre-cutover and accept a mixed window, following the
   precedent of other derivation-schema changes documented in
   `docs/MIGRATIONS.md`).
4. Update `compute_segment_slip` to prefer `observed_departure_ts` at
   the from-stop when present, falling back to the current
   arrival-to-arrival calculation otherwise.

## Dependencies

None hard. Builds on (but does not block) the segment-slip origin
guard PR's spatial+behavioral guard, which remains the fallback for
rows with no `observed_departure_ts`.
