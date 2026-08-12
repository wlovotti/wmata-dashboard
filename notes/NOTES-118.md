# NOTES-118. Investigate scheduled-span data quality in runs

**Severity: low-medium** *(metrics computed from scheduled spans —
excess trip time, "% of trips running long" — are silently garbage for
affected routes: A90's header showed "95% running long, median trip
20 min, schedule 0 min (+4286%)")*.
**Effort: medium (investigation-first — the fix is unscoped until the
causes are quantified; a degenerate-span guard is one possible outcome,
not the mandate)*.

Found 2026-08-11 while diagnosing the A90 route page. On 2026-08-07,
**67% of A90's `runs` rows have `sched_last_arrival_ts =
sched_first_arrival_ts`** (zero scheduled span), vs a 4–10% rate on
typical routes. Two route_ids look outright junk: `EXP` (99.8% of 433
runs zero-span) and `LCL` (28.8% of 347) — their provenance (GTFS-RT
route designations? feed artifacts?) is part of the investigation.
Contributing mechanisms observed so far: WMATA publishes A90 with
extremely sparse stop_times (~2.5 rows/trip, gapped sequences like
2,3,5, where seq 3→5 are 26 *seconds* apart in schedule), and the
trip_update source cannot observe trip origins, so a short express
pattern leaves a degenerate matched span — a 20-minute observed
traversal then computes as +4286% over a 26-second "schedule".

Investigation scope (before any fix): quantify zero/near-zero scheduled
spans per route and per agency across a multi-week window; classify
causes (sparse published stop_times, origin-blind trip_update matching,
junk route_ids, anything else that falls out); check whether SFMTA
exhibits the same shape; and decide per cause — e.g. compute scheduled
duration from the trip's full GTFS span instead of the matched span,
guard/suppress the metric below a minimum scheduled-span threshold,
and/or exclude junk routes upstream. Whatever ships must distinguish
"no meaningful schedule to compare against" (render N/A with a reason)
from a confident wrong percentage.

Acceptance: a written summary of prevalence + causes (in the closing
PR body), and the "% running long" tile no longer renders
degenerate-denominator percentages for affected routes.
