# NOTES-125. Segment-slip origin exclusion misses multi-bay terminals (layover contaminates the "second" segment)

**Severity: low-medium** *(the schedule-audit page's top-ranked slip row
is a measurement artifact, not a real operations finding — it misleads
the page's headline question)*. **Effort: low-medium** *(a guard in
`_assemble_segment_slip_output` or the slip SQL + tests + a
re-materialization of `route_diagnostic_segment`)*.

Surfaced 2026-08-13 during manual testing of PR #210: M60 direction 0's
top slip row, Fort Totten+Bay K → 1 Pl NE+Riggs Rd NE, shows
`mean_slip_sec` ≈ 522 (~8.7 min), which would mean the average bus
leaves its terminal nearly 9 minutes behind schedule. Investigation
(2026-07-10 → 2026-08-08 window) showed it is layover contamination:

- M60 dir-0 schedules start at `stop_sequence` **2** (Fort Totten+Bay
  A, stop 15403); seq 3 is Fort Totten+Bay **K** (21559) — the two bays
  are **~39 m apart**, both inside the terminal loop, and the proximity
  matcher's radius is **50 m** (`src/analytics.py:914`).
- A bus laying over in the loop is therefore "at" BOTH bays the moment
  it pulls in: mean arrival deviation is **−530 s at Bay A and −531 s
  at Bay K** (recorded ~9 min before scheduled departure), while the
  first street stop (1 Pl NE, seq 4) is **+91 s mean / +43 s median** —
  i.e. buses actually depart on time.
- `compute_segment_slip` (`src/route_diagnostics.py`) excludes only the
  minimum-`from_seq` segment per direction ("origin-departure …
  dominated by layover artifact"). That drops Bay A→Bay K but keeps
  Bay K→1 Pl NE, whose from-stop timestamp is equally
  layover-contaminated — the 522 s "slip" is terminal dwell measured as
  travel time. All of Bay K's skip-N rows (3→5, 3→6, 3→9: 438/510/392 s)
  carry the same additive contamination.

Work items:
1. Generalize the origin exclusion: drop segments whose from-stop is
   layover-contaminated. Two candidate guards (pick after a quick
   look at how many routes are affected): **spatial** — exclude
   from-stops within ~2× proximity radius (≈100 m) of the trip's first
   scheduled stop; or **behavioral** — exclude from-stops whose median
   arrival deviation over the window is grossly early (e.g. < −300 s),
   which would also catch mid-route holding points. The behavioral
   guard is more general but needs a threshold argument — don't assert
   one without checking the deviation distribution across routes.
2. Sweep the current `route_diagnostic_segment` top-N slip rows for
   other origin-adjacent from-stops (any multi-bay terminal: stations
   with several bays within 50 m — Fort Totten, likely others) to size
   the blast radius before choosing the guard.
3. Re-materialize `route_diagnostic_segment` after the fix (pipeline
   re-run; document in the closing PR per the no-inline-backfill
   convention) and confirm M60 dir-0's top slip row becomes a street
   segment with a plausible value.
4. Longer-term note: the principled fix is an observed *departure*
   timestamp (last ping within radius) for the slip from-stop —
   `stop_events` has no `observed_departure_ts` today; that is a
   derivation-schema change and belongs to its own item if pursued.

## Dependencies

None hard. Distinct from the trip-update origin blindness item
(NOTES-31 lineage): that is about the TU feed never observing origins;
this is the proximity source observing them *too well* during layover.
