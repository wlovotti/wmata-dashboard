# Code Review Notes

Forward-looking punch list. Completed items are removed in the same
PR that closes them — see git log and PR descriptions for history.
Item numbers (`NOTES-N`) are stable; new items take the next number.
NOTES.md edits ride on substantive PRs; standalone reconciliation PRs
are churn.

Last edited 2026-05-05 (PR closing NOTES-26 with NOTES-27/-28 follow-ups).

---

## Active priorities

The bulk of open work is a metrics redesign anchored on materialized
**stop events** as the foundational unit, replacing the daily-batch
recomputation from raw positions. The `stop_events` table is in place
(PRs #42, #43, #44), with two derivation paths (proximity + trip_update)
and a comparison harness confirming the two sources agree to within a
few seconds for 93% of events. The `runs` aggregation over `stop_events`
landed in PR #45, and the OTP origin/destination split (`src/otp_metrics.py`)
landed in PR #46. Downstream metrics build on that foundation —
sequencing still matters.

### P4 — Surface to API + UI

- **NOTES-18 Update grading rubric.** Currently OTP-only; should
  incorporate service-delivered and EWT now that both have shipped.
- **NOTES-27 Investigate observation-coverage gaps in trip_update
  derivation.** EWT was clamped at 0 in the API (was occasionally
  negative due to thin observed headways). Surface coverage_ratio in
  the UI so thin-data periods are visibly flagged.
- **NOTES-5 Per-run deviation chart.** Now a thin API + frontend wrapper
  over `runs` (PR #45) and `stop_events` (PRs #42, #43, #44).

### P5 — Cleanup

- **NOTES-19 Drop `route_metrics_daily` and `route_metrics_summary`.**
  Once the new metrics fully replace them. Coexist for now to avoid UI
  breakage during the transition.
- **NOTES-20 Tighter rider-experience OTP.** A stricter window alongside
  WMATA's official. Tracked but not yet scoped — user wants
  comparability with WMATA's scorecard for now.
- **NOTES-21 Retention job for `trip_update_snapshots`.** Raw feed table
  grows ~5 GB/day (measured: 247 bytes/row × ~20.6M rows/day).
  Append-only by design — the rows are evidence for the trip_update
  derivation pipeline (PR #43), not durable history. Add a daily DELETE
  for snapshots > 14 days old now that the derivation is in. ~6 weeks
  of disk runway, so not urgent yet.

### Independent of the redesign

- **NOTES-23 Schedule the GTFS reload.** Now that
  `reload_gtfs_complete.py` is transactional and FK-safe (PR #48),
  put it on a daily/weekly cron / GitHub Action with alerting on
  failure. Daily is overkill (WMATA revises GTFS roughly quarterly;
  added trips and suspended routes ride TripUpdates /
  VehiclePositions, not static GTFS), weekly probably right.
  Operational risk is silent staleness — that's how this got 6
  months stale before.
- **NOTES-24 Surface GTFS snapshot freshness in the dashboard.**
  Show the newest `gtfs_snapshots.snapshot_date` somewhere visible
  (footer on RouteList?) so a stale schedule is observable instead
  of silent.
- **NOTES-25 Add `tests/` to the lint scope.** CI lints
  `src/ scripts/ api/ pipelines/` only — `tests/` is omitted from
  the path list (not from `[tool.ruff]` config), so test code drifts.
  Small one-off: `ruff check tests/ --fix` clears the existing
  violations, then add `tests/` to both lint args in
  `.github/workflows/test.yml` and the CLAUDE.md command.
- **NOTES-28 Replace daily metrics batch with lazy/live derivation.**
  The redesign pipelines (`derive_stop_events*.py`, `aggregate_runs.py`,
  `compute_bunching.py`) have no orchestrator — NOTES-26 was a direct
  consequence. Before scheduling them, test cheaper alternatives:
  live PostGIS query for service-delivered, or lazy derivation on API
  cache miss. Falls back to scheduled batch if both fail at scorecard
  scale.

---

## NOTES-5. Add per-run schedule-deviation chart to the dashboard

**Severity: low (enhancement). Now unblocked: the `runs` table (PR #45)
plus the stop_events foundation (PRs #42, #43, #44) supply everything
the chart needs. Remaining work is API + frontend.**

### Idea

Line chart of schedule deviation (y-axis, seconds, +late / -early) vs.
stop_sequence (x-axis) for a single bus run. Shows how a bus drifts
across its trip — late starts that recover, early holds, accumulating
drift, segments where the bus loses time. The daily-batch metric can't
support this view; the per-run table can.

### Prototype

Section 4 of `analysis/run_quality.ipynb` builds the chart for one run
on D80 / 2025-10-20. The shape (orange line + green on-time band, axhline
at 0) is what the eventual UI version should resemble.

### Remaining work

1. API endpoint to expose one run's stop deviations:
   `/api/runs/{run_id}/deviations` returning `[{stop_sequence, stop_id,
   stop_name, scheduled, actual, deviation_sec}]`. Reads `stop_events`
   directly for the per-stop list; uses `runs` for the run summary.
2. Frontend route — could live on `RouteDetail` as a "recent runs" list
   that links into a per-run drill-down page.

### Open product questions

- Default selection: today's runs? last completed run? worst-deviation run?
- Should the chart show a single run, or overlay multiple runs of the
  same trip across days to make patterns visible?
- Tooltip needs to show the actual stop name and timestamps, not just
  numbers — useful for spotting where buses always lose time.

---

## NOTES-18. Grading rubric refresh

**Severity: low.**

Current grade (A–F) is OTP-only, computed in `api/aggregations.py`.
With service-delivered (PR #47) and EWT (PR #52) both shipped and now
surfaced through the UI, the rubric should incorporate both —
service-delivered especially, since that's the most rider-felt failure
mode. Worth a separate decision conversation about weighting before
implementing.

---

## NOTES-19. Drop `route_metrics_daily` / `route_metrics_summary`

**Severity: low (cleanup, after the new metrics fully replace them).**

Both tables and the daily batch pipeline that populates them
(`pipelines/compute_daily_metrics.py`) become dead code once the new
stop_events-based pipeline covers all current API consumers. Coexist
for now to avoid UI breakage during the transition. With NOTES-17
closed, the only remaining `route_metrics_summary` consumers are the
legacy scorecard fields (avg_headway_minutes, avg_speed_mph,
total_observations) and the OTP-only grade — track as one final cleanup
PR once those move to the new path.

---

## NOTES-20. Tighter rider-experience OTP

**Severity: low (deferred).**

User considers WMATA's −2 / +7 window lax but wants comparability with
WMATA's published scorecard for now. Future option: expose a stricter
"rider-experience OTP" alongside the official one (e.g., −60s / +180s)
for non-frequent routes (frequent routes get EWT instead — see `src/ewt.py`).
The constants live in `src/otp_constants.py`, so this is a one-line
change — could even be a query-parameter toggle on the API.

---

## NOTES-21. Retention job for `trip_update_snapshots`

**Severity: medium now that the trip_update derivation has shipped
(PR #43) — becomes urgent ~6 weeks after collection starts.**

### The problem

`trip_update_snapshots` is intentionally an append-only evidence trail,
not durable history. Measured storage as of 2026-05-03 (first snapshots):
**247 bytes/row including indexes**, ~7,150 rows per 30s tick →
**~20.6M rows/day, ~4.74 GB/day, ~33 GB/week**. With ~204 GB free on
the user's laptop, that's about 6 weeks of runway before disk pressure.
On a future cloud VM with smaller disks the runway shrinks further.

### Why it's not urgent yet

The point of the trip_update derivation pipeline (PR #43) is to convert
the trail of raw observations into one compact `stop_event` row per
actual arrival. Now that the pipeline is producing stop_events
reliably, the underlying raw rows for any (trip_id, stop_id) pair that's
been derived can be dropped. After derivation the steady state is
~50–80k stop_events per day, comparable to vehicle_positions — manageable
indefinitely.

So: keep collecting raw, then add retention.

### Implementation

1. Daily cron (or pipeline step appended to whatever derives stop_events):
   ```sql
   DELETE FROM trip_update_snapshots
    WHERE snapshot_ts < now() - interval '14 days';
   ```
   14 days gives a comfortable window to re-derive if the derivation
   pipeline has a bug that requires reprocessing.
2. After the first run, `VACUUM` (not `VACUUM FULL` — the table is
   high-churn, regular vacuum keeps bloat in check without locks).
3. If the table is still getting unwieldy on disk, switch to native
   Postgres partitioning by `snapshot_ts` (one partition per day).
   Retention then becomes `DROP PARTITION` — instant and lock-light,
   vs. a long DELETE on a multi-GB table. Only worth the complexity
   if (a) we keep the table for months, or (b) the daily DELETE
   becomes slow enough to interfere with collection writes.

### Dependencies

- Independent of NOTES-14 through NOTES-20.

---

## NOTES-23. Schedule the GTFS reload

**Severity: medium — operational hygiene. The reload now succeeds
reliably (PR #48); the next failure mode is forgetting to run it.**

A daily or weekly GitHub Action / cron invoking
`reload_gtfs_complete.py`, with alerting on failure. Weekly is
probably right: WMATA revises GTFS roughly quarterly, and real-time
operational changes (added trips, suspended routes) land in
TripUpdates / VehiclePositions, not static GTFS — so daily buys
nothing.

The failure mode to alert on is the script raising and rolling
back. The DB stays consistent (the transactional contract from
PR #48 guarantees that), but the schedule slowly goes stale, and
without alerting that's invisible — exactly how this got 6 months
stale before the script fix.

### Dependencies

- Builds on the script reliability landed in PR #48.
- Independent of NOTES-14 through NOTES-21.

---

## NOTES-24. Surface GTFS snapshot freshness in the dashboard

**Severity: low — observability.**

Display the most recent `gtfs_snapshots.snapshot_date` somewhere
visible in the UI (footer on RouteList?) so a stale schedule is
observable instead of silent. Useful even after NOTES-23 schedules
the reload — gives a "last refreshed" sanity check to anyone
viewing the dashboard, and is the first place to look when metrics
start looking off. Pure read; thin API addition.

### Dependencies

- Independent of NOTES-14 through NOTES-21 and NOTES-23.

---

## NOTES-25. Add `tests/` to the lint scope

**Severity: low — tooling hygiene.**

`.github/workflows/test.yml` and the CLAUDE.md commands lint
`src/ scripts/ api/ pipelines/` only. The `tests/` directory is
omitted from the path list — not from `[tool.ruff]` in
`pyproject.toml`, which has no per-directory exclusion — so test
code drifts. Probed 2026-05-04: 7 pre-existing violations
(unused imports, deprecated `typing.Generator`, unsorted
imports), all auto-fixable.

### Implementation

1. `uv run ruff check tests/ --fix && uv run ruff format tests/`
   to clear existing violations.
2. Add `tests/` to both lint args in `.github/workflows/test.yml`
   (the `ruff check` step and the `ruff format --check` step).
3. Update the CLAUDE.md `ruff check` command to include `tests/`.

### Dependencies

- Independent of every other open NOTES item.

---

## NOTES-27. Negative EWT root cause — observation coverage gaps

**Severity: low — the surfaced metric is now clamped at 0 (PR closing
NOTES-17), but the underlying observation gap that caused it is
unresolved.**

EWT (`src/ewt.py`) was producing negative values in some periods —
e.g., C21 PM Peak on 2026-05-03 showed AWT=308s, SWT=360s, EWT=−52s
with only 244 observed headways out of 2100 scheduled (11.6%). When
the trip_update derivation misses arrivals, the observed headways we
*do* capture are biased toward steady ones (the long gaps don't get
paired up because one endpoint of the pair is missing), so AWT comes
out artificially low and EWT goes negative.

A trip-level dimension of the same gap surfaced during the NOTES-26
investigation: ~38% of scheduled trips that show up in upstream feeds
(`vehicle_positions` or `trip_update_snapshots`) don't survive
derivation into a run with `stops_observed >= 3`. Probed 2026-05-05 on
D80 / 2026-05-03 — 261 of 270 GTFS-scheduled Sunday trips appear in
either feed (96.7%), but only 158 (58.5%) reached the delivered
threshold. So the ~50% delivered ratio left after closing NOTES-26 is
part-real, part-derivation-coverage; the headline service-delivered
metric understates true delivery until this is closed.

The clamp is a UI fix, not a data fix. The underlying issue is that
the trip_update derivation pipeline (`pipelines/derive_stop_events_*`)
is missing arrivals during certain periods on certain routes. Worth
investigating:

1. **Per-period coverage histogram**: for every (route, date, period),
   plot `n_observed_headways / n_scheduled_headways`. Routes/periods
   below ~50% coverage should be flagged in the UI as "thin data" so
   the EWT clamp's silent floor doesn't mask the gap.
2. **Cross-check with proximity derivation**: PR #44's proximity
   derivation has different blind spots than trip_update. If a period
   shows 11% TU coverage but 80% proximity coverage, the issue is
   trip_update-specific (and the bunching/EWT pipelines could
   optionally fall back to proximity for that period).
3. **Surface coverage in the API/UI**: add a `coverage_ratio` field
   alongside EWT/bunching so the frontend can show "data thin" badges
   on periods where the metric is unreliable.

### Dependencies

- Independent. Could be tackled alongside the per-run deviation chart
  (NOTES-5) since both probe the trip_update derivation's coverage.

---

## NOTES-28. Replace daily metrics batch with lazy/live derivation

**Severity: medium — architectural. Closes the class of bug NOTES-26
exposed.**

### Background

The metrics-redesign pipelines (`pipelines/derive_stop_events.py`,
`pipelines/derive_stop_events_trip_updates.py`,
`pipelines/aggregate_runs.py`, `pipelines/compute_bunching.py`) are
manual CLIs with no orchestrator. NOTES-26 was a direct consequence —
only 6 routes had been aggregated for 2026-05-03, and the headline
service-delivered metric showed 0% for the rest. Without a structural
fix this recurs on every new service date.

The reflexive answer is "schedule them" — daily cron / GitHub Action
running all four pipelines for yesterday's service date. That works
but is cumbersome, and the live data is right there. Worth
investigating cheaper alternatives first.

### Options

1. **Live PostGIS query for service-delivered.** Service-delivered for
   one (route, date) reduces to: count distinct `trip_id`s in
   `vehicle_positions` where the position lands within 50 m of ≥3 of
   the trip's scheduled stops. With a GiST index on `stops.geom` and
   the existing `(trip_id, timestamp)` index on positions this should
   be sub-second per route. No `stop_events` / `runs` needed for this
   metric specifically. Failure mode: the all-routes scorecard
   (~104 routes) may still need pre-aggregation.

2. **Lazy derivation on API cache miss.** Endpoint asks for
   `runs[route, date]`; if rows exist, use them; if not, run
   `derive_stop_events*` + `aggregate_runs` inline for that single
   (route, date) and persist the rows. First request after midnight
   pays the cost; subsequent reads are free. No cron, no orchestrator
   — the `runs` table is the cache.

3. **Streaming derivation in the collector.** First-detection-within-50 m
   is incremental-friendly: each 60 s tick, derive stop_events for
   newly-arrived positions. Eliminates batch entirely. Larger
   structural change in the hot collection path; deferred unless 1
   and 2 both fail.

4. **Postgres materialized view + cron REFRESH.** Strictly simpler
   than the Python pipeline but still batch — solves "cumbersome to
   run" only trivially. Skip.

### What to test first

Time `derive_stop_events.py --route C21 --date today` and
`derive_stop_events_trip_updates.py --route C21 --date today`
end-to-end. `aggregate_runs.py` is already known to be 0.05–0.2 s per
route. If the two derivation steps add up to under ~2 s for one route,
option 2 (lazy on cache miss) is realistic. If under ~500 ms, option 1
becomes attractive for the all-routes scorecard too.

### Dependencies

- Resolves the operational hazard that produced NOTES-26.
- Independent of NOTES-23 (GTFS reload scheduling) — different
  staleness domains.
- Closing this affects whatever consumer NOTES-5 reads from: if `runs`,
  the lazy path applies; if `stop_events` directly, both paths apply.

