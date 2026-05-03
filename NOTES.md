# Code Review Notes

Forward-looking punch list. Completed items are removed in the same
PR that closes them — see git log and PR descriptions for history.
Item numbers (`NOTES-N`) are stable; new items take the next number.
NOTES.md edits ride on substantive PRs; standalone reconciliation PRs
are churn.

Last edited 2026-05-03.

> Phases A1+A2 of NOTES-7 are in flight on `feature/stop-events-schema`:
> the `stop_events` table, model, and proximity-source derivation pipeline
> (`pipelines/derive_stop_events.py`) land here. NOTES-7 stays open until
> the trip_update derivation (Phase B1) and comparison harness (Phase B2)
> are in. Smoke-tested on 2026-05-03: 266k positions → 167k matches →
> 82k stop_events across all routes in 88 s, deviations bounded ±35 min.
> NOTES-22 (broken `reload_gtfs_complete.py`) folded in as a ride-along.

---

## Active priorities

The bulk of open work is a metrics redesign anchored on materialized
**stop events** as the foundational unit, replacing the daily-batch
recomputation from raw positions. Sequencing matters — the early phases
gate the later ones.

NOTES-7 derivation needs ~7 days of accumulated TripUpdates data before
it can run — earliest start ~2026-05-10.

### P0 — Foundation (gates the rest)

- **NOTES-7 `stop_events` table.** One row per (trip_id, stop_id, observed
  arrival), with `source` column (`'trip_update'` | `'proximity'`). Both
  sources can coexist — they're independent observations. Replaces the
  current re-derive-on-every-batch model. Blocked on ~7 days of
  TripUpdates collection (PRs #29, #30).
- **NOTES-8 `runs` aggregation.** Trivial roll-up over `stop_events`. No
  single `is_complete` flag — each metric applies its own filter at
  query time.

### P1 — Quick wins on the new foundation (small, no new tables)

- **NOTES-10 OTP at origin / destination split.** Already have stop-level
  data — just filter to first/last stop_sequence per trip. Distinguishes
  dispatch problems from run-time problems from recovery problems.

### P2 — Medium-effort metric additions

- **NOTES-11 Service-delivered ratio.** % of scheduled trips that actually
  ran. Most rider-felt failures are missing buses, not late ones, and
  we currently can't see this at all. Needs a schedule-side count from
  GTFS calendar/calendar_dates joined to trips, denominator-style.
- **NOTES-12 End-to-end excess trip time.** From `runs`: median, p95, %
  of runs with actual > 110% of scheduled. Captures dwell + in-vehicle
  delay, not just wait. The metric MBTA OPMI is rolling out for buses.
- **NOTES-13 Bunching count.** Count and rate of headways < 0.5 ×
  scheduled. Complements existing CV metric (which hides bunching in
  averages).
- **NOTES-14 Stop-skip rate.** Direct from TripUpdates `SKIPPED`
  stop_time_updates — data we can't derive from positions at all. Per
  route, per day, per stop. Unique value-add from the TripUpdates feed.
- **NOTES-15 Excess Wait Time (EWT) for frequent routes.** AWT =
  `mean(h²) / (2·mean(h))` from observed headways at each stop-hour;
  SWT same for scheduled. EWT = AWT − SWT, aggregated to (route, date,
  time_period) where the route is frequent (scheduled headway ≤ 15 min).
  TfL's standard rider-experience metric.

### P4 — Surface to API + UI

- **NOTES-17 New API fields and panels on `RouteDetail`.** Each new
  metric needs to land somewhere in the UI; sequencing here can lag the
  data layer.
- **NOTES-18 Update grading rubric.** Currently OTP-only; should
  incorporate service-delivered and EWT once those land.
- **NOTES-5 Per-run deviation chart.** Becomes feasible once
  `stop_events` and `runs` exist.

### P5 — Cleanup

- **NOTES-19 Drop `route_metrics_daily` and `route_metrics_summary`.**
  Once the new metrics fully replace them. Coexist for now to avoid UI
  breakage during the transition.
- **NOTES-20 Tighter rider-experience OTP.** A stricter window alongside
  WMATA's official. Tracked but not yet scoped — user wants
  comparability with WMATA's scorecard for now.
- **NOTES-21 Retention job for `trip_update_snapshots`.** Raw feed table
  grows ~5 GB/day (measured: 247 bytes/row × ~20.6M rows/day).
  Append-only by design — the rows are evidence for NOTES-7's
  derivation, not durable history. Add a daily DELETE for snapshots > 14
  days old once NOTES-7 ships. ~6 weeks of disk runway, so not urgent
  until then.

### Independent of the redesign

- **NOTES-22 Fix `reload_gtfs_complete.py` and put GTFS reload on a
  schedule.** Found while wiring up Phase A2: the script crashes mid-flow
  on FK violations, and even when it works it isn't scheduled, so our
  GTFS snapshot was 6 months stale. CLAUDE.md's claim that the script
  "correctly invalidates the prior snapshot" is wrong — fix the script,
  add a smoke test, then schedule it.

---

## NOTES-5. Add per-run schedule-deviation chart to the dashboard

**Severity: low (enhancement). Blocked on NOTES-7 `stop_events` and
NOTES-8 `runs`; once those exist the chart is a thin API + frontend
wrapper.**

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

### Blockers / dependencies

1. Run-level materialized table needs to land first (currently only an
   exploratory CSV exists). The chart needs per-stop deviation, which is
   not in `route_metrics_daily`.
2. API endpoint to expose one run's stop deviations:
   `/api/runs/{run_id}/deviations` returning `[{stop_sequence, stop_id,
   stop_name, scheduled, actual, deviation_sec}]`.
3. Frontend route — could live on `RouteDetail` as a "recent runs" list
   that links into a per-run drill-down page.

### Open product questions

- Default selection: today's runs? last completed run? worst-deviation run?
- Should the chart show a single run, or overlay multiple runs of the
  same trip across days to make patterns visible?
- Tooltip needs to show the actual stop name and timestamps, not just
  numbers — useful for spotting where buses always lose time.

---

## NOTES-7. Materialize `stop_events`

**Severity: high (foundational). Depends on TripUpdates collection
(PRs #29, #30) having a week of data.**

Replaces the current "re-derive arrivals from positions every nightly
batch" model. One row per observed arrival at a stop, with `source`
column allowing both derivation paths to coexist.

### Schema sketch

```
stop_events
  trip_id, vehicle_id, service_date    -- run key
  stop_id, stop_sequence               -- stop on the trip
  scheduled_arrival_ts                 -- from GTFS
  observed_arrival_ts                  -- inferred or predicted
  deviation_sec                        -- observed - scheduled
  source                               -- 'trip_update' | 'proximity'
  match_distance_m                     -- nullable, proximity source only
  was_skipped                          -- from TripUpdates SCHEDULED='SKIPPED'
```

### Derivation paths

- **`source='trip_update'`**: from `trip_update_snapshots` raw rows
  (PRs #29, #30). For each (trip_id, stop_id) observed across snapshots,
  the last seen `predicted_arrival_ts` before the row stops appearing =
  inferred actual arrival. Uncertainty bounded by polling interval (30s).
- **`source='proximity'`**: lift the existing logic from
  `src/analytics.py:calculate_line_level_otp()` (vehicle position
  within 50m of a scheduled stop) into the pipeline, materialize once.

### Comparison study after a week

For (trip_id, stop_id) pairs observed by both sources, measure:
agreement rate (both within 60s), median absolute difference, which
correlates better with GTFS scheduled time. Calibrates which to trust
as primary.

---

## NOTES-8. `runs` table as aggregation over `stop_events`

**Severity: medium. Depends on NOTES-7.**

Trivial aggregation per (trip_id, vehicle_id, service_date). No
materialized `is_complete` flag — each downstream metric applies the
filter it needs at query time:

- `RUN_HAS_ENDPOINTS`: `first_obs_seq ≤ 3 AND last_obs_seq ≥ stops_scheduled - 3`
- `RUN_FULLY_OBSERVED`: coverage ≥ 70% AND endpoints AND `max_gap_sec < 300`
- `RUN_EXISTED`: ≥ 3 stop_events

Lift `compute_schedule_anchor()` from `analysis/run_quality.py` into
the pipeline — the post-midnight rollover fix is load-bearing.

---

## NOTES-10. OTP split: origin vs. destination

**Severity: low.**

Add `otp_origin_pct` and `otp_destination_pct` alongside the existing
all-timepoints OTP. Origin lateness = dispatch problem. Destination
lateness = run-time / traffic problem. Mid-route lateness with
on-time destination = recovery in action. Three different operational
stories that the current single number conflates.

Implementation: filter `stop_events` to `stop_sequence == 1` and
`stop_sequence == max(stop_sequence)` per trip.

---

## NOTES-11. Service-delivered ratio

**Severity: medium-high (single most rider-felt failure mode we
currently can't see).**

Per route per date: `delivered_runs / scheduled_runs`. Scheduled count
comes from GTFS `calendar` + `calendar_dates` joined to `trips`.
Delivered count = distinct runs in `stop_events` with at least 3
observed stops (or whatever we settle on for `RUN_EXISTED`).

The MBTA's "76% of timepoints met standard" stat exists *because* they
measure scheduled vs. delivered. We don't.

---

## NOTES-12. End-to-end excess trip time

**Severity: medium.**

Per route per date: median actual trip duration, p95, and % of runs
where actual > 110% of scheduled. Computed from `runs` (which knows
first/last observed stop_event). Apply `RUN_HAS_ENDPOINTS` filter.
Captures dwell + in-vehicle delay; the metric MBTA OPMI is rolling
out for buses.

---

## NOTES-13. Bunching count

**Severity: low-medium.**

Count + rate of headways < 0.5 × scheduled headway, per (route, date,
time_period). Complements existing headway CV (which hides bunching
in averages). Likely lives in a new `route_headway_metrics` table
keyed by (route_id, date, time_period) since this is a stop-hour
roll-up.

---

## NOTES-14. Stop-skip rate

**Severity: medium (unique value-add from the TripUpdates feed
(PRs #29, #30) — not derivable from positions at all).**

Direct from TripUpdates `SKIPPED` stop_time_updates. Probe found 13.5%
of STUs flagged SKIPPED — significant, and operationally important
(skipped stops disproportionately hurt riders at low-frequency stops).
Per route per day, per stop. Could expose worst-skipped stops on
RouteDetail.

---

## NOTES-15. EWT (Excess Wait Time) for frequent routes

**Severity: medium-high. EWT is the standard rider-experience metric
for frequent service (TfL's flagship; also adopted by MBTA OPMI and
NYC MTA), and is the dashboard's answer to rider experience on
frequent routes — superseding any "headway+3min" OTP variant.**

For each (route, stop, hour) where the route is frequent (scheduled
headway ≤ 15 min):
- AWT = `mean(actual_headway²) / (2 · mean(actual_headway))`
- SWT = same formula on scheduled headways
- EWT = AWT − SWT (in seconds, rider-weighted)

Aggregate to (route, date, time_period). EWT in minutes is more
intuitive than headway CV for non-experts and weights bunching pain
correctly.

The `is_frequent` flag comes from the `route_service_profile` table
(PR #37) — derived from the GTFS schedule itself (mean scheduled
headway ≤ 15 min for that hour-of-day), **not from a hardcoded route
list**. WMATA's published "headway-based" designation (70, 79, X2,
90, 92, 16Y, Metroway) is operational policy and isn't encoded in
GTFS via `frequencies.txt`, so we don't trust it; we let the schedule
data classify routes itself.

This is also the reason the dashboard applies WMATA's −2/+7
schedule-based window uniformly to all routes (no special headway
OTP rule). Frequent-route OTP will look harsher than WMATA's
published frequent-route number; that's intentional, and EWT
provides the meaningful rider-experience comparison.

---

## NOTES-17. API + UI surface

**Severity: low (last step, depends on data layer).**

Each new metric needs an API field and a UI element. Probably one PR
per metric or grouped 2-3 at a time, since UI changes require manual
testing in the browser. RouteList scorecard and RouteDetail drill-down
both get updated.

---

## NOTES-18. Grading rubric refresh

**Severity: low.**

Current grade (A–F) is OTP-only, computed in `api/aggregations.py`.
With service-delivered and EWT landing, the rubric should incorporate
them — service-delivered especially, since that's the most
rider-felt failure mode. Worth a separate decision conversation
about weighting before implementing.

---

## NOTES-19. Drop `route_metrics_daily` / `route_metrics_summary`

**Severity: low (cleanup, after the new metrics fully replace them).**

Both tables and the daily batch pipeline that populates them
(`pipelines/compute_daily_metrics.py`) become dead code once the new
stop_events-based pipeline covers all current API consumers. Coexist
for now to avoid UI breakage during the transition. Track as one
final cleanup PR after NOTES-17 lands.

---

## NOTES-20. Tighter rider-experience OTP

**Severity: low (deferred).**

User considers WMATA's −2 / +7 window lax but wants comparability with
WMATA's published scorecard for now. Future option: expose a stricter
"rider-experience OTP" alongside the official one (e.g., −60s / +180s)
for non-frequent routes (frequent routes get EWT per NOTES-15 instead).
The constants live in `src/otp_constants.py`, so this is a one-line
change — could even be a query-parameter toggle on the API.

---

## NOTES-21. Retention job for `trip_update_snapshots`

**Severity: low until NOTES-7 ships, then medium (becomes urgent ~6
weeks after collection starts).**

### The problem

`trip_update_snapshots` is intentionally an append-only evidence trail,
not durable history. Measured storage as of 2026-05-03 (first snapshots):
**247 bytes/row including indexes**, ~7,150 rows per 30s tick →
**~20.6M rows/day, ~4.74 GB/day, ~33 GB/week**. With ~204 GB free on
the user's laptop, that's about 6 weeks of runway before disk pressure.
On a future cloud VM with smaller disks the runway shrinks further.

### Why it's not urgent yet

The whole point of NOTES-7 is to derive one compact `stop_event` row
per actual arrival from the trail of raw observations that supported
the derivation. Once NOTES-7 is producing stop_events reliably, the
underlying raw rows for any (trip_id, stop_id) pair that's been derived
can be dropped. After derivation the steady state is ~50-80k
stop_events per day, comparable to vehicle_positions — manageable
indefinitely.

So: keep collecting raw, ship NOTES-7, then add retention.

### Implementation

1. Daily cron (or pipeline step appended to whatever derives stop_events):
   ```sql
   DELETE FROM trip_update_snapshots
    WHERE snapshot_ts < now() - interval '14 days';
   ```
   14 days gives a comfortable window to re-derive if NOTES-7 has a bug
   that requires reprocessing.
2. After the first run, `VACUUM` (not `VACUUM FULL` — the table is
   high-churn, regular vacuum keeps bloat in check without locks).
3. If the table is still getting unwieldy on disk, switch to native
   Postgres partitioning by `snapshot_ts` (one partition per day).
   Retention then becomes `DROP PARTITION` — instant and lock-light,
   vs. a long DELETE on a multi-GB table. Only worth the complexity
   if (a) we keep the table for months, or (b) the daily DELETE
   becomes slow enough to interfere with collection writes.

### Dependencies

- Blocked on **NOTES-7 `stop_events`** landing first. Adding retention
  before stop_events would silently throw away derivation evidence.
- Independent of NOTES-8 through NOTES-20.

---

## NOTES-22. Fix `reload_gtfs_complete.py` and put GTFS reload on a schedule

**Severity: high — silently corrupts metrics. Discovered while wiring up
the Phase A2 proximity derivation: our GTFS snapshot was 6 months stale
(2025-10-28), and almost every RT trip_id resolved to a different route
in current GTFS (or to no trip at all).**

### Two problems, one root cause

1. **The reload script crashes mid-flow.** `scripts/reload_gtfs_complete.py`
   does `db.execute(text("DELETE FROM agencies"))` (line 288) without
   first invalidating the FK from `routes.agency_id`, so it raises
   `psycopg2.errors.ForeignKeyViolation` on any populated DB. CLAUDE.md's
   claim that the script "correctly invalidates the prior snapshot" is
   wrong — it invalidates routes/stops/trips/stop_times/calendar via
   UPDATE (lines 146–162), but for agencies/feed_info/timepoints/
   timepoint_times/route_service_profile it does a plain DELETE. The
   per-table commits before the failure mean partial migrations stick:
   running the script today left the DB with snapshot-5 routes/trips/
   stops/stop_times marked current, but agencies still on snapshot 4.

2. **There is no automated reload.** The script is invoked manually,
   and nobody invoked it for 6 months. By the time we noticed, every
   downstream metric was being computed against a schedule WMATA had
   long since revised. The OTP numbers in `route_metrics_daily` for
   any recent date are likely junk for routes whose trip_id space
   churned.

### Fix

1. **Make the script transactional or idempotent end-to-end.** Either
   wrap the whole reload in a single `BEGIN`/`COMMIT` so a partial
   failure rolls back, or rewrite each table's reload as
   "invalidate-then-insert-new-snapshot" (matching the pattern that
   already works for routes/stops/trips). The DELETE-then-INSERT
   pattern for agencies/feed_info/timepoints can't survive FKs; the
   versioned pattern can.
2. **Add a smoke test** that reloads against an empty DB and against a
   populated DB and asserts both succeed.
3. **Schedule it.** A daily or weekly cron / GitHub Action invoking
   `reload_gtfs_complete.py`, with alerting on failure. Frequency is
   a judgment call — WMATA revises GTFS roughly quarterly, but
   real-time operational schedule changes (added trips, suspended
   routes) only land in TripUpdates / VehiclePositions, not in static
   GTFS, so daily is overkill.
4. **Surface staleness in the dashboard.** The newest GTFS snapshot
   date should appear somewhere visible (a footer line on RouteList?)
   so a stale schedule is observable instead of silent.

### Scope decision

Fold the script fix into NOTES-22's PR. Cron / scheduling and dashboard
freshness indicator are separate items — file as follow-on once the
script is reliable.

### Dependencies

- Independent of NOTES-7 through NOTES-21. Can land any time.
- Side note: the 6-month-stale GTFS also means
  `route_metrics_daily` numbers for the last several months may be
  unreliable. We're slated to drop that table anyway (NOTES-19), so
  not worth backfilling. Worth flagging to anyone reviewing historical
  trend numbers in the meantime.

