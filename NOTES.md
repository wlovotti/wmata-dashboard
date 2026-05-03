# Code Review Notes

Findings from a code review on 2026-05-02 plus the metrics-redesign plan
captured 2026-05-03. Each item was verified against the code and database
directly, not inferred from CLAUDE.md.

---

## Active priorities (updated 2026-05-03)

The bulk of open work is a metrics redesign anchored on materialized
**stop events** as the foundational unit, replacing the daily-batch
recomputation from raw positions. Sequencing matters — the early phases
gate the later ones.

### P0 — Foundation (gates the rest)

- **#6 TripUpdates collection.** WMATA publishes a GTFS-RT TripUpdates feed
  we currently don't consume. Snapshot probes (`scripts/probe_trip_updates.py`,
  `scripts/probe_trip_updates_timeseries.py`) confirm 84.7% of stops carry
  predicted arrival times; 13.5% are explicitly `SKIPPED`. The "last
  prediction before the stop disappears from the feed" is the mechanism
  for deriving WMATA's own claimed actual arrival times. This must land
  first, then run for ~7 days to gather data before deriving stop_events.
- **#7 `stop_events` table.** One row per (trip_id, stop_id, observed
  arrival), with `source` column (`'trip_update'` | `'proximity'`). Both
  sources can coexist — they're independent observations. Replaces the
  current re-derive-on-every-batch model.
- **#8 `runs` aggregation.** Trivial roll-up over `stop_events`. No single
  `is_complete` flag — each metric applies its own filter at query time.

### P1 — Quick wins on the new foundation (small, no new tables)

- **#10 OTP at origin / destination split.** Already have stop-level data
  — just filter to first/last stop_sequence per trip. Distinguishes
  dispatch problems from run-time problems from recovery problems.

### P2 — Medium-effort metric additions

- **#11 Service-delivered ratio.** % of scheduled trips that actually
  ran. Most rider-felt failures are missing buses, not late ones, and
  we currently can't see this at all. Needs a schedule-side count from
  GTFS calendar/calendar_dates joined to trips, denominator-style.
- **#12 End-to-end excess trip time.** From `runs`: median, p95, % of
  runs with actual > 110% of scheduled. Captures dwell + in-vehicle
  delay, not just wait. The metric MBTA OPMI is rolling out for buses.
- **#13 Bunching count.** Count and rate of headways < 0.5 × scheduled.
  Complements existing CV metric (which hides bunching in averages).
- **#14 Stop-skip rate.** Direct from TripUpdates `SKIPPED`
  stop_time_updates — data we can't derive from positions at all. Per
  route, per day, per stop. Unique value-add from the TripUpdates feed.
- **#15 Excess Wait Time (EWT) for frequent routes.** AWT =
  `mean(h²) / (2·mean(h))` from observed headways at each stop-hour;
  SWT same for scheduled. EWT = AWT − SWT, aggregated to (route, date,
  time_period) where the route is frequent (scheduled headway ≤ 15 min).
  TfL's standard rider-experience metric.

### P3 — Reference data

- **#16 Service profile (span / frequency).** New `route_service_profile`
  table populated from GTFS only when schedule reloads. Columns:
  `route_id`, `day_type`, `hour`, `scheduled_trips`, `mean_headway_min`,
  `is_frequent`. Useful as denominator for #11 and as the source of the
  frequent-route flag for #15.

### P4 — Surface to API + UI

- **#17 New API fields and panels on `RouteDetail`.** Each new metric
  needs to land somewhere in the UI; sequencing here can lag the data
  layer.
- **#18 Update grading rubric.** Currently OTP-only; should incorporate
  service-delivered and EWT once those land.
- **#5 Per-run deviation chart.** Becomes feasible once `stop_events`
  and `runs` exist.

### P5 — Cleanup

- **#19 Drop `route_metrics_daily` and `route_metrics_summary`.** Once
  the new metrics fully replace them. Coexist for now to avoid UI
  breakage during the transition.
- **#20 Tighter rider-experience OTP.** A stricter window alongside
  WMATA's official. Tracked but not yet scoped — user wants
  comparability with WMATA's scorecard for now.
- **#21 Retention job for `trip_update_snapshots`.** Raw feed table grows
  ~5 GB/day (measured: 247 bytes/row × ~20.6M rows/day). Append-only by
  design — the rows are evidence for #7's derivation, not durable history.
  Add a daily DELETE for snapshots > 14 days old once #7 ships. ~6 weeks
  of disk runway, so not urgent until then.

### Independent of the redesign

- **#4 Python 3.9 → 3.11/3.12.** Small isolated PR, can land anytime.
- **#22 Direction-aware `find_reference_stop`.** Existing analytics
  function picks a reference stop without filtering to one direction
  when callers pass `direction_id=None`. On routes whose terminus stops
  serve both directions under one `stop_id` (most WMATA routes — D80
  Friendship Heights and Union Station each see all 268 daily trips),
  this auto-selects a terminus and produces ~2x-too-tight headways. Bug
  shape verified during #16 work. Independent fix; doesn't depend on
  the metrics redesign.

---

## 4. Bump Python from 3.9 to 3.11 or 3.12 — OPEN

**Severity: low (maintenance, not blocking). Independent of the metrics
redesign — can land anytime.**

### Evidence

- `pyproject.toml:6` pins `requires-python = ">=3.9"`.
- `pyproject.toml:43` pins `target-version = "py39"` for ruff.
- `.venv` runs Python 3.9.6.
- Python 3.9 reached end of life on 2025-10-31; VS Code's Jupyter
  extension surfaces a "no longer supported" warning when loading the
  kernel. Nothing breaks today, but no further security patches upstream.

### Fix

1. Pick a target — 3.11 or 3.12 are both safe; 3.13 is fine if you want
   the latest. None of the current deps (sqlalchemy 2, pandas 2,
   fastapi, gtfs-realtime-bindings, psycopg/psycopg2, jupyter) require
   anything older.
2. Update `requires-python` in `pyproject.toml`.
3. Update `target-version` in the ruff config (`py311` / `py312`).
4. `uv sync --extra postgres --extra viz --extra dev` to rebuild the
   venv against the new interpreter (uv will fetch it if not installed).
5. `uv run pytest -m smoke` and a one-off `uv run python -c "import api.main"` to confirm imports cleanly.
6. CI: check `.github/workflows/` for any `python-version: '3.9'` pins
   and bump them to match.

Not coupled to any other work — can be done in a 5-minute PR whenever.

---

## 5. Add per-run schedule-deviation chart to the dashboard — OPEN

**Severity: low (enhancement). Now blocked specifically on #7 `stop_events`
and #8 `runs`; once those exist the chart is a thin API + frontend wrapper.**

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

## 6. Collect WMATA TripUpdates feed — OPEN, P0

**Severity: high (foundational for the metrics redesign).**

WMATA publishes a GTFS-RT TripUpdates feed at
`https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb` that we currently
don't consume. It contains forward-looking predicted arrival timestamps
per (trip_id, stop_id) — the agency's own claim about when each bus will
hit each stop, refined as buses move.

### What probes confirmed (2026-05-03)

- 777 trip_updates per snapshot, 8,640 stop_time_updates, 70 routes.
- 84.7% of STUs have `arrival.time` populated.
- 13.5% are explicitly `SKIPPED` — data we cannot derive from positions
  at all (becomes #14 stop-skip rate).
- 40% of trip_updates carry `vehicle.id`; the other 60% are pure
  schedule predictions for trips not yet picked up by a bus.
- Median prediction volatility across snapshots = 8s (predictions are
  mostly stable but updated as buses progress).
- Stops drop out of the feed once buses pass them. The last predicted
  `arrival.time` before a stop disappears ≈ WMATA's inferred actual
  arrival. This is the mechanism for deriving stop_events.

### Implementation

1. New table `trip_update_snapshots`: append-only raw rows
   `(trip_id, stop_id, stop_sequence, snapshot_ts, predicted_arrival_ts,
   schedule_relationship, vehicle_id)`.
2. New `get_realtime_trip_updates()` method on
   `src/wmata_collector.py` mirroring the existing positions method.
3. New `scripts/continuous_trip_updates_collector.py` polling at 30s
   (vs. positions at 60s — TripUpdates needs faster polling because
   actuals are inferred from gap-to-dropoff).
4. API budget: 60s positions + 30s trip_updates = 4,320 calls/day,
   well under WMATA's 50,000/day limit.
5. Run for ~7 days before deriving stop_events from the data, to confirm
   the dropoff mechanism works at scale and to pick the right
   bus-passed-the-stop heuristic.

### Probe scripts

`scripts/probe_trip_updates.py` (single snapshot summary) and
`scripts/probe_trip_updates_timeseries.py` (multi-snapshot dropoff
analysis) are kept as ongoing diagnostics — useful for spot-checking
feed health.

---

## 7. Materialize `stop_events` — OPEN, P0

**Severity: high (foundational). Depends on #6 having a week of data.**

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

- **`source='trip_update'`**: from #6 raw snapshots. For each
  (trip_id, stop_id) observed across snapshots, the last seen
  `predicted_arrival_ts` before the row stops appearing = inferred
  actual arrival. Uncertainty bounded by polling interval (30s).
- **`source='proximity'`**: lift the existing logic from
  `src/analytics.py:calculate_line_level_otp()` (vehicle position
  within 50m of a scheduled stop) into the pipeline, materialize once.

### Comparison study after a week

For (trip_id, stop_id) pairs observed by both sources, measure:
agreement rate (both within 60s), median absolute difference, which
correlates better with GTFS scheduled time. Calibrates which to trust
as primary.

---

## 8. `runs` table as aggregation over `stop_events` — OPEN, P0

**Severity: medium. Depends on #7.**

Trivial aggregation per (trip_id, vehicle_id, service_date). No
materialized `is_complete` flag — each downstream metric applies the
filter it needs at query time:

- `RUN_HAS_ENDPOINTS`: `first_obs_seq ≤ 3 AND last_obs_seq ≥ stops_scheduled - 3`
- `RUN_FULLY_OBSERVED`: coverage ≥ 70% AND endpoints AND `max_gap_sec < 300`
- `RUN_EXISTED`: ≥ 3 stop_events

Lift `compute_schedule_anchor()` from `analysis/run_quality.py` into
the pipeline — the post-midnight rollover fix is load-bearing.

---

## 10. OTP split: origin vs. destination — OPEN, P1

**Severity: low.**

Add `otp_origin_pct` and `otp_destination_pct` alongside the existing
all-timepoints OTP. Origin lateness = dispatch problem. Destination
lateness = run-time / traffic problem. Mid-route lateness with
on-time destination = recovery in action. Three different operational
stories that the current single number conflates.

Implementation: filter `stop_events` to `stop_sequence == 1` and
`stop_sequence == max(stop_sequence)` per trip.

---

## 11. Service-delivered ratio — OPEN, P2

**Severity: medium-high (single most rider-felt failure mode we
currently can't see).**

Per route per date: `delivered_runs / scheduled_runs`. Scheduled count
comes from GTFS `calendar` + `calendar_dates` joined to `trips`.
Delivered count = distinct runs in `stop_events` with at least 3
observed stops (or whatever we settle on for `RUN_EXISTED`).

The MBTA's "76% of timepoints met standard" stat exists *because* they
measure scheduled vs. delivered. We don't.

---

## 12. End-to-end excess trip time — OPEN, P2

**Severity: medium.**

Per route per date: median actual trip duration, p95, and % of runs
where actual > 110% of scheduled. Computed from `runs` (which knows
first/last observed stop_event). Apply `RUN_HAS_ENDPOINTS` filter.
Captures dwell + in-vehicle delay; the metric MBTA OPMI is rolling
out for buses.

---

## 13. Bunching count — OPEN, P2

**Severity: low-medium.**

Count + rate of headways < 0.5 × scheduled headway, per (route, date,
time_period). Complements existing headway CV (which hides bunching
in averages). Likely lives in a new `route_headway_metrics` table
keyed by (route_id, date, time_period) since this is a stop-hour
roll-up.

---

## 14. Stop-skip rate — OPEN, P2

**Severity: medium (unique value-add from #6 — not derivable from
positions at all).**

Direct from TripUpdates `SKIPPED` stop_time_updates. Probe found 13.5%
of STUs flagged SKIPPED — significant, and operationally important
(skipped stops disproportionately hurt riders at low-frequency stops).
Per route per day, per stop. Could expose worst-skipped stops on
RouteDetail.

---

## 15. EWT (Excess Wait Time) for frequent routes — OPEN, P2

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

The `is_frequent` flag comes from #16 — derived from the GTFS
schedule itself (mean scheduled headway ≤ 15 min for that hour-of-day),
**not from a hardcoded route list**. WMATA's published "headway-based"
designation (70, 79, X2, 90, 92, 16Y, Metroway) is operational
policy and isn't encoded in GTFS via `frequencies.txt`, so we don't
trust it; we let the schedule data classify routes itself.

This is also the reason the dashboard applies WMATA's −2/+7
schedule-based window uniformly to all routes (no special headway
OTP rule). Frequent-route OTP will look harsher than WMATA's
published frequent-route number; that's intentional, and EWT
provides the meaningful rider-experience comparison.

---

## 16. Service profile table — OPEN, P3

**Severity: low (reference data, supports #11 and #15).**

New `route_service_profile` table populated from GTFS only when the
schedule reloads (so populated by `scripts/reload_gtfs_complete.py`).
Columns:
- `route_id`, `day_type` (weekday/Saturday/Sunday), `hour`
- `scheduled_trips` (count for that hour)
- `mean_headway_min`
- `is_frequent` (mean_headway_min ≤ 15)

Static-ish reference data; cheap to compute.

---

## 17. API + UI surface — OPEN, P4

**Severity: low (last step, depends on data layer).**

Each new metric needs an API field and a UI element. Probably one PR
per metric or grouped 2-3 at a time, since UI changes require manual
testing in the browser. RouteList scorecard and RouteDetail drill-down
both get updated.

---

## 18. Grading rubric refresh — OPEN, P4

**Severity: low.**

Current grade (A–F) is OTP-only, computed in `api/aggregations.py`.
With service-delivered and EWT landing, the rubric should incorporate
them — service-delivered especially, since that's the most
rider-felt failure mode. Worth a separate decision conversation
about weighting before implementing.

---

## 19. Drop `route_metrics_daily` / `route_metrics_summary` — OPEN, P5

**Severity: low (cleanup, after the new metrics fully replace them).**

Both tables and the daily batch pipeline that populates them
(`pipelines/compute_daily_metrics.py`) become dead code once the new
stop_events-based pipeline covers all current API consumers. Coexist
for now to avoid UI breakage during the transition. Track as one
final cleanup PR after #17 lands.

---

## 20. Tighter rider-experience OTP — OPEN, P5

**Severity: low (deferred).**

User considers WMATA's −2 / +7 window lax but wants comparability with
WMATA's published scorecard for now. Future option: expose a stricter
"rider-experience OTP" alongside the official one (e.g., −60s / +180s)
for non-frequent routes (frequent routes get EWT per #15 instead).
The constants live in `src/otp_constants.py`, so this is a one-line
change — could even be a query-parameter toggle on the API.

---

## 21. Retention job for `trip_update_snapshots` — OPEN, P5

**Severity: low until #7 ships, then medium (becomes urgent ~6 weeks
after collection starts).**

### The problem

`trip_update_snapshots` is intentionally an append-only evidence trail,
not durable history. Measured storage as of 2026-05-03 (first snapshots):
**247 bytes/row including indexes**, ~7,150 rows per 30s tick →
**~20.6M rows/day, ~4.74 GB/day, ~33 GB/week**. With ~204 GB free on
the user's laptop, that's about 6 weeks of runway before disk pressure.
On a future cloud VM with smaller disks the runway shrinks further.

### Why it's not urgent yet

The whole point of #7 is to derive one compact `stop_event` row per
actual arrival from the trail of raw observations that supported the
derivation. Once #7 is producing stop_events reliably, the underlying
raw rows for any (trip_id, stop_id) pair that's been derived can be
dropped. After derivation the steady state is ~50-80k stop_events per
day, comparable to vehicle_positions — manageable indefinitely.

So: keep collecting raw, ship #7, then add retention.

### Implementation

1. Daily cron (or pipeline step appended to whatever derives stop_events):
   ```sql
   DELETE FROM trip_update_snapshots
    WHERE snapshot_ts < now() - interval '14 days';
   ```
   14 days gives a comfortable window to re-derive if #7 has a bug
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

- Blocked on **#7 `stop_events`** landing first. Adding retention
  before stop_events would silently throw away derivation evidence.
- Independent of #8-#20.

---

## 22. Direction-aware `find_reference_stop` — OPEN

**Severity: medium (corrupts current headway numbers on affected routes).
Independent of the metrics redesign.**

### The problem

`src/analytics.py:326 find_reference_stop()` picks a reference stop by
counting trips per `stop_id` across the route's current trips. When the
caller passes `direction_id=None`, both directions are pooled. For most
WMATA routes the auto-selected stop is then a terminus / layover bay
that serves all trips in both directions under one `stop_id` (e.g., D80
Friendship Heights and Union Station each carry 268 = 134 dir-0 + 134
dir-1 daily trips). The downstream `calculate_headways()` then computes
gaps between consecutive arrivals at that bidirectional stop, producing
roughly 2x-too-tight headway numbers.

The function further sorts "common stops" by mean `stop_sequence`, but
sequence numbers in dir-0 and dir-1 are different orderings of different
stops — averaging them across directions is meaningless.

### Evidence

- Same bug shape as the one fixed in `src/service_profile.py` during
  #16 work (PR #37). Verified by inspecting D80 stop counts in the live
  DB — termini have all 268 trips, mid-route stops have 134.
- CLAUDE.md "Non-obvious gotchas" now documents the rule: per-route stop
  aggregations must group by `(route_id, direction_id, stop_id)`, never
  `(route_id, stop_id)` alone.

### Fix

1. Make `direction_id` a required parameter on `find_reference_stop` (or
   require callers to loop over directions and pass each explicitly).
2. Filter candidate stops to those served by exactly one `direction_id`
   on this route — `HAVING COUNT(DISTINCT t.direction_id) = 1`.
3. Audit callers (`calculate_headways`, `calculate_headways_batch`,
   anywhere reference stops are chosen) to ensure direction is plumbed.
4. Re-run a few representative routes' headways before/after to quantify
   the shift. Expect roughly 2x looser numbers on routes whose previous
   reference stop was a terminus.
5. Decide whether `route_metrics_daily` / `route_metrics_summary` need
   to be backfilled with corrected numbers, or accept that those tables
   are slated for replacement (#19) and don't bother.

### Dependencies

- Independent of #6-#21. Can land any time.
- Will probably shift `headway_*` columns in `route_metrics_daily` /
  `route_metrics_summary`, so worth flagging in the PR description.
