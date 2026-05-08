# Code Review Notes

Forward-looking punch list. Completed items are removed in the same
PR that closes them — see git log and PR descriptions for history.
Item numbers (`NOTES-N`) are stable; new items take the next number.
NOTES.md edits ride on substantive PRs; standalone reconciliation PRs
are churn.

Last edited 2026-05-08 (deferred NOTES-38 — server-side period-over-period deltas need ≥14 days of stop_events/runs data before they're interpretable; closed PR #81 documents the implementation for re-use).

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

### Operations-manager redesign

The dashboard today is observational and route-anchored. For an ops
GM trying to maximize performance under a fixed budget, the gaps are:
no trend / period-over-period framing (so "are we improving?" is
unanswered); no Pareto / contribution view (so attention isn't
directed); no drill-down to the *where* (stop) or *what* (block,
vehicle); and existing dead-code metrics that should be wired
through. The 12 items below close those gaps. Public/rider-facing
surface deferred; operator/dispatcher attribution out of scope
without internal WMATA feeds (vehicle_id and block_id used as
proxies instead).

**Trend & comparison (the "are we improving?" question)**

- **NOTES-38 Period-over-period deltas on every KPI** *(deferred — needs ≥14 days of data; closed PR #81)*. Augment the
  scorecard payload with deltas; add up/down/flat indicators
  throughout.
- **NOTES-47 Per-route targets / commitments config.** Configurable
  per-route targets so trend cards can show "vs target," not only
  "vs prior period."

**Diagnosis & Pareto (the "what's dragging us down?" question)**

- **NOTES-40 Stop-level diagnostic endpoint + UI.** Per-stop OTP,
  EWT, skip rate along the route's stop sequence — surfaces *where*
  trips slip.
- **NOTES-41 Day-type / time-period filter on RouteDetail.**
  Surface the day-type dimension that already exists in compute.
- **NOTES-42 Bunching cause decomposition.** Split bunching rate
  into leader-late vs trailer-early vs both — targets dispatch fixes
  vs running-time fixes.

**Decision support & operator-side proxies**

- **NOTES-44 Marginal-bus EWT model.** Per (route, period) ranking
  of where adding one trip would most reduce EWT.
- **NOTES-45 Block-level cascade view.** Surface `block_id` and
  visualize a vehicle's chained trips — identifies cascade lateness vs
  incidental misses.
- **NOTES-46 Vehicle performance leaderboard.** Aggregate per-`vehicle_id`
  median deviation / p95 / trip count over 30 days as a maintenance/age
  proxy (not an operator-blame view).

### P4 — Surface to API + UI

- **NOTES-18 Update grading rubric.** Currently OTP-only; should
  incorporate service-delivered and EWT now that both have shipped.

### P5 — Cleanup

- **NOTES-19 Drop `route_metrics_daily` and `route_metrics_summary`.**
  Once the new metrics fully replace them. Coexist for now to avoid UI
  breakage during the transition.
- **NOTES-20 Tighter rider-experience OTP.** A stricter window alongside
  WMATA's official. Tracked but not yet scoped — user wants
  comparability with WMATA's scorecard for now.

### Independent of the redesign

- **NOTES-34 service_delivered ceiling on 2-stop routes (TU structural
  exclusion).** Side effect of the NOTES-30 closing PR (proportional
  threshold). The new threshold is `max(2, stops_observable // 3)`; on a
  2-stop route, TU rows have `stops_observable = 1` and can never reach
  2, so TU never counts toward delivered. Proximity rows
  (`stops_observable = 2`) cover the gap when they observe both stops,
  but A90 weekday on 2026-05-05 came out at 61/127 delivered (48%)
  despite 88% OTP — the residual gap is partly proximity rows that only
  saw one stop and partly TU runs for trips with no proximity coverage.
  Acceptable trade-off for closing the 0%-everywhere bug, but documents
  a known ceiling on short-route delivered ratios. A more permissive
  short-route rule — e.g. accept `stops_observed >= 1` when
  `stops_observable <= 2`, or treat any observation at all as
  delivered once `stops_observable` is small enough — would lift the
  ceiling at the cost of admitting more single-ping ghost runs. Not
  urgent; revisit if a second short express route appears and the
  ~50% ceiling becomes a problem.

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

## NOTES-38. Period-over-period deltas on every KPI

**Severity: low (deferred — needs ≥14 days of data).**

Augment the scorecard payload from `/api/routes` (built in
`api/aggregations.py`) so every metric carries a 7-day-vs-prior-7-day
delta. Render up/down/flat indicators on the `RouteList` table and the
`RouteDetail` KPI cards. The RouteDetail OTP / service-delivered cards
already carry deltas client-side from the 30-day trend payload (PR #77);
this item generalizes the pattern to every KPI on every surface, with
the delta computed server-side so RouteList can show them too. Pay
attention to thin-data cases — if either window is below the EWT
coverage threshold, the delta should suppress rather than show a
misleading number.

**Deferred** (closed PR #81): the 7-vs-prior-7 windows require 14 days
of stop_events / runs data before deltas survive thin-data suppression
on most routes. Production data currently starts 2026-05-02; revisit
once the collector has accumulated ≥14 days of continuous data so the
feature is interpretable rather than "mostly suppressed." The closed
PR's commits remain retrievable via `gh pr diff 81` for re-use.

---

## NOTES-40. Stop-level diagnostic endpoint and UI

**Severity: low.**

The `stop_events` table has all the data needed for per-stop OTP, EWT,
skip rate, and median deviation, but no API endpoint exposes it. Add
`GET /api/routes/{route_id}/stops` returning per-(direction_id,
stop_id) metrics over a configurable window. Group strictly by
`(route_id, direction_id, stop_id)` per the CLAUDE.md rule —
otherwise termini and shared bays double-count and the metrics look
~2x too tight.

UI: render as a strip chart along the route's stop sequence on
`RouteDetail` — a horizontal heatmap from origin to destination,
colored by metric value. This is the answer to "where on the route do
trips slip?" and likely the single most actionable diagnostic the
dashboard can add.

---

## NOTES-41. Day-type / time-period filter on RouteDetail

**Severity: low.**

Day-type (weekday / Saturday / Sunday) and time-of-day period are
already preserved in computation (`src/ewt.py`, `src/bunching.py`,
`src/service_profile.py`) but the user can't slice by them. Add a
filter on `RouteDetail` that re-slices all KPIs and the trend — e.g.
"weekday AM peak" vs "Saturday evening." Mostly a frontend filter +
endpoint param; the underlying queries already group by these
dimensions.

---

## NOTES-42. Bunching cause decomposition

**Severity: low.**

`src/bunching.py` flags pairs where the observed headway is below the
threshold but doesn't tell us *why*. For each bunched pair, compare
both runs' deviations against schedule: if the leader is late and the
trailer is on-time, it's a recovery failure (running-time problem); if
the leader is on-time and the trailer is early, it's a dispatch
failure (departure-discipline problem); if both are off, it's
compounding. Add the breakdown to the bunching API surface and render
it on `PeriodDrilldown` as a stacked bar. Lets a GM target the right
intervention.

---

## NOTES-44. Marginal-bus EWT model

**Severity: low (modeling).**

Per (route, period), estimate the EWT reduction from adding one
scheduled trip. Closed-form approximation: SWT scales as half the
scheduled headway, so adding a trip in a period with N existing trips
reduces SWT by roughly `period_minutes / (2N(N+1))`; AWT impact
depends on how the new trip lands relative to existing variance.
Render as a ranked "where would the next bus help most" list — the
direct answer to "where should my next dollar go?"

Most ambitious item in this set. Document modeling assumptions
visibly in the UI; the absolute number is less reliable than the
relative ranking.

---

## NOTES-45. Block-level cascade view

**Severity: low.**

A `block_id` chains a vehicle's consecutive trips during a service
day — when one trip falls behind, the next trip on the same block
inherits the lateness. Today `block_id` lives on `Trip` but never
reaches the API. Expose it; add a "block timeline" view (either on
`RouteDetail` or a new `/blocks/:id` route) that strings together all
trips in a block and shows deviation propagation. Identifies
cascade-driven misses (one root cause, four bad trips) vs incidental
ones (four independent misses).

---

## NOTES-46. Vehicle performance leaderboard

**Severity: low.**

Aggregate per-`vehicle_id` median deviation, p95 deviation, and trip
count over the last 30 days. Render as a sortable table. Frame
explicitly as a maintenance / vehicle-age proxy — operators rotate
across vehicles and we have no operator IDs in the public feeds, so
this is *not* an operator-performance view. A persistent
underperformer is more likely an aging vehicle, a garage-assignment
quirk, or a maintenance backlog signal. Suppress vehicles with low
trip counts (e.g. <20) to avoid small-sample noise dominating the
ranking.

---

## NOTES-47. Per-route targets / commitments config

**Severity: low.**

To answer "vs target" rather than only "vs prior period," the system
needs a place to store per-route (and per-system) targets for OTP,
service-delivered, EWT, and bunching. Keep it simple: one number per
(route, metric); null means "use system default"; system default is a
single config row. Storage can be yaml in the repo or a small
`route_targets` table. Surface targets on the system trend cards
(PR #78, RouteList) and the per-route trend cards (PR #77, RouteDetail), and on
the contributors view (PR #80, where contribution is computed
against the system-window baseline today; swap to per-route target
once this item lands). Targets can stay editable by the operator, but
a sensible starting set should be checked in.

---

