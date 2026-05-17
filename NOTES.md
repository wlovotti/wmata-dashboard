# Code Review Notes

Forward-looking punch list. Completed items are removed in the same
PR that closes them — see git log and PR descriptions for history.
Item numbers (`NOTES-N`) are stable; new items take the next number.
NOTES.md edits ride on substantive PRs; standalone reconciliation PRs
are churn.

Last edited 2026-05-17. Closed NOTES-65 by building
`src/upsert_helpers.py:upsert_rows` and migrating all four per-route
pipelines (`derive_stop_events`, `derive_stop_events_trip_updates`,
`aggregate_runs`, `compute_bunching`) to call it. The helper wraps the
`pg_insert(...).on_conflict_do_update(constraint=..., set_=...)` boilerplate
behind a single call that takes the model, constraint name, and update-column
list as arguments. Postgres-only by design; callers guard with `if rows:`
before invoking. No behaviour change; all 462 tests pass.

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
through. The items below close those gaps. Public/rider-facing
surface deferred; operator/dispatcher attribution out of scope
without internal WMATA feeds (vehicle_id and block_id used as
proxies instead).

**Trend & comparison (the "are we improving?" question)**

- **NOTES-38 Period-over-period deltas on every KPI** *(deferred — needs ≥14 days of data; closed PR #81)*. Augment the
  scorecard payload with deltas; add up/down/flat indicators
  throughout.

**Information architecture & navigation**

- **NOTES-54 "What changed" panel on Overview** *(deferred — needs
  NOTES-38 + ≥14d data)*. Week-over-week movers split into
  improvements / degradations.

**Diagnostic outputs (route-level + system-wide)**

A new initiative — the dashboard today surfaces metrics; these items
surface *why* metrics are what they are and *where* operational
intervention has the highest leverage. Outputs target both the user
and a transit-interested public audience (the eventual public-site
goal in NOTES-50). Pure deterministic Python/SQL — no LLM in the
pipeline; the structured artifacts feed dashboard panels and ranked
target lists directly.

- **NOTES-58 RouteDetail per-route diagnosis panel.** Renders the
  route_diagnostic_profile foundation (PR #107) — slip trajectory
  chart (both directions, timepoint markers) + timepoint behavior
  table + LLM-generated diagnosis narrative (generated batch /
  on-demand by the user, persisted; public reads from cache). The
  D80 deep-dive shape, productized.
- **NOTES-59 Cross-route segment diagnostic (V1, stop-pair).**
  Aggregate slip across all routes per `(from_stop, to_stop)`
  segment → ranked infrastructure-investment candidates (TSP /
  queue-jumps / dedicated lanes). Segment-identity matching only;
  no geometric corridor rollup.
- **NOTES-62 Cross-route corridor diagnostic (V2, geometric rollup).**
  Roll NOTES-59's segment-level slip up to corridor / intersection
  level via shape-aware matching, so "the M St NW corridor from
  Wisconsin to Penn Ave" reads as one investment target rather
  than N stop-pairs. The framing that makes the output
  decision-useful for infrastructure planning.
- **NOTES-61 Hold-down policy / dispatching candidates page.**
  Ranked timepoint-leakage table (% of buses departing > N seconds
  early per timepoint per period) → operational fix targets, no
  capital required.

### Code-quality / DRY cleanup

Code-quality follow-ups from a codebase simplification scan
(2026-05-17). Each is a small-to-medium refactor with no
user-visible behavior change; the win is maintainability and
divergence prevention. Independent of each other — sequence to
taste.

- **NOTES-64 Custom `useMultiFetch` hook for the frontend.** Several
  pages hand-roll `useEffect` + `Promise.all` + manual
  error/cancellation handling. A shared hook removes the repetition
  and fixes latent race conditions on fast route switches.

### P5 — Cleanup

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

## NOTES-54. "What changed" panel on Overview

**Severity: low (deferred — needs ≥14 days of data).**

Augments the Overview page (delivered by PR #105) with a panel
showing week-over-week movers: the top routes whose OTP / SD /
EWT / bunching changed most vs the prior 7-day window. Split
into two sub-lists — "Improvements" and "Degradations" — so
positive movement is celebrated alongside negative.

**Deferred** until NOTES-38 (period-over-period deltas on every KPI)
lands. This panel is a thin renderer over that endpoint and has no
useful content without it. NOTES-38 itself is deferred for a
data-window reason: 7-vs-prior-7 needs ≥14 days of stop_events /
runs data before deltas survive thin-data suppression on most
routes. Production data started 2026-05-02; today is day 13 (so
tomorrow is the earliest possible landfall for NOTES-38, then this
item becomes implementable).

### Dependencies

Overview shell delivered by PR #105. NOTES-38 (period-over-period
deltas — deferred).

---

## NOTES-48. Cloud migration phase 1 — lift collector + DB to a small VM

**Severity: medium (data durability — single point of failure today).**

The collector and DB both live on the dev laptop, which depends on
`sudo pmset disablesleep 1` for lid-closed operation and on the disk
not failing. Two-plus months of accumulated WMATA data (since 2026-05-02)
is the most valuable artifact in the project — the WMATA feed has no
replay window, so any gap is permanent. Phase 1 is the minimum cloud
footprint that removes the laptop as a single point of failure: a
small Linux VM running self-hosted Postgres + the collector + the
existing archive job. API and frontend stay local for now (Phase 3).

Concrete steps:
1. Provision a small VM (Hetzner CPX21 / DigitalOcean basic / AWS t4g.small;
   ~$10-15/mo). Needs ≥150 GB disk so the post-dedup ~50 GB equilibrium
   plus parquet archives and headroom all fit.
2. Install Postgres (same major version as local — check
   `src/database.py` and `pyproject.toml`).
3. `pg_dump -Fc` the local DB, scp the dump, `pg_restore` on the VM.
   Plan for hours of transfer at consumer-internet upload speed; do it
   over a weekend or use `pg_dump | ssh | pg_restore` to avoid
   intermediate disk.
4. Move the WMATA API key onto the VM via `.env` (NOT in git).
5. Run the collector under systemd (`Restart=on-failure`,
   `StandardOutput=append:/var/log/wmata-collector.log`) so it survives
   crashes and reboots without `caffeinate` / `disablesleep` hacks.
6. Schedule `pipelines/archive_trip_update_snapshots.py` and
   `pipelines/run_daily_batch.py` via systemd timers (or cron).
7. Point the local API at the VM's Postgres via SSH tunnel
   (`ssh -L 5432:localhost:5432 vm` then `DATABASE_URL` to localhost),
   so dev workflow doesn't change. Note that some pipelines doing bulk
   writes (`derive_stop_events_*`, etc.) will be slow over a tunnel —
   acceptable for backfills, run them on the VM for routine work.
8. Park the local DB read-only as a backup until the cloud copy has
   ≥7 days of clean operation; only then drop it.
9. Once stable, `sudo pmset disablesleep 0` to reclaim normal sleep
   on the laptop.

Out of scope for Phase 1: managed Postgres (NOTES-49), public API
deployment (NOTES-50), automated backups beyond a weekly `pg_dump` to
S3/B2/R2 (one-line cron, include).

---

## NOTES-49. Cloud migration phase 2 — managed Postgres + backups

**Severity: low (only when the VM-hosted DB outgrows hand-maintenance).**

Trigger: any of (a) NOTES-48 has been stable for ≥30 days and we want
to stop hand-maintaining Postgres, (b) DB grows past ~150 GB and a
larger VM becomes more expensive than managed, (c) site goes
semi-public and a single accidental `DROP TABLE` becomes
unrecoverable. Until one of those, the VM-hosted Postgres from Phase 1
is fine.

Migration choices, roughly cheapest → most robust:
- **Neon** (serverless Postgres, branching, generous free tier; cold starts on idle, fine for a low-traffic dashboard).
- **Supabase** (managed Postgres + auth/storage we don't need, ~$25/mo for the relevant tier).
- **DigitalOcean Managed Postgres** (~$15/mo for the cheapest tier, automated daily backups + PITR).
- **AWS RDS** (most flexible, most expensive at this scale).

Concrete steps:
1. Pick the provider. Neon is simplest if cold-start latency on
   `/api/routes` is acceptable (the warm path is ~37 ms; first query
   after idle could be 1-2 s).
2. Provision; copy the connection string into `.env` on both VM and
   laptop.
3. `pg_dump -Fc` from VM, `pg_restore` into managed; run both in
   parallel for one week (collector double-writes via a small adapter
   in `src/wmata_collector.py`, or a logical replication slot — the
   adapter is simpler). Compare row counts daily.
4. Cut the collector over to writing only to managed Postgres.
5. Decommission the VM's Postgres; keep the VM for the collector
   process itself.

Backups: managed Postgres providers handle PITR. Until then, a weekly
`pg_dump | xz | aws s3 cp` (or B2 / R2 — both cheaper than S3) on a
cron is sufficient. Document the restore drill in `CLAUDE.md` or a
runbook so it's not first-time-when-needed.

---

## NOTES-50. Cloud migration phase 3 — deploy API + frontend

**Severity: low (only if/when the dashboard goes semi-public).**

Trigger: someone other than the user wants to view the dashboard
without a screenshare. Until then, running the API + Vite frontend on
the laptop pointed at the cloud DB is fine and keeps iteration speed
high.

Concrete steps:
1. **API** (`api/main.py`, FastAPI). Deploy options: Fly.io
   (geographically close to managed-Postgres region; ~$5/mo for a
   small instance), Render, Railway, or a separate VM. Ship as a
   container; uvicorn workers behind whatever load balancer the
   provider gives. Wire `DATABASE_URL` to the managed Postgres from
   NOTES-49 (or to the VM-hosted Postgres from NOTES-48 if NOTES-49
   isn't done yet).
2. **Frontend** (`frontend/`, Vite static build). Deploy to Cloudflare
   Pages / Vercel / Netlify — all free at this traffic level. Set
   `VITE_API_URL` to the deployed API.
3. **Domain + TLS.** Either provider issues TLS automatically; pick a
   cheap domain or use a subdomain.
4. **Auth.** Even for a "semi-public" dashboard, decide before launch
   whether to gate it (HTTP basic auth on the API + frontend is
   one-line in most providers; a shared bookmark with credentials is
   probably enough, no need for a real auth system).
5. **Monitoring.** Minimum: a healthcheck endpoint already exists
   (`/api/health` if it doesn't, add one); wire any uptime monitor
   (UptimeRobot free tier, etc.) to ping it. The collector should also
   surface its own health — `scripts/collector_status.py` exists for
   this.
6. **CORS.** `api/main.py` currently sets `allow_origins=["*"]` —
   tighten to the deployed frontend domain before launch.

Out of scope: scaling beyond a single API instance, real auth
(SSO/OAuth), CDN configuration beyond what Pages provides by default.

---

## NOTES-58. RouteDetail per-route diagnosis panel

**Severity: low.**

Surfaces the route_diagnostic_profile materialized in PR #107 on
RouteDetail as a new "Diagnosis" tab or panel. Two-direction layout (matching how slip
trajectory naturally splits):

- **Slip trajectory chart** — per-direction line chart of cumulative
  slip vs stop sequence with timepoint markers and labels. Bar overlay
  for per-segment slip (red = late, green = recovery). Period selector
  reuses the existing `period=` filter from RouteDetail.
  Reference visual: `visualizations/slip_trajectory.py` output.
- **Timepoint behavior table** — one row per timepoint on the route
  with: name, stop_sequence, classification badge (recovery / leaky /
  underpowered / neutral), median dev entering, median dev leaving,
  p10/p90 spread change. Per-period breakdown via the period selector.
- **LLM-generated diagnosis narrative** — 200-300 word interpretation
  of the route's diagnostic profile (PR #107): direction asymmetry,
  key delay zones, timepoint behavior (recovery / leaky / underpowered),
  2-3 ranked hypotheses with evidence, suggested intervention class.
  Generated in a batch / on-demand workflow, never in the request
  path, since the public site will eventually serve this content
  uncached:
  - New CLI `scripts/generate_route_diagnosis.py` invoked by the user
    (per route or `--all`). Reads the materialized profile from the
    `route_diagnostic_*` tables (PR #107), calls Claude with a
    structured prompt + the profile as context, writes the result to
    a new `route_diagnosis_narrative`
    table keyed by `(route_id, period)` with `generated_at`,
    `model_id`, `prompt_version`, `profile_snapshot_hash` columns.
  - API endpoint `GET /api/routes/{id}/diagnosis?period=...` reads
    from the cache table; never invokes Claude. Returns
    `is_stale=true` when `profile_snapshot_hash` differs from the
    current diagnostic profile (PR #107) so the panel can show a
    "diagnosis is out of date" badge — regeneration stays manual.
  - Example narrative (from the D80 May 2026 deep-dive): "Direction 0
    (Union Station → Friendship Heights): schedule under-budgets
    running time through downtown by ~5 min cumulative before
    recovering at Farragut Square (I St NW + 17 St NW). The recovery
    wedge appears over-sized for off-peak traffic — 26% of buses
    leave the timepoint early, consistent with non-enforced
    hold-downs. The PM-peak EWT (4.5 min) is largely attributable to
    this leakage. Suggested intervention: tighten hold-down
    enforcement at Farragut; consider headway-based dispatching for
    this route."

Audience: both the user (research deep-dive) and transit-interested
public. Public-facing copy should explain "slip" and "timepoint"
inline on first use; consider a "?" tooltip glossary linking to a
brief explainer page. The LLM is a build-time tool here, not a
runtime dependency — the public site doesn't call Anthropic and isn't
exposed to LLM cost / latency / availability.

### Dependencies

route_diagnostic_profile foundation (PR #107). The WMATA-designated
frequent-route list (`config/frequent_routes.yaml`, loaded via
`src/frequent_routes.py`) is available for the diagnosis text to
note "this route is on WMATA's Frequent Service Map" when
applicable; not a hard dependency.

---

## NOTES-59. Cross-route segment diagnostic (V1, stop-pair)

**Severity: low (highest-leverage novel output — V1 piece).**

Aggregates per-segment slip from the route_diagnostic_profile
foundation (PR #107) across *all* routes to identify
infrastructure-investment targets at the stop-pair level.
V1 deliberately uses segment-identity matching only — same
`(from_stop, to_stop)` across routes counts as the same segment —
and defers shape-aware corridor rollup to NOTES-62 so V1 can ship
without geometric matching infrastructure.

Computation:
- For each unique stop-pair `(from_stop, to_stop)` traversed by ≥2
  routes, aggregate total slip-seconds across all routes weighted by
  observed trip volume.
- Per time-of-day cuts; PM peak typically dominates for downtown.
- "Contributing routes" list with per-route breakdown for drilldown.

New page `/segments` (new top-level nav alongside Overview / Routes /
Blocks / Targets per PR #105's IA). Ranked list with columns: segment
description (from-stop → to-stop, distance), contributing routes
(count + names), total slip-min/hour, peak hour. Click-through shows
per-route contribution.

Caveat: stop-pair identity misses cases where two routes traverse the
same street segment using different stop_ids (NB and SB stops at
different intersections, or alternative spacing). V1 systematically
underestimates total corridor slip for those cases — NOTES-62
addresses this with shape-aware matching.

Use cases (V1):
- "Which intersections / short segments lose the most time
  system-wide?" — first cut at infrastructure-investment ranking.
- "Where do bus routes share a chokepoint?" — direct visibility into
  shared pain.

### Dependencies

route_diagnostic_profile foundation (PR #107).

---

## NOTES-62. Cross-route corridor diagnostic (V2, geometric rollup)

**Severity: low (decision-useful framing — V2 follow-up to NOTES-59).**

Rolls NOTES-59's stop-pair slip up to the corridor / intersection
level via shape-aware matching, so "the M St NW corridor from
Wisconsin Ave to Pennsylvania Ave" reads as a single investment
target rather than N stop-pairs. The framing transit planners,
advocates, and the public actually use — TSP and dedicated-lane
decisions are made at the corridor level, not the stop-pair level.

Computation requirements beyond V1:
- **Stop → corridor mapping.** Project each stop onto the route's
  GTFS shape; group stops within ~150 m of the same shape segment
  into a "corridor" identified by street + endpoints. Handle
  multi-street corridors (e.g., Wisconsin Ave NW from Friendship
  Heights to M St) via shape connectivity, not stop-name parsing.
- **Cross-route corridor identity.** Two routes traverse "the same"
  corridor if their shapes overlap for ≥N meters along the same
  street centerline. May require an external street-network dataset
  (OpenStreetMap road network) for robust matching — GTFS shapes
  alone may not give consistent corridor identity across routes that
  use slightly different alignments.
- **Aggregation.** Sum per-segment slip × trip volume across all
  stop-pairs that fall within a corridor, across all routes that
  traverse it.

UI: extend the `/segments` page (NOTES-59) with a corridor view
toggle, or new tab. Corridor cards: name, length, contributing
routes, total system-wide slip-hours/day, peak periods, drill-down
to constituent stop-pairs (NOTES-59 view).

Out of scope for V2: cost-of-intervention estimates (TSP install
cost, bus-lane construction cost — those are WMATA / DDOT planning
inputs, not derivable from operational data). The output ranks
candidates by *benefit* (system-wide delay reduction); pairing with
cost data is a separate exercise.

### Dependencies

NOTES-59 (segment-level aggregation). May benefit from any future
work on stop-to-shape projection or OSM road-network ingestion if
that lands independently.

---


---

## NOTES-61. Hold-down policy / dispatching candidates page

**Severity: low.**

Ranked timepoint-leakage table — which WMATA timepoints would benefit
most from enforced hold-downs (AVL alerting on early departures, or
operator-policy reminders). The operational complement to the
schedule-audit page's schedule-revision lever: zero capital, only
policy.

For each timepoint on each route, per period:
- **Leakage rate** — % of buses departing > 60s ahead of scheduled
  departure
- **Estimated downstream EWT impact** — expected reduction in
  next-bus headway variance at the first 2-3 downstream stops if
  early departures were eliminated
- **Affected daily trips** — count

Ranked descending by estimated EWT-savings × trip volume. Per-route
drill-down shows the full distribution at the timepoint.

Use cases:
- "Which timepoints should AVL flag early departures most aggressively?"
- "Where would headway-based dispatching (vs schedule adherence) most
  improve rider experience?" — the leakiest timepoints on frequent
  routes are also the best candidates for policy change.

### Dependencies

route_diagnostic_profile foundation (PR #107). "Frequent route"
filtering in the ranking (headway-based dispatching is the right
intervention specifically for frequent routes) uses the
WMATA-designated list in `config/frequent_routes.yaml`, loaded via
`src/frequent_routes.py`.

---

## NOTES-64. Custom `useMultiFetch` hook for the frontend

**Severity: low** (works, but latent race conditions on fast nav).
**Effort: medium** (touches 5+ components; cancellation needs care).

`RouteDetail.jsx:66-102`, `SystemTrend.jsx:62-91`, `RouteTrend.jsx`,
and several other components hand-roll the same `useEffect` →
`Promise.all([fetch, fetch, ...])` → `.then(setData)` →
`.catch(setError)` pattern, each with its own loading-state
management. Beyond the repetition, none of the implementations wire
up AbortController cancellation — fast route switches can race a
stale fetch's completion against the new route's render,
occasionally showing wrong-route data for a frame before the second
response wins.

Build `useMultiFetch(urls, transform)` in
`frontend/src/hooks/useMultiFetch.js` that returns
`{ data, loading, error }`, wires AbortController to the effect's
cleanup, and collapses the call sites to a single line. Migrate
components one at a time; the hook is opt-in per call-site, so the
refactor can land incrementally.


