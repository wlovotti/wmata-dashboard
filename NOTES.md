# Code Review Notes

Forward-looking punch list. Completed items are removed in the same
PR that closes them — see git log and PR descriptions for history.
Item numbers (`NOTES-N`) are stable; new items take the next number.
NOTES.md edits ride on substantive PRs; standalone reconciliation PRs
are churn.

Last edited 2026-05-21. Test-infra hardening (PR #136's first CI push
had 12 failures, 8 traced to migrate-script CREATE TABLE drift): extended
`check_schema_drift.py` to validate `migrate_create_*.py` SQL against the
model, isolated `test_migrate_trip_update_state.py` to its own engine,
softened the rigid column-set assertion in `test_models.py`, and added
`bin/test-with-pg` so the full suite runs locally before push (closed
in the test-infra hardening PR). NOTES-72 Phase D recovery in
flight: investigation on 2026-05-20 surfaced that the v2 derivation
pipeline silently failed 4 consecutive nightly batches (2026-05-16 → 19)
due to a `_resolve_side_table` SQLAlchemy bug AND that the
`trip_update_state` PK omitted `service_date`, so WMATA's repeating
day-over-day trip_ids overwrote prior-day state. Schema-fix PR adds
`service_date` to the PK, fixes the resolver, adds an idempotent JSONL
replay tool for historical re-derivation, simplifies cleanup to a single
date rule, and adds a row-count guard so silent-zero failures can't recur.
See
`docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md`.
Closed NOTES-70 (PR #133) — added `.where(trip_id == 'T1')` filters to all
bare `select(StopEvent)` and `select(TripUpdateState)` calls in
`tests/test_derive_stop_events_from_state.py`; tests now pass on any DB
(populated or empty). Closed NOTES-71 (PR #132) — per-process JSONL archive
filenames eliminate the multi-frame zstd hazard; rotate_archive.py now globs
all per-day files and merges them. Closed NOTES-38 (PR #125) — server-side
7-day-vs-prior-7-day deltas on every scorecard metric. API carries a `deltas`
block per route (shape: `{value, valid, current_n, prior_n}` per metric) on
both `/api/routes` and `/api/routes/{id}`; thin-data suppression generic
(<3 valid days) plus EWT-specific (<20 observed headways per window). RouteList
and RouteDetail KPI cards render up/down/flat arrows via an extended
`DeltaIndicator` (`lowerIsBetter` prop). The "What changed" panel (NOTES-54,
PR #138) is now live on the Overview page.

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

**Information architecture & navigation**

**Diagnostic outputs (route-level + system-wide)**

A new initiative — the dashboard today surfaces metrics; these items
surface *why* metrics are what they are and *where* operational
intervention has the highest leverage. Outputs target both the user
and a transit-interested public audience (the eventual public-site
goal in NOTES-50). Pure deterministic Python/SQL — no LLM in the
pipeline; the structured artifacts feed dashboard panels and ranked
target lists directly.

- **RouteDetail diagnosis panel (PR #124)** — slip trajectory chart
  (both directions, timepoint markers) + timepoint behavior table.
  LLM diagnosis narrative deferred to NOTES-69.
- **Cross-route segment diagnostic (PR #140).**
  Aggregate slip across all routes per `(from_stop, to_stop)`
  segment → ranked infrastructure-investment candidates (TSP /
  queue-jumps / dedicated lanes). Segment-identity matching only;
  no geometric corridor rollup.
- **NOTES-62 Cross-route corridor diagnostic (V2, geometric rollup).**
  Roll the cross-route segment diagnostic's (PR #140) stop-pair slip up to corridor / intersection
  level via shape-aware matching, so "the M St NW corridor from
  Wisconsin to Penn Ave" reads as one investment target rather
  than N stop-pairs. The framing that makes the output
  decision-useful for infrastructure planning.
- **NOTES-61 Hold-down policy / dispatching candidates page.**
  Ranked timepoint-leakage table (% of buses departing > N seconds
  early per timepoint per period) → operational fix targets, no
  capital required.

### P5 — Cleanup

- **NOTES-20 Tighter rider-experience OTP.** A stricter window alongside
  WMATA's official. Tracked but not yet scoped — user wants
  comparability with WMATA's scorecard for now.

### Independent of the redesign

- **NOTES-72 Trip-update state refactor — complete Phase D/E/F.** PR #128
  shipped the dual-write architecture and the side-by-side derivation
  (`stop_events` from snapshots vs `stop_events_v2` from state). This is
  the operational follow-up to finish the migration. Spec:
  `docs/superpowers/specs/2026-05-17-trip-update-state-refactor-design.md`
  + addendum `docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md`.

  **Phase D — validation (in flight, schema-fix PR pending merge as of
  2026-05-20).** The 2026-05-17 design used a PK of `(trip_id,
  stop_sequence)` and assumed each day's snapshot would be derived
  before the next day's runs overwrote state. Both assumptions failed:
  (1) WMATA's GTFS-RT trip_ids repeat day-over-day (94% reuse Mon→Tue),
  so the UPSERT silently overwrote prior-day state, and (2) the v2
  derivation crashed on every nightly batch from 2026-05-16 onward via
  a `_resolve_side_table` SQLAlchemy bug that the batch wrapper had no
  way to surface (the pipeline exited 0 after crashing at the first
  non-empty route). The schema-fix PR adds `service_date` to the PK,
  fixes the resolver, adds `pipelines/replay_archive_to_state.py` for
  idempotent historical recovery from the JSONL archive, collapses
  cleanup to a single `service_date < CURRENT_DATE - INTERVAL '7 days'`
  rule, and adds a row-count guard in `run_daily_batch.py` so the
  silent-zero failure mode can't recur.

  After the PR merges + the user runs the deployment sequence
  (stop collector → migrate → restart collector), Phase D restarts
  with two paths to the cutover bar (≥7 consecutive days at 100%
  agreement including ≥1 weekend day):
  - **Forward-only** (no backfill): earliest cutover 2026-05-27
    (covers weekend 5/23–24).
  - **Replay + forward** (backfill 5/18 + 5/19 from JSONL):
    earliest cutover 2026-05-25 (the original target).

  Both backfill commands are idempotent:
  ```
  uv run python pipelines/replay_archive_to_state.py --date 2026-05-18
  uv run python pipelines/derive_stop_events_from_state.py \
      --all-routes --date 2026-05-18 --target-table stop_events_v2
  ```

  **Phase E — cutover.** Stop dual-writing to `trip_update_snapshots`
  in `src/wmata_collector.py:_save_trip_updates`. Switch the primary
  daily-batch derivation from `derive_stop_events_trip_updates.py`
  (reads snapshots) to `derive_stop_events_from_state.py` (reads state).
  Update `pipelines/run_daily_batch.py` so the v2 alias becomes primary
  and the legacy entry is dropped.

  **Phase F — retirement.** Drop `trip_update_snapshots` and
  `stop_events_v2` (or rename v2 back to canonical). Delete the legacy
  `derive_stop_events_trip_updates.py`,
  `pipelines/compare_old_vs_new_derivation.py`, and
  `pipelines/cleanup_trip_update_state.py`. Schedule
  `pipelines/archive_trip_update_snapshots.py` (or its successor for
  `trip_update_state`) via launchd timer for ongoing retention —
  currently still manual after the 2026-05-17 one-shot run.

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

## NOTES-62. Cross-route corridor diagnostic (V2, geometric rollup)

**Severity: low (decision-useful framing — V2 follow-up to the cross-route segment diagnostic, PR #140).**

Rolls the stop-pair slip from the cross-route segment diagnostic (PR #140) up to the corridor / intersection
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

UI: extend the `/segments` page (PR #140) with a corridor view
toggle, or new tab. Corridor cards: name, length, contributing
routes, total system-wide slip-hours/day, peak periods, drill-down
to constituent stop-pairs (PR #140 stop-pair view).

Out of scope for V2: cost-of-intervention estimates (TSP install
cost, bus-lane construction cost — those are WMATA / DDOT planning
inputs, not derivable from operational data). The output ranks
candidates by *benefit* (system-wide delay reduction); pairing with
cost data is a separate exercise.

### Dependencies

Cross-route segment diagnostic (PR #140, segment-level aggregation). May benefit from any future
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

## NOTES-69. LLM-generated route diagnosis narrative

**Severity: low (deferred from NOTES-58, closed by PR #124).**

200–300 word interpretation of the route's diagnostic profile
(PR #107 foundation): direction asymmetry, key delay zones, timepoint
behavior (recovery / leaky / underpowered), 2–3 ranked hypotheses with
evidence, suggested intervention class. Deferred from NOTES-58 so the
chart + table panel could ship without a runtime Anthropic dependency.

The LLM is a build-time tool here, not a runtime dependency — the
public site must not call Anthropic and must not be exposed to LLM
cost / latency / availability.

### Workflow

- **New CLI `scripts/generate_route_diagnosis.py`** invoked by the
  user (per route or `--all`). Reads the materialized profile from the
  `route_diagnostic_*` tables (PR #107), calls Claude with a
  structured prompt + the profile as context, writes the result to a
  new `route_diagnosis_narrative` table keyed by `(route_id, period)`
  with `generated_at`, `model_id`, `prompt_version`,
  `profile_snapshot_hash` columns.
- **API endpoint `GET /api/routes/{id}/diagnosis?period=...`** reads
  from the cache table; never invokes Claude. Returns `is_stale=true`
  when `profile_snapshot_hash` differs from the current diagnostic
  profile (PR #107) so the panel can show a "diagnosis is out of
  date" badge — regeneration stays manual.
- The `profile_snapshot_hash` is a deterministic hash of the
  `route_diagnostic_segment` + `route_diagnostic_timepoint` rows for
  the route+period so staleness detection doesn't require a
  re-compute.

### Frontend integration

Add a "Narrative" sub-section in `RouteDiagnosisPanel.jsx` (PR #124)
below the timepoint table. Show the cached text; render a
"Diagnosis is out of date — re-run `generate_route_diagnosis.py
{route_id}`" banner when `is_stale=true`.

### Dependencies

`route_diagnostic_*` tables (PR #107). `RouteDiagnosisPanel` (PR #124,
slip trajectory + timepoint table). Claude API / Anthropic SDK
(`anthropic` Python package — already in the environment or add to
`pyproject.toml` extras).
