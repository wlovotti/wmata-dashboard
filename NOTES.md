# Code Review Notes

Forward-looking punch list. Completed items are removed in the same
PR that closes them — see git log and PR descriptions for history.
Item numbers (`NOTES-N`) are stable; new items take the next number.
NOTES.md edits ride on substantive PRs; standalone reconciliation PRs
are churn.

Last edited 2026-06-08. NOTES-48 live cutover: collector + Postgres now run on
AWS Lightsail (PG16) under systemd — the laptop is no longer the live system.
Fixed the systemd units in the same PR; NOTES-48 stays open for S3 backups,
retention timers, and laptop retirement. See the rewritten NOTES-48 +
`docs/DEPLOYMENT.md`. 2026-06-08 health check: VM collector verified healthy +
continuous since cutover; reconciled NOTES-48 item 4 (no parallel collection —
laptop stopped cleanly at cutover); raised collector MemoryMax 600M→1G (the
collector MemoryMax raise, PR #162) to eliminate scattered missed heartbeat
ticks caused by transient allocation peaks hitting the old ceiling.
Closed NOTES-78 — added the deploy runbook (`docs/DEPLOY.md`, PR #160):
ordered steps for pull → daemon-reload → restart → smoke check, rollback
procedure, and a one-liner to print the live SHA. Closed NOTES-79 — added the
migration safety ritual (`docs/MIGRATIONS.md`, PR #161): backup-first
checklist, test-on-prod-data step, transaction-wrapping guidance, and the
`--dry-run` convention for new migration scripts; pointer added to `CLAUDE.md`
and `docs/DEPLOY.md`. Same PR #159 aligned CI Postgres 15→16 to match the
prod VM (it was a stale third version) and reconciled the PG-version claims in
`CLAUDE.md`, NOTES-48, and `docs/DEPLOYMENT.md` — all three variously said
CI/prod ran 14.
Closed NOTES-72 Phase F — trip-update snapshot path retirement (PR #155):
deleted `derive_stop_events_trip_updates.py`, `compare_old_vs_new_derivation.py`, and
`archive_trip_update_snapshots.py`; removed the archive housekeeping entry from
`run_daily_batch.py`; added `pipelines/retain_trip_update_state.py` + launchd timer
(`scripts/launchd/com.wmata-dashboard.retain-trip-update-state.plist`); added
`scripts/migrate_drop_phase_f.py` (unrun manual runbook) for dropping `trip_update_snapshots`
and (if present) `stop_events_v2`. The E.2 stability gate (≥1 week since PR #151 merged
2026-05-25) was satisfied as of 2026-06-03.
Closed NOTES-34 — the short-route delivered-ceiling fix (PR #148):
for stops_observable <= 2, the delivered threshold is now 1 (any real observation counts),
lifting A90 weekday from 48% to 96% delivered (was 61/127; now 122/127) and aligning with
88% OTP. Before the fix, trip_update rows on 2-stop routes had stops_observable=1 and a
floor of 2 was structurally unreachable. Closed NOTES-76 — the data_quality column rollout
(PR #146): added `data_quality` (`'complete'|'partial'`) and `coverage_pct`
columns to `system_metrics_daily` and `route_metrics_daily_overlay`; the
completeness guard is now a *flagger* (persists partial rows) rather than a
*gate* (refuses upsert); partial rows are excluded from delta computations and
the prior-window mean; the system trend strip renders a grey dot with a
"Partial collection — X% coverage" hover badge for partial days. Closed NOTES-75 — the
collector pid-self-management fix (PR #145): the collector now writes
its own `logs/collector.pid` on startup, detects stale/live conflicts,
and removes the file on graceful shutdown. Also noted real-world incident
on NOTES-48 (2026-05-24 12:15 ET power loss killed the collector and
lost ~12.5h of WMATA feed — first concrete data point validating the
"single point of failure" framing); recovery shipped
`src/data_completeness.py` + guards on both `upsert_*` paths so future
partial days won't pollute the materialized aggregates. Closed NOTES-74
(PR #144) — applied the
NOTES-70 (PR #133) trip_id-filter pattern to 9 `trip_update_state`
tests across `test_upsert_trip_update_state`, `test_cleanup_trip_update_state`,
and `test_compare_derivations`; tests now pass on populated dev DBs and
in CI. Route
diagnosis narrative (NOTES-69, PR #141): offline CLI generates LLM
summaries from `route_diagnostic_*` tables, cached in
`route_diagnosis_narrative`; `GET /api/routes/{id}/diagnosis` serves
cache read-only; `RouteDiagnosisPanel` shows narrative + stale banner.
Claude never called at request time.

Test-infra hardening (PR #136's first CI push
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
  (both directions, timepoint markers) + timepoint behavior table +
  LLM diagnosis narrative (the route diagnosis narrative, PR #141).
- **Cross-route segment diagnostic (PR #140).**
  Aggregate slip across all routes per `(from_stop, to_stop)`
  segment → ranked infrastructure-investment candidates (TSP /
  queue-jumps / dedicated lanes). Segment-identity matching only;
  no geometric corridor rollup.
- **NOTES-61 Hold-down policy / dispatching candidates page.**
  Ranked timepoint-leakage table (% of buses departing > N seconds
  early per timepoint per period) → operational fix targets, no
  capital required.

### P5 — Cleanup

- **NOTES-20 Tighter rider-experience OTP.** A stricter window alongside
  WMATA's official. Tracked but not yet scoped — user wants
  comparability with WMATA's scorecard for now.

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

**Status (2026-06-05): live cutover complete — the laptop is no longer the
live system.** The authoritative plan and rationale are the design spec
(`docs/superpowers/specs/2026-05-28-cloud-migration-phase1-design.md`) and the
operator runbook (`docs/DEPLOYMENT.md`). The original step list here
(Hetzner/DO, ≥150 GB, Postgres-matching-local) is superseded by what shipped:

- **Host:** AWS Lightsail $12/mo (2 GB RAM), us-east-1, Ubuntu 24.04. `PGDATA`
  on a 64 GB attached block disk; Postgres bound to localhost (verified
  unreachable from the internet), SSH-key-only, 4 GB swap.
- **PostgreSQL 16**, deliberately *not* local's 14 — PG14 is EOL Nov 2026 and a
  14→16 `pg_restore` is routine. Local dev stays on 14; CI runs 16 to match
  prod (aligned from a stale 15 in the PG-16 reconciliation PR — CI is the
  prod-parity gate); a 16→14 restore would NOT work, so never dump from prod
  to restore locally (see CLAUDE.md).
- **Cutover:** two-phase, near-zero loss. Bulk `pg_dump -Fc | pg_restore
  --no-owner` ran while the collector kept collecting; then a delta-sync of the
  three live tables (`vehicle_positions`, `collector_heartbeats`,
  `trip_update_state`) plus a `setval` on `vehicle_positions_id_seq` so the
  resumed collector couldn't collide on ids. ~13 min collection gap; verified
  laptop == VM row-for-row on the live tables.
- **systemd (this PR fixes the units):** `wmata-collector` (`Restart=always`,
  enabled — survives reboots), nightly batch `wmata-metrics.timer` (2 AM ET),
  weekly `pg_dump` `wmata-backup.timer` (Sun 1 AM ET). Bugs fixed: ExecStart now
  runs the venv interpreter directly (`uv run` fails writing its cache under
  `ProtectHome=read-only`); `ReadWritePaths` extended to the JSONL archive + PID
  file dirs; collector `MemoryMax` 400M→600M (measured ~210 MB baseline); a
  double-trigger `OnCalendar` removed; both timers zone-pinned to
  `America/New_York` (the server clock is UTC).

**Remaining before this item closes:**
1. AWS CLI + IAM credentials → unlocks **S3 off-box backups** (wire
   `S3_BACKUP_BUCKET` into `wmata-backup`) and **automatic block-disk
   snapshots** (CLI-only; the Lightsail console can't enable them for disks).
2. **Retention timers** (`wmata-archive-positions`, `wmata-window-derived`) —
   deferred because they DELETE data: the tier-3 30-day window would drop the
   oldest ~4 days of `vehicle_positions` on first run (data starts 2026-05-02),
   and it archives to S3 (item 1). Enable only with S3 in place + explicit sign-off.
3. **SSH tunnel** for the local API/frontend → cloud DB (overlaps NOTES-50).
4. **Laptop retirement (read-only soak, in progress).** Note: there is *no
   ongoing parallel collection* — the laptop collector was stopped cleanly at
   cutover (last write 2026-06-05 01:12 UTC, `"Combined collector stopped
   successfully!"`), and the cutover already verified laptop == VM row-for-row.
   What remains is a read-only soak: keep the laptop DB as a cold fallback
   through ~2026-06-12 (7 days post-cutover), then `sudo pmset disablesleep 0`
   to let the laptop sleep again. **Verified 2026-06-08:** VM collecting
   continuously since cutover (`collector_status.py` ✓ healthy; position gaps
   ≤91s/day; 2026-06-04 counts match the laptop exactly at 917,293 rows);
   laptop still pinned awake (`SleepDisabled=1`) with nothing to collect.
5. **Collector `MemoryMax` tuning** — the 600M cap this item set was too tight
   on the VM (scattered missed heartbeat ticks); raised to 1G in the collector
   MemoryMax raise (PR #162).

Out of scope for Phase 1: managed Postgres (NOTES-49), public API deployment
(NOTES-50).

**2026-05-25 incident note.** Power loss on 2026-05-24 at 12:15 ET
killed the collector mid-tick and lost ~12.5 hours of WMATA real-time
feed (gap unrecoverable — feed has no replay window). The
`pmset disablesleep 1` setup protected against lid-close sleep but not
against actual power interruption (battery dying / wall power cut).
Recovery procedure: restart via
`nohup env PYTHONUNBUFFERED=1 uv run python scripts/continuous_combined_collector.py >> logs/collector.log 2>&1 &`
— the collector now writes its own pid file on startup (the
collector pid-self-management fix, PR #145), so no manual pid
bookkeeping is needed. First real-world data point validating this
item's "single point of failure" framing — until the VM lift, any
power event will keep causing permanent gaps.

**2026-06-03 artifact reconciliation.** `docs/DEPLOYMENT.md` rewritten from
DigitalOcean to AWS Lightsail per the approved spec
(`docs/superpowers/specs/2026-05-28-cloud-migration-phase1-design.md`); two
broken `ExecStart` script references in the systemd units fixed
(`continuous_collector` → `continuous_combined_collector`,
`compute_daily_metrics` [nonexistent] → `run_daily_batch`); RAM caps relaxed
for the 2 GB instance; retention/backup systemd timer pairs added
(`wmata-backup`, `wmata-archive-positions`, `wmata-window-derived`). Live VM
provisioning, data transfer, and cutover remain manual (this item stays open).

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


