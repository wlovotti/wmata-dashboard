# Code Review Notes

Forward-looking punch list. Completed items are removed in the same
PR that closes them — see git log and PR descriptions for history.
Item numbers (`NOTES-N`) are stable; new items take the next number.
NOTES.md edits ride on substantive PRs; standalone reconciliation PRs
are churn.

Last edited 2026-08-08. Closed NOTES-98: added `bin/prune-vm-archive.sh`,
a laptop-side verify-then-prune drain for the VM's raw JSONL archive —
run manually after each `aws s3 sync` step (docs/DEPLOYMENT.md), it
deletes VM-side files older than a safety window only after confirming
a byte-size-matching S3 copy exists — see PR #TODO.
Earlier same day: Closed NOTES-97: `bin/pull-and-derive.sh` now
rsyncs `archive/sfmta_raw_snapshots/` alongside the WMATA archive dirs,
and `s3://wmata-dashboard-backups/raw-jsonl-archive/sfmta/` was created
and backfilled manually from the laptop the same day (IAM `PutObject`
grant confirmed working by that sync) — see PR #182.
Earlier same day: Added NOTES-97 — SFMTA raw snapshots
(`archive/sfmta_raw_snapshots/`, writing since 7/22) have no sync path:
`bin/pull-and-derive.sh` rsyncs only `raw_snapshots` and no S3 prefix
exists, so the sidecar's output is single-copy on the VM root disk.
Added NOTES-98 — the "14-day JSONL buffer pruning" NOTES-95 refers to
was never installed; the manual laptop sync is the VM archive's only
drain, and the root disk was found at 87% on 2026-08-08 (~5–6 days of
runway) with 40 GB of unpruned archive.
Earlier (2026-07-18, third edit). Added NOTES-95: the
stateless-collector rewrite now has a tracking item — it was previously
referenced only as a dangling "tracked there" pointer in NOTES-91 while
four postmortem actions (staleness alarm, cutover job inventory,
downsize, retention-chore replacement) were deferred to it with no home.
Earlier same day: Closed NOTES-92: both rollup
pipelines accept `--gtfs-snapshot-id` (threaded through
`compute_service_delivered*` and `fetch_scheduled_cell_hours_for_routes`
via `gtfs_version_filter`); June 6/11–6/20 rollups re-run with snapshot 12
in the closing PR's deploy step.
Earlier same day: July recovery executed on the laptop (spec
2026-07-14, Status line has the tally); local `wmata_dashboard` is now the
system of record and the VM is demoted to collector + backup + VP-archive
(DEPLOYMENT.md banner). Added NOTES-92..94; NOTES-93 (loud
zero-file replay — silent no-op bit the 7/18 fold-in) and NOTES-94
(VP-path dead-man coverage) remain open. Fixed the `/health` endpoint's
SQLAlchemy-2.x `text("SELECT 1")` regression.
Earlier (2026-07-17). Shrunk NOTES-91: collector dead-man ping shipped
(`src/deadman.py`, PR #173); the batch-alerting half is superseded by the
2026-07-13 Path 2a decision to make derivation a manual laptop action
(see the July 2026 incident postmortem, draft; lands with the recovery docs).
Earlier (2026-07-09). Added NOTES-89..91 from the 2026-07-09 prod incident
diagnosis (nightly batch dead since 6/4 via unbounded `vehicle_positions`
seq scans — fixed in the same PR; GTFS stale since 5/31 because the weekly
reload died with the laptop): NOTES-89 VM GTFS reload timer (high severity —
this exact incident recurs at every WMATA service change without it),
NOTES-90 feed-expiry alarm in `/api/gtfs/freshness`, NOTES-91 nightly-batch
failure alerting (five weeks of silent `Failed with result 'timeout'`).
Updated NOTES-82 with incident context (bounded query now uses
`idx_route_timestamp`; the audit should NOT add a `trip_start_date` index).
Earlier (2026-06-14): Landed the dev/deploy environment work: local dev is
now fully on PostgreSQL 16 (upgraded from 14 on 2026-06-14, so local == CI ==
prod), and an on-demand `bin/refresh-dev-db.sh` S3 restore replaces the
tunnel-as-dev-DB setup — `bin/db-tunnel.sh` is demoted to ops-only. This
retires the 14↔16 skew and recontextualizes NOTES-88 (no longer blocks dev).
Earlier (2026-06-13): closed NOTES-48 items 3 + 4. Item 4 (laptop
retirement): soak ran clean to 2026-06-12, `pmset -a disablesleep 0` set
(`SleepDisabled 0` verified). Item 3 (SSH tunnel): on-demand `bin/db-tunnel.sh`
(local 5433 → VM 5432, avoiding local PG14 on 5432), `.env` repointed at the
VM, the three laptop launchd jobs durably disabled, runbook 5432→5433
collision fixed; verified live VM data through the tunnel (23s-fresh
positions, `/api/gtfs/freshness` 200 in 0.29s). **With NOTES-48 done except
the deferred Phase-2/3 items, only the deploy-driven follow-ons remain.**
Added NOTES-88 — `/api/routes` times out >90s over the tunnel (per-route N+1
amplified by ~9ms RTT); blocks remote dashboard use + NOTES-84.
Added NOTES-83..87 from the 2026-06-10 product
review — NOTES-83 blank RouteDetail visual baselines (medium: the CI gate
asserts a blank page; both `routedetail-d72-chromium-*.png` are empty
1280×720 frames); NOTES-84 Overview editorial redesign (big-number
verdict, system map, movers panel, nav collapse — **not
subagent-suitable**, needs an interactive design session); NOTES-85
frontend design-system pass (tokens/type scale replacing `App.css` +
inline styles — **not subagent-suitable**, sequence after 84); NOTES-86
system-level weekly narrative reusing the PR #141 offline-LLM-cache
pattern (code subagent-OK, generation run user-run); NOTES-87 small
honesty fixes (subtitle, Refresh reload, Off-target empty state — code
subagent-OK, baseline regen user-run).
NOTES-48 live cutover: collector + Postgres now run on
AWS Lightsail (PG16) under systemd — the laptop is no longer the live system.
Fixed the systemd units in the same PR; NOTES-48 stays open for S3 backups,
retention timers, and laptop retirement. See the rewritten NOTES-48 +
`docs/DEPLOYMENT.md`.
NOTES-48 item 2 DONE — retention timers enabled. Fixed `uv run`→venv-interpreter
bug in both service units, added `archive/vehicle_positions/` to `ReadWritePaths`,
zone-pinned both timer `OnCalendar` lines to `America/New_York`, and added the
enablement runbook to `docs/DEPLOYMENT.md` §5.6. Sign-off given + enablement
performed 2026-06-10. Items 3 and 4 remain open. 2026-06-08 health check: VM collector verified healthy +
continuous since cutover; reconciled NOTES-48 item 4 (no parallel collection —
laptop stopped cleanly at cutover); raised collector MemoryMax 600M→1G (the
collector MemoryMax raise, PR #162) to eliminate scattered missed heartbeat
ticks caused by transient allocation peaks hitting the old ceiling.
2026-06-09 NOTES-48 item 1 DONE — S3 off-box backups + automatic disk snapshots
are live (this PR documents it). Created the `wmata-dashboard-backups` S3 bucket
(us-east-1, public access blocked, versioned) and a least-privilege IAM user
(`wmata-vm-backup`, no `s3:DeleteObject`); AWS CLI v2 + creds wired into the
`wmata-backup` `EnvironmentFile`; weekly `pg_dump` upload validated end-to-end
(2.0 GiB landed); 90-day + 30-day-noncurrent lifecycle rule applied; daily
Lightsail auto-snapshots enabled on the `wmata-pgdata` disk at 08:00 UTC
(CLI-only — the console can't do disks). Corrected `docs/DEPLOYMENT.md` §5.2,
which wrongly suggested attaching an IAM role to the Lightsail instance —
Lightsail has no instance roles ("does not support service roles"), so the VM
must carry an IAM user key; added §5.5 for disk snapshots and checked in the
policy/lifecycle JSON under `deployment/aws/`. NOTES-48 stays open for the
destructive retention timers (item 2 — now S3-unblocked, awaits sign-off), the
SSH tunnel (item 3), and the laptop soak (item 4, through ~2026-06-12).
Closed NOTES-80 (PR #164) — hardened the `wmata` service account on the VM
(no interactive SSH, password locked, no sudo; humans log in as `ubuntu` and
use `sudo -u wmata`). Updated `docs/DEPLOYMENT.md` §2.2 and `docs/DEPLOY.md`
to document the hardened access model.
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

- **NOTES-84 Overview editorial redesign.** Rebuild the Overview as an
  editorial page (big-number verdict, system map colored by performance,
  ranked "movers" panel) and collapse the six tool-shaped nav items into
  question-shaped ones. **Not subagent-suitable** — needs an interactive
  design session.
- **NOTES-85 Frontend design-system pass.** Replace the 1,287-line
  hand-rolled `App.css` + ~200 inline styles with tokens, a type scale,
  and one chart idiom. **Not subagent-suitable** — aesthetic judgment +
  full visual-baseline regeneration. Sequence after NOTES-84.

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
- **NOTES-86 System-level weekly narrative.** Extend the NOTES-69 /
  PR #141 offline-LLM-cache pattern from per-route diagnosis to a
  system-wide "what happened on the network this week" summary on the
  Overview. Code is subagent-suitable; the generation run and tone
  review are not.

### P5 — Cleanup

- **NOTES-20 Tighter rider-experience OTP.** A stricter window alongside
  WMATA's official. Tracked but not yet scoped — user wants
  comparability with WMATA's scorecard for now.
- **NOTES-87 Small honesty fixes in the frontend chrome.** Drop the
  inaccurate "Real-time" subtitle, replace the full-page-reload Refresh
  button, and give the Off-target panel a useful empty state. Code is
  subagent-suitable, but the subtitle edit invalidates every Playwright
  baseline — the regen step is user-run.

### Production reliability (2026-07 incident follow-ups)

The 2026-07-09 diagnosis found the nightly batch dead since 6/4 (unbounded
`vehicle_positions` scans — fixed) and GTFS stale since 5/31 (the weekly
reload was a laptop launchd job, retired with the laptop). These items
prevent recurrence; the catch-up re-derivation itself is an ops runbook in
the fixing PR, not a NOTES item.

- **NOTES-89 GTFS reload automation on the VM.** Weekly
  `wmata-gtfs-reload.service`/`.timer` running
  `scripts/reload_gtfs_complete.py`, mirroring the retired laptop launchd
  cadence (Sundays 08:00 UTC). Without it, schedule staleness recurs at
  every WMATA service change.
- **NOTES-90 Feed-expiry alarm in `/api/gtfs/freshness`.** Compare
  `feed_info.feed_end_date` to Eastern today and surface expired/expiring
  state — turns silent schedule staleness into a visible signal.
- **NOTES-91 Nightly-batch failure alerting.** Collector dead-man ping
  shipped (PR #173, `src/deadman.py`); the batch-alerting half is
  superseded by the Path 2a migration to manual laptop derivation — see
  item body.
- **NOTES-93 Loud zero-file replay.** `replay_archive_to_state` exits 0
  when no archive files match the date — make it an error; the silent
  no-op masked missing files during the 7/18 fold-in.
- **NOTES-94 VP-path dead-man coverage.** Second healthcheck ping from
  `_save_vehicle_positions` so a VP-only failure can't starve
  proximity-source derivation while the TU check stays green. Subsumed
  by NOTES-95's staleness alarm if the rewrite lands first.
- **NOTES-95 Stateless-collector rewrite (Path 2a, second half).** VM
  becomes poll → JSONL → S3 + staleness alarm; no Postgres, smallest
  tier; laptop pulls from S3. Carries four deferred items as acceptance
  criteria (staleness alarm, cutover job inventory, downsize, retention
  chores replaced by upload). Needs its own spec/plan cycle.

### Independent of the redesign

- **NOTES-81 Phantom vehicle-reported timestamps.** ~2.26M
  `vehicle_positions` rows carried October-2025 timestamps predating
  collection; investigate scope + add a collector-side sanity guard.
- **NOTES-82 Redundant vehicle_positions indexes.** 9 indexes on the
  hottest write path; 3 single-column ones are composite-shadowed —
  measure usage and drop the dead ones. 2026-07 incident note: the
  derive pipelines now bound their scans to use `idx_route_timestamp`;
  do NOT add a `(route_id, trip_start_date)` index — it would deepen
  the write amplification this item exists to reduce.
- **NOTES-88 `/api/routes` latency cliff over the SSH tunnel.** The
  scorecard endpoint times out (>90s) when the DB is reached via the
  NOTES-48 tunnel — almost certainly a per-route N+1 amplified by the
  ~9ms network RTT that was free on the old local socket. Blocks remote
  use only (Overview + RouteList); fix before public deploy. No longer
  blocks dev or NOTES-84 now that dev runs on a local socket.
- **NOTES-83 Blank RouteDetail visual baselines.** Both
  `routedetail-d72-chromium-{darwin,linux}.png` are blank white 1280×720
  images — the CI visual gate for RouteDetail asserts that a blank page
  renders blank. Diagnose the crash, regenerate real baselines
  (regen step is user-run).

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
1. ~~AWS CLI + IAM credentials → **S3 off-box backups** + **automatic block-disk
   snapshots**.~~ **DONE 2026-06-09** (see the S3-backups/disk-snapshots PR and
   `docs/DEPLOYMENT.md` §5.2 + §5.5). Bucket `wmata-dashboard-backups` (us-east-1,
   versioned, public access blocked); least-privilege IAM user `wmata-vm-backup`
   (PutObject/GetObject on the `wmata-db-backups/`+`wmata-vp-archive/` prefixes,
   ListBucket, **no DeleteObject**); AWS CLI v2 + key in the `wmata-backup`
   `EnvironmentFile`; weekly `pg_dump` upload validated (2.0 GiB); 90-day +
   30-day-noncurrent lifecycle; daily auto-snapshots on `wmata-pgdata` at
   08:00 UTC. Note: the tier-3 archive bucket var is `S3_ARCHIVE_BUCKET`
   (distinct from `S3_BACKUP_BUCKET`); the one bucket serves both via separate
   prefixes, and the IAM policy already grants the archive prefix.
2. ~~**Retention timers** (`wmata-archive-positions`, `wmata-window-derived`) —
   deferred because they DELETE data: the tier-3 30-day window would drop the
   oldest ~4 days of `vehicle_positions` on first run (data starts 2026-05-02),
   and it archives to S3. S3 is now in place (item 1 done), so the only
   remaining gate is explicit sign-off — set `S3_ARCHIVE_BUCKET`, dry-run
   first, then enable.~~ **DONE 2026-06-10** — Fixed the latent `uv run`→venv-interpreter
   bug in both service units (`ExecStart` now uses `.venv/bin/python3`, matching
   the live units' cutover fix); added `archive/vehicle_positions/` to
   `ReadWritePaths` in `wmata-archive-positions.service` (the staging parquet
   dir the script writes before S3 upload); pinned both timer `OnCalendar` lines
   to `America/New_York` (server clock is UTC). Enablement runbook added to
   `docs/DEPLOYMENT.md` §5.6 with dry-run commands (run as `sudo -u wmata`), first-run
   expectations (tier-3 archives ~4 days; tier-2 is a no-op until 2027), and
   pointer to `docs/DEPLOY.md` §2 for the cp-units + daemon-reload step. Sign-off
   given 2026-06-10; enablement performed same day per the runbook. PR #166
   (same day) fixed `KEY_PREFIX` to match the IAM grant (`wmata-vp-archive/`,
   not `vehicle_positions/`) — caught in pre-enablement review; the dry-run
   can't see S3-permission mismatches. **First run executed + verified
   2026-06-10:** 10,030,782 rows / 16 UTC dates archived to S3 and deleted,
   VACUUM clean, post-run dry-run reports zero expired rows; both timers
   active (04:00 / 04:30 ET). The run surfaced ~2.26M rows with phantom
   October-2025 timestamps (now NOTES-81) and motivated NOTES-82 (9-index
   write amplification). Enablement also hit root-owned `.git` files from an
   earlier root-run pull (fixed via `chown`; warning added to
   `docs/DEPLOY.md` §1).
3. ~~**SSH tunnel** for the local API/frontend → cloud DB (overlaps NOTES-50).~~
   **DONE 2026-06-13.** On-demand tunnel via `bin/db-tunnel.sh` (local **5433**
   → VM 5432; 5433 deliberately avoids the local dev Postgres@14 on 5432 — the
   spec/runbook's `5432:5432` was a collision, now fixed in `docs/DEPLOYMENT.md`).
   Local `.env` `DATABASE_URL` points at `...@localhost:5433/...` with the
   local-PG14 URL preserved as a commented fallback. The three laptop launchd
   jobs (`daily-batch`, `gtfs-reload`, `retain-trip-update-state`) were booted
   out + durably disabled first (`.plist` files retained) so they can't
   double-run/delete against the VM. **Verified 2026-06-13:** `vehicle_positions`
   through the tunnel was 23s fresh (vs the local DB frozen at the 2026-06-05
   cutover); `/api/gtfs/freshness` served live VM data (126 routes) in 0.29s.
   **Caveat — see NOTES-88:** `/api/routes` (the scorecard driving Overview +
   RouteList) times out >90s over the tunnel, so the dashboard's main pages are
   not yet usable *remotely*; the tunnel itself is sound (RTT ~9ms). Dev is
   unaffected now that it runs on a local PG16 socket (the tunnel is demoted
   to ops-only) — fix before the NOTES-50 public deploy.
4. ~~**Laptop retirement (read-only soak).**~~ **DONE 2026-06-13.** There was
   *no ongoing parallel collection* — the laptop collector was stopped cleanly
   at cutover (last write 2026-06-05 01:12 UTC, `"Combined collector stopped
   successfully!"`), and the cutover already verified laptop == VM
   row-for-row. The 7-day read-only soak (laptop DB as cold fallback) ran to
   completion: **verified 2026-06-08** VM collecting continuously since cutover
   (`collector_status.py` ✓ healthy; position gaps ≤91s/day; 2026-06-04 counts
   match the laptop exactly at 917,293 rows). Soak window closed 2026-06-12 with
   no fallback needed; on 2026-06-13 `sudo pmset -a disablesleep 0` released the
   lid-open pin (`pmset -g` now reports `SleepDisabled 0`), so the laptop sleeps
   normally again and is no longer a live-system dependency in any form.
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
is fine. Revisit when multi-developer, public launch, or an automated
migration cadence makes the on-demand `bin/refresh-dev-db.sh` restore
insufficient (it replaces DB branching until then).

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

Seams: the **API** config seam is **done** — `api/config.py` reads
env-driven CORS (`CORS_ALLOW_ORIGINS`, dev defaults to `["*"]`), so the
API is deploy-ready. The **frontend** `VITE_API_URL` seam is **deferred
to this item** (still hardcoded for local dev). The recommended deploy
**co-locates API + DB in-region**, which resolves NOTES-88 (the
`/api/routes` N+1 only bites over the high-latency tunnel — back on a
local/in-region socket it disappears).

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


## NOTES-81. Phantom vehicle-reported timestamps in vehicle_positions

**Severity: low (data hygiene; the rows are preserved in S3).**

The 2026-06-10 first run of the tier-3 retention job surfaced six UTC
dates from **October 2025** in `vehicle_positions` — 2025-10-12, -16,
-18, -19, -20, -21, totaling ~2.26M rows (2025-10-20 alone had 1.53M
rows, more than any real collection day) — despite collection starting
2026-05-02. The `timestamp` column stores the GTFS-RT vehicle-reported
GPS-fix time, which is unvalidated: stale AVL clocks produce timestamps
months in the past. All six dates were archived to
`s3://wmata-dashboard-backups/wmata-vp-archive/2025-10-*.parquet` and
deleted from Postgres by the retention job, so the live table is clean
*today* — but nothing stops new phantom rows from accumulating.

Work:
1. **Collector-side sanity guard** — reject (or store with a flag) any
   vehicle timestamp more than a few hours away from collection time
   (`collected_at` exists for exactly this comparison). Log a counter so
   feed-quality regressions are visible in `collector_status.py`.
2. **Check downstream contamination** — per-date pipelines only process
   recent service dates, so the phantom dates were almost certainly
   never derived into `stop_events`; verify with a quick query against
   `stop_events`/`runs` for those dates and note the result here.
3. Optional forensics: the archived parquet files preserve the rows if
   the "which vehicles / which collection days" question ever matters.

### Dependencies

Independent.

---

## NOTES-82. Redundant indexes on vehicle_positions

**Severity: low (write amplification + maintenance cost).**

Production `vehicle_positions` carries **9 indexes**; the model defines
all of them, so this is design debt, not drift. Three single-column
indexes (`ix_vehicle_positions_vehicle_id`, `_route_id`, `_trip_id`,
from `index=True`) are shadowed by the composites
(`idx_vehicle_timestamp`, `idx_route_timestamp`, `idx_trip_timestamp`)
whose leading column serves the same lookups. `_collected_at` usage is
unknown. Costs observed 2026-06-10: every collector insert (~1M
rows/day) maintains all 9; the post-retention VACUUM index sweep — the
dominant cost of the nightly job's first run — scanned all 9.

Work:
1. Measure on the VM after ≥1 week of normal traffic:
   `SELECT indexrelname, idx_scan FROM pg_stat_user_indexes WHERE
   relname = 'vehicle_positions';` (stats accumulate since the last
   reset — confirm the window before trusting zeros).
2. Drop confirmed-unused indexes via the migration ritual
   (`docs/MIGRATIONS.md`): remove `index=True` in `src/models.py` and
   `DROP INDEX CONCURRENTLY` on the VM in the same change.
3. Expected win: lower insert overhead and faster nightly VACUUMs;
   a few GB of disk back.

### Dependencies

Independent. Don't start before ~2026-06-17 so step 1's stats window
covers a representative week.

---

## NOTES-83. Blank RouteDetail visual-regression baselines

**Severity: medium (the CI visual gate for RouteDetail asserts nothing —
visual regressions on that page ship silently).**
**Effort: low** *(medium if the root cause is a real fixture-path crash
rather than a stale capture)*.

Both checked-in baselines
(`frontend/tests/e2e/routedetail.spec.js-snapshots/routedetail-d72-chromium-darwin.png`
and `-linux.png`) are entirely blank white 1280×720 images. The spec
(`routedetail.spec.js`) waits for the "30-Day Trend" text to be visible
and then takes a `fullPage: true` screenshot, yet the baseline is an
empty viewport-sized frame — so at capture time the page was blank and
≤720px tall. Most likely something (RouteMap/leaflet under fixtures, or
a crash after the visibility check) blanks the page, and a
`--update-snapshots` run enshrined it; CI stays green because the page
consistently re-blanks the same way. Overview, RouteList, and Segments
baselines are all healthy, so the harness itself works.

Work: (1) run the spec headed/traced locally to see what the page
actually looks like at capture time; (2) fix the crash or add the
missing fixture; (3) regenerate BOTH baseline sets per the
`frontend/README.md` procedure (macOS local + Linux via Docker).

**Subagent note:** diagnosis and the code fix are subagent-suitable;
the baseline regeneration (macOS + Docker Playwright runs) is user-run —
the subagent should document the regen commands in the PR body instead
of running them.

---

## NOTES-84. Overview editorial redesign

**Severity: medium (product value — the core "how is the network doing /
what's getting worse" question is answered only implicitly today).**
**Effort: high (multi-PR; spans IA, new map surface, and baselined pages).**

The 2026-06-10 product review found the Overview has the right
ingredients (HealthPulse, 30-day trends, contributors panel, What
changed) but renders them as a thin banner, four noisy daily-granularity
sparklines, and three equal-weight tables — nothing is a headline, and
the user must do the analyst's synthesis themselves. Rebuild the
Overview as an *editorial* page:

- **A big-number verdict** with plain-language framing ("75% on time
  this week, down 2 pts"), not a one-line banner.
- **A system map** (leaflet + `/api/routes/{id}/shapes` already exist)
  with routes colored by performance — the most direct answer to
  "where is it going badly."
- **A "movers" panel** ranking worsening routes using the existing
  `deltas` block (PR #125) — promote "getting worse" to the top fold.
- **Trend smoothing** — 7-day rolling line with daily points ghosted,
  replacing the raw daily squiggles.
- **Nav collapse** — Overview / Routes / Blocks / Targets / Schedule
  audit / Segments (`frontend/src/App.jsx`) is tool-shaped; collapse to
  roughly Overview / Routes / Diagnostics with the rest as drill-downs.

Constraint: trend framing must stay inside the post-cutover-clean window
(pre-2026-05-25 partial-day aggregates are contaminated; collection
starts 2026-05-02), so "getting worse" means weeks-over-weeks for now.

**Not subagent-suitable.** This is design work — it needs an interactive
brainstorming/design session with the user (layout, what gets demoted,
visual tone), and it invalidates the Overview/RouteList visual baselines
(regen is user-run). A subagent dispatched cold will produce another
accretion, which is the problem being fixed.

### Dependencies

NOTES-48 item 3 (SSH tunnel) so the site is viewable locally during
iteration. Sequence before NOTES-85 (don't restyle panels that are about
to be rearranged); both touch the same files, so don't stack PRs.

---

## NOTES-85. Frontend design-system pass

**Severity: low (polish — but the generic internal-tool look is a stated
user dissatisfaction).**
**Effort: medium-high (touches every component; no behavior change).**

The frontend has no design language: one hand-rolled 1,287-line
`App.css` plus ~200 inline `style={{}}` blocks scattered across
components (RouteDiagnosisPanel alone has 54). Every panel made its own
micro-decisions on color, spacing, and type, which is why the UI reads
as generic and slightly inconsistent. Recharts and leaflet are already
in the dependency tree — the gap is deliberate tokens, not libraries.

Work: define CSS custom-property tokens (color roles, spacing scale,
type scale), one chart idiom (axis/grid/tooltip conventions applied to
every recharts instance), and migrate components off inline styles.
Decide deliberately whether to stay hand-rolled or adopt a utility/
component layer — that choice is the user's.

**Not subagent-suitable.** Aesthetic decisions need the user in the
loop, and the pass invalidates all Playwright visual baselines on both
platforms (regen is user-run). The mechanical migration *after* the
tokens are agreed could be subagent work, but not the design itself.

### Dependencies

After NOTES-84 — restyling panels the redesign is about to rearrange is
wasted work, and the two would conflict on the same files (no stacked
PRs).

---

## NOTES-86. System-level weekly narrative

**Severity: low.**
**Effort: medium (the pattern already exists end-to-end for routes).**

Every Overview surface speaks in metric acronyms (OTP, EWT, bunching)
with no translation into consequences — "EWT 73s" doesn't drive
anything home; "riders on frequent routes waited about a minute longer
than scheduled, 12% worse than two weeks ago" does. The machinery for
this already exists: the NOTES-69 route-diagnosis narrative (PR #141)
generates LLM summaries offline via CLI, caches them in
`route_diagnosis_narrative`, and serves them read-only — Claude is never
called at request time. Extend that exact pattern to one system-level
weekly narrative ("what happened on the network this week") sourced
from `system_metrics_daily` + the contributors/deltas data, cached in a
sibling table, rendered as the Overview's lede.

**Subagent note:** the code (CLI extension, cache table, endpoint,
panel) is subagent-suitable. The narrative *generation run* (live LLM
call) and the editorial tone review of the output are user-run — the
subagent should ship the machinery with a documented generation command,
not invoke it.

### Dependencies

Independent, but the rendered placement should land after (or inside)
the NOTES-84 Overview redesign so the lede has a home; coordinate to
avoid same-file PR stacking on `Overview.jsx`.

---

## NOTES-87. Small honesty fixes in the frontend chrome

**Severity: low (trust erosion, individually trivial).**
**Effort: low.**

Three small dishonesties surfaced in the 2026-06-10 product review:

1. The header subtitle says "Real-time transit performance metrics"
   (`frontend/src/App.jsx`) but the dashboard is daily-batch — say what
   it is ("Daily bus network performance" or similar).
2. The Refresh button is a bare `window.location.reload()` — either
   refetch data in place or drop the button.
3. The Off-target panel renders empty unless `config/route_targets.yaml`
   has hand-edited overrides — the empty state should explain that (it
   partially does) or the panel should hide until targets exist.

**Subagent note:** the code is subagent-suitable, but item 1 changes the
header on every baselined page, invalidating all Playwright baselines on
both platforms — the regen step is user-run; document it in the PR body
rather than running it. Consider bundling with another
baseline-invalidating PR (NOTES-84/85) to amortize the regen.

---

## NOTES-88. `/api/routes` latency cliff over the SSH tunnel

**Severity: medium (blocks remote use only — Overview and RouteList both
depend on `/api/routes`; surfaced the moment the DB moved off the local
socket. Fix before public deploy; dev itself is unaffected now that it runs
on a local socket).**
**Effort: medium (likely a query-shape fix in `api/aggregations.py`; unknown
until the round-trip count is profiled).**

Discovered 2026-06-13 while verifying the NOTES-48 tunnel. With the API
pointed at the VM through the SSH tunnel, `/api/routes` times out (>90s, no
response), while light single-query endpoints are instant
(`/api/gtfs/freshness` returned in 0.29s). Measured tunnel round-trip
latency is only ~9ms and single aggregate queries are fast (count over
`route_metrics_daily_overlay` 782ms; a 30-day windowed query 126ms), so this
is not a slow VM or a slow link — it's almost certainly a per-route **N+1
query pattern** (iterating ~126 routes, ×metrics, ×days, plus the server-side
`deltas` block from PR #125) that was free on the old sub-millisecond local
Unix socket and explodes at ~9ms × thousands of round-trips over the network.
NOTES-49's "warm path ~37ms" figure was measured against the local socket and
silently stopped holding at cutover.

Recontextualized 2026-06-14: dev now runs on a local socket (no tunnel), so
this no longer blocks dev or NOTES-84. It becomes a co-locate-API+DB task for
the NOTES-50 public deploy.

Work:
1. Profile the endpoint's query count (SQLAlchemy echo / `pg_stat_statements`
   on the VM) to confirm the N+1 and find the loop in `api/aggregations.py`.
2. Collapse the per-route loop into one (or a few) set-based queries —
   `GROUP BY route_id` over the window rather than a query per route.
3. Re-verify over the tunnel: target a cold `/api/routes` well under a few
   seconds. The server-side 60s cache only helps the second caller; the
   first (and the cache-miss after TTL) must be fast on its own.

### Dependencies

Blocks NOTES-84 (Overview redesign) in practice — the page it redesigns
won't load remotely until this is fixed. Independent of the other items.

---

## NOTES-89. GTFS reload automation on the VM

**Severity: high (without it, schedule staleness recurs at every WMATA
service change — the 2026-06/07 incident's second root cause; feed
S1000249 expired 2026-06-20 and nothing noticed).**
**Effort: medium (systemd service + timer + runbook; the reload script
itself already exists and ran weekly on the laptop for months).**

The weekly GTFS refresh (`scripts/reload_gtfs_complete.py`) ran as a laptop
launchd job (`gtfs-reload`, Sundays 08:00 UTC) and was durably disabled
2026-06-13 during laptop retirement — but the NOTES-48 cutover never created
a VM equivalent, so the last reload anywhere was 2026-05-31. When WMATA's
service change took effect ~2026-06-21, only 32% of live GTFS-RT trip_ids
matched the stale `trips` table and proximity stop_events collapsed from
~110k/day to a few hundred.

Work: add `deployment/systemd/wmata-gtfs-reload.service` + `.timer`
(weekly, Sunday 08:00 UTC, zone-pinned like the retention timers), running
the reload as the `wmata` user with the venv interpreter (not `uv run` —
same fix as the other units). Deploy section must cite `docs/DEPLOY.md` §2
(cp units + daemon-reload); a `git pull` alone does not install timers.
Verify a manual first run end-to-end before enabling the timer.

---

## NOTES-90. Feed-expiry alarm in `/api/gtfs/freshness`

**Severity: medium (pure observability — turns silent schedule staleness
into a visible signal; NOTES-89 is the actual fix).**
**Effort: low (one endpoint change + a small frontend surface).**

`feed_info.feed_end_date` is a machine-checkable "schedule is stale" signal
that sat expired for three weeks unnoticed. Extend `/api/gtfs/freshness` to
compare `feed_end_date` (YYYYMMDD string) against Eastern today
(`src/timezones.eastern_today`) and return an explicit
`expired` / `expiring_soon` (≤7 days) / `ok` status plus the raw dates.
Surface it in the dashboard chrome (a warning banner is enough) so a stale
feed is visible without ssh.

---

## NOTES-91. Nightly-batch failure alerting

**Status: mechanism 1 shipped (collector), mechanism 2 superseded.**
`src/deadman.py`'s dead-man ping (PR #173) is wired into the collector's
per-tick trip-update commit, not the nightly batch — the collector is the
process meant to run unattended 24/7, so it's the higher-value target.
The original ask (alarm on nightly-batch death specifically) is superseded
by the Path 2a migration decision (2026-07-13; see the July 2026 incident
postmortem, draft; lands with the recovery docs): derivation becomes a
manual laptop action rather than a scheduled batch, so there's no unattended
batch process left to page on. The equivalent freshness signal for the new
architecture (an S3-upload-staleness alarm) lands with the stateless-collector
rewrite — tracked as NOTES-95.

---

## NOTES-93. `replay_archive_to_state` must fail loudly on zero files

**Severity: medium (silent no-op masks missing archive data — bit us
2026-07-18).**
**Effort: low.**

`pipelines/replay_archive_to_state.py --date D` exits 0 when NO archive
files match the date. During the recovery driver's fold-in phase this
turned "the 6/11–6/12 JSONL hasn't been rsynced yet" into a clean-looking
success, the failure guard never tripped, and derivation ran against empty
state. Exit non-zero (or require an explicit `--allow-empty`) when the
file glob matches nothing — a replay with no input is almost always an
operator error, and the whole June incident started with silent no-ops.

---

## NOTES-94. Dead-man coverage for the vehicle-positions path

**Severity: low (TU-path ping shipped in PR #173 covers whole-process
death; this covers VP-only failure).**
**Effort: low.**

`ping_healthcheck` fires from `_save_trip_updates` only. The VP fetch loop
(60 s cadence vs TU's 30 s) can fail independently — API-key issues on the
`BusPositions` endpoint, a VP-specific parse error — while TU keeps
committing and the check stays green, silently starving proximity-source
derivation (origin OTP, segment slips; see the vehicle-positions-necessity
note). Add a second healthchecks.io check pinged from
`_save_vehicle_positions` via a `COLLECTOR_VP_HEALTHCHECK_URL` env var,
same fire-on-commit-success pattern.

---

## NOTES-95. Stateless-collector rewrite (Path 2a, second half)

**Severity: medium (the current interim topology works but carries
manual chores and a full Postgres on a 2 GB box that no longer needs
one).**
**Effort: high (its own brainstorm → spec → plan cycle; do not start as
a side-task).**

Complete the Path 2a architecture decided 2026-07-13 and half-executed
2026-07-18 (see `docs/POSTMORTEM_2026-07.md`, "Architecture decision").
Target: the VM polls WMATA → writes raw zstd JSONL for both feeds →
uploads to S3 on a short cadence → pings a healthcheck. No Postgres, no
timers beyond the upload loop, smallest Lightsail tier. The laptop pulls
from S3 (not rsync-over-ssh) and derives on demand.

Four items are explicitly deferred TO this rewrite and land with it —
they are its acceptance criteria, not separate work:

1. **S3-upload-staleness alarm** (NOTES-91's successor): the dead-man
   signal becomes "newest object in the raw prefix is older than N
   minutes," replacing the DB-commit ping. Covers both feeds by
   construction, which also subsumes NOTES-94 (VP-path coverage) if the
   rewrite lands first.
2. **Cutover job inventory** (postmortem lesson #3): the rewrite spec
   must enumerate every recurring job on both machines — what runs
   where, how each is verified — and the cutover follows that checklist.
   This incident's RC2 and both disk fills were cutover-gap failures.
3. **Instance downsize** (lesson #6): once Postgres leaves the box,
   drop to the smallest tier; the 2 GB instance and its swap/OOM
   mitigations stop mattering.
4. **Upload replaces retention chores** (lesson #5): the hourly S3
   upload obsoletes the VM's 14-day JSONL buffer pruning, the
   `wmata-archive-positions` timer, the weekly `pg_dump` backup (no DB
   to back up), and `bin/pull-and-derive.sh`'s rsync+tunnel path
   (pull from S3 instead).

Also fold in: laptop-side GTFS reload cadence (NOTES-89's home after
the VM DB retires — the weekly reload targets the laptop DB, likely as
a step in the pull-and-derive flow) and the collector VP-timestamp
sanity guard (NOTES-81) if the collector is being rewritten anyway.

Interim state until this lands (documented in DEPLOYMENT.md's 2026-07-18
banner): VM = collector + backup + VP-archive timers with hc-ping
dead-man; laptop = system of record with 30-day JSONL working set;
S3 `raw-jsonl-archive/` = permanent raw store, appended by manual
laptop-side sync.

---

## NOTES-96. Replay archive support for multi-agency pipelines

**Severity: medium (blocking Plan-2 SFMTA integration until replay
archives work correctly).**
**Effort: medium (two changes to `pipelines/replay_archive_to_state.py`).**

The SFMTA comparison design (spec `docs/superpowers/specs/2026-07-21-sfmta-comparison-design.md` §3)
requires replaying raw SFMTA JSONL archives through `pipelines/replay_archive_to_state.py` to build
state for stop-events derivation. The replay script currently has two blockers:

1. **Agency-aware service-date timezone.** The script calls `_service_date_for_row` without
   an agency context, applying the hardcoded Eastern default. SFMTA service dates run on Pacific
   time. Add an agency parameter to the replay flow (pass via CLI or config) and thread it
   through `_service_date_for_row` so each agency's local tz is respected.

2. **Duplicate stop_sequence dedup.** SFMTA 511.org feed repeats `stop_sequence` values within
   a trip in ~0.24% of rows (likely due to multiple vehicles on the same block/trip, or
   feed artifacts); the collector's `_save_trip_updates` gained a dedup filter (PR #180) to
   avoid CardinalityViolation crashes. Replay archives carry raw rows by design, so
   `pipelines/replay_archive_to_state.py` must apply the same dedup before upsert.

Work:
1. Add `--agency SFMTA` (or load from a config) to the replay CLI and thread the agency
   through to `_service_date_for_row`.
2. Extract the stop_sequence dedup logic from `src/wmata_collector.py:_save_trip_updates`
   into a shared helper and call it in the replay path before upsert.

---

