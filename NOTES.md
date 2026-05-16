# Code Review Notes

Forward-looking punch list. Completed items are removed in the same
PR that closes them — see git log and PR descriptions for history.
Item numbers (`NOTES-N`) are stable; new items take the next number.
NOTES.md edits ride on substantive PRs; standalone reconciliation PRs
are churn.

Last edited 2026-05-15. Dropped NOTES-44 — marginal-bus EWT model
— from the punch list in this PR. A prototype (closed PR #103)
implemented the closed-form `period_minutes / (2·N·(N+1))` SWT
reduction under a frequent-service gate. Review concluded the
SWT-only ranking collapses to "routes with smallest N in a
frequent-service period," equivalent to sorting by trips-per-period
ascending — no real signal beyond the input. The interesting
variation lives in AWT (observed headway variance), which the
NOTES item itself flagged but the prototype didn't deliver. Rather
than re-scoping to AWT, dropping the item — closed PR #103's
diff remains retrievable via `gh pr diff 103` if the direction
gets revived later.

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

- **NOTES-52 Overview page + primary navigation.** Add top-level
  nav (Overview / Routes / Blocks / Targets); make `/` an Overview
  page anchored by a health-pulse banner, the existing system
  trend strip, and a contributors top-5.
- **NOTES-53 "Off target" panel on Overview** *(needs NOTES-52)*.
  Rank routes by gap to their configured per-route target —
  complements the volume-weighted contributors view with a
  target-relative cut.
- **NOTES-54 "What changed" panel on Overview** *(deferred — needs
  NOTES-38 + ≥14d data)*. Week-over-week movers split into
  improvements / degradations.

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

## NOTES-52. Overview page + primary navigation

**Severity: low.**

The app has no top-level navigation today — `/` is the route table,
and `RouteDetail` / `BlockTimeline` / `RunDetail` are only reachable
by clicking into rows. There's no place to land that answers "are we
OK right now, and where should I look?" without parsing a table.

Add a real navigation bar across the header: **Overview · Routes ·
Blocks · Targets**. Move the current `RouteList` (table view) to
`/routes`. Make `/` a new `Overview` component that answers operator
questions in order:

1. **System health pulse** — a single-line status banner at top.
   "OTP 76% (▼ 2pp wk) · 14 routes below target". Background tint
   red / yellow / green based on worst-of-{OTP, SD, EWT, bunching}
   target gap.
2. **30-day system trend** — reuse `<SystemTrend>` unchanged.
3. **Where to look (top contributors)** — reuse the
   `/api/routes/contributors` endpoint and render the top 5 for the
   selected metric (default OTP), metric selector inline. One click
   → `RouteDetail`. Effectively promotes the existing
   "contributors" mode from a buried toggle to the primary
   attention-direction widget.
4. Footer link "See all routes →" to `/routes`.

Also add a `/blocks` index page (today blocks are only reachable
from the `RouteDetail` Blocks tab) that lists active blocks ranked
by trip count or cascade lateness. The Targets tab reads
`config/route_targets.yaml` and renders system defaults + per-route
overrides; editing stays git-only for now (NOTES-47's design
explicitly allowed this).

### Dependencies

Independent. Pairs with the landing-page declutter (PR #101) —
either order works, but doing the declutter first means less to
throw away if this item slips.

---

## NOTES-53. "Off target" panel on Overview

**Severity: low.**

Augments the Overview page (NOTES-52) with an additional panel
ranking routes by their **gap to per-route target** for the selected
metric. Distinct from "Where to look" (volume-weighted contribution):
this one is target-relative percentage gap, which a manager
monitoring SLA commitments wants to see independently from
system-wide blast radius — a small-volume route can be far off
target without showing up as a big contributor, and vice versa.

Render: ranked list of "Route 30N · OTP 62% · -13 pp below target"
with the metric selector shared with the contributors panel. Show
only routes with a configured per-route override (not those falling
through to the system default) — otherwise the panel ranks the same
routes the contributors panel does. If no per-route overrides are
set, the panel collapses to an empty state: "Set per-route targets
in `config/route_targets.yaml` to populate this view."

Backend already exposes the needed data: the `targets` block on
`/api/routes` and the `reference_value` / `reference_source` fields
on `/api/routes/contributors` both land via PR #99.

### Dependencies

NOTES-52 (Overview shell). NOTES-47 (per-route targets — closed
PR #99) already provides the data plumbing.

---

## NOTES-54. "What changed" panel on Overview

**Severity: low (deferred — needs ≥14 days of data).**

Augments the Overview page (NOTES-52) with a panel showing
week-over-week movers: the top routes whose OTP / SD / EWT /
bunching changed most vs the prior 7-day window. Split into two
sub-lists — "Improvements" and "Degradations" — so positive movement
is celebrated alongside negative.

**Deferred** until NOTES-38 (period-over-period deltas on every KPI)
lands. This panel is a thin renderer over that endpoint and has no
useful content without it. NOTES-38 itself is deferred for a
data-window reason: 7-vs-prior-7 needs ≥14 days of stop_events /
runs data before deltas survive thin-data suppression on most
routes. Production data started 2026-05-02; today is day 13 (so
tomorrow is the earliest possible landfall for NOTES-38, then this
item becomes implementable).

### Dependencies

NOTES-52 (Overview shell). NOTES-38 (period-over-period deltas —
deferred).

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

