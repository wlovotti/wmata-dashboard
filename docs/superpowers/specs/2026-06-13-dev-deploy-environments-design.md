# Dev/Prod Environment Design for the WMATA Dashboard

**Date:** 2026-06-13
**Status:** Approved (brainstorming) — pending implementation plan
**Supersedes:** the SSH-tunnel-as-dev-DB-connection approach shipped with
NOTES-48 item 3 (the tunnel is retained but demoted; see §6).

## 1. Context & problem

The cloud migration (NOTES-48) is complete: the **data plane** — collector +
PostgreSQL 16 + nightly batch — runs 24/7 on an AWS Lightsail VM. The web tier
(FastAPI + React/Vite) still runs only locally. After cutover, the local dev
loop was wired to the VM over an SSH tunnel (`bin/db-tunnel.sh`, local 5433 →
VM 5432). That works but feels wrong, and verification surfaced why: it points
the dev app directly at the irreplaceable production database, with no buffer
between experiments and the live dataset, and it drags network latency into
the hot loop (NOTES-88: `/api/routes` times out >90s over the tunnel).

This design defines a sustainable dev environment and the production-deploy
on-ramp for a **solo developer** who expects **ongoing schema churn** (new
metrics/features will change the schema; it is not rock-solid today).

## 2. Reframe — why "typical" advice only half-applies

A standard cloud-DB web-app dev flow assumes the database is **reproducible**:
seed dev/staging from fixtures or migrations and wipe them freely. This project
violates that assumption in the way that matters most, while compensating in
another:

- **The production *data* is irreplaceable.** It is continuously collected from
  the WMATA real-time feed, which has no replay window — any gap is permanent.
  The dataset *is* the product. You cannot reseed it.
- **The web tier is read-only.** The API only ever *reads* the database; the
  only writers are the collector and the nightly batch/migrations. So the
  usual fear — "dev accidentally writes to prod data" — is structurally absent.
- **There is no PII.** WMATA positions/arrivals are public transit data, so the
  standard reason teams avoid "restore prod into dev" (leaking personal data)
  does not apply. Restoring prod into dev is unusually safe here.

The risk model is therefore inverted from a typical app. The two real risks are
(a) a schema migration or ops slip damaging the one dataset, and (b) the
collector stopping. The environment design optimizes around *those*.

## 3. Goals & non-goals

**Goals**
- A fast, fully local dev loop that runs against realistic, prod-shaped data.
- Keep the irreplaceable production dataset out of the dev loop entirely.
- Make schema-change rehearsal a one-command, low-risk, repeatable operation.
- Leave a clean, near-zero-cost on-ramp to public deployment (NOTES-50).
- Cost: ~$0 of new spend.

**Non-goals (deliberately scoped out — YAGNI until a trigger fires)**
- No standing/always-on staging server.
- No Docker requirement (see §3.1).
- No CI auto-deploy pipeline.
- No auth, CDN, custom domain, or public hosting yet (all NOTES-50).
- No managed-Postgres / DB-branching migration yet (NOTES-49 / §7).

### 3.1 Decisions locked during brainstorming

1. **Near-term goal:** a reliable *personal* dev loop + dataset protection.
   Public deploy stays deferred (NOTES-50).
2. **Dev data strategy:** develop against a **local copy refreshed on demand**
   from a recent prod snapshot — not live prod, not synthetic fixtures.
3. **Local Postgres:** **upgrade local dev from PG14 → PG16** to match CI and
   prod, so prod snapshots restore cleanly and the version-skew constraint
   disappears.
4. **Environment model:** **Approach A + on-demand scratch.** Two real
   environments (local dev, VM prod). "Staging" is an on-demand throwaway
   database materialized only when rehearsing a migration — not a standing
   system.
5. **Scratch isolation:** **all-native** — the scratch DB is a separate
   database in the same local PG16 cluster, **not** Docker. Rationale: the
   scratch DB exists to validate migration *logic against real data*; macOS↔
   Linux OS-level fidelity (collation/build) is already a dev-vs-prod fact
   regardless of scratch, and **CI (PG16 on Linux) is the true prod-parity
   gate** for that. Adding Docker only for scratch would create a second
   Postgres setup that doesn't even match the dev DB. Docker is the right move
   *only* if the entire local stack is containerized for cross-machine/
   multi-developer reproducibility — a coherent future choice, not a now one.
6. **Config separation:** pull the env-config portion of NOTES-50 forward now
   (it is near-free and unblocks everything later).

## 4. Design

### 4.1 Environment topology

Two environments, defined by what durable state each holds:

- **Production (data plane)** — the Lightsail VM: collector + PostgreSQL 16 +
  nightly batch, 24/7. The only environment holding irreplaceable state. The
  web tier is **not** deployed here yet (NOTES-50).
- **Development (laptop)** — Vite + API + local PostgreSQL 16 holding a recent
  restore of prod. Fully self-contained; nothing in the dev loop touches the
  VM.

"Staging" is not a standing system — it is the **on-demand scratch database**
(§4.4) used to rehearse migrations.

Source-of-truth split: **prod is the source of truth for *data*; git is the
source of truth for *code + schema*.** Dev consumes a snapshot of the former
and the live state of the latter.

### 4.2 Local Postgres 14 → 16 upgrade (one-time)

- Install `postgresql@16` (Homebrew); stop `postgresql@14`; start `@16` on the
  conventional local port **5432**.
- Recreate the `wmata_dashboard` database (local trust auth, owned by the OS
  user — preserving the current passwordless local connection style).
- Load data via the refresh script (§4.3).
- The frozen 2026-06-05 copy currently on local PG14 is already preserved in S3
  and on the VM, so it is safe to discard. PG14 may be left installed-but-
  stopped or removed.
- Result: **local = CI = prod = PostgreSQL 16.** The "never restore 16→14"
  footgun is eliminated.
- The tunnel keeps using local port **5433**; the dev DB stays on **5432** — no
  collision.

### 4.3 Dev data refresh — `bin/refresh-dev-db.sh`

One script; disposable target; prod snapshot as source.

```
bin/refresh-dev-db.sh                # slim (default): drop+recreate local `wmata_dashboard`, restore S3 dump WITHOUT the raw-feed tables
bin/refresh-dev-db.sh --full         # include raw-feed tables (vehicle_positions, trip_update_state) so the pipeline can run
bin/refresh-dev-db.sh --prune-gtfs   # additionally delete is_current=False GTFS history after restore (claws back ~8 GB; transient spike + VACUUM)
bin/refresh-dev-db.sh --scratch      # restore into `wmata_dashboard_scratch` — leaves the dev DB untouched (combine with --full for pipeline rehearsal)
bin/refresh-dev-db.sh --from-vm      # fresh pg_dump over the tunnel instead of S3 (for today's data)
```

- **Default source: the weekly S3 dump** under
  `s3://wmata-dashboard-backups/wmata-db-backups/` (`pg_dump -Fc`, ~2 GiB) →
  `pg_restore --no-owner --no-privileges` into a freshly dropped+recreated
  target DB. Fully decoupled from prod; no tunnel involved.
  - Prerequisite: local AWS credentials with `s3:GetObject` (+ `ListBucket`) on
    that prefix (the existing `wmata-vm-backup` IAM grant covers it, or the
    admin identity). One-time setup.
  - The script resolves "latest" by listing the prefix and selecting the most
    recent object (exact key-naming confirmed during implementation).
- **Slim is the default — disk footprint, measured.** The read-only API never
  queries the raw-feed tables (verified: `vehicle_positions` and
  `trip_update_state` appear nowhere in `api/`; they are pipeline *input* only).
  Slim excludes them at restore time via `pg_restore -L` (TOC filter), so those
  ~14 GB are **never written to disk** — no transient spike. Excluded set:
  `vehicle_positions`, `trip_update_state`, `timepoint_times`,
  `collector_heartbeats`. Footprints (from the frozen 2026-06-05 full copy;
  current VM is smaller post-retention):
  - **Full** ≈ 31 GiB · **Slim (default)** ≈ 17 GiB · **Slim + `--prune-gtfs`**
    ≈ 9 GiB.
- **Accepted consequence of slim:** a slim dev DB **cannot run the derivation
  pipeline** (no raw inputs). This is fine — pure UI/API/schema work uses slim;
  pipeline or migration-rehearsal work that touches the pipeline uses `--full`
  (or `--scratch --full`). See §4.4.
- **`--prune-gtfs`** deletes `is_current=False` rows from the versioned GTFS
  tables (`stop_times` is 88% stale history — 30.3M of 34.4M rows — that the app
  never selects, since every GTFS query filters `is_current=True`). Unlike the
  table-level slim exclusion, this is a post-restore `DELETE` + `VACUUM FULL`, so
  the full `stop_times` (~9.4 GB) is materialized transiently before the prune.
  Opt-in for that reason.
- **`--scratch`** restores into a *separate database in the same local PG16
  cluster* (`wmata_dashboard_scratch`), isolated for schema/data purposes
  (migrations are per-database). The dev DB is untouched. Honors `--full` /
  `--prune-gtfs`.
- **`--from-vm`** is the only path that uses the tunnel — a `pg_dump -Fc` over
  `bin/db-tunnel.sh` for when weekly-stale data is not fresh enough.
- The script must be safe to re-run (idempotent drop+recreate) and must refuse
  to target anything other than the known local DB names.

### 4.4 Schema-change & migration safety flow

1. **Develop** the migration + feature against the **dev DB** (apply migration,
   build feature, iterate normally).
2. **Rehearse** before prod: `bin/refresh-dev-db.sh --scratch` (add `--full` if
   the migration or its verification touches the pipeline / raw-feed tables) →
   apply the migration to the pristine scratch copy → verify with
   `scripts/check_schema_drift.py` and a pipeline smoke run. This is the
   `docs/MIGRATIONS.md` "test on restored prod data" step, now a one-command
   rehearsal that **does not disturb in-progress dev work**.
3. **Apply to prod** via the existing `docs/MIGRATIONS.md` ritual: backup →
   wrap in a transaction → `--dry-run` where available.
4. **CI (PG16) is the automated gate** — same engine as prod; green CI means
   schema parity. `bin/test-with-pg` mirrors CI locally.

### 4.5 Config separation (pulled forward from NOTES-50)

Make the same code run dev-now / prod-later without edits:

- **Frontend** (`frontend/`): introduce `VITE_API_URL`. In dev it defaults to
  the existing Vite proxy (`/api` → `http://localhost:8000`); a deployed build
  sets it to the public API origin. Today the app issues relative `/api/...`
  calls through the proxy — this adds the seam without changing dev behavior.
- **API** (`api/main.py`): move `allow_origins` and `DATABASE_URL` behind an
  env-driven settings module (e.g. `api/config.py`). CORS stays `*` in dev,
  restricted in prod. `DATABASE_URL` already comes from env.
- **`.env`:** repoint dev `DATABASE_URL` back to **local** PG16
  (`postgresql://localhost:5432/wmata_dashboard`); the tunnel leaves the hot
  loop. Update `.env.example` to document the dev vs prod profiles.

### 4.6 The tunnel's demoted role

`bin/db-tunnel.sh` is retained but only for **occasional ops**: ad-hoc prod
`psql`, and backing `--from-vm` refreshes. It is no longer the dev DB
connection. This is the direct resolution of "the tunnel isn't the right
approach" — it was never meant to be the dev loop.

### 4.7 Documented future triggers (not built now)

- **Public deploy (NOTES-50):** when wanted, §4.5's config work is already done;
  add API hosting (Fly.io/Render) + frontend hosting (Cloudflare Pages/Vercel)
  and **co-locate the API and DB in-region**, which also sidesteps the NOTES-88
  latency cliff.
- **Managed Postgres / Neon branching (NOTES-49):** the trigger to revisit is
  multi-developer, public launch, or an automated migration cadence — at which
  point copy-on-write DB branching replaces the refresh script. Note the data
  plane (24/7 collector) is what would migrate, which is why this is deferred.

## 5. NOTES / docs reconciliation (follow-up step, not this spec)

- **NOTES-88** (`/api/routes` latency): recontextualize — no longer a dev
  blocker (dev runs on a sub-ms local socket). It becomes a "fix before public
  deploy + co-locate API/DB" item. Downgrade urgency from blocker.
- **CLAUDE.md:** rewrite the load-bearing PG-version constraint — local is now
  16, so the 14-vs-16 skew note and the "never restore 16→14" warning are
  obsolete and should be replaced with the local-copy-refresh workflow.
- **`docs/MIGRATIONS.md`:** add the `--scratch` rehearsal step (§4.4).
- **`docs/DEPLOYMENT.md` / `docs/DEPLOY.md`:** document the dev-data refresh
  workflow and the tunnel's demoted role.
- **NOTES-49 / NOTES-50:** annotate with the triggers in §4.7.

## 6. Acceptance criteria

- Local PostgreSQL 16 cluster running; `wmata_dashboard` restored from a prod
  snapshot; app serves against it with no tunnel running.
- `bin/refresh-dev-db.sh` reloads the dev DB from the latest S3 dump in one
  command; the default (slim) restore excludes the raw-feed tables and lands
  ~17 GiB with the app fully functional; `--full` includes them so the pipeline
  can run; `--prune-gtfs` reaches ~9 GiB; `--scratch` produces an isolated
  `wmata_dashboard_scratch` without touching the dev DB; `--from-vm` works over
  the tunnel.
- `VITE_API_URL` and an API settings module exist; default dev behavior is
  unchanged; prod origins are configurable without code edits.
- `.env` points dev at local PG16; `.env.example` documents both profiles.
- A migration can be rehearsed end-to-end against `--scratch` and verified with
  `check_schema_drift.py` before any VM change.
- Docs/NOTES reconciled per §5.

## 7. Open questions / risks

- **Exact S3 backup key-naming** for "latest" resolution — confirm during
  implementation (the weekly `pg_dump` upload path/pattern).
- **Restore time** for the ~2 GiB custom-format dump into local PG16 — expected
  to be a few minutes for the slim default (raw-feed tables skipped at the TOC
  level, so their data is never restored); confirm it is fast enough to make
  "just re-refresh" genuinely cheap (the premise of the disposability argument).
  `--prune-gtfs` adds a `VACUUM FULL` pass — confirm that cost is acceptable.
- **Local AWS credential setup** must be documented so the refresh script is
  not first-run-when-needed.

**Resolved during review:** dev disk footprint. Measured against the frozen
2026-06-05 copy: full ≈ 31 GiB, slim ≈ 17 GiB, slim + `--prune-gtfs` ≈ 9 GiB.
Slim is the default; the user accepted that a slim DB cannot run the derivation
pipeline (use `--full` for that).
