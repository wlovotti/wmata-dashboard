# Cloud Migration Phase 1 — Lift collector + DB off the laptop (NOTES-48)

**Status:** Design approved 2026-05-28. Implements NOTES-48 (Cloud migration
phase 1). Supersedes the provider/topology specifics written in the NOTES-48
punch-list entry, which were sketched under an older, much larger memory
footprint and before the 2026-05-24 power-loss incident.

## 1. Problem & goal

The collector and Postgres both live on the dev laptop, kept alive for
lid-closed operation via `sudo pmset disablesleep 1`. The WMATA real-time feed
has **no replay window**, so any collection gap is permanent. On 2026-05-24 a
power loss killed the collector mid-tick and lost ~12.5 hours of feed — the
first concrete confirmation that the laptop is a single point of failure for
the project's most valuable artifact (continuous data since 2026-05-02).

**Goal of Phase 1:** move the database and collector onto durable, always-on
cloud infrastructure that survives power events and reboots, at hobby-project
cost. API and frontend stay local (deferred to NOTES-50).

## 2. Footprint reality (why the original NOTES-48 plan was re-scoped)

The original NOTES-48 entry assumed "~50 GB equilibrium, ≥150 GB disk." The
current shape is different:

| | Size |
|---|---|
| Total DB today (2026-05-28) | 95 GB |
| `trip_update_snapshots` (no longer written since Phase E.2, PR #151) | 63 GB |
| `stop_events_v2` (validation table, dropped/renamed in Phase F) | 4 GB |
| **Dead weight retired in NOTES-72 Phase F (~2026-06-01)** | **~67 GB** |
| **Real footprint after Phase F** | **~28 GB** |

Growth (measured over the 10 days ending 2026-05-28):

- `vehicle_positions`: ~340 MB/day (~124 GB/year) — the dominant driver, raw input.
- `stop_events`: ~220 MB/day (~80 GB/year) — foundational derived data, keep-forever.
- Total uncontrolled growth: ~570 MB/day ≈ **~200 GB/year**.

Two consequences drive the design: (1) sequence the Phase F `DROP` **before**
the data transfer so we move ~28 GB instead of ~95 GB; (2) growth is a
first-class concern — the DB is a ~28 GB base growing ~80 GB/year even after we
tame raw positions, so storage must be resizable, not fixed.

## 3. Decisions (and the reasoning behind each)

These were settled through a costed evaluation on 2026-05-28. Recorded here
because the *why* is the durable record once NOTES-48 is closed.

### 3.1 Self-hosted Postgres on a VM (not managed Postgres)

Managed Postgres cannot meet a hobby budget at this footprint. Confirmed
against current (May 2026) pricing: every managed free tier is far too small
for a 28 GB DB (Neon 0.5 GB, Supabase 0.5 GB + idle auto-pause, Aiven 1 GB,
Render 1 GB + 30-day expiry). The cheapest *paid* managed PG (DigitalOcean,
$15.15/mo) hits the budget ceiling with a 1 GB database and scales far past it
at 150 GB. Self-hosting on a cheap VM is the only sub-$15/mo path. This keeps
NOTES-48's original "self-host first" stance — but now for an explicitly
costed reason, not a deferral. Managed PG remains NOTES-49 if the operational
burden ever outweighs the savings.

### 3.2 Host: AWS Lightsail, $12/mo plan (2 GB RAM, 60 GB SSD), us-east-1

Evaluated Hetzner (cheapest, ~$5–14/mo), DigitalOcean (best beginner docs,
~$11–21/mo), AWS Lightsail (~$7–19/mo), and Oracle Always Free ($0, but ARM
capacity/idle-reclamation/single-region risk that sits poorly against a
durability goal). Hetzner is the best raw value but the user (a first-time
cloud operator who wants hand-holding) could not load the Hetzner site and
hit its US-signup friction. The deciding factors became *non-price*: US-based
billing/support, smooth onboarding, and **transferable cloud skill** — on
which AWS leads. Lightsail is AWS's deliberately-simple VPS product with
bundled transfer (no egress traps), so it avoids the complexity/egress pitfalls
of raw EC2+EBS+RDS.

The **$12/2 GB** tier (over the $7/1 GB) was chosen because the nightly batch
(`derive_stop_events*`, `aggregate_runs`) is memory-heavy and the laptop has
far more RAM than a 1 GB VM; 2 GB + swap gives comfortable headroom for a
durability-focused project at ~$60/year more.

Region: **us-east-1 (N. Virginia)** — closest to DC, minimizing latency to the
WMATA API.

### 3.3 Postgres data directory on an attached, resizable block-storage disk

Even with raw-position archival, the DB grows ~80 GB/year (`stop_events` is
keep-forever per the project's architecture), so it will exceed the instance's
included 60 GB SSD within the first year. `PGDATA` therefore lives on an
attached Lightsail block-storage disk (starts ~50 GB, $0.10/GB-month ≈ $5/mo)
so storage grows by resizing the disk — not by rebuilding the instance.

The disk is attached and `PGDATA` placed on it **during initial provisioning,
before any production data exists**, rather than starting on the included SSD
and migrating later. Moving `PGDATA` on a live Postgres (stop server, copy,
remount) is risky and exactly the kind of operation a first-time operator
should not run against the production box. Paying ~$5/mo from day one buys out
that future risk; the included 60 GB SSD holds only the OS.

### 3.4 Object storage: AWS S3 (private bucket)

Used for weekly `pg_dump` backups and the parquet position archives. S3 chosen
over Cloudflare R2 for ecosystem consistency (one bill, one console, AWS-native
learning); the cost delta vs R2 is pennies at this scale and Lightsail→S3
transfer is cheap.

### 3.5 Retention: archive raw `vehicle_positions`, keep a 90-day rolling window

Raw positions are the fastest-growing table and are an *input* to `stop_events`
(rarely needed once derived). A nightly job moves positions older than 90 days
to compressed parquet in S3, then `DELETE`s them from Postgres. 90 days covers
re-deriving a full quarter from the archive if ever needed. `stop_events` and
`runs` are **not** archived — they are the foundational kept data. NOTE: raw
positions are **not** archived anywhere today (the JSONL archive in
`archive/raw_snapshots/` holds only the trip-update feed), so this is net-new
code.

### 3.6 Sequencing: Approach A — lift first, archival fast-follow

Get off the laptop *soonest* (durability is the goal and the laptop already
failed once), in the smallest safe steps, then harden. Provisioning and
archival-code work happen in parallel now; the data transfer waits for the
Phase F gate (~2026-06-01) but that is not on the critical path because
provisioning takes longer than the ~4 days until the gate lifts.

## 4. Target architecture

```
┌──────────────── AWS Lightsail $12 plan (us-east-1, 2 GB RAM) ─────────────────┐
│                                                                               │
│  continuous_combined_collector.py ──(30s/60s)──► PostgreSQL 14                │
│      (systemd, Restart=on-failure)                  │  PGDATA on attached     │
│                                                     │  block disk (~50 GB,    │
│  run_daily_batch.py        (systemd timer, nightly) │  resizable)             │
│  pg_dump → S3              (systemd timer, weekly) ──┤                         │
│  archive vehicle_positions → S3 parquet (nightly,   │                         │
│      fast-follow)                                   ┘                         │
│                                                                               │
└──────────────────────────────┬────────────────────────────────────────────-─┘
                                │  SSH tunnel (-L 5432:localhost:5432)
                                ▼
                 Local laptop: API (uvicorn) + Vite frontend
                 (dev workflow unchanged; Postgres never exposed publicly)
                                │
                                ▼
                  AWS S3 (private): weekly pg_dump + parquet archives
```

- WMATA API key in `.env` on the VM, never in git.
- Postgres binds to localhost; reached from the laptop only via SSH tunnel.
- Lightsail firewall allows SSH (22) only, ideally restricted to the user's IP.
- SSH key auth; password auth disabled.

## 5. Migration runbook

### Phase 0 — Prep (now, parallel, zero downtime)

1. Provision the Lightsail instance ($12/2 GB, us-east-1, Ubuntu LTS) + attach
   a ~50 GB block-storage disk + create a private S3 bucket.
2. Install PostgreSQL **14** (match local 14.23). Move `PGDATA` to the attached
   disk. Configure `pg_hba.conf` for localhost/tunnel-only access. Set up SSH
   key auth and disable password login.
3. Write and test the `vehicle_positions` → parquet archival script locally.

### Phase 1 — Shrink the source (after NOTES-72 Phase F gate, ~2026-06-01)

4. On the laptop DB, run the Phase F drops: `DROP TABLE trip_update_snapshots`,
   `DROP TABLE stop_events_v2`. `pg_dump` reads only live tables, so this alone
   shrinks the transfer from ~95 GB to ~28 GB — no `VACUUM FULL` needed before
   the dump.

### Phase 2 — Cutover (short downtime, minutes)

5. Stop the laptop collector gracefully (SIGINT — the handler added in PR #129).
6. Transfer: `pg_dump -Fc | ssh vm 'pg_restore -d wmata_dashboard'` — streams
   ~28 GB with no intermediate disk.
7. Move the WMATA API key into the VM `.env`; start the collector on the VM
   under systemd; verify rows climbing.
8. Point the local API at the VM via SSH tunnel (`ssh -L 5432:localhost:5432`),
   `DATABASE_URL` → localhost.

### Phase 3 — Harden

9. Install systemd timers for the nightly batch and the weekly `pg_dump → S3`.
10. Deploy the archival cron (fast-follow; can land just before or after cutover).
11. Keep the laptop DB **read-only as a backup for ≥7 days**. Only after 7 clean
    days on the VM: `sudo pmset disablesleep 0` on the laptop and decommission
    the local DB.

## 6. Backups & durability

- Weekly `pg_dump -Fc | xz` → S3, with an S3 lifecycle rule to expire old dumps.
- Optional Lightsail automatic snapshots (cheap point-in-time-ish insurance).
- Document the restore drill (dump → `pg_restore`) in `CLAUDE.md` or a runbook
  so it is not first-attempted under pressure.

## 7. Rollback

The laptop DB stays intact and authoritative until the VM is proven. Because
`pg_dump` is a read and the laptop keeps collecting until the final moment,
there is no single irreversible cutover step. Any failure → restart the laptop
collector; worst case is the brief cutover-window gap, retried the next day.
The read-only laptop copy is retained ≥7 days as the ultimate fallback.

## 8. Verification

- Per-table `COUNT(*)` match between laptop and VM immediately post-restore.
- Collector writing on the VM (`vehicle_positions` row count climbing).
- Local API serves correctly through the SSH tunnel.
- One full nightly batch (`run_daily_batch.py`) completes on the VM with zero
  failures.
- `src/data_completeness.py` reports healthy coverage after the first full day.

## 9. Risks

- **Nightly-batch memory on a small instance.** Bulk `derive_stop_events*` /
  `aggregate_runs` are memory-heavy; the laptop has far more RAM than 2 GB.
  Mitigations: 2 GB plan + swap, `work_mem` tuning. The psycopg2+pyarrow
  streaming refactor (PR #131) already cut peak memory on the big reads.
- **Consumer upload speed** for the ~28 GB stream — possibly a couple of hours;
  run on a stable connection. (Far better than 95 GB.)
- **Phase F dependency** — the transfer waits for the "stable ≥1 week" gate
  (~2026-06-01); provisioning runs in parallel so it is not on the critical path.

## 10. Out of scope (later phases)

- Managed Postgres + automated PITR (NOTES-49).
- Deploying the API + frontend publicly (NOTES-50).
- Scaling beyond a single instance; real auth; CDN.

## 11. Cross-references

- NOTES-48 — this design implements it. The punch-list entry's provider/sizing
  specifics are superseded by §3.
- NOTES-72 Phase F — prerequisite for the shrink step (§5 Phase 1).
- NOTES-49 / NOTES-50 — explicitly deferred (§10).
