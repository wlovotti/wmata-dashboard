# Stateless collector rewrite (Path 2a, second half) — design

**Date:** 2026-08-22
**Status:** approved design, pre-implementation
**Closes:** NOTES-95; subsumes NOTES-94; folds in NOTES-81
**Prior art:** `docs/POSTMORTEM_2026-07.md` "Architecture decision";
`docs/superpowers/specs/2026-07-14-laptop-recovery-design.md`

## Goal

Finish the Path 2a architecture: the VM becomes a dumb, cheap poller —
fetch GTFS-RT for both agencies → append zstd JSONL → upload to S3 every
15 minutes → ping a dead-man check. No Postgres on the VM, no timers
beyond the collector loop itself, smallest Lightsail tier. The laptop
(system of record, sporadically on) pulls from S3 and derives on demand,
exactly as it does today — but over `aws s3 sync` instead of an SSH
tunnel, and with nothing at risk when it stays closed.

## Current state (what this replaces)

- VM `wmata-data` (`small_3_0`, $12/mo + 64 GB `wmata-pgdata` disk
  ~$6.40/mo + auto-snapshots): full Postgres 16;
  `wmata-collector.service` (TU 30 s → DB + JSONL, VP 60 s → **DB
  only**); SFMTA sidecar collector (TU/VP → sidecar DB, TU → JSONL);
  `wmata-backup.timer` (weekly pg_dump → S3);
  `wmata-archive-positions.timer` (daily VP parquet).
- Laptop freshness: manual `bin/pull-and-derive.sh` = rsync-over-ssh
  (TU JSONL) + `\copy` VP delta over `bin/db-tunnel.sh`.
- S3 permanent raw archive appended only by manual laptop-side
  `bin/prune-vm-archive.sh` runs.
- Exposure: VP rows exist **only** in the VM's Postgres until a manual
  pull; SFMTA VP never reaches the laptop at all.

## Decisions (made with the user, 2026-08-22)

1. **Upload cadence 15 min; staleness alarm ~45 min grace.** At most
   15 min of data at risk on total VM loss; outage detected within the
   hour; ~100 S3 objects/day/feed is negligible cost.
2. **One script, two systemd instances.** New
   `scripts/stateless_collector.py --agency {wmata,sfmta}` run as
   templated units `collector@wmata.service` / `collector@sfmta.service`.
   One code path; per-agency isolation and env files.
3. **Fresh `nano_3_0` + ≥1 week parallel run.** Lightsail cannot
   downsize in place, so the smallest tier requires a new instance
   anyway. New box uploads to S3 alongside the untouched old VM; after
   verified overlap, the old VM, its disk, and its snapshots are
   decommissioned. Net cost: ~$18–20/mo → $5/mo.

## Section 1 — Stateless collector (VM)

`scripts/stateless_collector.py --agency {wmata,sfmta}`:

- **Fetch + parse:** reuse `WMATADataCollector.get_realtime_trip_updates()`
  and `get_realtime_vehicle_positions()` (already agency-parameterized
  via `config/agencies/*.yaml` / `src/agency_config.py`). Never
  constructs a DB session; never calls `_save_*`, `init_db`, or the
  heartbeat table. If the constructor's coupling to DB-adjacent state
  makes that awkward, extract the fetch/parse layer rather than
  importing DB modules into the stateless path.
- **Cadences unchanged:** WMATA TU 30 s / VP 60 s; SFMTA TU 120 s /
  VP 180 s (511.org token budget unchanged).
- **Archive:** TU rows go through `src/archive_writer.py:
  JsonlArchiveWriter` in the existing row format —
  `pipelines/replay_archive_to_state.py` must consume the new files
  unchanged (byte-format compatibility is an acceptance test). **New:**
  VP rows get their own `JsonlArchiveWriter` per agency (4 streams
  total), one JSON line per vehicle per poll, fields matching the
  `vehicle_positions` columns that `bin/pull-and-derive.sh` copies
  today (minus the DB-assigned `id`).
- **Rotation:** extend `JsonlArchiveWriter` to rotate on a 15-minute
  wall boundary in addition to the existing UTC-midnight rule. Replay
  discovers per-day files by glob, so ~96 files/day/feed is compatible
  by construction. Filenames keep the per-process scheme with a
  rotation timestamp for uniqueness.
- **Upload:** on each rotation, upload the just-closed file
  (boto3 put + size verification) to the S3 layout below. Local files
  are kept ~48 h after verified upload, then deleted by the collector
  itself (no separate pruning timer). The 20 GB nano disk holds weeks
  of zstd JSONL.
- **Raw stays raw:** the NOTES-81 phantom-timestamp guard is a
  laptop-load concern (Section 3), not a collection filter.
- **Signals:** SIGINT/SIGTERM force-installed handlers (PR #129
  lesson) flush the zstd footer and upload the final partial file on
  shutdown.

## Section 2 — S3 layout and staleness alarm

Extends the existing permanent prefix; laptop pulls are `aws s3 sync`
into the same local dirs the pipelines already read:

| S3 prefix (`s3://wmata-dashboard-backups/raw-jsonl-archive/`) | Local dir |
|---|---|
| `` (root — existing WMATA TU) | `archive/raw_snapshots/` |
| `sfmta/` (existing SFMTA TU) | `archive/sfmta_raw_snapshots/` |
| `vp/` (new — WMATA VP) | `archive/vp_snapshots/` |
| `sfmta_vp/` (new — SFMTA VP) | `archive/sfmta_vp_snapshots/` |

**Alarm:** two new healthchecks.io checks (one per agency), 15-min
period / 45-min grace. The collector pings **only after an upload cycle
that shipped fresh data from both of that agency's feeds** — a wedged
VP poll fails the ping even while TU flows. This is what subsumes
NOTES-94, and it makes the healthcheck equivalent to "newest object in
the prefix is fresh" without AWS-side alarm infrastructure: process
death, disk full, credential rot, and feed wedge all converge on "no
ping." The commit-based dead-man ping retires with the DB writes; the
feed-expiry alarm (PR #185) carries over unchanged.

## Section 3 — Laptop: pull-and-derive rework

`bin/pull-and-derive.sh` keeps its name, manual cadence, and derive
semantics. New ingest half:

1. **GTFS reload first:** if the published GTFS feed is newer than the
   loaded version, run `scripts/reload_gtfs_complete.py` before replay.
   The never-loaded launchd job
   (`scripts/launchd/com.wmata-dashboard.gtfs-reload.plist`) and its
   runbook are retired.
2. **`aws s3 sync`** the four prefixes → local archive dirs. Replaces
   the tunnel-up guard, `VM_DB_URL`, both rsyncs, and the `\copy` VP
   delta. Incremental and resumable.
3. **New `pipelines/load_vp_archive.py --agency {wmata,sfmta}`:**
   parses VP `jsonl.zst` → inserts into `vehicle_positions` (WMATA DB
   and SFMTA sidecar DB respectively). Idempotency via a manifest table
   `vp_archive_loaded_files` (file path + row count + loaded_at): each
   immutable S3 file loads exactly once; re-runs are no-ops. Inserts go
   through `src/upsert_helpers.py` conventions where applicable. The
   **NOTES-81 guard** lives here: rows whose vehicle-reported
   `timestamp` falls more than 15 minutes outside the file's collection
   window (its rotation interval) are dropped and counted, with the
   drop count in the load summary — this catches the documented
   +20–24 h phantom timestamps while tolerating ordinary AVL lag. This also delivers SFMTA VP to the laptop for the first
   time (relevant to the NOTES-104 completeness-numerator cap).
4. **TU replay + derive unchanged:** same per-date
   `replay_archive_to_state.py` loop with NOTES-93 fail-loudly
   semantics, same `run_daily_batch.py` invocation.

## Section 4 — Cutover (postmortem lesson 3: job inventory)

### Job inventory after cutover

| Job | Machine | Mechanism | Verification |
|---|---|---|---|
| WMATA polling + archive + upload | new nano | `collector@wmata.service` | hc check green; new objects under root + `vp/` |
| SFMTA polling + archive + upload | new nano | `collector@sfmta.service` | hc check green; new objects under `sfmta/` + `sfmta_vp/` |
| Staleness alarm (×2) | healthchecks.io | ping-after-verified-upload | pause a unit → alert within grace |
| Feed-expiry alarm | collector (carried over) | PR #185 path | existing check |
| Local buffer pruning | new nano | collector's own 48-h cleanup | disk usage flat over a week |
| GTFS reload | laptop | step 1 of `pull-and-derive.sh` | version check in script output |
| Replay + VP load + derive | laptop | manual `pull-and-derive.sh` | script's per-step failure gates |
| **Retired:** VM Postgres, `wmata-backup.timer`, `wmata-archive-positions.timer`, `bin/db-tunnel.sh`, `bin/prune-vm-archive.sh`, launchd GTFS-reload job, 14-day VM buffer pruning | — | — | absent from new box |

### Sequence

1. Provision `nano_3_0`, deploy repo + templated units + per-agency env
   files (API keys, hc URLs, S3 creds via instance role or
   EnvironmentFile), start both instances. Old VM untouched.
2. **≥7-day overlap.** Verify: per-day VP row counts from S3 files vs
   old VM `vehicle_positions`; TU replay parity on a sample day
   (replay new-collector files into a scratch DB, compare stop_events
   counts — NOTES-96-style check).
3. Flip the laptop to the new pull path; run end-to-end at least twice
   during overlap.
4. Decommission: final VP `\copy` delta + final `pg_dump` to S3 from
   the old VM; delete instance, `wmata-pgdata` disk, auto-snapshots;
   static IP either re-pointed or replaced (decided at execution —
   nothing laptop-side may reference the IP after step 3). Update
   `docs/DEPLOYMENT.md` (remove the 2026-07-18 interim banner; new
   topology + runbook) and CLAUDE.md's freshness line.

Heavy ops steps (provisioning, unit installs, decommission, parity
psql runs) are **user-run** with commands supplied — consistent with
the VM-ops-via-one-shot-ssh practice and DEPLOY §2 unit-install rules.

## Section 5 — Testing

- **Unit (TDD):** 15-min rotation boundary; upload→verify→48-h-buffer
  lifecycle; ping gating (both feeds fresh → ping; one wedged → no
  ping); VP row serialization round-trip; loader manifest idempotency
  (double-load → no dupes); NOTES-81 bound flagging.
- **Replay compatibility:** golden-file test that a
  stateless-collector TU file is byte-format-consumable by
  `replay_archive_to_state.py`.
- **Local integration:** run the stateless collector on the laptop for
  a few live ticks (no DB needed) → replay the TU output, load the VP
  output into a scratch DB.
- **Cutover verification:** Section 4's overlap parity checks.

## Out of scope

- Any change to derive/aggregate pipelines or metrics.
- Public deploy concerns (NOTES-88, NOTES-50).
- Backfills (NOTES-102/113) — they get easier (S3 pull) but ship
  separately.
