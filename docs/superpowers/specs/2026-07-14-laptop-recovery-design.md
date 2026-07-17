# Laptop recovery completion — design

**Date:** 2026-07-14
**Status:** Approved (user-reviewed design; spec pending review)
**Context:** July 2026 incident recovery (see `docs/POSTMORTEM_2026-07.md`);
Path 2a architecture decision (2026-07-13). This plan starts Path 2a early:
the VM's recovery derivation job is abandoned in favor of finishing on the
laptop, which becomes the permanent home of Postgres and derivation.

## 1. Problem

The VM recovery job's per-date wall clock degraded from 1.7 h to 12.9 h
(measured from `runs.derived_at` gaps, 6/13→6/22) because the working set
outgrew the 2 GB no-swap instance. At that pace the 18 remaining dates
finish ~7/24, worsening. The laptop (local PG16, fast NVMe) can finish in
roughly a day, and under Path 2a it is the destination anyway.

## 2. Decisions (made 2026-07-14, user-confirmed)

| # | Decision | Choice |
|---|----------|--------|
| 1 | Plan scope | Recovery + interim pull; stateless-collector rewrite is a separate follow-up plan |
| 2 | VM recovery job | ~~Kill now (SIGINT), before the sync~~ **REVISED 2026-07-16: let it finish** (see Revision note below) |
| 3 | Interim raw-feed pull | rsync JSONL from VM + per-date VP `COPY` over tunnel (no B2 fast-track) |
| 4 | Local derivation cadence | Manual, on demand (`--lookback-days` self-catch-up) |
| 5 | 6/11–6/12 dirty dates | Fold into this recovery (final, optional phase step) |
| 6 | Baseline approach | Sync-then-extend (`refresh-dev-db.sh --from-vm --full`) + rebuild-verify spot check on one date |

Rationale for #6: rebuild-from-raw was priced at ~20–28 h vs ~8–12 h and is
not actually "pure" — S3 VP parquet coverage stops ~6/12 (the archive timer
archives >30-day-old rows and last ran 7/12), so raw VP for the recovery
window exists only in the VM Postgres. The purist rebuildability property
(VP as files) is deliverable only by the collector rewrite.

## 2a. Revision 2026-07-16 — let the VM job finish

The kill rationale (13 h/date, worsening) stopped holding: by 7/16 the
job had rebuilt 6/23–6/30, all passing the verification bands, at ~3 h/date
average (with high variance, 0.7–11.6 h). User decision: let it run to
completion (est. ~7/18–19 for 7/01–7/11), keeping the kill available if it
stalls again. Consequences for the phases:

- **Phase 0** becomes: wait for job completion, then verify the full
  6/13–7/11 range against the bands and capture the log's `FAILED` count.
- **Phase 1** is unchanged (sync runs against a quiet box after the job
  ends), but the rsync scope shrinks to 6/11–6/12 only — every other
  date's state travels in the dump or is already derived.
- **Phase 2** reduces to: the 6/15/16/18 re-runs (`--gtfs-snapshot-id 12`),
  the `--lookback-days` catch-up sweep for 7/12→now, and the 6/11–6/12
  fold-in. The 6/24–7/03 replay/derive loop evaporates.
- Phases 3–4 and all repo deliverables are unchanged. The dead-man
  alerting PR (NOTES-91) proceeds immediately — it never depended on the
  job's outcome.

## 2b. Revision 2026-07-17 — the disk pulled the kill-switch

Root disk hit 100% at ~04:05 UTC on 7/17 (undrained archive + logs; second
wedge in five days): the recovery job died mid-7/01 (`ENOSPC`, log truncated
mid-line at 04:16) and the collector wedged silently for 9.2 h
(04:05–13:15 UTC — permanent raw-data loss, added to the postmortem tally).
Remediation same session: ~11 GB of already-derived archive dates
(6/05–6/14) **moved** to `/mnt/pgdata/archive-overflow/` (root now 81%),
journal vacuumed, collector restarted and verified beating, 2 GB swapfile
recreated. Job log's total `FAILED` count is 3 = exactly the known
deadlock trio; no hidden failures across the entire run.

The job is NOT restarted. Final VM tally: **6/13–6/30 rebuilt and
verified; 7/01 state replayed but derivation partial.** Phase 2 scope is
therefore: replay 7/02–7/03 (rsync those dates), derive 7/01–7/11, the
6/15/16/18 re-runs, the catch-up sweep, and the 6/11–6/12 fold-in.
Note for Phase 1/4: rsync must ALSO pull `archive-overflow/` on the
pgdata volume — the archive is now split across two directories on the VM.

- `vehicle_positions` on prod covers trip_start_date 6/12 → today; every
  recovery date present (~500–770 k rows/weekday).
- The pg_dump carries `trip_update_state` rows already replayed on the VM:
  6/23 (467 k) and the three deadlock dates 6/15/16/18 (~420–440 k
  un-derived each). **These four dates need no local replay.**
- rsync scope is therefore only 6/11, 6/12, 6/24–7/03 (~12 dates,
  ~1.1 GB/date zstd JSONL). Rotation never ran on the VM, so all files
  6/05→now are still on its root disk.
- GTFS snapshots 12 (for pre-6/21 dates) and 15 (6/21+, `is_current`)
  arrive in the dump with ids matching the VM — no snapshot-id skew.
- Verification bands from the VM-rebuilt dates: ~20–24 k runs/weekday,
  ~16–18 k runs/weekend; 126 weekday / 104 weekend distinct routes;
  ~99% trip-match rate on 6/21+ (snapshot 15).
- Deadlock class from PR #172 cannot recur locally: no live collector
  writes to the laptop DB during derivation.
- Disk: ~40 GB peak local footprint vs 109 GB free.

## 4. Design

### Phase 0 — Quiesce the VM (user-run one-shot ssh)

1. SIGINT the recovery job; confirm dead; record the log's `grep -c FAILED`
   count (the log's "exit 0" lines are unreliable — known script bug).
2. Check root disk; recreate the 2 GB swapfile (incident queue item 4).
3. Collector stays running throughout.

### Phase 1 — Sync + raw pull

1. Tunnel up; `caffeinate` on; `bin/refresh-dev-db.sh --from-vm --full`
   (drops and recreates local `wmata_dashboard` — the final time this is
   acceptable; see guard change below).
2. Verify restore: DB ≈ 32 GB; runs-per-date matches the recorded VM
   numbers for 6/13–6/22; VP coverage 6/12→today; snapshots 12 and 15
   present; `is_current` rows = snapshot 15.
3. rsync `archive/raw_snapshots/` files for 6/11, 6/12, 6/24–7/03 to a
   local archive root.

### Phase 2 — Local recovery loop

A bash driver (local sibling of the VM's `phase_bc_recovery.sh`;
`PYTHONUNBUFFERED=1`, per-date `|| continue` guards **that preserve `$?`**
— fix the `$(stamp)` clobber bug from the VM script — logs to a file,
runs under `caffeinate`):

1. Per date 6/24–7/03: replay → six pipelines (snapshot 15 is
   `is_current`; no override flag).
2. 6/23 and 7/04–7/11: six pipelines only (state already present).
3. 6/15, 6/16, 6/18: `derive_stop_events_from_state` + `aggregate_runs` +
   `compute_bunching` + the two upserts, with `--gtfs-snapshot-id 12`.
4. `run_daily_batch.py --lookback-days 35` — sweeps 7/12→now plus any
   stragglers (it self-targets zero-run dates) and runs housekeeping.
   **Ordering constraint:** housekeeping includes
   `cleanup_trip_update_state`, which deletes un-derived state rows older
   than 7 days — the June footgun. The driver must not reach this step
   until steps 1–3 have derived every backfill date, and step 5's
   replay→derive must run back-to-back for the same reason.
5. 6/11–6/12 fold-in: load VP from S3 parquet
   (`s3://wmata-dashboard-backups/wmata-vp-archive/`) via a new small
   parquet→`vehicle_positions` loader script, replay TU, derive with
   snapshot 12.

### Phase 3 — Verification

- Runs-per-date within the empirical bands for all 29 dates.
- Rebuild-verify spot check: re-derive 6/17 from raw JSONL + synced VP
  into `wmata_dashboard_scratch`; diff runs/stop_events counts and
  spot-check OTP/EWT values against the synced rows.
- Trip-match rate ~99% on 6/21+ dates.
- Frontend smoke test over the recovered window.

### Phase 4 — VM demotion (interim state)

- Restart only `wmata-backup.timer` and `wmata-archive-positions.timer`
  (they protect the VP buffer — the one dataset not yet externalized).
  `wmata-metrics.timer` and `wmata-window-derived.timer` retire
  permanently; derivation never runs on the VM again.
- Drain the 43 GB JSONL backlog: delete from the VM **only** files for
  dates that have been rsync'd down AND passed Phase 3 verification
  (user-run one-shots; never before verification).
- Accepted interim leak: VM `trip_update_state` grows ~500 k rows/day
  with batch retention off — weeks of headroom; the rewrite plan owns
  the real fix.

### Repo deliverables

1. **Dead-man alerting (NOTES-91) — first PR, independent:** collector
   pings a free healthcheck endpoint each cycle; silence alerts by email.
2. **`bin/pull-and-derive.sh`** — the one-command interim loop: rsync new
   JSONL from the VM → per-date VP `COPY` over the tunnel for dates since
   the last run → replay → `run_daily_batch --lookback-days N`.
3. **`refresh-dev-db.sh` guard:** local `wmata_dashboard` is no longer
   disposable. Drop-and-recreate of the primary DB requires an explicit
   new flag; `--scratch` remains the default path for disposable restores.
4. **Local recovery driver** (Phase 2 script) — checked in as
   `scripts/local_recovery_2026_07.sh` for the record.
5. **Parquet→VP loader** for the 6/11–6/12 fold-in (checked first whether
   an existing tool already covers this).

## 5. Risks and rollback

- Tunnel drop mid-dump: rerun; the dump is read-only against the VM.
- Laptop sleep: `caffeinate` on every long step (snapshot-15 lesson).
- Until Phase 4's deletions, the VM remains a complete fallback — nothing
  destructive happens before Phase 3 verification passes.
- The dump is the only step that competes with the collector for VM I/O;
  it runs after the job is dead, on a quiet box.

## 6. Out of scope (follow-up plans)

- Stateless-collector rewrite: VP file-archiving (closes the
  rebuildability gap), TU upload loop, healthcheck consolidation,
  VM downsize to smallest tier, tunnel retirement.
- Local `stop_events` retention window (caps DB at ~30–40 GB; not urgent).
- Postmortem TBDs (fill after Phase 3) and the promised retrospective.
- NOTES fallout on migration completion: retire NOTES-49/50/88; new items
  for the rewrite plan. Fold NOTES.md edits into the PRs that close them
  (per working agreement).
