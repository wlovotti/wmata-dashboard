# Postmortem: June–July 2026 data-plane outage

**Status:** Final — recovery completed and verified 2026-07-18.
**Window:** 2026-06-04 → 2026-07-18
**Author:** wlovotti, with Claude

## Summary

The nightly derivation batch silently died on **June 4** and nobody knew
for five weeks. Independently, the GTFS schedule was never refreshed
after the VM cutover, so the feed **expired June 20** and live trip
matching collapsed from ~90% to ~32%. Diagnosing and repairing these two
root causes in July triggered a cascade of secondary failures — three VM
wedges, a retention job deleting 16M rows of recoverable data, two full
disks, and a latent SQL deadlock — each traceable to a small set of
structural gaps rather than bad luck.

The VM-based recovery job was ultimately abandoned when its per-date
pace collapsed 8× (the working set outgrew the 2 GB box); the recovery
**finished on the laptop in ~3 hours** — the same work the VM had spent
five days failing to complete — which accelerated the already-decided
Path 2a migration. As of 7/18 the laptop's Postgres is the system of
record, the VM is a collector with dead-man alerting, and S3 holds the
complete raw archive.

No data was permanently lost except ~35.7 hours of collector downtime
spread across four outages. Everything else was recovered from the
system's own artifacts.

## Impact

- **Derived metrics wrong or missing 6/4–7/17** (~6 weeks): stale GTFS
  contaminated OTP/EWT/bunching for 6/13–7/11; those rows were deleted
  and fully rebuilt (verification below).
- **Permanent raw-data loss ≈ 35.7h of GTFS-RT** across four outages:
  two VM wedges (~12h + ~9h, 7/10–7/11), a full root disk (~5.5h,
  7/12), and a second full root disk (~9.2h, 7/17 — the collector
  wedged silently *during* the recovery, before the dead-man alerting
  it motivated had shipped).
- **6/11 and 6/12 fully recovered** (S3 VP parquet + JSONL replay,
  snapshot 12) — the "6/12 remains dirty" caveat from the draft closed.
- Roughly a week of operator/assistant time on diagnosis and recovery.

## Root causes

**RC1 — nightly batch death (6/4).** `vehicle_positions` grew past a
size cliff where unbounded scans stopped fitting in memory/time; the
batch died nightly from then on. Fixed by bounding scans to the
service-date window (PR #169).

**RC2 — GTFS feed expiry (6/20).** `scripts/reload_gtfs_complete.py`
existed and worked, but **was never scheduled on the VM** after the
June 5 cutover. The schedule data aged past its feed validity window.
Fixed operationally 7/12 (snapshot 15, 99.2% live match) — but nothing
was scheduled anywhere to keep it that way, so the fix was a one-time
manual reload, not a recurring one. Six days later, Path 2a (7/18)
made the laptop the system of record, meaning the reload's natural
home was now the laptop `launchd` job
(`scripts/launchd/com.wmata-dashboard.gtfs-reload.plist`) rather than
a VM systemd unit — but that job had been disabled since the 6/13
laptop retirement and stayed disabled. Result: the 7/12 fix silently
re-staled over the following month (discovered 2026-08-11,
`gtfs_snapshots.created_at` still pinned at 7/12). PR #196 fixed the
scoping (laptop, not VM) and confirmed the plist/wrapper aren't
bitrotted, but **did not load the job** — as of 2026-08-11
`launchctl list | grep wmata` still returns nothing. RC2 is not fully
closed until the Install + First-run verification steps in
`scripts/launchd/README.md` are actually run.

## Amplifiers (why two bugs became six weeks)

1. **No alerting, anywhere.** The batch failed nightly for 5 weeks and
   the feed expired for 3 with zero signal. Every subsequent problem was
   cheap to fix and expensive to *not know about*. The point was proven
   a fourth time mid-recovery: the 7/17 disk fill wedged the collector
   silently for 9.2h. (Closed for the collector path 7/17: PR #173
   dead-man ping, live in production; VP-path second check is NOTES-94.)
2. **Repo state ≠ VM state.** Two jobs that existed in the repo
   (`reload_gtfs_complete.py`, `rotate_archive.py`) were never scheduled
   on the VM. The cutover had no checklist enumerating every recurring
   job the laptop used to run. The same gap recurred in miniature twice:
   systemd units updated in git but never re-copied to
   `/etc/systemd/system/` (PR #167's fix sat uninstalled for a month).
3. **Retention assumed outages don't happen.** `cleanup_trip_update_state`
   treats un-derived rows >7 days old as noise and deletes them with no
   archive. During a batch outage that's exactly the backlog. The first
   successful batch after the fix deleted 16M recoverable June rows.
   (Recovered via raw archive replay — see below.)
4. **2GB instance.** The GTFS reload OOM'd twice and took the VM's
   network stack down with it (two reboots). The box runs collector +
   Postgres + batch + backups with no headroom for exceptional work —
   and the recovery job itself proved it: per-date wall clock degraded
   1.7h → 12.9h as the working set (trip_update_state tripled by
   replays) outgrew memory, pushing the ETA past viability.
5. **Unrotated archive filled the root disk. Twice.** 43GB of raw JSONL
   accumulated because rotation was never scheduled (amplifier #2
   again); the disk hit 100% mid-recovery on 7/12 and again on 7/17 —
   the second fill killed the recovery job outright and cost 9.2h of
   collection.
6. **Recovery tooling never exercised at scale.**
   `replay_archive_to_state.py` worked correctly but at ~7h/date
   (2,880 per-snapshot upserts); unusable for a 21-date backfill until
   rewritten as fold-then-upsert (~25 min/date, PR #171).
7. **A latent underspecified WHERE.** The derivation's `derived_at` mark
   UPDATE bound only 2 of the natural key's 3 columns; safe under normal
   timing, it deadlocked against the live collector during all-day
   backfills and cost three dates a re-run (PR #172).
8. **Silent no-ops read as success.** `replay_archive_to_state.py`
   exits 0 when zero archive files match the date. During the laptop
   recovery's fold-in phase this turned "the files haven't been rsynced
   yet" into a clean-looking success and slipped past the driver's
   failure guard (harmless that time — the derivation converged on
   re-run — but the whole June incident began with a silent no-op).
   Fixed by the loud zero-file replay change (PR #184).

## What went well

- **The raw archive saved everything.** The collector's habit of writing
  every raw TripUpdates row to zstd JSONL — kept "just in case" — turned
  a 16M-row permanent loss into a replayable inconvenience.
- **Transactional GTFS reload.** Two OOM kills and two aborted attempts
  left zero partial state; snapshot 15 committed atomically on attempt 5.
- **Idempotent, per-date pipelines.** Every failure's cure was "run it
  again." This is also what made the half-completed VM job, the
  partially-run fold-in, and the mid-recovery strategy change (VM →
  laptop) safe: pure upserts converge no matter how many times or from
  where they're run.
- **Dual-source stop_events.** Deadlocked dates still got their
  proximity-source derivation; no date came out empty.
- **The DB volume split.** Postgres on its own disk meant the root-disk
  fills never threatened data integrity.
- **Derivation is deterministic and rebuildable — now proven, not
  assumed.** The 6/17 spot check rebuilt the date from raw inputs alone
  (JSONL archive + VP rows + GTFS snapshot 12) in a scratch database and
  matched the primary exactly: 12,619 + 11,446 runs, 197,796 + 447,400
  stop_events, row-for-row on both sources.
- **GTFS snapshot versioning paid off.** Because `reload_gtfs_complete`
  never deletes superseded schedule rows, June dates could be re-derived
  against snapshot 12 weeks after snapshot 15 went live (PR #170's
  `--gtfs-snapshot-id`, extended to the rollup pipelines in PR #176).

## Architecture decision (2026-07-13, executed 2026-07-18)

The incident forced the question the project had been deferring: the
dashboard's audience is one person, so a 24/7 production data plane is
over-built for the goal. Decision: **migrate to a stateless collector**
(poll → raw JSONL → hourly S3 upload, smallest instance tier, one
staleness alarm) **with derivation and the dashboard on the laptop**,
where the dev database already lives. S3 remains the permanent raw
system of record — the role it proved this month.

The migration's first half executed ahead of schedule: when the VM
recovery job's pace collapsed (amplifier #4), the recovery moved to the
laptop (spec: `docs/superpowers/specs/2026-07-14-laptop-recovery-design.md`).
As of 2026-07-18:

- Laptop PG16 `wmata_dashboard` is the system of record; all derived
  data 5/02–7/17 lives there and serves the dashboard.
- The VM runs exactly three things: the collector (with dead-man ping),
  the weekly S3 backup timer, and the daily VP-archive timer.
  `wmata-metrics` and `wmata-window-derived` are stopped *and disabled*.
- The raw archive lifecycle is three-tier: S3 permanent
  (`s3://wmata-dashboard-backups/raw-jsonl-archive/`, 71.5 GB,
  5/03–7/18, byte-verified), laptop 30-day working set, VM rolling
  14-day buffer.
- Interim freshness is a manual `bin/pull-and-derive.sh` run; the
  stateless-collector rewrite (S3-only, no VM Postgres, smallest tier)
  is the remaining half, planned separately.

The original objection to this shape — the database outgrowing the
laptop — was retired by intervening work: the 64GB snapshots table is
gone (NOTES-72), trip_update_state is ~600MB, raw positions rotate to
S3 parquet, and the slim database (~27GB with the recovery) already
runs locally.

## Lessons → actions

| # | Lesson | Action | Tracking |
|---|--------|--------|----------|
| 1 | Silence is the outage | Dead-man alerting: collector pings healthchecks.io on each successful commit (ping *after* commit, never unconditionally — the 7/17 wedge had a live process and zero writes); S3-staleness alarm after the stateless rewrite | **Done** PR #173; VP path NOTES-94 |
| 2 | Feed validity is a clock | Feed-expiry alarm; GTFS reload becomes a laptop-side step of on-demand derivation | **Done** alarm PR #185 (NOTES-90); reload re-scoped to laptop `launchd` job + install runbook PR #196, job **not yet loaded** as of 2026-08-11 (`launchctl list | grep wmata` empty) — see `scripts/launchd/README.md` |
| 3 | A cutover needs a job inventory | Checklist in DEPLOYMENT.md: every recurring job, where it runs, how it's verified — applies to the stateless rewrite too | open (fold into rewrite spec) |
| 4 | Retention must archive before deleting un-derived data | Moot after migration (no DB retention decisions left on the VM); the S3 raw archive now precedes every delete by construction | superseded |
| 5 | Schedule the archive rotation | Done differently than planned: full archive uploaded to S3 7/18; VM keeps a 14-day buffer pruned manually until the rewrite makes upload the collector's core loop | **Done** (interim) |
| 6 | The instance is undersized | Inverted by migration: downsize to the smallest tier once Postgres leaves the box | with rewrite |
| 7 | Bind full natural keys in UPDATE/DELETE | Done (PR #172); grep for other 2-of-3-key writes | done / audit |
| 8 | Perf-test recovery tools at production scale | Done for replay (PR #171); note in MIGRATIONS.md | done |
| 9 | Zero-input runs must fail loudly | `replay_archive_to_state` exits 0 on zero matching files; make it an error (or `--allow-empty`) | **Done** PR #184 |
| 10 | Backfill must pin the schedule version end-to-end | Derive pipelines got `--gtfs-snapshot-id` in PR #170 but the rollup pipelines stayed `is_current`-only and June rollups were computed against the wrong schedule; closed by PR #176 + re-run | **Done** PR #176 |
| 11 | Time-windowed jobs make review findings perishable | The "fold-in must precede the sweep" ordering constraint was real on 7/17 (lookback window reached exactly 6/12) and vacuous on 7/18 (window moved past it). Re-derive date-window safety arguments on the actual execution date | practice, not a ticket |

## Recovery outcome (final, 2026-07-18)

- **Dates rebuilt: 6/11–7/17, all verified.** VM phase rebuilt
  6/13–6/30 before dying; laptop driver did the rest in ~3h wall
  (replay 7/02–7/03 at ~4× VM pace; derive 7/01–7/11; deadlock-trio
  re-runs; 6/11–6/12 fold-in with snapshot 12; catch-up sweep through
  7/17) with **zero failures**.
- **Runs per date, all in expected bands:** weekdays 24.1k–25.8k runs /
  126–127 routes; weekends 16.9k–20k / 104; reduced only on the outage
  dates (7/11 ≈ 5.3k, 7/12 ≈ 15k, 7/17 ≈ 1.4k partial).
- **Deadlock trio (6/15, 6/16, 6/18):** re-run with snapshot 12;
  23.7k–23.9k runs each (~11.3k trip-update), matching neighbors.
- **Fold-in (6/11, 6/12):** 24,171 and 23,717 runs; VP restored from S3
  parquet (871k + partial rows), TU replayed from the overflow archive.
- **Rebuild-verify spot check (6/17): exact match** — scratch DB rebuilt
  from raw inputs reproduced the primary row-for-row (runs and
  stop_events, both sources).
- **June rollups (6/11–6/20) re-run with snapshot 12** after PR #176
  made the rollup pipelines snapshot-aware.
- **Timers:** backup + archive-positions running; metrics +
  window-derived permanently disabled. Dead-man alerting live.
- **Disk:** VM root 48% (from 100%), pgdata 59%; laptop 72 GiB free
  after archive rotation to S3.

## Appendix A: condensed timeline

| Date (2026) | Event |
|------|-------|
| 6/04 | Nightly batch begins failing silently (RC1) |
| 6/05 | Cloud cutover: collector + Postgres to Lightsail VM |
| 6/13 | Laptop data plane retired |
| 6/20 | GTFS feed expires (RC2); trip matching → ~32% |
| 7/10 | Outage discovered; diagnosis begins; RC1 fixed (PR #169) |
| 7/10–11 | Two reload OOMs wedge the VM; two reboots; ~21h data lost |
| 7/12 02:11 ET | First healthy batch; retention deletes 16M un-derived June rows |
| 7/12 06:50 UTC | Snapshot 15 committed via laptop tunnel; 99.2% match |
| 7/12 08:46 UTC | Root disk 100% (43GB unrotated archive); collector down 5.5h |
| 7/12 | Contaminated derived rows deleted; VACUUM FULLs; recovery job launched |
| 7/12–13 | Replay rewritten (PR #171, ~7h→~25min/date); job relaunched |
| 7/13 | Deadlock found on 3 dates; root cause fixed + deployed (PR #172); Path 2a decided |
| 7/14 | VM job pace collapsing (1.7h→13h/date); laptop-recovery spec approved |
| 7/16 | Job pace briefly recovers; decision to let it run |
| 7/17 04:05 UTC | Second root-disk fill kills the job and silently wedges the collector 9.2h |
| 7/17 | VM remediated (archive split to overflow disk, swap recreated); Phase 1 sync to laptop; dead-man alerting deployed (PR #173) and verified |
| 7/18 04:04 UTC | Laptop recovery driver: all phases clean in ~3h |
| 7/18 | 6/11–6/12 folded in; 6/17 spot check exact match; VM demoted; archive drained to S3 (71.5GB verified); rollups made snapshot-aware (PR #176) |

## Appendix B: ops lessons (small, sharp, learned the hard way)

- **`pgrep` matches itself** when the pattern appears in its own command
  line via `bash -c` — quote-split the pattern or use `pgrep -f` with an
  anchored pattern you've tested.
- **`&` after a `&&` chain backgrounds the whole chain**, not the last
  command. Use `;` separators in one-shot ssh commands.
- **`sudo -u wmata bash -c` gets a non-login shell** — no `~/.local/bin`
  on PATH, so `uv` isn't found. Use `bash -lc`.
- **Glob expansion happens before `sudo`** — a glob under
  `/home/wmata/...` expands (or fails) as the ssh user; put globs inside
  the `sudo bash -c` string.
- **Two-disk `df` confusion:** the VM's root disk and `/mnt/pgdata` fill
  independently; "the VACUUM freed space" and "the disk is full" were
  both true — on different disks. Always `df -h / /mnt/pgdata`.
- **VACUUM marks pages reusable; VACUUM FULL returns them to the OS** —
  and pg's progress counters don't move mid-statement; watch
  `pg_current_wal_lsn()` deltas instead.
- **A watchdog inside an ssh session dies with the session.** Long
  remote jobs need `systemd-run`, `nohup`, or a unit — not a shell loop.
- **`caffeinate -i` blocks idle sleep, not lid-close sleep.** Two tunnel
  sessions and one reload died to laptop sleep before this was
  internalized.
- **Interactive `aws` on the VM ≠ the service's credentials.** The
  backup unit carries its IAM key in the systemd `EnvironmentFile`;
  `sudo -u wmata aws ...` reads a different (denied) context. Uploads
  ran from the laptop instead.
- **rsync `-a` preserves mtimes**, which makes `find -mtime +N` a safe
  retention predicate on a mirrored archive — but only because of `-a`.
- **Vehicle-reported timestamps are not monotonic** (NOTES-81); any
  delta-pull watermark must use a server-side monotonic column (`id`).
- **Buffered stdout hides progress:** long pipelines need
  `PYTHONUNBUFFERED=1` (and the systemd unit needs it *installed*, not
  just committed — see amplifier #2).
- **Shell guards must capture `$?` immediately.** The VM driver's
  `$(stamp)` call between command and capture clobbered every exit code
  and logged failures as "exit 0"; the laptop driver's `run()` helper
  captures first, stamps after.
