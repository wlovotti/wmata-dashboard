# `stop_events_v2` cutover plan (Phase E execution)

**Status:** Active (executes Phase E of the trip-update state refactor)
**Author:** wlovotti + Claude
**Type:** Migration plan
**Date:** 2026-05-23

## Relationship to prior specs

This spec executes Phase E of the migration designed in:

- `docs/superpowers/specs/2026-05-17-trip-update-state-refactor-design.md` (original design)
- `docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md` (schema fix + Phase D paths)

Where this spec differs from the prior specs, this spec wins. Specifically:

- **Relaxes the pre-cutover bar** from "≥7 consecutive days incl. ≥1 weekend day" to "≥5 consecutive days incl. ≥1 *full* weekend (Sat + Sun)" plus stricter v2-only stability.
- **Compresses Phase F wait** from "14 days" to "7 days for everything" (legacy code deletion AND `DROP TABLE trip_update_snapshots`).
- **Adds a one-batch-cycle delay** before dropping `stop_events_v2` (rather than dropping in the same maintenance window).

The justification for each deviation is recorded in the corresponding section below.

## Goal

Switch the production source of `stop_events.source='trip_update'` rows from `pipelines/derive_stop_events_trip_updates.py` (reads `trip_update_snapshots`) to `pipelines/derive_stop_events_from_state.py` (reads `trip_update_state`), writing into the existing `stop_events` table. Drop `stop_events_v2` after one nightly batch cycle of evidence. Retire the snapshot table and legacy code 7 days post-cutover.

`source='proximity'` rows are unaffected by this work. They continue to be produced by the existing proximity derivation, written into the same `stop_events` table.

## Pre-cutover evidence

As of 2026-05-23:

| service_date | DoW | agreement | matched / total | v2-only | diverging routes |
|---|---|---|---|---|---|
| 2026-05-20 | Wed | 100.00% | 504,159 / 504,159 | 5,228 (1.04%) | 0 |
| 2026-05-21 | Thu | 99.99% | 505,441 / 505,467 | 4,976 (0.98%) | 0 |
| 2026-05-22 | Fri | 100.00% | 497,730 / 497,730 | 5,058 (1.02%) | 0 |

Interpretation: the trip_update derivation produced by v2 is byte-identical to the legacy derivation on every row both produce (per `IS NOT DISTINCT FROM` comparison on `observed_arrival_ts`, `schedule_relationship`, `deviation_sec`). The ~1.0% v2-only rows are stable across days, consistent with v2 catching trip endpoints the legacy trip-update derivation misses (NOTES-31 / trip-update origin blind spot).

## Cutover bar

| Criterion | Value |
|---|---|
| Consecutive clean days | ≥5 |
| Weekend coverage | ≥1 full weekend (Sat + Sun) |
| Per-day agreement | ≥99.5% |
| Per-route divergence | 0 routes >1% |
| v2-only fraction stability | ≤2% AND in the range 0.7%–1.3% (baseline 1.0% ± 0.3pp, observed 0.98–1.04% on 2026-05-20/21/22) |

The relaxation from the prior spec's ≥7 days reflects empirical evidence the original spec lacked. The strict weekend requirement (Sat + Sun, not "≥1 weekend day") and the new v2-only stability check compensate for the shorter day count.

## Timeline

| Date | Action |
|---|---|
| **Sat 2026-05-23** (today, before ~21:00 ET) | Open + merge monitoring PR (Section 1) |
| Sun 2026-05-24 03:00 ET | Batch runs; service date 2026-05-23 (Sat) compared |
| Mon 2026-05-25 03:00 ET | Batch runs; service date 2026-05-24 (Sun) compared |
| **Mon 2026-05-25 midday** | Pre-cutover gate check; if pass, merge cutover PR (Section 2) |
| Mon 2026-05-25 afternoon | Smoke test: re-derive 2026-05-20/21/22 into `stop_events` with `--target-table stop_events` |
| Tue 2026-05-26 03:00 ET | First nightly batch under new code (processes service date 5/25) |
| **Tue 2026-05-26 morning** | Verify Tue batch log; `DROP TABLE stop_events_v2` |
| Tue 2026-05-26 → Mon 2026-06-01 | Phase F countdown (~7 days); morning checks per Section 4 |
| **Mon 2026-06-01** | Phase F: stop collector dual-write, `DROP TABLE trip_update_snapshots`, delete legacy code, update docs |

Critical sequencing constraints:
- Monitoring PR (Section 1) MUST merge before tonight's 03:00 batch.
- Cutover PR (Section 2) MUST NOT merge before the Mon 5/25 03:00 batch completes (the Sun 5/24 comparison must land).
- `DROP TABLE stop_events_v2` MUST wait for the Tue 5/26 batch to complete and pass.
- Phase F MUST NOT execute before Mon 6/01 03:00 batch completes cleanly.

## Section 1 — Monitoring PR (Sat 2026-05-23)

**Scope:** ~30 lines, one PR.

**Changes to `pipelines/run_daily_batch.py`:**

- Add a housekeeping step after both derivation pipelines complete for each target date: invoke `compare_old_vs_new_derivation.py --date <service_date>`.
- The step's exit code does NOT fail the batch (informational only).
- Parse the comparison output. If `agreement_pct < 99.5` OR any route exceeds 1% disagreement OR v2-only count exceeds 2% of total, write a `WARN compare_old_vs_new_derivation: <reason>` line to the log.
- The warning is grep-able for morning checks.

**Verification before merge:**
- Run locally against 2026-05-22; expect 100% agreement output to be parsed correctly and no WARN line emitted.
- Run locally with a contrived agreement_pct < 99.5 (e.g., manually wrap the script's output) to confirm the WARN logic fires.

**Why non-failing:** A failed comparison shouldn't kill the downstream batch steps (system_metrics_daily, route diagnostic profile refresh, cross-route segments). The warning is visible without being destructive.

## Section 2 — Cutover PR (Mon 2026-05-25)

**Pre-merge gate (must pass to proceed):**

1. Open `logs/daily_batch_2026-05-25.log` (the batch run at 03:00 Mon processes Sun 5/24).
2. Verify Sat 5/23 and Sun 5/24 both pass: agreement ≥99.5%, 0 routes >1%, v2-only ≤2% and within ±0.3pp of 1.0% baseline.
3. If any check fails: STOP. Investigate. Defer cutover.

**Cutover PR changes (~50 lines):**

`pipelines/run_daily_batch.py`:

- Change v2 derivation step from `--target-table stop_events_v2` to `--target-table stop_events` (or remove the flag if `stop_events` is the default).
- **Remove** the legacy `derive_stop_events_trip_updates.py` step.
- **Remove** the comparison step added in Section 1.
- **Replace** the row-count guard's target: was `SELECT COUNT(*) FROM stop_events_v2 WHERE service_date = :d`; becomes `SELECT COUNT(*) FROM stop_events WHERE service_date = :d AND source = 'trip_update'`. Threshold check (non-zero) is unchanged. Add a comment explaining the load-bearing role of this guard from the 5/16→19 silent-failure incident.

`tests/`:

- Update any tests referencing `stop_events_v2` semantics to point at `stop_events` with the appropriate source filter.

**No collector change in this PR.** Collector dual-write to `trip_update_snapshots` is retained through Phase F to preserve the legacy-derivation rollback path.

**Day-of cutover sequence:**

1. Open + merge cutover PR.
2. Smoke test: manually re-derive 2026-05-20, 2026-05-21, 2026-05-22 into `stop_events`:
   ```
   uv run python -m pipelines.derive_stop_events_from_state \
       --all-routes --date 2026-05-XX --target-table stop_events
   ```
   The UPSERT writes byte-identical values (proven by 100% comparison). Confirm trip_update row counts in `stop_events` for those dates are unchanged.
3. Wait for next nightly batch (Tue 5/26 03:00).
4. Tuesday morning: verify the batch log shows clean execution under new code AND trip_update rows landed for service date 5/25 AND row-count guard passed.
5. **After** Tue batch passes (manual command, not in the cutover PR):
   ```
   psql -d wmata_dashboard -c "DROP TABLE stop_events_v2;"
   ```

The Tue-morning gap between cutover code landing and `DROP TABLE stop_events_v2` provides one batch cycle of evidence in the exact operational context (launchd-invoked, unattended) that produced the original 5/16→19 silent failure.

## Section 3 — Post-cutover monitoring (Tue 2026-05-26 → Mon 2026-06-01)

7 nightly batches under the new code. No automated alerts; daily morning eyeball check.

**Daily check (~30 seconds each morning):**

1. Tail previous night's batch log; confirm "0 (pipeline, date) failures".
2. Confirm trip_update row count for yesterday's service date is within 497k–510k (the recent baseline).
3. Spot-check `system_metrics_daily` for yesterday — agency-level OTP%, service-delivered %, EWT — within ±2pp of the prior week's same-DoW value.

**Regression signals worth investigating:**

| Signal | What it could mean |
|---|---|
| trip_update row count off by >5% from baseline | v2 is over/under-producing — derivation bug |
| `system_metrics_daily` EWT shifts >10% on weekday vs prior 4 same-DoW values | trip_update-driven metric drift |
| Bunching count shifts >20% | same class of concern |
| OTP shifts >5pp same-DoW | partially affected (proximity unchanged, trip_update changed) |
| Anomalies in specific routes that were fine pre-cutover | localized derivation bug |

Thresholds are illustrative, not formal. WMATA OTP normally varies ±2–3pp day-to-day; a ±5pp shift right after cutover on the same DoW is the kind of signal that warrants a closer look.

## Section 4 — Rollback plan

Three tiers based on how far through the migration we are.

### Tier 1: Mon 2026-05-25 → Tue 2026-05-26 morning (~24h)

**State:** cutover PR merged, smoke test passed, `stop_events_v2` still exists, legacy code + snapshots intact.

**Trigger:** any of the following:
- Smoke test pipeline exits non-zero, OR
- Smoke test changes trip_update row count in `stop_events` for 2026-05-20/21/22 (UPSERT should write byte-identical rows; any count change indicates a row-construction bug), OR
- Tue 5/26 batch log shows new pipeline failed (non-zero exit) OR the row-count guard reports zero trip_update rows for service date 2026-05-25, OR
- Any Section 3 signal trips immediately on Tue morning.

**Procedure (~10 min):**
1. `git revert` the cutover PR.
2. Next batch (Wed 5/27 03:00) runs old code.
3. `stop_events_v2` still present; comparison resumes immediately.

### Tier 2: Tue 2026-05-26 → Mon 2026-06-01 Phase F (~7 days)

**State:** `stop_events_v2` dropped. Legacy code still in repo. Collector still dual-writes to `trip_update_snapshots`.

**Trigger:** a Section 3 metric signal trips during morning checks in this window.

**Procedure (~30–60 min):**
1. `git revert` the cutover PR.
2. Run `pipelines/derive_stop_events_trip_updates.py --date <affected_date>` for each impacted service date. UPSERT overwrites v2-derived rows with old-derivation rows. Uses `trip_update_snapshots` (still populated — no archive replay needed).
3. Optional: recreate `stop_events_v2` via `scripts/migrate_create_stop_events_v2.py` and resume side-by-side validation.
4. Defer Phase F indefinitely.

### Tier 3: After Phase F (Mon 2026-06-01+)

**State:** legacy code deleted, `trip_update_snapshots` dropped, collector dual-write stopped. JSONL/parquet archive preserved.

**Trigger:** problem discovered post Phase F.

**Procedure (~several hours):**
1. Restore `derive_stop_events_trip_updates.py` and `trip_update_snapshots` schema from git history.
2. For each affected date: replay from parquet archive into a recreated snapshot table.
3. Run restored legacy derivation against the restored snapshots.
4. UPSERT into `stop_events` as in Tier 2.

The JSONL/parquet archive is the insurance policy preventing true data loss post Phase F.

## Section 5 — Phase F (Mon 2026-06-01, 7 days post-cutover)

**Pre-flight:** All 7 morning checks (Section 3) clean. No Section 3 signal tripped.

**Actions (single PR + maintenance window):**

1. `scripts/continuous_combined_collector.py`: remove the dual-write to `trip_update_snapshots`. Keep the UPSERT to `trip_update_state` and JSONL archive append.
2. Stop the collector, deploy new collector code, restart.
3. `DROP TABLE trip_update_snapshots;` (irreversible from DB; recoverable via archive).
4. Delete `pipelines/derive_stop_events_trip_updates.py`.
5. Delete `pipelines/compare_old_vs_new_derivation.py`.
6. Delete `pipelines/archive_trip_update_snapshots.py` (replaced by `rotate_archive.py`).
7. Delete `scripts/migrate_create_stop_events_v2.py` (table no longer relevant).
8. Update `CLAUDE.md`: remove the dual-derivation paragraph and silent-failure-guard exposition that no longer applies.
9. Update `NOTES.md`: close NOTES-72 (or mark its Phase E/F as complete) and link this spec.

## Acceptance criteria

The migration is considered complete when:

1. `stop_events.source='trip_update'` rows are produced by `derive_stop_events_from_state.py` writing to the canonical `stop_events` table.
2. `stop_events_v2` table dropped.
3. `trip_update_snapshots` table dropped.
4. Collector no longer dual-writes to `trip_update_snapshots`.
5. Legacy `derive_stop_events_trip_updates.py` and `compare_old_vs_new_derivation.py` removed from the repo.
6. `pipelines/run_daily_batch.py` runs cleanly with the v2-only lineup, including the row-count guard against `stop_events` trip_update rows.
7. All existing tests pass; `cd frontend && npm test` passes.
8. `system_metrics_daily` for the 7 days post-cutover shows no metric anomalies (per Section 3 signals).
9. `CLAUDE.md` and `NOTES.md` updated.

## Open considerations

- **Frontend visual regression**: the existing Playwright baselines depend on metric values that flow through `stop_events`. The 100% pre-cutover agreement says these should not visibly change. If any visual-regression test fails post-cutover, that is itself a Section 3 signal worth investigating before declaring success.
- **Period-over-period deltas** (introduced in #125): the "what changed" panel and KPI deltas compute relative to prior periods. Cutover doesn't change baseline computation, but the day-of-cutover transition is a "regime change" worth noting in the daily check for the first 2-3 days post-cutover.

## Out of scope

- Promoting `stop_events_v2` to canonical via rename (Plan B in the brainstorm). Considered and rejected — proximity row migration cost not justified vs the cleaner Plan A.
- Backfilling pre-2026-05-20 historical trip_update rows in `stop_events` with v2-derived values. Considered and rejected — 100% agreement makes this cosmetic only.
- Migrating proximity-source derivation. Out of scope; not affected by this work.
- Any API or frontend changes. None required.
