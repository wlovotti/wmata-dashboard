#!/usr/bin/env bash
# scripts/local_recovery_2026_07.sh — one-time Phase 2 driver for the July
# 2026 recovery (spec docs/superpowers/specs/2026-07-14-laptop-recovery-
# design.md rev 2b). Scope: replay 7/02-7/03; derive 7/01-7/11; re-run the
# 6/15/16/18 deadlock trio (snapshot 12); 6/11-6/12 fold-in; catch-up sweep.
# The fold-in must run BEFORE the sweep: run_daily_batch.py's --lookback-days
# window targets zero-run dates and would otherwise derive 6/12 against
# is_current (the wrong schedule for June) before the snapshot-12 fold-in
# gets a chance to claim it — and pipelines are pure upserts, so the sweep
# can't be undone by a later fold-in. Housekeeping (cleanup_trip_update_state)
# runs ONLY in the final sweep, after all backfill derivation — it deletes
# >7-day un-derived state rows.
set -uo pipefail   # deliberately NOT -e: per-date guards continue past failures

LOG="logs/local_recovery_$(date +%Y%m%dT%H%M%S).log"
mkdir -p logs
echo "Logging to $LOG"

run() {
  # run <label> <cmd...> — logs, preserves the command's exit code in $?.
  # (The VM driver's guard called $(stamp) after the command, clobbering
  # $? and logging every failure as "exit 0". No command substitution may
  # sit between the command and the rc capture.)
  local label="$1"; shift
  echo "== $(date -u +%FT%TZ) $label ==" | tee -a "$LOG"
  PYTHONUNBUFFERED=1 "$@" >> "$LOG" 2>&1
  local rc=$?
  [ $rc -ne 0 ] && echo "FAILED rc=$rc: $label" | tee -a "$LOG"
  return $rc
}

snap_args() {
  # snap_args <pipeline> <snapshot_id> — echoes --gtfs-snapshot-id N if the
  # pipeline advertises the flag (PR #170 added it selectively).
  if uv run python "pipelines/$1.py" --help 2>/dev/null | grep -q "gtfs-snapshot-id"; then
    echo "--gtfs-snapshot-id $2"
  fi
}

ALL_PIPELINES="derive_stop_events derive_stop_events_from_state aggregate_runs compute_bunching upsert_system_metrics_daily upsert_route_metrics_overlay"
STATE_PIPELINES="derive_stop_events_from_state aggregate_runs compute_bunching upsert_system_metrics_daily upsert_route_metrics_overlay"

derive_date() {
  # derive_date <date> "<pipeline names space-separated>" [snapshot_id]
  # (string list, not a bash nameref — macOS /bin/bash is 3.2, no `local -n`)
  local d="$1"; local plist="$2"; local snap="${3:-}"
  for p in $plist; do
    local extra=""
    [ -n "$snap" ] && extra="$(snap_args "$p" "$snap")"
    # shellcheck disable=SC2086
    run "$p $d${snap:+ snap$snap}" uv run python "pipelines/$p.py" --all-routes --date "$d" $extra || return 1
  done
}

echo "=== Phase A: replay 7/02-7/03 ===" | tee -a "$LOG"
for d in 2026-07-02 2026-07-03; do
  run "replay $d" uv run python pipelines/replay_archive_to_state.py --date "$d" || exit 1
done

echo "=== Phase B: derive 7/01-7/11 (snapshot 15 = is_current) ===" | tee -a "$LOG"
for d in 2026-07-01 2026-07-02 2026-07-03 2026-07-04 2026-07-05 2026-07-06 \
         2026-07-07 2026-07-08 2026-07-09 2026-07-10 2026-07-11; do
  derive_date "$d" "$ALL_PIPELINES" || echo "date $d had failures — continuing" | tee -a "$LOG"
done

echo "=== Phase C: deadlock trio (state-side re-run, snapshot 12) ===" | tee -a "$LOG"
for d in 2026-06-15 2026-06-16 2026-06-18; do
  derive_date "$d" "$STATE_PIPELINES" 12 || echo "date $d had failures — continuing" | tee -a "$LOG"
done

echo "=== Phase D: 6/11-6/12 fold-in (snapshot 12) ===" | tee -a "$LOG"
for d in 2026-06-11 2026-06-12; do
  run "vp-parquet $d" uv run python pipelines/load_vp_from_parquet.py --date "$d" || continue
  run "replay $d" uv run python pipelines/replay_archive_to_state.py --date "$d" || continue
  derive_date "$d" "$ALL_PIPELINES" 12 || echo "date $d had failures — continuing" | tee -a "$LOG"
done

echo "=== Phase E: catch-up sweep (7/12 -> now; housekeeping runs here) ===" | tee -a "$LOG"
run "daily batch sweep" uv run python pipelines/run_daily_batch.py --lookback-days 35

echo "=== Verification: runs per date ===" | tee -a "$LOG"
psql -d wmata_dashboard -c "
  SELECT service_date, count(*) AS runs,
         count(*) FILTER (WHERE source='trip_update') AS tu_runs,
         count(DISTINCT route_id) AS routes
  FROM runs WHERE service_date >= '2026-06-11'
  GROUP BY 1 ORDER BY 1" | tee -a "$LOG"
echo "FAILED count: $(grep -c FAILED "$LOG")" | tee -a "$LOG"
