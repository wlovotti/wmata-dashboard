#!/usr/bin/env bash
# bin/pull-and-derive.sh — Path 2a freshness, one service date at a time:
# reload stale static GTFS, then for each date fetch just that date's raw
# files from S3, load VP, replay trip updates, derive, clean old
# trip_update_state, and prune the local raw files no later date needs
# (issue #261). Run on demand.
#
#   bin/pull-and-derive.sh                                 # catch up (both agencies)
#   bin/pull-and-derive.sh --start 2026-09-29 --end 2026-11-23
#   bin/pull-and-derive.sh --agency wmata --date 2026-09-29 --skip-housekeeping
#   bin/pull-and-derive.sh --date 2026-10-03 --agency sfmta --force   # re-derive a done date
#   bin/pull-and-derive.sh --dry-run                       # plan only: no reload, no lock, no writes
#   bin/pull-and-derive.sh 35                              # legacy: = --lookback-days 35
#
# Every argument is passed to pipelines/refresh_by_day.py — its module
# docstring is the full contract (date selection incl. the raw-window
# completeness margin, progress file vs DB, failure policy, per-date
# trip_update_state cleanup). For each (agency, date), oldest first:
#
#   1. fetch from S3 only that date's files — trip updates UTC D and D+1,
#      vehicle positions UTC D..D+2 — verified by size + ETag;
#   2. load VP for those UTC days (load_vp_archive.py --utc-date);
#   3. replay trip updates for D (SFMTA keeps --allow-empty);
#   4. derive D (run_daily_batch.py --date D --skip-housekeeping
#      --exclude-date <held/failed dates>); exit 3 = "held", run continues;
#   5. delete trip_update_state before min(D, today-7) and VACUUM it;
#   6. prune local raw files dated before the next date's first needed UTC
#      date, only if byte-identical in S3 and recorded in the manifests.
#
# After the last date: WMATA housekeeping once (~1.5 h; skipped if a WMATA
# date failed, or with --skip-housekeeping) and SFMTA's 7-day
# cleanup_trip_update_state once (skipped if an SFMTA date failed).
#
# Single instance: a non-dry run holds logs/.pull-and-derive.lock.d
# (src/run_lock.py) for the whole run, GTFS reload included; a second run
# exits 75 naming the holder. bin/pull-and-derive-nightly.sh relies on this
# lock instead of taking its own.
#
# --dry-run writes nothing: it skips the GTFS reload and the lock and only
# prints the plan. --agency X reloads only X's GTFS.
#
# Free-disk floor: a date refuses to start below --min-free-gb (default 15).
# To clear an already-full local archive: scripts/prune_local_archive.py
# (dry run by default; --apply deletes).
#
# The trip_id match-rate canary that used to run here
# (scripts/gtfs_trip_match_canary.py, still usable by hand) is superseded by
# run_daily_batch.py's per-date guard, which checks every derived date
# against that date's own resolved GTFS snapshot (exit 4 = the guard could
# not run; reported as a warning).
#
# Requires: AWS credentials with read access (s3:ListBucket + s3:GetObject)
# to the four raw-jsonl-archive S3 prefixes (root, sfmta/, vp/, sfmta_vp/),
# AND — for the GTFS reload step — s3:GetObject + s3:PutObject on
# arn:aws:s3:::wmata-dashboard-backups/gtfs-archive/* and s3:ListBucket on
# arn:aws:s3:::wmata-dashboard-backups with an s3:prefix condition of
# gtfs-archive/* (ListBucket is a bucket-level action; granting it on an
# object ARN grants nothing) (a reload archives the downloaded zip there
# before loading and fails, DB untouched, if it can't — see
# scripts/reload_gtfs_complete.py), and local DATABASE_URL +
# SFMTA_DATABASE_URL (.env).
#
# Local archive dirs default to each agency config's archive_dir /
# vp_archive_dir; LOCAL_ARCHIVE, LOCAL_ARCHIVE_SFMTA, LOCAL_ARCHIVE_VP and
# LOCAL_ARCHIVE_SFMTA_VP still override them.
#
# Dates before 2026-06-14 are refused (June dates need snapshot 12 and go
# through scripts/local_recovery_2026_07.sh).
set -euo pipefail
cd "$(dirname "$0")/.."

args=("$@")
if [ "$#" -eq 1 ] && [[ "$1" =~ ^[0-9]+$ ]]; then
  args=(--lookback-days "$1")
fi

dry_run=0
agency="both"
prev=""
for a in ${args[@]+"${args[@]}"}; do
  case "$a" in
    --dry-run) dry_run=1 ;;
    --agency=*) agency="${a#--agency=}" ;;
  esac
  if [ "$prev" = "--agency" ]; then agency="$a"; fi
  prev="$a"
done

if [ "$dry_run" -eq 1 ]; then
  echo "== dry run: no GTFS reload, no lock, nothing written =="
  PYTHONUNBUFFERED=1 uv run python -m pipelines.refresh_by_day ${args[@]+"${args[@]}"}
  exit $?
fi

lock_rc=0
uv run python -m src.run_lock acquire --pid $$ --label "bin/pull-and-derive.sh $*" || lock_rc=$?
if [ "$lock_rc" -ne 0 ]; then
  echo "pull-and-derive: could not take the refresh lock (rc=$lock_rc) — see above." >&2
  exit "$lock_rc"
fi
trap 'uv run python -m src.run_lock release --pid $$ >/dev/null 2>&1 || true' EXIT
export PULL_AND_DERIVE_LOCK_PID=$$

echo "== GTFS reload if stale (>7 days): agency=$agency =="
# A transient WMATA (or SFMTA) API failure here must not block an
# otherwise-healthy freshness run — stale-but-valid GTFS is the status quo.
# Capture each agency's exit code and keep going. A reload landing a feed
# effective today does not hurt older dates: run_daily_batch.py resolves
# the GTFS snapshot per service date (issue #258).
gtfs_rc_wmata=0
gtfs_rc_sfmta=0
if [ "$agency" = "both" ] || [ "$agency" = "wmata" ]; then
  PYTHONUNBUFFERED=1 uv run python scripts/run_gtfs_reload.py --agency wmata --max-age-days 7 || gtfs_rc_wmata=$?
fi
if [ "$agency" = "both" ] || [ "$agency" = "sfmta" ]; then
  PYTHONUNBUFFERED=1 uv run python scripts/run_gtfs_reload.py --agency sfmta --max-age-days 7 || gtfs_rc_sfmta=$?
fi

echo "== per-day refresh (fetch → load VP → replay → derive → clean state → prune) =="
refresh_rc=0
PYTHONUNBUFFERED=1 uv run python -m pipelines.refresh_by_day ${args[@]+"${args[@]}"} || refresh_rc=$?

if [ "$gtfs_rc_wmata" -ne 0 ] || [ "$gtfs_rc_sfmta" -ne 0 ] || [ "$refresh_rc" -ne 0 ]; then
  echo "== summary: step(s) reported errors ==" >&2
  echo "  gtfs_reload wmata rc=$gtfs_rc_wmata sfmta rc=$gtfs_rc_sfmta (a transient API failure just leaves that agency's GTFS stale; rerun to retry)" >&2
  echo "  refresh rc=$refresh_rc (1 = a date failed, a warning (guard could not run / state cleanup)," >&2
  echo "  or end-of-run housekeeping/cleanup failed — see the refresh summary above;" >&2
  echo "  3 = only match-rate-held dates: reload that agency's GTFS, then rerun — docs/DEPLOYMENT.md 12.1;" >&2
  echo "  2 = bad arguments). Rerunning resumes at the first unfinished date." >&2
  echo "Done (with errors — see summary above)." >&2
  exit 1
fi
echo "Done."
