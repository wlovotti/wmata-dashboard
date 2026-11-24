#!/usr/bin/env bash
# bin/pull-and-derive.sh — Path 2a freshness, one service date at a time:
# reload stale static GTFS, then for each date fetch just that date's raw
# files from S3, load VP, replay trip updates, derive, and prune the local
# raw files no later date needs (issue #261). Run on demand.
#
#   bin/pull-and-derive.sh                                 # catch up to yesterday (both agencies)
#   bin/pull-and-derive.sh --start 2026-09-29 --end 2026-11-23
#   bin/pull-and-derive.sh --agency wmata --date 2026-09-29 --skip-housekeeping
#   bin/pull-and-derive.sh --date 2026-10-03 --agency sfmta --force   # re-derive a done date
#   bin/pull-and-derive.sh --dry-run                       # print the plan only
#   bin/pull-and-derive.sh 35                              # legacy: = --lookback-days 35
#
# Every argument is passed to pipelines/refresh_by_day.py (see its module
# docstring for the full contract). The default range, per agency, is the
# day after the latest derived service date (max runs.service_date)
# through agency-local yesterday, plus any zero-runs date in the last
# --lookback-days (default 14). Dates run oldest first, interleaved by
# agency; for each (agency, date):
#
#   1. fetch from S3 only that date's files — trip updates UTC D and D+1
#      (replay's primary + late-evening supplement), vehicle positions UTC
#      D..D+2 (the derive's VP window, local midnight D to local midnight
#      D+2, widened by the loader's 30-min timestamp guard);
#   2. load VP for those UTC days (pipelines/load_vp_archive.py --utc-date;
#      a day whose rows were pruned is reloaded whole, atomically);
#   3. replay trip updates for D (replay_archive_to_state.py; SFMTA keeps
#      --allow-empty, WMATA stays strict);
#   4. derive D (run_daily_batch.py --date D --skip-housekeeping; its
#      per-date GTFS snapshot + match-rate guard from issue #258 apply —
#      exit 3 marks the date "held" and the run continues);
#   5. prune local raw files dated before the next date's first needed UTC
#      date, but only those byte-identical in S3 (size + ETag) and recorded
#      in tu_archive_replayed_files / vp_archive_loaded_files.
#
# Peak extra disk: ~2 UTC days of TU + 3 of VP per agency (~3 GB WMATA,
# ~1 GB SFMTA). A hard failure stops that agency at the failed date (the
# other agency continues); progress is in logs/refresh_progress_<agency>.json
# so rerunning resumes there. After the last date: WMATA housekeeping once
# (run_daily_batch.py --housekeeping-only — includes WMATA's
# trip_update_state cleanup and the ~1.5 h diagnostic refresh; skip with
# --skip-housekeeping), and SFMTA's cleanup_trip_update_state once unless an
# SFMTA date failed. Neither runs between dates, so each date's replayed
# state survives until its own derive is done.
#
# Free-disk floor: a date refuses to start below --min-free-gb (default 15).
# To clear an already-full local archive: scripts/prune_local_archive.py
# (dry run by default; --apply deletes).
#
# The trip_id match-rate canary that used to run here
# (scripts/gtfs_trip_match_canary.py, still usable by hand) is superseded by
# run_daily_batch.py's per-date guard, which checks every derived date —
# not just yesterday — against that date's own resolved GTFS snapshot.
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

echo "== GTFS reload if stale (>7 days), per agency =="
# A transient WMATA (or SFMTA) API failure here must not block an
# otherwise-healthy freshness run — stale-but-valid GTFS is the status quo.
# Capture each agency's exit code and keep going. A reload landing a feed
# effective today does not hurt older dates: run_daily_batch.py resolves
# the GTFS snapshot per service date (issue #258).
gtfs_rc_wmata=0
PYTHONUNBUFFERED=1 uv run python scripts/run_gtfs_reload.py --agency wmata --max-age-days 7 || gtfs_rc_wmata=$?
gtfs_rc_sfmta=0
PYTHONUNBUFFERED=1 uv run python scripts/run_gtfs_reload.py --agency sfmta --max-age-days 7 || gtfs_rc_sfmta=$?

echo "== per-day refresh (fetch → load VP → replay → derive → prune), per agency =="
refresh_rc=0
PYTHONUNBUFFERED=1 uv run python -m pipelines.refresh_by_day ${args[@]+"${args[@]}"} || refresh_rc=$?

if [ "$gtfs_rc_wmata" -ne 0 ] || [ "$gtfs_rc_sfmta" -ne 0 ] || [ "$refresh_rc" -ne 0 ]; then
  echo "== summary: step(s) reported errors ==" >&2
  echo "  gtfs_reload wmata rc=$gtfs_rc_wmata sfmta rc=$gtfs_rc_sfmta (a transient API failure just leaves that agency's GTFS stale; rerun to retry)" >&2
  echo "  refresh rc=$refresh_rc (1 = a date, housekeeping, or cleanup failed — see the refresh summary above;" >&2
  echo "  3 = only match-rate-held dates: reload that agency's GTFS, then rerun — docs/DEPLOYMENT.md 12.1;" >&2
  echo "  2 = bad arguments). Rerunning resumes at the first unfinished date." >&2
  echo "Done (with errors — see summary above)." >&2
  exit 1
fi
echo "Done."
