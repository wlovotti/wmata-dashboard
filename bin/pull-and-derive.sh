#!/usr/bin/env bash
# bin/pull-and-derive.sh — Path 2a ingest: sync fresh raw data from S3,
# load + replay locally, derive. Run on demand (manual cadence).
#
#   bin/pull-and-derive.sh          # replay+derive lookback of 14 days
#   bin/pull-and-derive.sh 35       # wider catch-up
#
# Requires: AWS credentials with read access to the four
# raw-jsonl-archive S3 prefixes (root, sfmta/, vp/, sfmta_vp/), and a
# local DATABASE_URL (.env) for the loader/replay/derive steps below.
#
# LOOKBACK_DAYS note: widening past 2026-06-13 is unsafe — June dates need
# snapshot 12 and must go through scripts/local_recovery_2026_07.sh.
set -euo pipefail

S3_BASE="s3://wmata-dashboard-backups/raw-jsonl-archive"
LOCAL_ARCHIVE="${LOCAL_ARCHIVE:-archive/raw_snapshots}"
LOCAL_ARCHIVE_SFMTA="${LOCAL_ARCHIVE_SFMTA:-archive/sfmta_raw_snapshots}"
LOCAL_ARCHIVE_VP="${LOCAL_ARCHIVE_VP:-archive/vp_snapshots}"
LOCAL_ARCHIVE_SFMTA_VP="${LOCAL_ARCHIVE_SFMTA_VP:-archive/sfmta_vp_snapshots}"
LOOKBACK_DAYS="${1:-14}"
mkdir -p "$LOCAL_ARCHIVE" "$LOCAL_ARCHIVE_SFMTA" "$LOCAL_ARCHIVE_VP" "$LOCAL_ARCHIVE_SFMTA_VP"

echo "== GTFS reload if stale (>7 days), per agency =="
# A transient WMATA (or SFMTA) API failure here must not block an
# otherwise-healthy freshness run — stale-but-valid GTFS is the status quo,
# not a reason to skip syncing/replaying/deriving fresh GTFS-RT data.
# Capture each agency's exit code and keep going, same treatment as the VP
# loaders below.
#
# NOTES-134: this gate used to run WMATA only — SFMTA's static GTFS had no
# staleness gate at all, and was reloaded only when someone remembered to
# run `reload_gtfs_complete.py --agency sfmta` by hand. Muni's 2026-08-30
# fall service change landed while the SFMTA snapshot was still "fresh" by
# this same 7-day age proxy, so the age gate alone doesn't fully close the
# gap either agency — the post-replay match-rate canary below is the
# second, stronger check for exactly that failure shape.
gtfs_rc_wmata=0
PYTHONUNBUFFERED=1 uv run python scripts/run_gtfs_reload.py --agency wmata --max-age-days 7 || gtfs_rc_wmata=$?
gtfs_rc_sfmta=0
PYTHONUNBUFFERED=1 uv run python scripts/run_gtfs_reload.py --agency sfmta --max-age-days 7 || gtfs_rc_sfmta=$?

echo "== s3 sync raw archives (scoped to the lookback window's months) =="
# The S3 prefixes hold the FULL history (they are the permanent raw
# record); an unscoped sync mirrors all of it locally — 100GB+ and
# growing — so restrict each sync to files whose YYYY-MM- name prefix
# falls inside the lookback window. Everything is excluded by default
# and only the window's months are re-included; a file deleted locally
# to reclaim disk therefore stays deleted unless the window covers it.
# Filenames start with the service date (YYYY-MM-DD.<pid>.<epoch>...),
# and an include pattern can't match the sibling sub-prefixes' keys
# (they start with "sfmta/", "vp/", "sfmta_vp/"), so the root sync
# needs no extra sub-prefix excludes.
# The loop walks the window oldest-to-newest, so months arrive in order
# and deduping against the previous iteration's month is sufficient.
month_includes=()
last_month=""
for i in $(seq "$LOOKBACK_DAYS" -1 0); do
  m=$(date -v -"${i}"d +%Y-%m)   # macOS date; this script is laptop-only
  if [ "$m" != "$last_month" ]; then
    month_includes+=("${m}-*")
    last_month="$m"
  fi
done
sync_filters=(--exclude "*")
for m in "${month_includes[@]}"; do sync_filters+=(--include "$m"); done
aws s3 sync "$S3_BASE/" "$LOCAL_ARCHIVE/" "${sync_filters[@]}"
aws s3 sync "$S3_BASE/sfmta/" "$LOCAL_ARCHIVE_SFMTA/" "${sync_filters[@]}"
aws s3 sync "$S3_BASE/vp/" "$LOCAL_ARCHIVE_VP/" "${sync_filters[@]}"
aws s3 sync "$S3_BASE/sfmta_vp/" "$LOCAL_ARCHIVE_SFMTA_VP/" "${sync_filters[@]}"

echo "== load VP archives (manifest-idempotent; phantom-timestamp guard at load) =="
# A loader failure must not take down replay+derive below — capture the
# exit code and keep going; the manifest table means a rerun of this
# script is exactly how a failed file gets retried, so surfacing the
# status here (and again in the summary at the bottom) is enough.
vp_wmata_rc=0
PYTHONUNBUFFERED=1 uv run python pipelines/load_vp_archive.py --agency wmata --archive-root "$LOCAL_ARCHIVE_VP" || vp_wmata_rc=$?
vp_sfmta_rc=0
PYTHONUNBUFFERED=1 uv run python pipelines/load_vp_archive.py --agency sfmta --archive-root "$LOCAL_ARCHIVE_SFMTA_VP" || vp_sfmta_rc=$?

echo "== replay TU archive for the lookback window (idempotent) =="
# replay_archive_to_state.py fails loudly on a zero-file match; derivation
# must never run past a replay failure (the NOTES-93 incident).
failed=()
for i in $(seq "$LOOKBACK_DAYS" -1 0); do
  d=$(date -v -"${i}"d +%F)   # macOS date; this script is laptop-only
  if ! PYTHONUNBUFFERED=1 uv run python pipelines/replay_archive_to_state.py --date "$d" --archive-root "$LOCAL_ARCHIVE"; then
    failed+=("$d")
  fi
done
if [ "${#failed[@]}" -gt 0 ]; then
  echo "Replay failed for: ${failed[*]} — aborting before derive." >&2
  echo "Upstream status at abort: gtfs_reload wmata rc=$gtfs_rc_wmata sfmta rc=$gtfs_rc_sfmta, vp_wmata rc=$vp_wmata_rc, vp_sfmta rc=$vp_sfmta_rc." >&2
  exit 1
fi

echo "== post-replay GTFS trip_id match-rate canary (NOTES-134) =="
# The age-based reload gate above is a proxy — it bounds staleness but
# can't see a service change land while the loaded snapshot is still
# "fresh" by that clock (the 2026-08-30 Muni fall service change: SFMTA's
# snapshot was < 7 days old, so the gate skipped a reload, but the feed's
# trip_ids had moved to a brand-new space). This canary recomputes the
# same feed-trip_id ∩ current-GTFS-trip_id intersection both derive paths
# gate on, for the latest agency-local service date, and fails loudly
# (nonzero exit) if it has collapsed — deliberately NOT auto-reloading or
# auto-re-deriving; see the script's docstring and the PR description for
# why fail-loud was chosen over auto-recovery. A collapsed match rate
# means derive would silently write ~0 stop_events (while exiting 0) and
# poison that date's `runs` rows (NOTES-113's failure shape), so this
# aborts before derive runs — same treatment as a replay failure above.
canary_failed=()
for agency in wmata sfmta; do
  if ! PYTHONUNBUFFERED=1 uv run python scripts/gtfs_trip_match_canary.py --agency "$agency"; then
    canary_failed+=("$agency")
  fi
done
if [ "${#canary_failed[@]}" -gt 0 ]; then
  echo "GTFS trip_id match-rate canary failed for: ${canary_failed[*]} — aborting before derive." >&2
  echo "See the canary output above for the agency/date/match-rate and the recovery steps." >&2
  echo "Upstream status at abort: gtfs_reload wmata rc=$gtfs_rc_wmata sfmta rc=$gtfs_rc_sfmta, vp_wmata rc=$vp_wmata_rc, vp_sfmta rc=$vp_sfmta_rc." >&2
  exit 1
fi

echo "== derive (self-targets zero-run dates) =="
# Echo upstream status before derive, not just in the summary below: a
# derive failure trips `set -e` right here, which would otherwise skip
# straight past the summary block and lose this information.
echo "Upstream status going into derive: gtfs_reload wmata rc=$gtfs_rc_wmata sfmta rc=$gtfs_rc_sfmta, vp_wmata rc=$vp_wmata_rc, vp_sfmta rc=$vp_sfmta_rc." >&2
PYTHONUNBUFFERED=1 uv run python pipelines/run_daily_batch.py --lookback-days "$LOOKBACK_DAYS"

if [ "$gtfs_rc_wmata" -ne 0 ] || [ "$gtfs_rc_sfmta" -ne 0 ] || [ "$vp_wmata_rc" -ne 0 ] || [ "$vp_sfmta_rc" -ne 0 ]; then
  echo "== summary: upstream step(s) reported errors — replay + derive completed anyway ==" >&2
  echo "  gtfs_reload wmata rc=$gtfs_rc_wmata sfmta rc=$gtfs_rc_sfmta (a transient WMATA/511.org API failure just leaves that agency's GTFS stale; rerun to retry)" >&2
  echo "  vp_wmata   rc=$vp_wmata_rc" >&2
  echo "  vp_sfmta   rc=$vp_sfmta_rc" >&2
  echo "  (VP loader rc!=0: see loader output above for the failed file(s); rerun this" >&2
  echo "  script to retry — the manifest table skips whatever already loaded cleanly)" >&2
  if [ "$vp_wmata_rc" -ne 0 ] || [ "$vp_sfmta_rc" -ne 0 ]; then
    echo "  NOTE: derive already ran against the partial VP data — those dates now have" >&2
    echo "  runs rows and will NOT be auto-revisited by a future run_daily_batch.py" >&2
    echo "  (determine_target_dates only picks up zero-runs-row dates — NOTES-113's" >&2
    echo "  failure shape). After fixing the loader, either delete the affected dates'" >&2
    echo "  runs rows and rerun this script, or re-run the per-date derive pipelines" >&2
    echo "  directly (pipelines/derive_stop_events_from_state.py --all-routes --date D)." >&2
  fi
  echo "Done (with errors — see summary above)." >&2
  exit 1
fi
echo "Done."
