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

echo "== GTFS reload if stale (>7 days) =="
# A transient WMATA API failure here must not block an otherwise-healthy
# freshness run — stale-but-valid GTFS is the status quo, not a reason to
# skip syncing/replaying/deriving fresh GTFS-RT data. Capture the exit
# code and keep going, same treatment as the VP loaders below.
gtfs_rc=0
PYTHONUNBUFFERED=1 uv run python scripts/run_gtfs_reload.py --max-age-days 7 || gtfs_rc=$?

echo "== s3 sync raw archives =="
# Root prefix holds WMATA TU directly; exclude the sibling sub-prefixes.
aws s3 sync "$S3_BASE/" "$LOCAL_ARCHIVE/" \
  --exclude "sfmta/*" --exclude "vp/*" --exclude "sfmta_vp/*"
aws s3 sync "$S3_BASE/sfmta/" "$LOCAL_ARCHIVE_SFMTA/"
aws s3 sync "$S3_BASE/vp/" "$LOCAL_ARCHIVE_VP/"
aws s3 sync "$S3_BASE/sfmta_vp/" "$LOCAL_ARCHIVE_SFMTA_VP/"

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
  echo "Upstream status at abort: gtfs_reload rc=$gtfs_rc, vp_wmata rc=$vp_wmata_rc, vp_sfmta rc=$vp_sfmta_rc." >&2
  exit 1
fi

echo "== derive (self-targets zero-run dates) =="
# Echo upstream status before derive, not just in the summary below: a
# derive failure trips `set -e` right here, which would otherwise skip
# straight past the summary block and lose this information.
echo "Upstream status going into derive: gtfs_reload rc=$gtfs_rc, vp_wmata rc=$vp_wmata_rc, vp_sfmta rc=$vp_sfmta_rc." >&2
PYTHONUNBUFFERED=1 uv run python pipelines/run_daily_batch.py --lookback-days "$LOOKBACK_DAYS"

if [ "$gtfs_rc" -ne 0 ] || [ "$vp_wmata_rc" -ne 0 ] || [ "$vp_sfmta_rc" -ne 0 ]; then
  echo "== summary: upstream step(s) reported errors — replay + derive completed anyway ==" >&2
  echo "  gtfs_reload rc=$gtfs_rc (a transient WMATA API failure just leaves GTFS stale; rerun to retry)" >&2
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
