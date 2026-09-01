#!/usr/bin/env bash
# bin/pull-and-derive.sh — Path 2a ingest: sync fresh raw data from S3,
# load + replay locally, derive. Run on demand (manual cadence).
#
#   bin/pull-and-derive.sh          # replay+derive lookback of 14 days
#   bin/pull-and-derive.sh 35       # wider catch-up
#
# Requires: AWS credentials with read access to the four
# raw-jsonl-archive S3 prefixes (root, sfmta/, vp/, sfmta_vp/), and
# local DATABASE_URL + SFMTA_DATABASE_URL (.env) for the loader/replay/
# derive steps below — the SFMTA replay/derive legs hard-require
# SFMTA_DATABASE_URL.
#
# LOOKBACK_DAYS note: widening past 2026-06-13 is unsafe — June dates need
# snapshot 12 and must go through scripts/local_recovery_2026_07.sh.
#
# GTFS_CANARY_SKIP=1 skips the post-replay GTFS trip_id match-rate canary
# (see the "post-replay GTFS trip_id match-rate canary" section below) —
# a manual escape hatch for a legitimately-0% backfill date (e.g. a
# service date with no real ridership yet). Off by default; use it
# deliberately, not routinely — it disables the check the canary exists
# to provide.
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
# Before the GTFS reload gate fix (PR #226), this gate ran WMATA only —
# SFMTA's static GTFS had no staleness gate at all, and was reloaded only
# when someone remembered to run `reload_gtfs_complete.py --agency sfmta`
# by hand. Muni's 2026-08-30 fall service change landed while the SFMTA
# snapshot was still "fresh" by this same 7-day age proxy, so the age gate
# alone doesn't fully close the gap for either agency — the post-replay
# match-rate canary below is the second, stronger check for exactly that
# failure shape.
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

echo "== replay TU archive for the lookback window (idempotent), per agency =="
# replay_archive_to_state.py fails loudly on a zero-file match for a
# date; derivation must never run past a replay failure (the NOTES-93
# incident) — that protection stays as-is for WMATA. Both agencies'
# archives are replayed here (the SFMTA pull-and-derive automation, PR
# #228) — SFMTA's VP archive was already synced/loaded above, but
# trip_update_state previously only ever advanced for WMATA, leaving
# stop_events/runs/the aggregate chain for SFMTA fully manual.
#
# SFMTA's raw-jsonl-archive/sfmta/ S3 prefix only starts 2026-07-22
# (WMATA's root prefix starts 2026-05-03), so any LOOKBACK_DAYS wide
# enough to reach before that — or any single-day hole in the SFMTA
# archive, e.g. a post-outage catch-up gap — would otherwise fail that
# date's replay and, pre-PR-#228-review, abort the whole script before
# either agency derived. --allow-empty makes a missing/empty SFMTA
# archive day a loud per-date notice + rc=0 instead of a failure; WMATA
# keeps the strict (no --allow-empty) behavior since a WMATA archive
# gap is a genuine anomaly, not an expected pre-onboarding hole.
#
# Failures are tracked in separate per-agency arrays (not merged into
# one list) so a WMATA replay failure below skips only WMATA's derive,
# and a genuine SFMTA replay failure (a real error, not a missing
# archive day — those are absorbed by --allow-empty above) skips only
# SFMTA's — neither agency's replay failure masks or aborts the
# other's path.
failed_wmata=()
failed_sfmta=()
for i in $(seq "$LOOKBACK_DAYS" -1 0); do
  d=$(date -v -"${i}"d +%F)   # macOS date; this script is laptop-only
  if ! PYTHONUNBUFFERED=1 uv run python pipelines/replay_archive_to_state.py --date "$d" --archive-root "$LOCAL_ARCHIVE"; then
    failed_wmata+=("wmata:$d")
  fi
  if ! PYTHONUNBUFFERED=1 uv run python pipelines/replay_archive_to_state.py --date "$d" --agency sfmta --archive-root "$LOCAL_ARCHIVE_SFMTA" --allow-empty; then
    failed_sfmta+=("sfmta:$d")
  fi
done
skip_wmata=0
skip_sfmta=0
if [ "${#failed_wmata[@]}" -gt 0 ]; then
  echo "WMATA replay failed for: ${failed_wmata[*]} — skipping WMATA derive below (SFMTA is unaffected)." >&2
  skip_wmata=1
fi
if [ "${#failed_sfmta[@]}" -gt 0 ]; then
  echo "SFMTA replay failed for: ${failed_sfmta[*]} — skipping SFMTA derive below (WMATA is unaffected)." >&2
  skip_sfmta=1
fi

echo "== post-replay GTFS trip_id match-rate canary =="
# The age-based reload gate above is a proxy — it bounds staleness but
# can't see a service change land while the loaded snapshot is still
# "fresh" by that clock (the 2026-08-30 Muni fall service change: SFMTA's
# snapshot was < 7 days old, so the gate skipped a reload, but the feed's
# trip_ids had moved to a brand-new space). This canary recomputes the
# same feed-trip_id ∩ current-GTFS-trip_id intersection both derive paths
# gate on, for the latest agency-local service date.
#
# Residual exposure: the canary only checks each agency's latest
# agency-local ("yesterday") service date. `determine_target_dates`
# (inside run_daily_batch.py, below) also re-targets any older
# zero-`runs`-row date within the lookback window — those catch-up dates
# are NOT covered by this check.
#
# Exit codes from gtfs_trip_match_canary.py: 0 ok/skip, 1 a real
# match-rate collapse, 2 an operational error (the check itself couldn't
# run — unset config, unknown agency, a transient DB failure). The derive
# below (`run_daily_batch.py`) now runs both agencies (the SFMTA
# pull-and-derive automation, PR #228). A *collapse* (rc=1) skips only
# that agency's derive below (via skip_wmata/skip_sfmta) — it does NOT
# abort the whole script, preserving the prior invariant that a
# single-agency canary collapse never freezes the other, healthy
# agency's freshness (a repeat of the 2026-08-30 Muni service change
# must not also stall WMATA). An operational error (rc=2) for either
# agency is captured as a nonzero rc and reported in the summary at the
# end (same cross-agency rc convention as the GTFS reload / VP loader
# steps above) and does not skip that agency's derive.
# GTFS_CANARY_SKIP=1 skips this whole check (see the header comment) —
# for a legitimately-0% backfill date; this is now a secondary escape
# hatch (single-agency collapses no longer need it to protect the other
# agency) but stays available for a deliberate whole-run bypass.
canary_rc_wmata=0
canary_rc_sfmta=0
if [ "${GTFS_CANARY_SKIP:-0}" = "1" ]; then
  echo "GTFS_CANARY_SKIP=1 — GTFS trip_id match-rate canary skipped by operator." >&2
else
  if [ "$skip_wmata" -eq 0 ]; then
    PYTHONUNBUFFERED=1 uv run python scripts/gtfs_trip_match_canary.py --agency wmata || canary_rc_wmata=$?
  fi
  if [ "$skip_sfmta" -eq 0 ]; then
    PYTHONUNBUFFERED=1 uv run python scripts/gtfs_trip_match_canary.py --agency sfmta || canary_rc_sfmta=$?
  fi

  if [ "$canary_rc_wmata" -eq 1 ]; then
    echo "GTFS trip_id match-rate canary: WMATA COLLAPSED (rc=1) — skipping WMATA derive below, NOT aborting SFMTA. See the canary output above for the date/match-rate, and docs/DEPLOYMENT.md for the recovery procedure." >&2
    skip_wmata=1
  fi
  if [ "$canary_rc_sfmta" -eq 1 ]; then
    echo "GTFS trip_id match-rate canary: SFMTA COLLAPSED (rc=1) — skipping SFMTA derive below, NOT aborting WMATA. See the canary output above for the date/match-rate, and docs/DEPLOYMENT.md for the recovery procedure." >&2
    skip_sfmta=1
  fi
  if [ "$canary_rc_wmata" -ne 0 ] && [ "$canary_rc_wmata" -ne 1 ]; then
    echo "GTFS trip_id match-rate canary: WMATA reported rc=$canary_rc_wmata (an operational error, not a collapse) — continuing; see summary at the end." >&2
  fi
  if [ "$canary_rc_sfmta" -ne 0 ] && [ "$canary_rc_sfmta" -ne 1 ]; then
    echo "GTFS trip_id match-rate canary: SFMTA reported rc=$canary_rc_sfmta (an operational error, not a collapse) — continuing; see summary at the end." >&2
  fi
fi

echo "== derive (self-targets zero-run dates), per agency =="
# Echo upstream status before derive, not just in the summary below: a
# later step failing here shouldn't lose this information.
echo "Upstream status going into derive: gtfs_reload wmata rc=$gtfs_rc_wmata sfmta rc=$gtfs_rc_sfmta, vp_wmata rc=$vp_wmata_rc, vp_sfmta rc=$vp_sfmta_rc, canary wmata rc=$canary_rc_wmata sfmta rc=$canary_rc_sfmta, skip_wmata=$skip_wmata skip_sfmta=$skip_sfmta." >&2
# Each agency's derive rc is captured (not a bare command under
# set -euo pipefail) so a WMATA-only failure can't abort the script
# before the SFMTA derive call ever runs, or skip the summary below —
# run_daily_batch.py returns 1 for ANY failure count, including
# documented non-blocking housekeeping soft-failures
# (pipelines/run_daily_batch.py), so treating its rc as fatal-and-fast
# would abort a healthy SFMTA derive over a WMATA housekeeping hiccup
# (PR #228 review).
batch_rc_wmata=0
batch_rc_sfmta=0
if [ "$skip_wmata" -eq 1 ]; then
  echo "Skipping WMATA derive (replay failure or canary collapse above)." >&2
else
  PYTHONUNBUFFERED=1 uv run python pipelines/run_daily_batch.py --lookback-days "$LOOKBACK_DAYS" || batch_rc_wmata=$?
fi
if [ "$skip_sfmta" -eq 1 ]; then
  echo "Skipping SFMTA derive (replay failure or canary collapse above)." >&2
else
  PYTHONUNBUFFERED=1 uv run python pipelines/run_daily_batch.py --agency sfmta --lookback-days "$LOOKBACK_DAYS" || batch_rc_sfmta=$?
fi

echo "== SFMTA trip_update_state retention (WMATA's own is handled by run_daily_batch.py's housekeeping loop above) =="
# run_daily_batch.py's housekeeping loop (pipelines/run_daily_batch.py) skips
# wholesale for any non-default agency, including cleanup_trip_update_state.py
# even though that one script IS agency-aware (--agency) — see its module
# docstring. Before the SFMTA replay+derive automation (PR #227/#228) this was
# a latent gap: SFMTA's trip_update_state only grew when someone ran the
# SFMTA replay by hand. Automating that replay on every freshness run turned
# the missing retention into standing, unbounded growth (the retention
# hookup added here, PR #229) — so run it explicitly, once, right after the
# SFMTA derive step above. Non-blocking: a cleanup failure must not fail an
# otherwise-healthy freshness run, same treatment as the other steps above.
cleanup_rc_sfmta=0
PYTHONUNBUFFERED=1 uv run python pipelines/cleanup_trip_update_state.py --agency sfmta || cleanup_rc_sfmta=$?

overall_failure=0
if [ "$gtfs_rc_wmata" -ne 0 ] || [ "$gtfs_rc_sfmta" -ne 0 ] || [ "$vp_wmata_rc" -ne 0 ] || [ "$vp_sfmta_rc" -ne 0 ] \
  || [ "$canary_rc_wmata" -ne 0 ] || [ "$canary_rc_sfmta" -ne 0 ] || [ "$skip_wmata" -eq 1 ] || [ "$skip_sfmta" -eq 1 ] \
  || [ "$batch_rc_wmata" -ne 0 ] || [ "$batch_rc_sfmta" -ne 0 ] || [ "$cleanup_rc_sfmta" -ne 0 ]; then
  overall_failure=1
fi

if [ "$overall_failure" -eq 1 ]; then
  echo "== summary: upstream step(s) reported errors ==" >&2
  echo "  gtfs_reload wmata rc=$gtfs_rc_wmata sfmta rc=$gtfs_rc_sfmta (a transient WMATA/511.org API failure just leaves that agency's GTFS stale; rerun to retry)" >&2
  echo "  vp_wmata   rc=$vp_wmata_rc" >&2
  echo "  vp_sfmta   rc=$vp_sfmta_rc" >&2
  echo "  (VP loader rc!=0: see loader output above for the failed file(s); rerun this" >&2
  echo "  script to retry — the manifest table skips whatever already loaded cleanly)" >&2
  echo "  replay: wmata failed=${failed_wmata[*]:-none} sfmta failed=${failed_sfmta[*]:-none}" >&2
  echo "  canary_wmata rc=$canary_rc_wmata canary_sfmta rc=$canary_rc_sfmta (1=collapse — that agency's derive below was skipped, not" >&2
  echo "  the other's; 2=operational error — reported here but does not skip that agency's derive)" >&2
  echo "  derive: wmata rc=$batch_rc_wmata skipped=$skip_wmata; sfmta rc=$batch_rc_sfmta skipped=$skip_sfmta" >&2
  echo "  cleanup_trip_update_state sfmta rc=$cleanup_rc_sfmta (WMATA's own runs inside run_daily_batch.py's" >&2
  echo "  housekeeping loop above, folded into its derive rc; non-blocking either way — see cleanup output" >&2
  echo "  above for the failure if rc!=0)" >&2
  echo "  (a nonzero derive rc with skipped=0 can be a non-blocking housekeeping soft-failure" >&2
  echo "  inside run_daily_batch.py — see the derive output above for which step failed)" >&2
  if { [ "$vp_wmata_rc" -ne 0 ] && [ "$skip_wmata" -eq 0 ]; } || { [ "$vp_sfmta_rc" -ne 0 ] && [ "$skip_sfmta" -eq 0 ]; }; then
    echo "  NOTE: derive already ran against the partial VP data for the affected" >&2
    echo "  agency(ies) — those dates now have runs rows and will NOT be" >&2
    echo "  auto-revisited by a future run_daily_batch.py (determine_target_dates" >&2
    echo "  only picks up zero-runs-row dates — the standing trap that required the" >&2
    echo "  manual SFMTA 8/9-8/10 top-up, PR #227). After fixing the loader, either" >&2
    echo "  delete the affected dates' runs rows and rerun this script, or re-run" >&2
    echo "  the per-date derive pipelines directly" >&2
    echo "  (pipelines/derive_stop_events_from_state.py --all-routes --date D" >&2
    echo "  [--agency sfmta])." >&2
  fi
  echo "Done (with errors — see summary above)." >&2
  exit 1
fi
echo "Done."
