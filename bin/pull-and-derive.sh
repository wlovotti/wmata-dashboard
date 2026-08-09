#!/usr/bin/env bash
# bin/pull-and-derive.sh — interim Path 2a ingest: pull fresh raw data from
# the VM, replay locally, derive. Run on demand (manual cadence — spec
# 2026-07-14 decision 4). Requires: bin/db-tunnel.sh up, VM_DB_URL set.
#
#   bin/pull-and-derive.sh          # replay+derive lookback of 14 days
#   bin/pull-and-derive.sh 35       # wider catch-up
#
# LOOKBACK_DAYS note: widening this past 2026-06-13 is unsafe — June dates
# need snapshot 12 and must go through scripts/local_recovery_2026_07.sh,
# not this script.
set -euo pipefail

VM="ubuntu@52.54.130.186"
REMOTE_ARCHIVE="/home/wmata/wmata-dashboard/archive/raw_snapshots"
REMOTE_OVERFLOW="/mnt/pgdata/archive-overflow"
REMOTE_ARCHIVE_SFMTA="/home/wmata/wmata-dashboard/archive/sfmta_raw_snapshots"
LOCAL_ARCHIVE="${LOCAL_ARCHIVE:-archive/raw_snapshots}"
LOCAL_ARCHIVE_SFMTA="${LOCAL_ARCHIVE_SFMTA:-archive/sfmta_raw_snapshots}"
TUNNEL_PORT="${TUNNEL_PORT:-5433}"
LOOKBACK_DAYS="${1:-14}"
: "${VM_DB_URL:?Set VM_DB_URL (see the commented tunnel line in .env)}"

if ! lsof -nP -iTCP:"${TUNNEL_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Tunnel not up. Run bin/db-tunnel.sh first." >&2; exit 1
fi
mkdir -p "$LOCAL_ARCHIVE" "$LOCAL_ARCHIVE_SFMTA"

echo "== rsync raw TU archive (both dirs — rev 2b split) =="
rsync -av --rsync-path="sudo rsync" "$VM:$REMOTE_ARCHIVE/" "$LOCAL_ARCHIVE/"
rsync -av --rsync-path="sudo rsync" "$VM:$REMOTE_OVERFLOW/" "$LOCAL_ARCHIVE/"

echo "== rsync raw SFMTA archive (single copy on the VM) =="
rsync -av --rsync-path="sudo rsync" "$VM:$REMOTE_ARCHIVE_SFMTA/" "$LOCAL_ARCHIVE_SFMTA/"

echo "== pull vehicle_positions delta over tunnel =="
VP_COLS="id, vehicle_id, route_id, trip_id, latitude, longitude, speed, current_stop_sequence, stop_id, current_status, direction_id, trip_start_date, timestamp, collected_at"
LOCAL_MAX_ID=$(psql -d wmata_dashboard -Atc "SELECT COALESCE(max(id), 0) FROM vehicle_positions")
echo "local VP high-water mark: id $LOCAL_MAX_ID"
# Strictly-greater window on the VM-assigned monotonic id (not the GTFS-RT
# vehicle-reported timestamp, which is non-monotonic and lags collection
# per-vehicle — NOTES-81 documented phantom-timestamp rows that a
# timestamp-based watermark would permanently skip). Single-writer
# collector: allocation order = commit order, so a strictly-greater id
# window cannot collide on the PK and cannot skip rows.
psql "$VM_DB_URL" -c "\copy (SELECT $VP_COLS FROM vehicle_positions WHERE id > $LOCAL_MAX_ID) TO STDOUT" \
  | psql -d wmata_dashboard -c "\copy vehicle_positions ($VP_COLS) FROM STDIN"
psql -d wmata_dashboard -Atc "SELECT setval('vehicle_positions_id_seq', (SELECT COALESCE(MAX(id),1) FROM vehicle_positions))" >/dev/null

echo "== replay TU archive for the lookback window (idempotent) =="
for i in $(seq "$LOOKBACK_DAYS" -1 0); do
  d=$(date -v -"${i}"d +%F)   # macOS date; this script is laptop-only
  ls "$LOCAL_ARCHIVE/$d".*.jsonl.zst >/dev/null 2>&1 || continue
  PYTHONUNBUFFERED=1 uv run python pipelines/replay_archive_to_state.py --date "$d"
done

echo "== derive (self-targets zero-run dates) =="
PYTHONUNBUFFERED=1 uv run python pipelines/run_daily_batch.py --lookback-days "$LOOKBACK_DAYS"
echo "Done."
