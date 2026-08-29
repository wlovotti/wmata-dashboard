#!/usr/bin/env bash
# bin/refresh-dev-db.sh — restore a local copy of the prod dataset for development.
# Path 2a (2026-07): wmata_dashboard is now the primary system of record and will
# NOT be dropped without explicit acknowledgment.
# See docs/superpowers/specs/2026-06-13-dev-deploy-environments-design.md §4.3.
#
#   bin/refresh-dev-db.sh                # REFUSING: wmata_dashboard is primary (use --scratch or --clobber-primary)
#   bin/refresh-dev-db.sh --scratch      # slim (default): restore into wmata_dashboard_scratch, leaving primary untouched
#   bin/refresh-dev-db.sh --scratch --full  # include raw-feed tables so the pipeline can run (~31 GiB)
#   bin/refresh-dev-db.sh --clobber-primary # drop+recreate wmata_dashboard, restore the latest S3 dump (~17 GiB slim)
#   bin/refresh-dev-db.sh --prune-gtfs   # after restore, delete is_current=False stop_times history (~9 GiB; VACUUM FULL)
#   bin/refresh-dev-db.sh --from-vm      # source a fresh pg_dump over a manual SSH tunnel instead of S3
#                                         # (the bin/db-tunnel.sh wrapper was retired with the tunnel-based
#                                         # ingest path; open the tunnel yourself first — see below)
#
# Slim (default) excludes the raw-feed tables at the pg_restore TOC level, so their
# data is never written to disk (no transient spike). The read-only API never reads
# them; only the collector/pipeline does. A slim DB therefore CANNOT run the
# derivation pipeline — use --full for that.
set -euo pipefail

BUCKET="${REFRESH_BUCKET:-wmata-dashboard-backups}"
PREFIX="${REFRESH_PREFIX:-wmata-db-backups}"
LOCAL_PORT="${REFRESH_PORT:-5432}"
TUNNEL_PORT="${REFRESH_TUNNEL_PORT:-5433}"   # a manual `ssh -N -L 5433:localhost:5432 ...` tunnel forwards this -> VM 5432
VM_DB_USER="${REFRESH_VM_DB_USER:-wmata}"

# Raw-feed tables the read-only API never queries (verified: no inbound FKs).
EXCLUDE_TABLES=(vehicle_positions trip_update_state timepoint_times collector_heartbeats)

MODE_FULL=0; MODE_SCRATCH=0; MODE_PRUNE_GTFS=0; MODE_FROM_VM=0; MODE_CLOBBER=0
for arg in "$@"; do
  case "$arg" in
    --full)             MODE_FULL=1 ;;
    --scratch)          MODE_SCRATCH=1 ;;
    --prune-gtfs)       MODE_PRUNE_GTFS=1 ;;
    --from-vm)          MODE_FROM_VM=1 ;;
    --clobber-primary)  MODE_CLOBBER=1 ;;
    *) echo "Unknown flag: $arg" >&2; exit 2 ;;
  esac
done

# Path 2a (2026-07): local wmata_dashboard is the PRIMARY database, not a
# disposable dev copy. Refuse to drop it unless explicitly told to.
if [ "$MODE_SCRATCH" -eq 0 ] && [ "$MODE_CLOBBER" -eq 0 ]; then
  echo "REFUSING: 'wmata_dashboard' is the system of record (Path 2a, 2026-07)." >&2
  echo "Use --scratch for disposable restores, or --clobber-primary to really replace it." >&2
  exit 3
fi

# Hard safety rail: only ever target the two known local DB names.
if [ "$MODE_SCRATCH" -eq 1 ]; then
  TARGET="wmata_dashboard_scratch"
else
  TARGET="wmata_dashboard"
fi
case "$TARGET" in
  wmata_dashboard|wmata_dashboard_scratch) : ;;
  *) echo "Refusing to target '$TARGET'." >&2; exit 1 ;;
esac

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
DUMP="$TMP/dump.fc"

if [ "$MODE_FROM_VM" -eq 1 ]; then
  echo "Sourcing fresh pg_dump from the VM over the tunnel (localhost:${TUNNEL_PORT})..."
  if ! lsof -nP -iTCP:"${TUNNEL_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Tunnel not up. Open one in another terminal first:" >&2
    echo "  ssh -N -L ${TUNNEL_PORT}:localhost:5432 ubuntu@<vm-ip> &" >&2
    exit 1
  fi
  pg_dump -Fc -h localhost -p "${TUNNEL_PORT}" -U "${VM_DB_USER}" wmata_dashboard > "$DUMP"
else
  KEY="$(aws s3 ls "s3://${BUCKET}/${PREFIX}/" | awk '{print $4}' | grep -E '\.dump\.xz$' | sort | tail -1)"
  [ -n "$KEY" ] || { echo "No *.dump.xz found under s3://${BUCKET}/${PREFIX}/" >&2; exit 1; }
  echo "Latest snapshot: ${KEY}"
  aws s3 cp "s3://${BUCKET}/${PREFIX}/${KEY}" "$TMP/${KEY}"
  echo "Decompressing..."
  xz -dc "$TMP/${KEY}" > "$DUMP"
fi

echo "Recreating database '${TARGET}' on port ${LOCAL_PORT}..."
dropdb -p "${LOCAL_PORT}" --if-exists "${TARGET}"
createdb -p "${LOCAL_PORT}" "${TARGET}"

# Build the restore TOC. Slim (default) omits the raw-feed TABLE DATA entries so
# pg_restore never reads/writes those rows; the (empty) tables still exist.
pg_restore -l "$DUMP" > "$TMP/toc.full"
if [ "$MODE_FULL" -eq 1 ]; then
  cp "$TMP/toc.full" "$TMP/toc.use"
  echo "Restore mode: FULL (raw-feed tables included)"
else
  EXCL_RE="TABLE DATA (public )?($(IFS='|'; echo "${EXCLUDE_TABLES[*]}")) "
  grep -vE "$EXCL_RE" "$TMP/toc.full" > "$TMP/toc.use"
  echo "Restore mode: SLIM (excluding: ${EXCLUDE_TABLES[*]})"
fi

echo "Restoring..."
pg_restore --no-owner --no-privileges -d "${TARGET}" -p "${LOCAL_PORT}" -L "$TMP/toc.use" "$DUMP"

if [ "$MODE_PRUNE_GTFS" -eq 1 ]; then
  echo "Pruning is_current=False stop_times history (VACUUM FULL)..."
  psql -p "${LOCAL_PORT}" -d "${TARGET}" -v ON_ERROR_STOP=1 \
    -c "DELETE FROM stop_times WHERE is_current = false;" \
    -c "VACUUM FULL stop_times;"
fi

echo "Done. '${TARGET}' size:"
psql -p "${LOCAL_PORT}" -d "${TARGET}" -At \
  -c "SELECT pg_size_pretty(pg_database_size('${TARGET}'));"
