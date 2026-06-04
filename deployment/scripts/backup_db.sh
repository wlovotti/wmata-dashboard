#!/bin/bash
#
# PostgreSQL Backup Script for WMATA Dashboard
#
# Creates a weekly pg_dump -Fc backup, compresses it with xz, keeps the
# last 7 local copies, and optionally uploads to S3 if S3_BACKUP_BUCKET
# is set in the environment (sourced from the EnvironmentFile of the
# wmata-backup systemd service).
#
# Usage:
#   ./backup_db.sh
#
# S3 upload (optional):
#   Set S3_BACKUP_BUCKET=<bucket-name> in the environment. The upload is
#   skipped silently when the variable is unset, so the script remains
#   locally runnable without AWS credentials.
#
# NOTE: The S3 upload step has not been tested against a real S3 bucket.
#   Verify with a dry-run (aws s3 cp --dryrun ...) before relying on it.
#
# Restore from a custom-format backup:
#   xz -d < wmata_db_YYYYMMDD_HHMMSS.dump.xz | \
#     pg_restore -U wmata -d wmata_dashboard --no-owner

set -e

# Configuration
DB_NAME="wmata_dashboard"
DB_USER="wmata"
BACKUP_DIR="/home/wmata/backups"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/wmata_db_${DATE}.dump.xz"
DAYS_TO_KEEP=7
S3_PREFIX="wmata-db-backups"

# Create backup directory if it doesn't exist
mkdir -p "${BACKUP_DIR}"

# Log start
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting database backup..."

# Create backup: pg_dump in custom format, compress with xz
pg_dump -U "${DB_USER}" -Fc "${DB_NAME}" | xz > "${BACKUP_FILE}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backup completed: ${BACKUP_FILE}"

# Get backup file size
BACKUP_SIZE=$(du -h "${BACKUP_FILE}" | cut -f1)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backup size: ${BACKUP_SIZE}"

# Delete old local backups (keep only last N days)
find "${BACKUP_DIR}" -name "wmata_db_*.dump.xz" -type f -mtime +"${DAYS_TO_KEEP}" -delete
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cleaned up local backups older than ${DAYS_TO_KEEP} days"

BACKUP_COUNT=$(find "${BACKUP_DIR}" -name "wmata_db_*.dump.xz" -type f | wc -l)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Total local backups retained: ${BACKUP_COUNT}"

# ---------------------------------------------------------------------------
# OPTIONAL: upload to S3
# Only runs when S3_BACKUP_BUCKET is set in the environment.
# NOTE: untested against a real S3 bucket — verify with --dryrun first.
# Requires the 'aws' CLI to be installed and credentials configured
# (IAM role attached to the Lightsail instance, or ~/.aws/credentials).
# ---------------------------------------------------------------------------
if [ -n "${S3_BACKUP_BUCKET}" ]; then
    S3_DEST="s3://${S3_BACKUP_BUCKET}/${S3_PREFIX}/$(basename "${BACKUP_FILE}")"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Uploading to ${S3_DEST} ..."
    aws s3 cp "${BACKUP_FILE}" "${S3_DEST}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] S3 upload complete."
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] S3_BACKUP_BUCKET not set — skipping S3 upload."
fi
