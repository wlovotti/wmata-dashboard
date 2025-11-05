#!/bin/bash
#
# PostgreSQL Backup Script for WMATA Dashboard
#
# This script creates daily backups of the PostgreSQL database
# and maintains backups for the last 7 days.
#
# Usage:
#   ./backup_db.sh
#
# Cron example (daily at 3am):
#   0 3 * * * /home/wmata/wmata-dashboard/deployment/scripts/backup_db.sh

set -e

# Configuration
DB_NAME="wmata_dashboard"
DB_USER="wmata"
BACKUP_DIR="/home/wmata/backups"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/wmata_db_${DATE}.sql.gz"
DAYS_TO_KEEP=7

# Create backup directory if it doesn't exist
mkdir -p "${BACKUP_DIR}"

# Log start
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting database backup..."

# Create backup
pg_dump -U "${DB_USER}" "${DB_NAME}" | gzip > "${BACKUP_FILE}"

# Check if backup was successful
if [ $? -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backup completed successfully: ${BACKUP_FILE}"

    # Get backup file size
    BACKUP_SIZE=$(du -h "${BACKUP_FILE}" | cut -f1)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backup size: ${BACKUP_SIZE}"

    # Delete old backups (keep only last N days)
    find "${BACKUP_DIR}" -name "wmata_db_*.sql.gz" -type f -mtime +${DAYS_TO_KEEP} -delete
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cleaned up backups older than ${DAYS_TO_KEEP} days"

    # Count remaining backups
    BACKUP_COUNT=$(find "${BACKUP_DIR}" -name "wmata_db_*.sql.gz" -type f | wc -l)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Total backups retained: ${BACKUP_COUNT}"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Backup failed!" >&2
    exit 1
fi
