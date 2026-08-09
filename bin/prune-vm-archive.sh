#!/usr/bin/env bash
# bin/prune-vm-archive.sh — laptop-side verify-then-prune drain for the VM's
# raw JSONL archive (NOTES-98).
#
# `bin/pull-and-derive.sh` rsyncs the VM archive dirs down to the laptop,
# and the manual "aws s3 sync" step (docs/DEPLOYMENT.md, "Manual S3 sync")
# copies them up to S3 — but neither one deletes the VM-side originals.
# Without a drain, the VM root disk fills at ~1.3 GB/day combined
# (WMATA + SFMTA collectors), the same failure mode that caused the July
# 2026 disk-fill incident. Run this manually, from the laptop, after each
# S3 sync completes.
#
# Covers all three VM archive locations:
#   /home/wmata/wmata-dashboard/archive/raw_snapshots/   -> s3://wmata-dashboard-backups/raw-jsonl-archive/<filename>
#   /mnt/pgdata/archive-overflow/                        -> s3://wmata-dashboard-backups/raw-jsonl-archive/<filename>
#   /home/wmata/wmata-dashboard/archive/sfmta_raw_snapshots/ -> s3://wmata-dashboard-backups/raw-jsonl-archive/sfmta/<filename>
#
# For each VM-side file older than the safety window (default 7 days), the
# same-named S3 object is checked with `aws s3api head-object` and its
# byte size compared to the local file. A file is deleted ONLY when the S3
# object exists AND the sizes match exactly. A file missing from S3, or
# with a mismatched size, is NEVER deleted — it is reported so the sync
# can be re-run first.
#
# Dry-run is the default: the script only prints what it would delete/skip.
# Pass --delete to actually delete verified files.
#
#   bin/prune-vm-archive.sh                  # dry run, 7-day safety window
#   bin/prune-vm-archive.sh --delete         # actually prune
#   bin/prune-vm-archive.sh --days 14        # wider safety window, dry run
#   bin/prune-vm-archive.sh --days 14 --delete
#
# Requires: SSH access to the VM (archive files are owned by the `wmata`
# user, so remote listing/deletion goes through sudo — same VM-addressing
# convention as bin/pull-and-derive.sh) and the AWS CLI configured locally
# (S3 checks run locally via `aws s3api head-object`, not over SSH).
set -euo pipefail

VM="ubuntu@52.54.130.186"
S3_BUCKET="wmata-dashboard-backups"

REMOTE_DIRS=(
  "/home/wmata/wmata-dashboard/archive/raw_snapshots"
  "/mnt/pgdata/archive-overflow"
  "/home/wmata/wmata-dashboard/archive/sfmta_raw_snapshots"
)
S3_PREFIXES=(
  "raw-jsonl-archive"
  "raw-jsonl-archive"
  "raw-jsonl-archive/sfmta"
)

SAFETY_DAYS=7
DELETE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --delete)
      DELETE=1
      shift
      ;;
    --days)
      SAFETY_DAYS="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--days N] [--delete]"
      echo "  --days N   safety window in days (default 7); files younger than this are never touched"
      echo "  --delete   actually delete verified files (default is dry-run / report-only)"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if ! [[ "$SAFETY_DAYS" =~ ^[0-9]+$ ]]; then
  echo "--days must be a non-negative integer, got: $SAFETY_DAYS" >&2
  exit 1
fi

NOW_EPOCH=$(date +%s)
SAFETY_SECONDS=$(( SAFETY_DAYS * 86400 ))

VERIFIED_COUNT=0
UNSYNCED_COUNT=0
YOUNG_COUNT=0

for idx in "${!REMOTE_DIRS[@]}"; do
  REMOTE_DIR="${REMOTE_DIRS[$idx]}"
  S3_PREFIX="${S3_PREFIXES[$idx]}"

  echo "== $REMOTE_DIR -> s3://$S3_BUCKET/$S3_PREFIX/ =="

  # sudo because the archive files are owned by the wmata user. Redirect
  # find's stderr to suppress noise if a dir doesn't exist (e.g. the
  # overflow dir on a VM that never spilled onto /mnt/pgdata) — the
  # trailing `|| true` keeps a nonzero find/ssh exit from tripping set -e,
  # so a missing dir is treated as "no files" rather than a hard failure.
  # shellcheck disable=SC2029  # intentional: $REMOTE_DIR expands client-side to build the remote command
  LISTING=$(ssh "$VM" "sudo find '$REMOTE_DIR' -maxdepth 1 -type f -printf '%f\t%s\t%T@\n' 2>/dev/null" || true)

  if [[ -z "$LISTING" ]]; then
    echo "  (no files found)"
    echo
    continue
  fi

  DELETE_LIST=()

  while IFS=$'\t' read -r fname fsize fmtime; do
    [[ -z "$fname" ]] && continue
    fmtime_int="${fmtime%.*}"
    age=$(( NOW_EPOCH - fmtime_int ))

    if (( age < SAFETY_SECONDS )); then
      echo "  skip (too young): $fname"
      YOUNG_COUNT=$(( YOUNG_COUNT + 1 ))
      continue
    fi

    s3_size=$(aws s3api head-object --bucket "$S3_BUCKET" --key "$S3_PREFIX/$fname" --query ContentLength --output text 2>/dev/null || echo "MISSING")

    if [[ "$s3_size" == "MISSING" || -z "$s3_size" || "$s3_size" == "None" ]]; then
      echo "  skip (not found in S3): $fname"
      UNSYNCED_COUNT=$(( UNSYNCED_COUNT + 1 ))
      continue
    fi

    if [[ "$s3_size" != "$fsize" ]]; then
      echo "  skip (size mismatch: local=$fsize s3=$s3_size): $fname"
      UNSYNCED_COUNT=$(( UNSYNCED_COUNT + 1 ))
      continue
    fi

    VERIFIED_COUNT=$(( VERIFIED_COUNT + 1 ))
    if (( DELETE )); then
      echo "  delete: $fname"
    else
      echo "  would delete: $fname"
    fi
    DELETE_LIST+=("$fname")
  done <<< "$LISTING"

  if (( DELETE )) && [[ ${#DELETE_LIST[@]} -gt 0 ]]; then
    # Batch into a single ssh call per directory. Each remote path is
    # shell-quoted locally with printf %q before being embedded in the
    # remote command string, so filenames are safe to pass through even
    # though this archive's filenames are not expected to contain spaces.
    REMOTE_CMD="sudo rm -f --"
    for f in "${DELETE_LIST[@]}"; do
      REMOTE_CMD+=" $(printf '%q' "$REMOTE_DIR/$f")"
    done
    # shellcheck disable=SC2029  # intentional: $REMOTE_CMD is fully assembled client-side above
    ssh "$VM" "$REMOTE_CMD"
  fi

  echo
done

echo "== Summary =="
if (( DELETE )); then
  echo "$VERIFIED_COUNT verified+deleted"
else
  echo "$VERIFIED_COUNT verified (would-delete; re-run with --delete to prune)"
fi
echo "$UNSYNCED_COUNT skipped-unsynced (missing from S3 or size mismatch)"
echo "$YOUNG_COUNT skipped-too-young (within ${SAFETY_DAYS}-day safety window)"
