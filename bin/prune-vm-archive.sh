#!/usr/bin/env bash
# bin/prune-vm-archive.sh — laptop-side verify-then-prune drain for the VM's
# raw JSONL archive (PR #183).
#
# `bin/pull-and-derive.sh` rsyncs the VM archive dirs down to the laptop,
# and the manual "aws s3 sync" step (docs/DEPLOYMENT.md, "Manual S3 sync")
# copies them up to S3 — but neither one deletes the VM-side originals.
# Without a drain, the VM root disk fills at ~1.3 GB/day combined
# (WMATA + SFMTA collectors), the same failure mode that caused the July
# 2026 disk-fill incident. Run this manually, from the laptop, after each
# S3 sync completes.
#
# Archive layout duplicated in bin/pull-and-derive.sh and
# docs/DEPLOYMENT.md ("Manual S3 sync") — adding a new archive dir?
# update all three. Covers all three VM archive locations:
#   /home/wmata/wmata-dashboard/archive/raw_snapshots/   -> s3://wmata-dashboard-backups/raw-jsonl-archive/<filename>
#   /mnt/pgdata/archive-overflow/                        -> s3://wmata-dashboard-backups/raw-jsonl-archive/<filename>
#   /home/wmata/wmata-dashboard/archive/sfmta_raw_snapshots/ -> s3://wmata-dashboard-backups/raw-jsonl-archive/sfmta/<filename>
#
# raw_snapshots/ and archive-overflow/ share ONE S3 prefix, so a file that
# exists under the same name in both dirs is a collision: whichever copy
# gets deleted first, the survivor's later sync silently overwrites the
# S3 object with (possibly different) bytes. Colliding filenames are never
# deleted from either dir — they are reported so a human can reconcile
# which copy is authoritative.
#
# For each VM-side file older than the safety window (default 7 days —
# minimum 1 day, since a 0-day window can catch the collector's live,
# currently-open file for today), the same-named S3 object is looked up
# in a single `aws s3api list-objects-v2` listing per prefix (not a
# per-file `head-object` — that's both slow on a multi-week backlog and
# collapses a 403/credential error into an indistinguishable "missing"
# result) and its byte size compared to the local file. A file is
# eligible for deletion ONLY when the S3 object exists AND the sizes
# match exactly.
#
# Verification runs across ALL THREE dirs before anything is deleted. If
# even one file anywhere is missing from S3 or size-mismatched, the
# script deletes NOTHING and exits 1 — a partially-failed sync should
# block the whole drain, not just the affected file, so a persistently
# broken sync can't quietly bleed off VM disk anyway while going unnoticed.
#
# Any SSH failure (bad VM_HOST, dead ssh-agent, sudo/auth failure) also
# aborts hard rather than being swallowed as "no files found" — a VM
# archive dir that genuinely doesn't exist is reported as an explicit
# "(dir not present on VM)", which is the only case that legitimately
# looks like zero files.
#
# Dry-run is the default: the script only prints what it would delete/skip.
# Pass --delete to actually delete verified, non-colliding files.
#
#   export VM_HOST=ubuntu@<vm-ip>            # required, same convention as bin/db-tunnel.sh
#   bin/prune-vm-archive.sh                  # dry run, 7-day safety window
#   bin/prune-vm-archive.sh --delete         # actually prune
#   bin/prune-vm-archive.sh --days 14        # wider safety window, dry run
#   bin/prune-vm-archive.sh --days 14 --delete
#
# Requires: SSH access to the VM (archive files are owned by the `wmata`
# user, so remote listing/deletion goes through sudo) and the AWS CLI
# configured locally (S3 checks run locally via `aws s3api
# list-objects-v2`, not over SSH).
set -euo pipefail

VM="${VM_HOST:?Set VM_HOST (e.g. ubuntu@<vm-ip>) — same convention as bin/db-tunnel.sh}"
S3_BUCKET="wmata-dashboard-backups"

REMOTE_RAW_DIR="/home/wmata/wmata-dashboard/archive/raw_snapshots"
REMOTE_OVERFLOW_DIR="/mnt/pgdata/archive-overflow"
REMOTE_SFMTA_DIR="/home/wmata/wmata-dashboard/archive/sfmta_raw_snapshots"

S3_PREFIX_RAW="raw-jsonl-archive"
S3_PREFIX_SFMTA="raw-jsonl-archive/sfmta"

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
      echo "  --days N   safety window in days (default 7, minimum 1); files younger than this are never touched"
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
if (( SAFETY_DAYS < 1 )); then
  echo "--days must be >= 1. A 0-day window can size-match and delete the" >&2
  echo "collector's live, currently-open JSONL file for today (it can" >&2
  echo "momentarily match S3 right after a sync, before the day's" >&2
  echo "remaining rows are written) — the rest of the day's data would be" >&2
  echo "lost on rotation." >&2
  exit 1
fi

NOW_EPOCH=$(date +%s)
SAFETY_SECONDS=$(( SAFETY_DAYS * 86400 ))

# Temp files: two S3 listings (one per prefix) as "key<TAB>size" lines, plus
# two sorted filename lists for the raw/overflow collision check. No bash
# associative arrays anywhere in this script — it needs to run on macOS's
# stock bash 3.2, which predates `declare -A`.
S3_RAW_TMP=$(mktemp)
S3_SFMTA_TMP=$(mktemp)
RAW_NAMES_TMP=$(mktemp)
OVERFLOW_NAMES_TMP=$(mktemp)
trap 'rm -f "$S3_RAW_TMP" "$S3_SFMTA_TMP" "$RAW_NAMES_TMP" "$OVERFLOW_NAMES_TMP"' EXIT

# fetch_listing REMOTE_DIR — prints tab-separated "name\tsize\tmtime_epoch"
# lines for every file directly under REMOTE_DIR, or the literal string
# __ABSENT__ if REMOTE_DIR does not exist on the VM. Aborts the whole
# script on any other failure (bad host, dead ssh-agent, sudo/auth
# failure, etc.) rather than treating it as "no files".
#
# The presence check and the find both run inside ONE `sudo sh -c '...'`
# invocation. That matters: if sudo itself can't even start (auth
# failure), the inner script never runs, so __ABSENT__ is never printed
# and the ssh command exits nonzero — which is exactly what triggers the
# hard-fail below. A structure that ran `sudo test -d` as its own
# separate step would conflate "sudo failed" and "directory legitimately
# absent" into the same nonzero-exit code, and both would look identical
# to the caller.
fetch_listing() {
  local remote_dir="$1"
  local cmd="sudo sh -c '[ -d $remote_dir ] || { echo __ABSENT__; exit 0; }; find $remote_dir -maxdepth 1 -type f -printf \"%f\\t%s\\t%T@\\n\"'"
  local output status=0
  # shellcheck disable=SC2029  # intentional: $remote_dir expands client-side to build the remote command
  output=$(ssh "$VM" "$cmd" 2>&1) || status=$?
  if (( status != 0 )); then
    echo "ERROR: ssh to $VM failed while listing $remote_dir (exit $status):" >&2
    echo "$output" >&2
    echo "If this looks like an authentication failure, check your ssh-agent:" >&2
    echo "  ssh-add --apple-use-keychain ~/.ssh/id_ed25519" >&2
    exit 1
  fi
  printf '%s\n' "$output"
}

# fetch_s3_listing PREFIX TMP_FILE — writes "key<TAB>size" lines for every
# object under s3://$S3_BUCKET/PREFIX/ to TMP_FILE. One call per prefix
# covers the whole prefix regardless of object count: list-objects-v2 is
# paginated by the AWS CLI automatically (it only stops auto-paginating
# if --no-paginate or --max-items is passed, neither of which we pass),
# so this single call already covers a >1000-object backlog. Aborts hard
# on any AWS CLI failure — a credentials/network error must never be
# silently treated as "no objects" (that would look identical to a
# genuinely-empty prefix and everything in it would report as unsynced).
fetch_s3_listing() {
  local prefix="$1" tmp_file="$2"
  local raw status=0
  raw=$(aws s3api list-objects-v2 --bucket "$S3_BUCKET" --prefix "$prefix/" \
    --query 'Contents[].[Key,Size]' --output text 2>&1) || status=$?
  if (( status != 0 )); then
    echo "ERROR: aws s3api list-objects-v2 failed for s3://$S3_BUCKET/$prefix/:" >&2
    echo "$raw" >&2
    exit 1
  fi
  if [[ "$raw" == "None" || -z "$raw" ]]; then
    : > "$tmp_file"
  else
    printf '%s\n' "$raw" > "$tmp_file"
  fi
}

# s3_lookup_size TMP_FILE KEY — prints the byte size for an exact-match
# key from a TMP_FILE built by fetch_s3_listing, or nothing if absent.
s3_lookup_size() {
  local tmp_file="$1" key="$2"
  awk -F'\t' -v k="$key" '$1==k{print $2; exit}' "$tmp_file"
}

YOUNG_COUNT=0
UNSYNCED_COUNT=0
COLLISION_COUNT=0
VERIFIED_COUNT=0
DELETED_COUNT=0
DELETE_FAILURES=0

# classify_file NAME SIZE MTIME S3_TMP S3_PREFIX IN_COLLISION — prints a
# one-line verdict for a single file and updates the matching global
# counter. Sets IS_ELIGIBLE=1 (and leaves it 0 otherwise) so the caller
# knows whether to add NAME to its dir's deletion candidate list.
classify_file() {
  local fname="$1" fsize="$2" fmtime="$3" s3_tmp="$4" s3_prefix="$5" in_collision="$6"
  local fmtime_int age s3_size

  IS_ELIGIBLE=0

  if (( in_collision )); then
    echo "  COLLISION: $fname exists in both raw_snapshots/ and archive-overflow/ — resolve manually, not deleting either copy" >&2
    COLLISION_COUNT=$(( COLLISION_COUNT + 1 ))
    return
  fi

  fmtime_int="${fmtime%.*}"
  age=$(( NOW_EPOCH - fmtime_int ))
  if (( age < SAFETY_SECONDS )); then
    echo "  skip (too young): $fname"
    YOUNG_COUNT=$(( YOUNG_COUNT + 1 ))
    return
  fi

  s3_size=$(s3_lookup_size "$s3_tmp" "$s3_prefix/$fname")
  if [[ -z "$s3_size" ]]; then
    echo "  skip (not found in S3): $fname"
    UNSYNCED_COUNT=$(( UNSYNCED_COUNT + 1 ))
    return
  fi
  if [[ "$s3_size" != "$fsize" ]]; then
    echo "  skip (size mismatch: local=$fsize s3=$s3_size): $fname"
    UNSYNCED_COUNT=$(( UNSYNCED_COUNT + 1 ))
    return
  fi

  echo "  verified: $fname"
  VERIFIED_COUNT=$(( VERIFIED_COUNT + 1 ))
  IS_ELIGIBLE=1
}

# is_collision NAME — true if NAME is in the precomputed COLLISIONS list.
# Guarded with a length check before "${COLLISIONS[@]}": macOS's stock
# bash (3.2) raises "unbound variable" under `set -u` when expanding a
# zero-element array with [@], a bug not fixed until bash 4.4. Every
# whole-array [@]/[*] expansion in this script is guarded the same way;
# ${#arr[@]} and single-index ${arr[$i]} access are unaffected and safe
# unguarded.
is_collision() {
  local target="$1" n
  [[ ${#COLLISIONS[@]} -eq 0 ]] && return 1
  for n in "${COLLISIONS[@]}"; do
    [[ "$n" == "$target" ]] && return 0
  done
  return 1
}

# delete_batch REMOTE_DIR LABEL FILE... — deletes all FILE args from
# REMOTE_DIR in one ssh call. Only increments DELETED_COUNT (and prints
# the "deleted" line) after the remote rm actually succeeds — a failed
# batch must never be reported as done.
delete_batch() {
  local remote_dir="$1" label="$2"
  shift 2
  local files=("$@")
  (( ${#files[@]} == 0 )) && return
  local remote_cmd="sudo rm -f --"
  local f
  for f in "${files[@]}"; do
    remote_cmd+=" $(printf '%q' "$remote_dir/$f")"
  done
  local status=0
  # shellcheck disable=SC2029  # intentional: $remote_cmd is fully assembled client-side above
  ssh "$VM" "$remote_cmd" || status=$?
  if (( status != 0 )); then
    echo "ERROR: deletion failed for $label (exit $status) — some files may remain undeleted." >&2
    DELETE_FAILURES=$(( DELETE_FAILURES + 1 ))
    return
  fi
  DELETED_COUNT=$(( DELETED_COUNT + ${#files[@]} ))
  echo "deleted ${#files[@]} files from $label"
}

print_summary() {
  echo
  echo "== Summary =="
  if (( DELETE )); then
    echo "$VERIFIED_COUNT verified; $DELETED_COUNT actually deleted"
  else
    echo "$VERIFIED_COUNT verified (would-delete; re-run with --delete to prune)"
  fi
  echo "$UNSYNCED_COUNT skipped-unsynced (missing from S3 or size mismatch)"
  echo "$YOUNG_COUNT skipped-too-young (within ${SAFETY_DAYS}-day safety window)"
  echo "$COLLISION_COUNT skipped-collision (same filename present in both raw_snapshots and archive-overflow)"
}

echo "== fetching S3 listings =="
fetch_s3_listing "$S3_PREFIX_RAW" "$S3_RAW_TMP"
fetch_s3_listing "$S3_PREFIX_SFMTA" "$S3_SFMTA_TMP"

echo "== fetching VM listings =="
RAW_FNAMES=()
RAW_SIZES=()
RAW_MTIMES=()
RAW_LISTING=$(fetch_listing "$REMOTE_RAW_DIR")
if [[ "$RAW_LISTING" == "__ABSENT__" ]]; then
  RAW_ABSENT=1
else
  RAW_ABSENT=0
  while IFS=$'\t' read -r fname fsize fmtime; do
    [[ -z "$fname" ]] && continue
    RAW_FNAMES+=("$fname")
    RAW_SIZES+=("$fsize")
    RAW_MTIMES+=("$fmtime")
  done <<< "$RAW_LISTING"
fi

OVERFLOW_FNAMES=()
OVERFLOW_SIZES=()
OVERFLOW_MTIMES=()
OVERFLOW_LISTING=$(fetch_listing "$REMOTE_OVERFLOW_DIR")
if [[ "$OVERFLOW_LISTING" == "__ABSENT__" ]]; then
  OVERFLOW_ABSENT=1
else
  OVERFLOW_ABSENT=0
  while IFS=$'\t' read -r fname fsize fmtime; do
    [[ -z "$fname" ]] && continue
    OVERFLOW_FNAMES+=("$fname")
    OVERFLOW_SIZES+=("$fsize")
    OVERFLOW_MTIMES+=("$fmtime")
  done <<< "$OVERFLOW_LISTING"
fi

SFMTA_FNAMES=()
SFMTA_SIZES=()
SFMTA_MTIMES=()
SFMTA_LISTING=$(fetch_listing "$REMOTE_SFMTA_DIR")
if [[ "$SFMTA_LISTING" == "__ABSENT__" ]]; then
  SFMTA_ABSENT=1
else
  SFMTA_ABSENT=0
  while IFS=$'\t' read -r fname fsize fmtime; do
    [[ -z "$fname" ]] && continue
    SFMTA_FNAMES+=("$fname")
    SFMTA_SIZES+=("$fsize")
    SFMTA_MTIMES+=("$fmtime")
  done <<< "$SFMTA_LISTING"
fi

# Collision check: any filename present in BOTH raw_snapshots and
# archive-overflow (they share one S3 prefix) is never deleted from
# either side.
if [[ ${#RAW_FNAMES[@]} -gt 0 ]]; then
  printf '%s\n' "${RAW_FNAMES[@]}" | sort > "$RAW_NAMES_TMP"
else
  : > "$RAW_NAMES_TMP"
fi
if [[ ${#OVERFLOW_FNAMES[@]} -gt 0 ]]; then
  printf '%s\n' "${OVERFLOW_FNAMES[@]}" | sort > "$OVERFLOW_NAMES_TMP"
else
  : > "$OVERFLOW_NAMES_TMP"
fi
COLLISIONS=()
while IFS= read -r name; do
  [[ -z "$name" ]] && continue
  COLLISIONS+=("$name")
done < <(comm -12 "$RAW_NAMES_TMP" "$OVERFLOW_NAMES_TMP")

echo
echo "== $REMOTE_RAW_DIR -> s3://$S3_BUCKET/$S3_PREFIX_RAW/ =="
# Declared unconditionally (not just in the non-absent branch below) —
# under `set -u`, referencing it in the deletion phase later would
# otherwise be a genuinely unset variable when the dir is absent, not
# just an empty array.
RAW_ELIGIBLE=()
if (( RAW_ABSENT )); then
  echo "  (dir not present on VM)"
else
  if [[ ${#RAW_FNAMES[@]} -eq 0 ]]; then
    echo "  (no files found)"
  fi
  for (( i = 0; i < ${#RAW_FNAMES[@]}; i++ )); do
    in_collision=0
    is_collision "${RAW_FNAMES[$i]}" && in_collision=1
    classify_file "${RAW_FNAMES[$i]}" "${RAW_SIZES[$i]}" "${RAW_MTIMES[$i]}" "$S3_RAW_TMP" "$S3_PREFIX_RAW" "$in_collision"
    (( IS_ELIGIBLE )) && RAW_ELIGIBLE+=("${RAW_FNAMES[$i]}")
  done
fi

echo
echo "== $REMOTE_OVERFLOW_DIR -> s3://$S3_BUCKET/$S3_PREFIX_RAW/ =="
OVERFLOW_ELIGIBLE=()
if (( OVERFLOW_ABSENT )); then
  echo "  (dir not present on VM)"
else
  if [[ ${#OVERFLOW_FNAMES[@]} -eq 0 ]]; then
    echo "  (no files found)"
  fi
  for (( i = 0; i < ${#OVERFLOW_FNAMES[@]}; i++ )); do
    in_collision=0
    is_collision "${OVERFLOW_FNAMES[$i]}" && in_collision=1
    classify_file "${OVERFLOW_FNAMES[$i]}" "${OVERFLOW_SIZES[$i]}" "${OVERFLOW_MTIMES[$i]}" "$S3_RAW_TMP" "$S3_PREFIX_RAW" "$in_collision"
    (( IS_ELIGIBLE )) && OVERFLOW_ELIGIBLE+=("${OVERFLOW_FNAMES[$i]}")
  done
fi

echo
echo "== $REMOTE_SFMTA_DIR -> s3://$S3_BUCKET/$S3_PREFIX_SFMTA/ =="
SFMTA_ELIGIBLE=()
if (( SFMTA_ABSENT )); then
  echo "  (dir not present on VM)"
else
  if [[ ${#SFMTA_FNAMES[@]} -eq 0 ]]; then
    echo "  (no files found)"
  fi
  for (( i = 0; i < ${#SFMTA_FNAMES[@]}; i++ )); do
    classify_file "${SFMTA_FNAMES[$i]}" "${SFMTA_SIZES[$i]}" "${SFMTA_MTIMES[$i]}" "$S3_SFMTA_TMP" "$S3_PREFIX_SFMTA" 0
    (( IS_ELIGIBLE )) && SFMTA_ELIGIBLE+=("${SFMTA_FNAMES[$i]}")
  done
fi

# Fail-closed: a partially-failed sync anywhere blocks deletion
# everywhere. Dry-run still exits 1 here so scripted callers notice.
if (( UNSYNCED_COUNT > 0 )); then
  echo
  echo "ERROR: $UNSYNCED_COUNT file(s) are not confirmed in S3 (missing or size-mismatched)." >&2
  echo "Re-run the S3 sync (docs/DEPLOYMENT.md, 'Manual S3 sync') and re-run this script before pruning anything." >&2
  print_summary
  exit 1
fi

if (( DELETE )); then
  echo
  echo "== deleting =="
  # Guarded expansion (see is_collision's comment above) — an eligible
  # list can legitimately be empty (nothing to delete in that dir).
  [[ ${#RAW_ELIGIBLE[@]} -gt 0 ]] && delete_batch "$REMOTE_RAW_DIR" "raw_snapshots" "${RAW_ELIGIBLE[@]}"
  [[ ${#OVERFLOW_ELIGIBLE[@]} -gt 0 ]] && delete_batch "$REMOTE_OVERFLOW_DIR" "archive-overflow" "${OVERFLOW_ELIGIBLE[@]}"
  [[ ${#SFMTA_ELIGIBLE[@]} -gt 0 ]] && delete_batch "$REMOTE_SFMTA_DIR" "sfmta_raw_snapshots" "${SFMTA_ELIGIBLE[@]}"
fi

print_summary

if (( DELETE_FAILURES > 0 )); then
  exit 1
fi
