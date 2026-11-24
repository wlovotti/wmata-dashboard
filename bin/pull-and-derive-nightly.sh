#!/usr/bin/env bash
# bin/pull-and-derive-nightly.sh — thin launchd wrapper around
# bin/pull-and-derive.sh (issue #246).
#
# Installed via deployment/launchd/com.wmata-dashboard.pull-and-derive.plist
# (StartCalendarInterval, 06:30 local — see that plist for why 06:30 and
# not earlier (SFMTA's owl service ending ~03:00 Pacific = 06:00 Eastern
# is the binding constraint, not WMATA's earlier Eastern day-roll), the
# exact fire time, and install/uninstall steps; docs/DEPLOYMENT.md §12
# has the full runbook). launchd's StartCalendarInterval fires a missed
# run on next wake, so a laptop that was asleep at 06:30 catches up as
# soon as it wakes — "fresh within the hour of opening it," not "fresh
# only if the laptop happened to be awake at 06:30."
#
# What this wrapper adds on top of bin/pull-and-derive.sh itself:
#   1. cd to the repo root (launchd jobs don't inherit a shell's cwd).
#   2. mkdir -p logs (launchd already needs this to exist to spawn the
#      job at all — see the plist's StandardOutPath comment — but the
#      wrapper also creates it defensively for a manual invocation from
#      a fresh clone).
#   3. Skip cleanly (exit 0) when another refresh holds the lock:
#      bin/pull-and-derive.sh takes logs/.pull-and-derive.lock.d itself
#      (issue #261, manual runs included) and exits 75 when it can't.
#   4. Source .env if present, so DATABASE_URL / SFMTA_DATABASE_URL / AWS
#      credentials are available the same way an interactive `uv run`
#      invocation picks them up (launchd does not source shell rc files).
#   5. Run bin/pull-and-derive.sh itself.
#   6. Best-effort healthchecks.io dead-man ping on success/failure via
#      $PULL_AND_DERIVE_HEALTHCHECK_URL (unset = skip, loudly logged —
#      same pattern the collector's PingGate follows for its own checks,
#      see docs/DEPLOYMENT.md §13.5).
#
# Usage: invoked by launchd only. To validate manually:
#   bin/pull-and-derive-nightly.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"
mkdir -p logs

# Single-instance guard (review finding, PR #259): a launchd catch-up
# fire (a sleeping laptop woken after a missed 06:30) can otherwise land
# in the middle of a manual bin/pull-and-derive.sh run, or overlap a
# second catch-up fire. Since issue #261 bin/pull-and-derive.sh itself
# takes the lock (logs/.pull-and-derive.lock.d via src/run_lock.py, for
# manual runs too) and exits 75 when another run holds it; this wrapper
# no longer locks (taking it here as well would make the script refuse
# its own wrapper) and just treats 75 as "skip".

if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
  set +a
fi

echo "== $(date -u +%FT%TZ) pull-and-derive-nightly starting =="

"$REPO_ROOT/bin/pull-and-derive.sh"
rc=$?
if [ "$rc" -eq 75 ]; then
  echo "pull-and-derive-nightly: another pull-and-derive holds the lock — skipping." >&2
  exit 0
fi

if [ "$rc" -eq 0 ]; then
  echo "== $(date -u +%FT%TZ) pull-and-derive-nightly succeeded =="
  if [ -n "${PULL_AND_DERIVE_HEALTHCHECK_URL:-}" ]; then
    curl -fsS -m 10 --retry 3 "$PULL_AND_DERIVE_HEALTHCHECK_URL" >/dev/null 2>&1 \
      || echo "pull-and-derive-nightly: healthcheck success ping failed (non-fatal)" >&2
  else
    echo "pull-and-derive-nightly: PULL_AND_DERIVE_HEALTHCHECK_URL unset — skipping dead-man ping." >&2
  fi
else
  echo "== $(date -u +%FT%TZ) pull-and-derive-nightly FAILED (rc=$rc) — see output above ==" >&2
  if [ -n "${PULL_AND_DERIVE_HEALTHCHECK_URL:-}" ]; then
    curl -fsS -m 10 --retry 3 "${PULL_AND_DERIVE_HEALTHCHECK_URL}/fail" >/dev/null 2>&1 \
      || echo "pull-and-derive-nightly: healthcheck failure ping failed (non-fatal)" >&2
  else
    echo "pull-and-derive-nightly: PULL_AND_DERIVE_HEALTHCHECK_URL unset — skipping dead-man fail ping." >&2
  fi
fi

exit "$rc"
