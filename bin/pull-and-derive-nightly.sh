#!/usr/bin/env bash
# bin/pull-and-derive-nightly.sh — thin launchd wrapper around
# bin/pull-and-derive.sh (issue #246).
#
# Installed via deployment/launchd/com.wmata-dashboard.pull-and-derive.plist
# (StartCalendarInterval, ~03:30 local — see that plist for the exact
# fire time and install/uninstall steps, and docs/DEPLOYMENT.md §12 for
# the full runbook). launchd's StartCalendarInterval fires a missed run
# on next wake, so a laptop that was asleep at 03:30 catches up as soon
# as it wakes — "fresh within the hour of opening it," not "fresh only
# if the laptop happened to be awake at 03:30."
#
# What this wrapper adds on top of bin/pull-and-derive.sh itself:
#   1. cd to the repo root (launchd jobs don't inherit a shell's cwd).
#   2. Source .env if present, so DATABASE_URL / SFMTA_DATABASE_URL / AWS
#      credentials are available the same way an interactive `uv run`
#      invocation picks them up (launchd does not source shell rc files).
#   3. Run bin/pull-and-derive.sh itself.
#   4. Best-effort healthchecks.io dead-man ping on success/failure via
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

if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
  set +a
fi

echo "== $(date -u +%FT%TZ) pull-and-derive-nightly starting =="

"$REPO_ROOT/bin/pull-and-derive.sh"
rc=$?

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
