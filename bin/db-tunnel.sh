#!/usr/bin/env bash
# bin/db-tunnel.sh — open an on-demand SSH tunnel from this laptop to the
# cloud VM's Postgres (NOTES-48 item 3).
#
# The VM's Postgres binds to localhost only (never exposed to the internet),
# so the local API/frontend reach it through an SSH tunnel. This forwards
# local port 5433 -> VM port 5432. We deliberately use 5433, NOT 5432, so the
# tunnel never collides with the local dev Postgres@14 that stays bound to
# 5432 for fast logic iteration and ad-hoc psql (see CLAUDE.md).
#
# With this tunnel up and .env's DATABASE_URL pointed at
# postgresql://wmata:<pw>@localhost:5433/wmata_dashboard, the local API serves
# live cloud data:
#
#   bin/db-tunnel.sh            # in one terminal — runs in foreground
#   uv run uvicorn api.main:app --reload   # in another
#   psql -h localhost -p 5433 -U wmata wmata_dashboard   # ad-hoc queries
#
# Ctrl-C closes the tunnel cleanly. This is an on-demand tunnel by design —
# we do NOT run a persistent/auto-reconnecting tunnel, to keep the laptop
# decoupled from the VM (the whole point of the cloud migration). A 24/7
# tunnel only makes sense once the API is publicly deployed (NOTES-50).
#
# Env overrides:
#   VM_HOST   default ubuntu@52.54.130.186
#   LOCAL_PORT default 5433
#   REMOTE_PORT default 5432
set -euo pipefail

VM_HOST="${VM_HOST:-ubuntu@52.54.130.186}"
LOCAL_PORT="${LOCAL_PORT:-5433}"
REMOTE_PORT="${REMOTE_PORT:-5432}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"

# Bail early if something already holds the local port (a stale tunnel, or a
# second Postgres) — silently layering tunnels leads to confusing failures.
if lsof -nP -iTCP:"${LOCAL_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port ${LOCAL_PORT} is already in use. A tunnel may already be up:" >&2
  lsof -nP -iTCP:"${LOCAL_PORT}" -sTCP:LISTEN >&2
  exit 1
fi

# After a laptop reboot the ssh-agent is empty, so key-based auth to the VM
# fails until the key is re-added from the keychain. Add it if absent
# (idempotent — a no-op when the key is already loaded).
if ! ssh-add -l 2>/dev/null | grep -q "$(ssh-keygen -lf "${SSH_KEY}.pub" 2>/dev/null | awk '{print $2}')"; then
  echo "Adding SSH key to agent from keychain..."
  ssh-add --apple-use-keychain "${SSH_KEY}"
fi

echo "Opening tunnel: localhost:${LOCAL_PORT} -> ${VM_HOST}:${REMOTE_PORT}"
echo "Leave this running. Ctrl-C to close."
# -N: no remote command (tunnel only). ServerAlive* drops the tunnel within
# ~30s of the connection dying rather than hanging on a half-open socket.
exec ssh -N \
  -o ServerAliveInterval=15 \
  -o ServerAliveCountMax=2 \
  -L "${LOCAL_PORT}:localhost:${REMOTE_PORT}" \
  "${VM_HOST}"
