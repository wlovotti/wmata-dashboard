# Dev/Prod Environment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SSH-tunnel-as-dev-DB setup with a fully local PostgreSQL 16 dev loop that restores a slim, disposable copy of the prod dataset on demand, plus a near-free config seam and reconciled docs/NOTES.

**Architecture:** Upgrade local Postgres 14→16 so local == CI == prod. Add `bin/refresh-dev-db.sh` that pulls the weekly S3 `pg_dump` and restores it into a disposable local DB — slim by default (raw-feed tables excluded at the TOC level so their ~14 GB is never written), `--full` for pipeline work, `--scratch` for migration rehearsal. Repoint `.env` at the local socket; demote the tunnel to occasional ops. Centralize API CORS config behind an env-driven settings module.

**Tech Stack:** Bash, Homebrew PostgreSQL 16, AWS CLI v2 (S3), `pg_restore -L` (TOC filtering), `xz`, FastAPI/pydantic-free settings, pytest.

---

## Spec

Implements `docs/superpowers/specs/2026-06-13-dev-deploy-environments-design.md`. Read it first.

## Ground truth established before this plan (do not re-discover)

- **S3 dump:** `s3://wmata-dashboard-backups/wmata-db-backups/wmata_db_YYYYMMDD_HHMMSS.dump.xz` — an **xz-compressed `pg_dump -Fc`** (custom format). The `YYYYMMDD_HHMMSS` name sorts lexically = chronologically, so "latest" is a `sort | tail -1`. (Source: `deployment/scripts/backup_db.sh`.)
- **Slim is FK-safe.** No table has a foreign key *into* the excluded raw-feed tables (`vehicle_positions`, `trip_update_state`, `timepoint_times`, `collector_heartbeats`), verified against the live schema. Excluding their data cannot break `pg_restore`'s constraint creation. The only inbound FKs are GTFS tables → `gtfs_snapshots`, which slim keeps.
- **Measured footprints** (frozen 2026-06-05 full copy): full ≈ 31 GiB, slim ≈ 17 GiB, slim + prune-gtfs ≈ 9 GiB. `stop_times` is 88% `is_current=False` stale history (30.3M of 34.4M rows) the app never selects.
- **Toolchain present:** `postgresql@14` (running, port 5432), `aws` 2.35, `pg_restore`/`psql`/`pg_dump` (currently 14), `scripts/check_schema_drift.py`, `.env.example`. **Absent:** `postgresql@16`, `api/config.py`, `xz` (verify in Task 0).
- **Frontend** issues relative `fetch('/api/...')` at ~20 sites; dev relies on the Vite proxy (`/api` → `localhost:8000`). No central API client.
- **Branch:** do all implementation work on a feature branch (`feature/dev-deploy-environments`), not on `main` and not on the spec branch. Open one PR at the end.

## File structure

- **Create** `bin/refresh-dev-db.sh` — the only new runtime artifact; one responsibility: materialize a disposable local DB from a prod snapshot.
- **Create** `api/config.py` — env-driven settings (CORS origins; DATABASE_URL passthrough). One responsibility: turn environment into typed config.
- **Create** `tests/api/test_config.py` — unit tests for the settings parser.
- **Modify** `api/main.py` — read CORS origins from `api/config.py` instead of the hardcoded `["*"]`.
- **Modify** `.env`, `.env.example` — repoint dev `DATABASE_URL` to local PG16; document profiles.
- **Modify** `CLAUDE.md`, `docs/MIGRATIONS.md`, `docs/DEPLOYMENT.md`, `NOTES.md` — reconcile per spec §5.

---

## Task 0: Branch + clean working tree + pre-flight checks

**Files:** none new (commits pre-existing tunnel work)

The spec and this plan already live on branch `docs/dev-deploy-environments-design`. Continue implementation on that branch and open one PR from it — branching from `main` would orphan the spec/plan commits.

- [ ] **Step 1: Confirm the branch and commit the pre-existing tunnel work first**

The NOTES-48 item-3 tunnel work is dirty on the working tree (`bin/db-tunnel.sh` new; `docs/DEPLOYMENT.md`, `NOTES.md` modified). Commit it now so later doc edits (Task 5) land as clean, separate commits on the same files.

```bash
cd /Users/wlovotti/repos/wmata-dashboard
git branch --show-current   # expect: docs/dev-deploy-environments-design (spec + plan already committed here)
git add bin/db-tunnel.sh docs/DEPLOYMENT.md NOTES.md
git commit -m "feat: on-demand DB tunnel (bin/db-tunnel.sh) + NOTES-48 item 3/4 closeout"
git status   # expect: clean tree
```

- [ ] **Step 2: Confirm the tools the script needs exist**

```bash
which aws xz pg_restore || echo "MISSING — install before proceeding"
aws s3 ls s3://wmata-dashboard-backups/wmata-db-backups/ | tail -3
```

Expected: `aws` and `pg_restore` resolve; the `aws s3 ls` lists at least one `wmata_db_*.dump.xz` object. If `xz` is missing: `brew install xz`. If the `aws s3 ls` errors with AccessDenied, fix local AWS credentials (need `s3:GetObject`+`s3:ListBucket` on that prefix) before continuing — this is the spec's documented prerequisite.

---

## Task 1: Upgrade local Postgres 14 → 16

This is one-time environment surgery, not TDD. Each step has an explicit verification.

**Files:** none (Homebrew + local cluster)

- [ ] **Step 1: Install PostgreSQL 16 and make its client binaries default**

```bash
brew install postgresql@16
brew link --overwrite --force postgresql@16
hash -r
psql --version
```

Expected: `psql (PostgreSQL) 16.x`. If still 16-not-found, add `/opt/homebrew/opt/postgresql@16/bin` to PATH in your shell profile.

- [ ] **Step 2: Stop the PG14 service, start PG16 (both use port 5432)**

```bash
brew services stop postgresql@14
brew services start postgresql@16
sleep 2
psql -p 5432 -d postgres -c "SHOW server_version;"
```

Expected: `16.x`. The frozen PG14 data is preserved in S3 and on the VM, so PG14 staying stopped is safe. Leave `postgresql@14` installed-but-stopped for now (removable later).

- [ ] **Step 3: Create the empty dev database**

```bash
createdb -p 5432 wmata_dashboard
psql -p 5432 -d wmata_dashboard -c "\conninfo"
```

Expected: connects to `wmata_dashboard` on port 5432 as your OS user. (Objects will be owned by this user after the `--no-owner` restore.)

- [ ] **Step 4: Commit nothing — environment change only**

No repo change in this task. Proceed to Task 2; the dev DB gets populated by `refresh-dev-db.sh`.

---

## Task 2: Write `bin/refresh-dev-db.sh`

**Files:**
- Create: `bin/refresh-dev-db.sh`

- [ ] **Step 1: Write the script**

Create `bin/refresh-dev-db.sh` with exactly this content:

```bash
#!/usr/bin/env bash
# bin/refresh-dev-db.sh — materialize a disposable local copy of the prod
# dataset for development. See docs/superpowers/specs/2026-06-13-dev-deploy-
# environments-design.md §4.3.
#
#   bin/refresh-dev-db.sh                # slim (default): drop+recreate the dev DB,
#                                        #   restore the latest S3 dump WITHOUT the raw-feed tables (~17 GiB)
#   bin/refresh-dev-db.sh --full         # include raw-feed tables so the pipeline can run (~31 GiB)
#   bin/refresh-dev-db.sh --prune-gtfs   # after restore, delete is_current=False stop_times history (~9 GiB; VACUUM FULL)
#   bin/refresh-dev-db.sh --scratch      # restore into wmata_dashboard_scratch, leaving the dev DB untouched
#   bin/refresh-dev-db.sh --from-vm      # source a fresh pg_dump over the tunnel (bin/db-tunnel.sh) instead of S3
#
# Slim excludes the raw-feed tables at the pg_restore TOC level, so their data
# is never written to disk (no transient spike). The read-only API never reads
# them; only the collector/pipeline does. A slim DB therefore CANNOT run the
# derivation pipeline — use --full for that.
set -euo pipefail

BUCKET="${REFRESH_BUCKET:-wmata-dashboard-backups}"
PREFIX="${REFRESH_PREFIX:-wmata-db-backups}"
LOCAL_PORT="${REFRESH_PORT:-5432}"
TUNNEL_PORT="${REFRESH_TUNNEL_PORT:-5433}"   # bin/db-tunnel.sh forwards 5433 -> VM 5432
VM_DB_USER="${REFRESH_VM_DB_USER:-wmata}"

# Raw-feed tables the read-only API never queries (verified: no inbound FKs).
EXCLUDE_TABLES=(vehicle_positions trip_update_state timepoint_times collector_heartbeats)

MODE_FULL=0; MODE_SCRATCH=0; MODE_PRUNE_GTFS=0; MODE_FROM_VM=0
for arg in "$@"; do
  case "$arg" in
    --full)       MODE_FULL=1 ;;
    --scratch)    MODE_SCRATCH=1 ;;
    --prune-gtfs) MODE_PRUNE_GTFS=1 ;;
    --from-vm)    MODE_FROM_VM=1 ;;
    *) echo "Unknown flag: $arg" >&2; exit 2 ;;
  esac
done

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
    echo "Tunnel not up. Run bin/db-tunnel.sh in another terminal first." >&2; exit 1
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
```

- [ ] **Step 2: Make it executable and lint it**

```bash
chmod +x bin/refresh-dev-db.sh
shellcheck bin/refresh-dev-db.sh
```

Expected: shellcheck clean (or only style-level SC notes you accept). If `shellcheck` is absent: `brew install shellcheck`.

- [ ] **Step 3: Run the real slim restore (the actual acceptance test)**

```bash
bin/refresh-dev-db.sh
```

Expected: prints the latest `wmata_db_*.dump.xz` key, decompresses, restores, ends with a printed DB size in the ~15–18 GiB range. Restore should take a few minutes (raw-feed data skipped).

- [ ] **Step 4: Verify slim correctness — excluded tables exist but are empty, read tables are populated**

```bash
psql -p 5432 -d wmata_dashboard -At -F'|' -c "
SELECT 'vehicle_positions', count(*) FROM vehicle_positions
UNION ALL SELECT 'trip_update_state', count(*) FROM trip_update_state
UNION ALL SELECT 'stop_events', count(*) FROM stop_events
UNION ALL SELECT 'stop_times_current', count(*) FROM stop_times WHERE is_current
UNION ALL SELECT 'routes', count(*) FROM routes;"
```

Expected: `vehicle_positions` and `trip_update_state` = **0** (tables present, no rows); `stop_events`, `stop_times_current`, `routes` all **> 0**. If the excluded tables are missing entirely (not just empty), the TOC filter over-matched — it must drop only `TABLE DATA`, never the `TABLE` definition.

- [ ] **Step 5: Smoke the app against the slim local DB**

```bash
# Ensure .env points at local PG16 first (Task 4 Step 4 does this); for now:
DATABASE_URL=postgresql://localhost:5432/wmata_dashboard uv run python -c "
from src.database import get_session
from sqlalchemy import text
db = get_session()
print('routes:', db.execute(text('SELECT count(*) FROM routes')).scalar())
db.close()
print('OK')"
```

Expected: prints a route count and `OK`. No tunnel running.

- [ ] **Step 6: Commit**

```bash
git add bin/refresh-dev-db.sh
git commit -m "feat: add bin/refresh-dev-db.sh — disposable local prod-snapshot restore

Slim by default (excludes raw-feed tables the read-only API never reads,
via pg_restore TOC filter so their data is never written). --full for
pipeline work, --prune-gtfs to drop stale GTFS history, --scratch for
migration rehearsal, --from-vm for fresh data over the tunnel."
```

---

## Task 3: API config module (env-driven CORS)

The frontend `VITE_API_URL` seam from spec §4.5 is **deferred** — see Task 5 note. This task does the API half, which is genuinely small and makes CORS prod-ready.

**Files:**
- Create: `api/config.py`
- Create: `tests/api/test_config.py`
- Modify: `api/main.py` (CORS block, ~line 95)

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_config.py`:

```python
"""Tests for api.config environment-driven settings."""

import importlib

import api.config


def _reload(monkeypatch, **env):
    """Reload api.config with a patched environment and return the module."""
    for key in ("CORS_ALLOW_ORIGINS", "DATABASE_URL"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return importlib.reload(api.config)


def test_cors_defaults_to_wildcard(monkeypatch):
    """With no CORS_ALLOW_ORIGINS set, dev keeps the permissive wildcard."""
    cfg = _reload(monkeypatch)
    assert cfg.settings.cors_allow_origins == ["*"]


def test_cors_parses_comma_separated_origins(monkeypatch):
    """A comma-separated CORS_ALLOW_ORIGINS becomes a trimmed list."""
    cfg = _reload(monkeypatch, CORS_ALLOW_ORIGINS="https://a.example, https://b.example")
    assert cfg.settings.cors_allow_origins == ["https://a.example", "https://b.example"]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/api/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'api.config'`.

- [ ] **Step 3: Write the minimal implementation**

Create `api/config.py`:

```python
"""Application configuration sourced from environment variables.

Centralizes the env reads that differ between dev and a future public
deployment (spec 2026-06-13-dev-deploy-environments-design §4.5). Dev keeps
permissive defaults; a deployed instance overrides via the environment with
no code change.
"""

import os


class Settings:
    """Typed view over the process environment.

    Attributes:
        database_url: The SQLAlchemy URL (already consumed by src.database;
            mirrored here so config lives in one place).
        cors_allow_origins: Allowed CORS origins. ``CORS_ALLOW_ORIGINS`` is a
            comma-separated list; unset means the dev wildcard ``["*"]``.
    """

    def __init__(self) -> None:
        """Read settings from the current environment."""
        self.database_url = os.environ.get("DATABASE_URL", "")
        raw_origins = os.environ.get("CORS_ALLOW_ORIGINS", "*")
        self.cors_allow_origins = [o.strip() for o in raw_origins.split(",") if o.strip()]


settings = Settings()
"""Module-level singleton; import as ``from api.config import settings``."""
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/api/test_config.py -v`
Expected: both tests PASS.

- [ ] **Step 5: Wire the settings into `api/main.py`**

In `api/main.py`, add the import alongside the other `from api...` imports:

```python
from api.config import settings
```

Replace the CORS block (currently around line 95):

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

with:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins,  # dev: ["*"]; prod: set CORS_ALLOW_ORIGINS
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

- [ ] **Step 6: Verify the app still imports and the smoke suite passes**

```bash
uv run python -c "import api.main; print('import OK')"
uv run pytest -m smoke -q
```

Expected: `import OK`; smoke suite green.

- [ ] **Step 7: Lint + format**

```bash
uv run ruff check api/ tests/
uv run ruff format --check api/ tests/
```

Expected: both clean. If format flags files, run `uv run ruff format api/ tests/` and re-check.

- [ ] **Step 8: Commit**

```bash
git add api/config.py tests/api/test_config.py api/main.py
git commit -m "feat: drive API CORS origins from api.config (env-configurable)

Dev keeps the ['*'] wildcard; a deployed instance sets CORS_ALLOW_ORIGINS.
Pulls the API half of the NOTES-50 config seam forward (spec §4.5)."
```

---

## Task 4: Repoint dev config at local PG16

**Files:**
- Modify: `.env` (gitignored — edit in place, not committed)
- Modify: `.env.example`

- [ ] **Step 1: Repoint `.env` `DATABASE_URL` to the local socket**

Edit `.env`: comment out the tunnel `DATABASE_URL` line and set the active one to local PG16:

```
# ACTIVE: local PostgreSQL 16 dev copy (refreshed via bin/refresh-dev-db.sh).
DATABASE_URL=postgresql://localhost:5432/wmata_dashboard

# OPS ONLY: cloud VM over the SSH tunnel (open bin/db-tunnel.sh first; forwards
# local 5433 -> VM 5432). Use for ad-hoc prod psql, not the dev loop.
# DATABASE_URL=postgresql://wmata:5K18cmKVj0q4sr@localhost:5433/wmata_dashboard
```

- [ ] **Step 2: Verify the app serves against local PG16 with no tunnel**

```bash
uv run uvicorn api.main:app --port 8000 &
sleep 4
curl -s localhost:8000/api/gtfs/freshness | head -c 200; echo
kill %1
```

Expected: a JSON freshness payload (HTTP 200), served from the local DB with no tunnel process running.

- [ ] **Step 3: Update `.env.example` to document both profiles**

Replace the Database Configuration block in `.env.example` with:

```
# Database Configuration
# ---------------------------------------------------------------------------
# Local dev (default): PostgreSQL 16 holding a recent prod snapshot loaded by
# bin/refresh-dev-db.sh. local == CI == prod engine version.
DATABASE_URL=postgresql://localhost:5432/wmata_dashboard

# Ops-only: reach the cloud VM's Postgres over the SSH tunnel (bin/db-tunnel.sh,
# local 5433 -> VM 5432). Not for the dev loop.
# DATABASE_URL=postgresql://wmata:<vm-password>@localhost:5433/wmata_dashboard

# A deployed API restricts CORS via:
# CORS_ALLOW_ORIGINS=https://your-frontend.example
```

- [ ] **Step 4: Commit (`.env.example` only — `.env` is gitignored)**

```bash
git add .env.example
git commit -m "docs: point dev .env.example at local PG16; demote tunnel to ops-only"
```

---

## Task 5: Reconcile docs + NOTES (spec §5)

> **Frontend seam deferral (read before starting):** Spec §4.5 proposed pulling `VITE_API_URL` forward. On inspection the frontend issues relative `/api/...` at ~20 sites and the recommended prod deploy co-locates API + frontend behind one origin (spec §4.7), where relative paths work unchanged. Converting 20 call sites for an unused seam fails YAGNI, so the frontend half is **deferred** to NOTES-50 (the API half shipped in Task 3). This task records that decision. If the user wants the frontend seam now, add it as a separate task: create `frontend/src/api.js` exporting `apiUrl(p) = (import.meta.env.VITE_API_URL ?? '') + p`, convert the `fetch('/api/...')` sites, and regenerate Playwright baselines.

**Files:**
- Modify: `CLAUDE.md`
- Modify: `docs/MIGRATIONS.md`
- Modify: `docs/DEPLOYMENT.md`
- Modify: `NOTES.md`

- [ ] **Step 1: Rewrite the CLAUDE.md PostgreSQL-version constraint**

In `CLAUDE.md`, replace the `**Version skew:**` portion of the "PostgreSQL only" bullet:

Old:

```
  **Version skew:** the production Lightsail VM **and CI** run **PostgreSQL
  16**; local dev runs **14** for fast logic iteration — CI is the
  prod-parity gate, so a green build exercises prod's engine, not a third
  version. A 14→16 `pg_restore` is routine (it's how the cloud DB was
  loaded), but a 16→14 restore is *not* supported — never `pg_dump` from
  prod to restore into a local 14 cluster.
```

New:

```
  **Uniform PostgreSQL 16.** The production Lightsail VM, CI, **and local
  dev** all run **PostgreSQL 16** (local upgraded from 14 on 2026-06-13).
  Local dev loads a recent prod snapshot via `bin/refresh-dev-db.sh` —
  slim by default (drops the raw-feed tables the read-only API never reads;
  ~17 GiB), `--full` to include them for pipeline work. See
  `docs/DEPLOYMENT.md`. The former 14↔16 skew and "never restore 16→14"
  footgun are retired.
```

- [ ] **Step 2: Add the `--scratch` rehearsal step to `docs/MIGRATIONS.md`**

In the "test on a restored prod-data copy" part of `docs/MIGRATIONS.md`, add:

```
Rehearse against a throwaway copy without disturbing your dev DB:

    bin/refresh-dev-db.sh --scratch          # schema-only migrations
    bin/refresh-dev-db.sh --scratch --full   # if the migration touches the pipeline / raw-feed tables

Apply the migration to `wmata_dashboard_scratch`, then verify with
`scripts/check_schema_drift.py` and a pipeline smoke run before touching the VM.
```

- [ ] **Step 3: Document the dev-data refresh + tunnel demotion in `docs/DEPLOYMENT.md`**

Add a "Local development data" subsection near the tunnel runbook:

```
### Local development data

Dev runs against a local PostgreSQL 16 copy of the prod dataset, refreshed on
demand — never against the live VM DB.

    bin/refresh-dev-db.sh              # slim (~17 GiB): everything the API reads, no raw-feed tables
    bin/refresh-dev-db.sh --full       # add raw-feed tables so the pipeline can run (~31 GiB)
    bin/refresh-dev-db.sh --prune-gtfs # also drop stale GTFS history (~9 GiB)

It pulls the latest weekly dump from
`s3://wmata-dashboard-backups/wmata-db-backups/` (needs local AWS creds with
`s3:GetObject`+`ListBucket` on that prefix). The SSH tunnel (`bin/db-tunnel.sh`)
is now ops-only — ad-hoc prod `psql` and `bin/refresh-dev-db.sh --from-vm` — not
the dev DB connection.
```

- [ ] **Step 4: Reconcile NOTES.md**

Make these edits in `NOTES.md`:

1. **NOTES-88** (the `/api/routes` latency item, ~line 230 and ~line 734): downgrade from blocker to "fix before public deploy." Add a sentence: *"Recontextualized 2026-06-13: dev now runs on a local socket (no tunnel), so this no longer blocks dev or NOTES-84. It becomes a co-locate-API+DB task for the NOTES-50 public deploy."*
2. **NOTES-49** (managed Postgres, ~line 392): append to its trigger list: *"Revisit when multi-developer, public launch, or an automated migration cadence makes the on-demand `bin/refresh-dev-db.sh` restore insufficient (it replaces DB branching until then)."*
3. **NOTES-50** (deploy API+frontend, ~line 430): note that the **API** config seam (env-driven CORS) is **done** (Task 3 of this work); the **frontend** `VITE_API_URL` seam is deferred to this item, and the recommended deploy co-locates API+DB in-region (resolves NOTES-88).
4. Update the **"Last edited"** preamble (line 9) to note this environment work landed and the dev loop is now fully local on PG16.

- [ ] **Step 5: Verify markdown + commit**

```bash
git add CLAUDE.md docs/MIGRATIONS.md docs/DEPLOYMENT.md NOTES.md
git commit -m "docs: reconcile CLAUDE.md/MIGRATIONS/DEPLOYMENT/NOTES for local-PG16 dev loop

Uniform PG16 (skew note retired), --scratch migration rehearsal, dev-data
refresh + tunnel demotion runbook, NOTES-88 downgraded, NOTES-49/50 annotated."
```

---

## Task 6: Open the PR

The tunnel work was committed in Task 0; the spec/plan are already on this branch. Everything rides one PR.

- [ ] **Step 1: Run the full gates before pushing**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
uv run pytest -m smoke -q
```

Expected: all green.

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin docs/dev-deploy-environments-design
gh pr create --title "Local-PG16 dev loop + on-demand prod-snapshot refresh" \
  --body "Implements docs/superpowers/specs/2026-06-13-dev-deploy-environments-design.md.

- Local Postgres upgraded 14→16 (local == CI == prod)
- bin/refresh-dev-db.sh: disposable local restore, slim default (~17 GiB)
- API CORS now env-driven (api/config.py); frontend VITE_API_URL deferred to NOTES-50
- .env repointed to local socket; tunnel demoted to ops-only
- Docs/NOTES reconciled; closes NOTES-48 item 3/4

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Acceptance criteria (from spec §6)

- [ ] Local PG16 running; `wmata_dashboard` restored from a prod snapshot; app serves with no tunnel.
- [ ] `bin/refresh-dev-db.sh` slim reload in one command (~17 GiB, raw-feed tables empty); `--full` includes them; `--prune-gtfs` reaches ~9 GiB; `--scratch` isolates into `wmata_dashboard_scratch`; `--from-vm` works over the tunnel.
- [ ] `api/config.py` exists; dev CORS unchanged (`["*"]`); prod origins configurable via env.
- [ ] `.env` points dev at local PG16; `.env.example` documents both profiles.
- [ ] A migration can be rehearsed against `--scratch` and verified with `check_schema_drift.py` before any VM change.
- [ ] Docs/NOTES reconciled per spec §5 (frontend seam deferral recorded).
