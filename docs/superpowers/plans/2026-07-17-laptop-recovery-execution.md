# Laptop Recovery Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the July 2026 recovery on the laptop per
`docs/superpowers/specs/2026-07-14-laptop-recovery-design.md` (rev 2a/2b):
dead-man alerting, migration tooling, local Phase 2 derivation, verification,
and VM demotion.

**Architecture:** Two independent PRs (alerting → VM; tooling → laptop-only)
followed by ops execution. The Phase 1 sync (`refresh-dev-db.sh --from-vm
--full`) is already running in the background; ops tasks gate on its
completion. Local `wmata_dashboard` becomes the system of record when the
sync lands.

**Tech Stack:** Python 3.12 / SQLAlchemy / psycopg2, bash, pyarrow+boto3,
pytest (SQLite `db_session` + Postgres `pg_session` fixtures), healthchecks.io.

## Global Constraints

- Never commit to `main`; feature branches + PRs (user global policy).
- Every function/class/method gets a docstring (user global policy).
- Run `uv run ruff check src/ scripts/ api/ pipelines/ tests/` AND
  `uv run ruff format --check src/ scripts/ api/ pipelines/ tests/` before
  every commit — CI gates both separately.
- Datetime storage is naive UTC; never `datetime.now()` for date math
  (`src/timezones.py`).
- Two branches, disjoint files, NOT stacked: `feature/collector-deadman`
  (Tasks 1–2) and `feature/laptop-migration-tooling` (Tasks 3–6). NOTES.md
  is touched ONLY by Tasks 2 and 9 (which are sequential), never by 3–6.
- The prod VM is user-SSH-only: every VM command in Tasks 2/8/9 is handed
  to the user as a `! ssh ...` one-shot, never run by the implementer.
- Ops tasks (8–9) run in this session, not via subagents (heavy backfills
  exhaust subagent budgets — standing project rule).

---

### Task 1: Dead-man ping module

**Files:**
- Create: `src/deadman.py`
- Test: `tests/test_deadman.py`

**Interfaces:**
- Produces: `ping_healthcheck(url: str | None, timeout: float = 5.0) -> bool`
  — consumed by Task 2.

- [ ] **Step 1: Branch**

```bash
git checkout main && git pull && git checkout -b feature/collector-deadman
```

- [ ] **Step 2: Write the failing test**

```python
"""Tests for src/deadman.py — the dead-man's-switch ping helper."""

import requests

from src.deadman import ping_healthcheck


def test_none_url_is_noop_and_false(monkeypatch):
    """A missing/empty URL disables pinging entirely — no HTTP call is made."""
    calls = []
    monkeypatch.setattr(requests, "post", lambda *a, **k: calls.append(a))
    assert ping_healthcheck(None) is False
    assert ping_healthcheck("") is False
    assert calls == []


def test_successful_ping_returns_true(monkeypatch):
    """Any completed HTTP exchange counts as a delivered ping."""
    seen = {}

    def fake_post(url, timeout):
        seen["url"], seen["timeout"] = url, timeout

    monkeypatch.setattr(requests, "post", fake_post)
    assert ping_healthcheck("https://hc-ping.example/uuid") is True
    assert seen == {"url": "https://hc-ping.example/uuid", "timeout": 5.0}


def test_network_failure_swallowed_returns_false(monkeypatch):
    """Network failures must never propagate into the collector tick."""

    def fake_post(url, timeout):
        raise requests.ConnectionError("refused")

    monkeypatch.setattr(requests, "post", fake_post)
    assert ping_healthcheck("https://hc-ping.example/uuid") is False
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_deadman.py -v`
Expected: FAIL / ERROR with `ModuleNotFoundError: No module named 'src.deadman'`

- [ ] **Step 4: Write the implementation**

```python
"""Dead-man's-switch ping for long-running daemons (NOTES-91).

The receiving service (healthchecks.io or similar) alerts when pings STOP
arriving. The caller must therefore invoke :func:`ping_healthcheck` only
when the work it guards actually succeeded — pinging unconditionally from
a wedged loop defeats the purpose (the 2026-07-17 collector wedge kept its
process alive for 9 hours while writing nothing).
"""

import requests


def ping_healthcheck(url: str | None, timeout: float = 5.0) -> bool:
    """POST a liveness ping to ``url``; never raises.

    Args:
        url: Healthcheck endpoint. ``None`` or empty disables pinging.
        timeout: Request timeout in seconds — kept short so a slow alerting
            service can never stall a collector tick.

    Returns:
        ``True`` if an HTTP exchange completed (any status counts — the
        alerting service registers receipt, not status), ``False`` when
        disabled or on any network failure.
    """
    if not url:
        return False
    try:
        requests.post(url, timeout=timeout)
        return True
    except requests.RequestException:
        return False
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_deadman.py -v`
Expected: 3 passed

- [ ] **Step 6: Lint + commit**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add src/deadman.py tests/test_deadman.py
git commit -m "feat: dead-man ping helper for collector liveness (NOTES-91)"
```

---

### Task 2: Wire the ping into the collector, close NOTES-91, PR

**Files:**
- Modify: `src/wmata_collector.py` (ctor ~line 53; `_save_trip_updates` ~line 656)
- Modify: `scripts/continuous_combined_collector.py` (~line 206, the
  `WMATADataCollector(API_KEY)` call)
- Modify: `NOTES.md` (item NOTES-91, ~line 868)
- Test: `tests/test_collector_dual_write.py` (add one test)

**Interfaces:**
- Consumes: `ping_healthcheck` from Task 1.
- Produces: `WMATADataCollector(__init__ ..., healthcheck_url: str | None = None)`;
  env var name `COLLECTOR_HEALTHCHECK_URL`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_collector_dual_write.py`, reusing that file's existing
collector/session fixtures and row-builder helper (do NOT invent a new row
shape — the file already constructs valid trip-update rows for
`_save_trip_updates`):

```python
def test_save_trip_updates_pings_healthcheck_after_commit(db_session, monkeypatch):
    """The dead-man ping fires exactly once per successful tick, after commit.

    Wedge protection: the ping must be tied to the same success condition as
    the collector_heartbeats write (2026-07-17 incident: process alive 9h,
    zero heartbeats — an unconditional ping would have masked it).
    """
    import src.wmata_collector as wc

    pings = []
    monkeypatch.setattr(wc, "ping_healthcheck", lambda url: pings.append(url))
    collector = <use this file's existing constructor pattern>(
        healthcheck_url="https://hc-ping.example/uuid"
    )
    rows = <use this file's existing row-builder for one valid row>
    collector._save_trip_updates(rows)
    assert pings == ["https://hc-ping.example/uuid"]
```

The two `<...>` markers mean "copy the construction pattern already used by
the neighboring tests in this exact file" — read the file first.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_collector_dual_write.py -k healthcheck -v`
Expected: FAIL with `TypeError` (unexpected keyword `healthcheck_url`) or
`AttributeError: ping_healthcheck`

- [ ] **Step 3: Implement**

In `src/wmata_collector.py`:
1. Add import: `from src.deadman import ping_healthcheck`
2. Ctor: add keyword param `healthcheck_url: str | None = None`, store as
   `self._healthcheck_url = healthcheck_url`, and document it in the ctor
   docstring: "Dead-man endpoint pinged once per successful trip-update
   tick; None disables (NOTES-91)."
3. In `_save_trip_updates`, immediately AFTER `self.db.commit()` (line
   ~656) and before the summary `print`, add:

```python
        # Dead-man ping: fires only when the tick's archive+upsert+heartbeat
        # all committed — a wedged collector goes silent and the alerting
        # service pages on the missing ping (NOTES-91).
        ping_healthcheck(self._healthcheck_url)
```

In `scripts/continuous_combined_collector.py` (env is loaded via
`load_dotenv()` at line 51; `os` is already imported for `API_KEY`):

```python
    collector = WMATADataCollector(
        API_KEY,
        healthcheck_url=os.environ.get("COLLECTOR_HEALTHCHECK_URL"),
    )
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_collector_dual_write.py tests/test_deadman.py -v`
Expected: all pass. Then the smoke suite: `uv run pytest -m smoke` — all pass.

- [ ] **Step 5: NOTES-91 edit (invoke the update-notes-in-pr skill)**

Invoke `update-notes-in-pr`. Intended outcome for the NOTES-91 body
(~line 868): shrink it to record that the collector dead-man ping shipped
in this PR (mechanism 1, applied to the collector rather than the batch),
and that batch-freshness alerting is superseded by the Path 2a migration
(derivation is now a manual laptop action; the S3-staleness alarm lands
with the stateless-collector rewrite). Also update the summary line at
~line 252.

- [ ] **Step 6: Lint, commit, push, PR**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add -A && git commit -m "feat: collector dead-man ping (closes NOTES-91)"
git push -u origin feature/collector-deadman
gh pr create --title "Collector dead-man ping (NOTES-91)" --body "Adds src/deadman.py and wires a per-tick liveness ping into the collector, fired only after the tick's archive+upsert+heartbeat commit (an unconditional ping would have masked the 2026-07-17 wedge). Env: COLLECTOR_HEALTHCHECK_URL (unset = disabled). Closes NOTES-91 (see NOTES.md edit). Deploy (user-run one-shots, code+env only, no unit change): see Deploy section below."
```

PR body must include a **Deploy** section with the user-run one-shots
(the VM is user-SSH-only) and note this is a code+env change, no systemd
unit change:

```
! ssh ubuntu@52.54.130.186 "sudo -u wmata bash -lc 'cd ~/wmata-dashboard && git pull'"
! ssh ubuntu@52.54.130.186 "sudo -u wmata bash -lc 'echo COLLECTOR_HEALTHCHECK_URL=<paste-url> >> ~/wmata-dashboard/.env'"
! ssh ubuntu@52.54.130.186 "sudo systemctl restart wmata-collector.service"
```

- [ ] **Step 7: User setup (blocking input)**

Ask the user to create a check at healthchecks.io (free tier): period
**5 minutes**, grace **5 minutes** (ticks are 30 s; ~10 min to page), and
paste the ping URL for the deploy one-shot. Verify after deploy: the check
shows pings AND `SELECT max(ts) FROM collector_heartbeats` advances.

---

### Task 3: `refresh-dev-db.sh` clobber guard

**Files:**
- Modify: `bin/refresh-dev-db.sh` (flag parse ~line 27; guard placed
  immediately after the flag loop, BEFORE any S3/tunnel access)

**Interfaces:**
- Produces: new flag `--clobber-primary`; refusal exit code 3.

- [ ] **Step 1: Branch**

```bash
git checkout main && git pull && git checkout -b feature/laptop-migration-tooling
```

- [ ] **Step 2: Implement**

Add to the flag loop: `--clobber-primary) MODE_CLOBBER=1 ;;` (initialize
`MODE_CLOBBER=0` beside the other MODE vars). Immediately after the loop:

```bash
# Path 2a (2026-07): local wmata_dashboard is the PRIMARY database, not a
# disposable dev copy. Refuse to drop it unless explicitly told to.
if [ "$MODE_SCRATCH" -eq 0 ] && [ "$MODE_CLOBBER" -eq 0 ]; then
  echo "REFUSING: 'wmata_dashboard' is the system of record (Path 2a, 2026-07)." >&2
  echo "Use --scratch for disposable restores, or --clobber-primary to really replace it." >&2
  exit 3
fi
```

Update the usage comment block at the top of the script to document both
the new flag and the changed default posture.

- [ ] **Step 3: Test manually**

```bash
bash -n bin/refresh-dev-db.sh                       # syntax OK
bin/refresh-dev-db.sh; echo "exit=$?"               # prints REFUSING, exit=3, instantly (no S3 access)
bin/refresh-dev-db.sh --scratch --full 2>&1 | head -2 &  # passes guard (reaches "Latest snapshot:"/aws); Ctrl-C or let it fail
```

- [ ] **Step 4: Commit**

```bash
git add bin/refresh-dev-db.sh
git commit -m "feat: refuse to clobber primary DB without --clobber-primary (Path 2a)"
```

---

### Task 4: Parquet→vehicle_positions loader

**Files:**
- Create: `pipelines/load_vp_from_parquet.py`
- Test: `tests/test_load_vp_from_parquet.py`

**Interfaces:**
- Consumes: `ARCHIVE_SCHEMA`, `ARCHIVE_COLUMNS`, `KEY_PREFIX` from
  `pipelines/archive_vehicle_positions.py`; `VehiclePosition` from
  `src.models`; S3 key layout `wmata-vp-archive/<YYYY-MM-DD>.parquet`
  in bucket `S3_ARCHIVE_BUCKET`.
- Produces: CLI `uv run python pipelines/load_vp_from_parquet.py --date
  2026-06-11 [--file /path/to.parquet] [--bucket NAME]`; function
  `load_parquet_into_vp(db, table: pyarrow.Table) -> int` (rows inserted).
  Consumed by Task 6 (Phase E).

- [ ] **Step 1: Write the failing test**

```python
"""Tests for pipelines/load_vp_from_parquet.py (pg_session — uses ON CONFLICT)."""

from datetime import datetime

import pyarrow as pa

from pipelines.archive_vehicle_positions import ARCHIVE_SCHEMA
from pipelines.load_vp_from_parquet import load_parquet_into_vp
from src.models import VehiclePosition


def _table(rows: list[dict]) -> pa.Table:
    """Build a pyarrow Table in the archive schema from row dicts."""
    cols = {name: [r.get(name) for r in rows] for name in ARCHIVE_SCHEMA.names}
    return pa.table(cols, schema=ARCHIVE_SCHEMA)


def _row(row_id: int) -> dict:
    """One minimal-but-valid archived VP row."""
    return {
        "id": row_id,
        "vehicle_id": f"bus-{row_id}",
        "route_id": "D72",
        "trip_id": "t1",
        "latitude": 38.9,
        "longitude": -77.0,
        "speed": 5.0,
        "current_stop_sequence": 3,
        "stop_id": "1001",
        "current_status": 2,
        "direction_id": 0,
        "trip_start_date": "20260611",
        "timestamp": datetime(2026, 6, 11, 12, 0, 0),
        "collected_at": datetime(2026, 6, 11, 12, 0, 1),
    }


def test_load_inserts_rows(pg_session):
    """All rows land; the count returned matches."""
    inserted = load_parquet_into_vp(pg_session, _table([_row(9000001), _row(9000002)]))
    assert inserted == 2
    assert (
        pg_session.query(VehiclePosition).filter(VehiclePosition.id.in_([9000001, 9000002])).count()
        == 2
    )


def test_load_is_idempotent_on_id_conflict(pg_session):
    """Re-loading the same file is a no-op, not an error (ON CONFLICT DO NOTHING)."""
    t = _table([_row(9000003)])
    assert load_parquet_into_vp(pg_session, t) == 1
    assert load_parquet_into_vp(pg_session, t) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_load_vp_from_parquet.py -v`
Expected: ERROR `ModuleNotFoundError: pipelines.load_vp_from_parquet`

- [ ] **Step 3: Implement**

```python
"""Restore a day of ``vehicle_positions`` from the S3 parquet archive.

Inverse of ``pipelines/archive_vehicle_positions.py``: downloads
``s3://<bucket>/wmata-vp-archive/<date>.parquet`` (or reads ``--file``)
and inserts the rows back with their original ids, skipping ids already
present (``ON CONFLICT DO NOTHING`` — safe to re-run, safe on overlap
with synced data). Built for the 6/11–6/12 fold-in of the 2026-07
recovery (spec 2026-07-14, Phase 2 step 5).

Usage:
    uv run python pipelines/load_vp_from_parquet.py --date 2026-06-11
    uv run python pipelines/load_vp_from_parquet.py --date 2026-06-11 --file /tmp/x.parquet
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from pipelines.archive_vehicle_positions import KEY_PREFIX
from src.database import get_session
from src.models import VehiclePosition

BATCH_SIZE = 50_000


def load_parquet_into_vp(db: Session, table: pa.Table) -> int:
    """Insert archived rows into ``vehicle_positions``; return rows inserted.

    Original ``id`` values are preserved; conflicts are skipped so the
    loader composes with synced data and with itself. Note: this is a
    restore tool, not a pipeline upsert — ``upsert_rows`` (the standard
    helper) always updates on conflict, which is wrong here, hence the
    direct ``ON CONFLICT DO NOTHING``. Commits once per BATCH_SIZE chunk.
    """
    inserted = 0
    rows = table.to_pylist()
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        stmt = (
            pg_insert(VehiclePosition)
            .values(chunk)
            .on_conflict_do_nothing(index_elements=["id"])
        )
        result = db.execute(stmt)
        inserted += result.rowcount
        db.commit()
    # Keep the sequence ahead of explicit ids so future inserts can't collide.
    db.execute(
        text(
            "SELECT setval('vehicle_positions_id_seq', "
            "(SELECT COALESCE(MAX(id), 1) FROM vehicle_positions))"
        )
    )
    db.commit()
    return inserted


def main() -> int:
    """CLI entry: fetch the day's parquet (S3 or --file) and load it."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (UTC archive day)")
    parser.add_argument("--file", help="Local parquet path (skips S3 download)")
    parser.add_argument("--bucket", default=os.environ.get("S3_ARCHIVE_BUCKET"))
    args = parser.parse_args()

    if args.file:
        path = Path(args.file)
    else:
        if not args.bucket:
            print("No --bucket and S3_ARCHIVE_BUCKET unset", file=sys.stderr)
            return 2
        key = f"{KEY_PREFIX}/{args.date}.parquet"
        path = Path(tempfile.mkdtemp()) / f"{args.date}.parquet"
        print(f"Downloading s3://{args.bucket}/{key} ...")
        boto3.client("s3").download_file(args.bucket, key, str(path))

    table = pq.read_table(path)
    db = get_session()
    try:
        inserted = load_parquet_into_vp(db, table)
    finally:
        db.close()
    print(f"{args.date}: {table.num_rows} rows in file, {inserted} inserted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_load_vp_from_parquet.py -v`
Expected: 2 passed (pg_session hits local Postgres —
`postgresql:///wmata_dashboard`; SAVEPOINT teardown rolls the writes back).

- [ ] **Step 5: Lint + commit**

```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
git add pipelines/load_vp_from_parquet.py tests/test_load_vp_from_parquet.py
git commit -m "feat: parquet->vehicle_positions restore loader for 6/11-6/12 fold-in"
```

---

### Task 5: `bin/pull-and-derive.sh` — interim ingest loop

**Files:**
- Create: `bin/pull-and-derive.sh` (mode 755)

**Interfaces:**
- Consumes: VM host `ubuntu@52.54.130.186`; tunnel on 5433
  (`bin/db-tunnel.sh`); env `VM_DB_URL`
  (`postgresql://wmata:<pw>@localhost:5433/wmata_dashboard` — the
  commented line in `.env`); both remote archive dirs (rev 2b split).
- Produces: `bin/pull-and-derive.sh [LOOKBACK_DAYS]` (default 14) — the
  manual-cadence command from spec decision 4.

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# bin/pull-and-derive.sh — interim Path 2a ingest: pull fresh raw data from
# the VM, replay locally, derive. Run on demand (manual cadence — spec
# 2026-07-14 decision 4). Requires: bin/db-tunnel.sh up, VM_DB_URL set.
#
#   bin/pull-and-derive.sh          # replay+derive lookback of 14 days
#   bin/pull-and-derive.sh 35       # wider catch-up
set -euo pipefail

VM="ubuntu@52.54.130.186"
REMOTE_ARCHIVE="/home/wmata/wmata-dashboard/archive/raw_snapshots"
REMOTE_OVERFLOW="/mnt/pgdata/archive-overflow"
LOCAL_ARCHIVE="${LOCAL_ARCHIVE:-archive/raw_snapshots}"
TUNNEL_PORT="${TUNNEL_PORT:-5433}"
LOOKBACK_DAYS="${1:-14}"
: "${VM_DB_URL:?Set VM_DB_URL (see the commented tunnel line in .env)}"

if ! lsof -nP -iTCP:"${TUNNEL_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Tunnel not up. Run bin/db-tunnel.sh first." >&2; exit 1
fi
mkdir -p "$LOCAL_ARCHIVE"

echo "== rsync raw TU archive (both dirs — rev 2b split) =="
rsync -av --rsync-path="sudo rsync" "$VM:$REMOTE_ARCHIVE/" "$LOCAL_ARCHIVE/"
rsync -av --rsync-path="sudo rsync" "$VM:$REMOTE_OVERFLOW/" "$LOCAL_ARCHIVE/"

echo "== pull vehicle_positions delta over tunnel =="
VP_COLS="id, vehicle_id, route_id, trip_id, latitude, longitude, speed, current_stop_sequence, stop_id, current_status, direction_id, trip_start_date, timestamp, collected_at"
LOCAL_MAX=$(psql -d wmata_dashboard -Atc "SELECT COALESCE(max(timestamp), '2026-01-01'::timestamp) FROM vehicle_positions")
echo "local VP high-water mark: $LOCAL_MAX"
# Strictly-greater window: ids are VM-assigned and the local table came from
# the same lineage, so a non-overlapping window cannot collide on the PK.
psql "$VM_DB_URL" -c "\copy (SELECT $VP_COLS FROM vehicle_positions WHERE timestamp > '$LOCAL_MAX') TO STDOUT" \
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
```

- [ ] **Step 2: Syntax check + dry validation**

```bash
chmod +x bin/pull-and-derive.sh && bash -n bin/pull-and-derive.sh
VM_DB_URL= bin/pull-and-derive.sh 2>&1 | head -1   # expect the VM_DB_URL error message
```

Full end-to-end run happens in Task 8 (after the sync lands) — do not run
it against the VM during this task.

- [ ] **Step 3: Commit**

```bash
git add bin/pull-and-derive.sh
git commit -m "feat: pull-and-derive interim ingest loop (Path 2a manual cadence)"
```

---

### Task 6: `scripts/local_recovery_2026_07.sh` — one-time Phase 2 driver

**Files:**
- Create: `scripts/local_recovery_2026_07.sh` (mode 755)

**Interfaces:**
- Consumes: the six per-date pipelines from `run_daily_batch.PIPELINES`
  (`derive_stop_events`, `derive_stop_events_from_state`, `aggregate_runs`,
  `compute_bunching`, `upsert_system_metrics_daily`,
  `upsert_route_metrics_overlay`), `replay_archive_to_state.py`,
  `load_vp_from_parquet.py` (Task 4).
- Produces: the Phase 2 execution artifact for Task 8; log file
  `logs/local_recovery_<ts>.log`; `grep -c FAILED` as the failure signal
  (with `$?` preserved — fixes the VM script's `$(stamp)` clobber bug).

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# scripts/local_recovery_2026_07.sh — one-time Phase 2 driver for the July
# 2026 recovery (spec docs/superpowers/specs/2026-07-14-laptop-recovery-
# design.md rev 2b). Scope: replay 7/02-7/03; derive 7/01-7/11; re-run the
# 6/15/16/18 deadlock trio (snapshot 12); catch-up sweep; 6/11-6/12 fold-in.
# Housekeeping (cleanup_trip_update_state) runs ONLY in the final sweep,
# after all backfill derivation — it deletes >7-day un-derived state rows.
set -uo pipefail   # deliberately NOT -e: per-date guards continue past failures

LOG="logs/local_recovery_$(date +%Y%m%dT%H%M%S).log"
mkdir -p logs
echo "Logging to $LOG"

run() {
  # run <label> <cmd...> — logs, preserves the command's exit code in $?.
  # (The VM driver's guard called $(stamp) after the command, clobbering
  # $? and logging every failure as "exit 0". No command substitution may
  # sit between the command and the rc capture.)
  local label="$1"; shift
  echo "== $(date -u +%FT%TZ) $label ==" | tee -a "$LOG"
  PYTHONUNBUFFERED=1 "$@" >> "$LOG" 2>&1
  local rc=$?
  [ $rc -ne 0 ] && echo "FAILED rc=$rc: $label" | tee -a "$LOG"
  return $rc
}

snap_args() {
  # snap_args <pipeline> <snapshot_id> — echoes --gtfs-snapshot-id N if the
  # pipeline advertises the flag (PR #170 added it selectively).
  if uv run python "pipelines/$1.py" --help 2>/dev/null | grep -q "gtfs-snapshot-id"; then
    echo "--gtfs-snapshot-id $2"
  fi
}

ALL_PIPELINES="derive_stop_events derive_stop_events_from_state aggregate_runs compute_bunching upsert_system_metrics_daily upsert_route_metrics_overlay"
STATE_PIPELINES="derive_stop_events_from_state aggregate_runs compute_bunching upsert_system_metrics_daily upsert_route_metrics_overlay"

derive_date() {
  # derive_date <date> "<pipeline names space-separated>" [snapshot_id]
  # (string list, not a bash nameref — macOS /bin/bash is 3.2, no `local -n`)
  local d="$1"; local plist="$2"; local snap="${3:-}"
  for p in $plist; do
    local extra=""
    [ -n "$snap" ] && extra="$(snap_args "$p" "$snap")"
    # shellcheck disable=SC2086
    run "$p $d${snap:+ snap$snap}" uv run python "pipelines/$p.py" --all-routes --date "$d" $extra || return 1
  done
}

echo "=== Phase A: replay 7/02-7/03 ===" | tee -a "$LOG"
for d in 2026-07-02 2026-07-03; do
  run "replay $d" uv run python pipelines/replay_archive_to_state.py --date "$d" || exit 1
done

echo "=== Phase B: derive 7/01-7/11 (snapshot 15 = is_current) ===" | tee -a "$LOG"
for d in 2026-07-01 2026-07-02 2026-07-03 2026-07-04 2026-07-05 2026-07-06 \
         2026-07-07 2026-07-08 2026-07-09 2026-07-10 2026-07-11; do
  derive_date "$d" "$ALL_PIPELINES" || echo "date $d had failures — continuing" | tee -a "$LOG"
done

echo "=== Phase C: deadlock trio (state-side re-run, snapshot 12) ===" | tee -a "$LOG"
for d in 2026-06-15 2026-06-16 2026-06-18; do
  derive_date "$d" "$STATE_PIPELINES" 12 || echo "date $d had failures — continuing" | tee -a "$LOG"
done

echo "=== Phase D: catch-up sweep (7/12 -> now; housekeeping runs here) ===" | tee -a "$LOG"
run "daily batch sweep" uv run python pipelines/run_daily_batch.py --lookback-days 35

echo "=== Phase E: 6/11-6/12 fold-in (snapshot 12) ===" | tee -a "$LOG"
for d in 2026-06-11 2026-06-12; do
  run "vp-parquet $d" uv run python pipelines/load_vp_from_parquet.py --date "$d" || continue
  run "replay $d" uv run python pipelines/replay_archive_to_state.py --date "$d" || continue
  derive_date "$d" "$ALL_PIPELINES" 12 || echo "date $d had failures — continuing" | tee -a "$LOG"
done

echo "=== Verification: runs per date ===" | tee -a "$LOG"
psql -d wmata_dashboard -c "
  SELECT service_date, count(*) AS runs,
         count(*) FILTER (WHERE source='trip_update') AS tu_runs,
         count(DISTINCT route_id) AS routes
  FROM runs WHERE service_date >= '2026-06-11'
  GROUP BY 1 ORDER BY 1" | tee -a "$LOG"
echo "FAILED count: $(grep -c FAILED "$LOG")" | tee -a "$LOG"
```

- [ ] **Step 2: Validate without executing pipelines**

```bash
chmod +x scripts/local_recovery_2026_07.sh && bash -n scripts/local_recovery_2026_07.sh
uv run python pipelines/derive_stop_events_from_state.py --help | grep -c gtfs-snapshot-id   # confirm snap_args probe works (expect ≥1)
```

- [ ] **Step 3: Commit**

```bash
git add scripts/local_recovery_2026_07.sh
git commit -m "feat: one-time local Phase 2 recovery driver (spec rev 2b)"
```

---

### Task 7: Tooling PR

- [ ] **Step 1: Full test + lint pass**

```bash
uv run pytest -m smoke && bin/test-with-pg
uv run ruff check src/ scripts/ api/ pipelines/ tests/ && uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
```

- [ ] **Step 2: Push + PR**

```bash
git push -u origin feature/laptop-migration-tooling
gh pr create --title "Laptop migration tooling (Path 2a recovery)" --body "Tooling for docs/superpowers/specs/2026-07-14-laptop-recovery-design.md (rev 2b): refresh-dev-db.sh clobber guard (local DB is now the system of record), parquet->vehicle_positions restore loader, bin/pull-and-derive.sh interim ingest, scripts/local_recovery_2026_07.sh one-time Phase 2 driver. Laptop-only; no VM deploy."
```

PR body: link the spec, list the four deliverables (guard, loader,
pull-and-derive, recovery driver), Deploy section = "laptop-only; no VM
deploy". Do NOT open this PR until the Task 2 PR is merged IF Task 2's
NOTES edit and anything here were to overlap — they don't (disjoint
files), so both PRs may be open concurrently.

---

### Task 8: OPS — run the recovery (this session, after the sync lands)

Preconditions: background sync finished cleanly (`sync_from_vm.log` ends
with a size line ~32 GB); tooling branch merged or checked out locally.

- [ ] **Step 1: Verify the restore** — run and compare against the recorded
  VM numbers (6/13–6/30 runs-per-date; snapshots 12 AND 15 in
  `gtfs_snapshots`; VP `trip_start_date` coverage 6/12→today):

```bash
psql -d wmata_dashboard -c "SELECT service_date, count(*) FROM runs WHERE service_date>='2026-06-13' GROUP BY 1 ORDER BY 1"
psql -d wmata_dashboard -c "SELECT pg_size_pretty(pg_database_size('wmata_dashboard'))"
```

- [ ] **Step 2: rsync the raw archive** (both dirs; ~13 GB first time):

```bash
mkdir -p archive/raw_snapshots
rsync -av --rsync-path="sudo rsync" ubuntu@52.54.130.186:/home/wmata/wmata-dashboard/archive/raw_snapshots/ archive/raw_snapshots/
rsync -av --rsync-path="sudo rsync" ubuntu@52.54.130.186:/mnt/pgdata/archive-overflow/ archive/raw_snapshots/
```

- [ ] **Step 3: Launch the driver** under caffeinate, in the background:

```bash
caffeinate -i scripts/local_recovery_2026_07.sh
```

Monitor via the log; the driver ends with the verification table and its
`FAILED count`.

- [ ] **Step 4: Verify** — all dates 6/11–7/15 within bands
  (~20–24 k runs / 126 routes weekdays; ~16–18 k / 104 weekends; reduced
  totals expected ONLY on known-outage dates 7/10, 7/11, 7/12, 7/17);
  `FAILED count: 0`.

- [ ] **Step 5: Rebuild-verify spot check (6/17)** — local-to-local scratch:

```bash
createdb wmata_dashboard_scratch
pg_dump -d wmata_dashboard -t routes -t trips -t stops -t stop_times -t calendar -t calendar_dates -t agencies -t gtfs_snapshots | psql -q -d wmata_dashboard_scratch
psql -d wmata_dashboard -c "\copy (SELECT * FROM vehicle_positions WHERE trip_start_date='20260617') TO STDOUT" | psql -d wmata_dashboard_scratch -c "CREATE TABLE vehicle_positions (LIKE public.vehicle_positions INCLUDING ALL)" -c "\copy vehicle_positions FROM STDIN"
```

Then replay 6/17 + run the six pipelines with
`DATABASE_URL=postgresql:///wmata_dashboard_scratch`, and diff:

```bash
psql -d wmata_dashboard_scratch -c "SELECT source, count(*) FROM runs WHERE service_date='2026-06-17' GROUP BY 1"
psql -d wmata_dashboard        -c "SELECT source, count(*) FROM runs WHERE service_date='2026-06-17' GROUP BY 1"
```

Counts must match exactly (derivation is deterministic). `dropdb
wmata_dashboard_scratch` afterwards. If the scratch schema fight exceeds
~30 min, downgrade to comparing `stop_events` counts only and note it.

- [ ] **Step 6: Frontend smoke** — `cd frontend && npm run dev`, open the
  Overview and a RouteDetail page, confirm the June–July window renders
  with data (no metric holes after 6/13).

---

### Task 9: OPS — VM demotion (user one-shots) + docs

- [ ] **Step 1: Restart the two surviving timers** (user runs):

```
! ssh ubuntu@52.54.130.186 "sudo systemctl start wmata-backup.timer wmata-archive-positions.timer; systemctl list-timers | grep wmata"
```

Expected: exactly two wmata timers listed. `wmata-metrics.timer` and
`wmata-window-derived.timer` stay stopped forever (do NOT start them; also
run `sudo systemctl disable` on both so a reboot can't resurrect them).

- [ ] **Step 2: Drain the archive backlog** — only AFTER Task 8 Step 4
  passes. First prove the laptop copy is complete (dry-run shows no
  missing files), then the user deletes:

```
rsync -avn --rsync-path="sudo rsync" ubuntu@52.54.130.186:/home/wmata/wmata-dashboard/archive/raw_snapshots/ archive/raw_snapshots/ | head
```

Expected: file list is empty (nothing left to transfer). Then, user-run,
delete remote JSONL older than 14 days in BOTH dirs (keep a rolling
2-week on-box buffer until the rewrite):

```
! ssh ubuntu@52.54.130.186 "sudo bash -c 'find /home/wmata/wmata-dashboard/archive/raw_snapshots /mnt/pgdata/archive-overflow -name \"*.jsonl.zst\" -mtime +14 -delete'; df -h /"
```

- [ ] **Step 3: Docs** — on the `docs/laptop-recovery-spec` branch: mark the
  spec Status line "Executed 2026-07-<NN>"; add a DEPLOYMENT.md note that
  the VM no longer runs derivation (metrics/window-derived retired,
  archive split across two dirs, laptop is the system of record); PR the
  docs branch.

- [ ] **Step 4: Update the postmortem** recovery-outcome TBDs
  (`docs/POSTMORTEM_2026-07.md` is the user's draft — propose the edits,
  let the user apply/approve them) and close out the incident memory.
