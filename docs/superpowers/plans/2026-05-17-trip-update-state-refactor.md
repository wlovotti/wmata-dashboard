# Trip Update State Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the append-only `trip_update_snapshots` table (~21M rows/day, 129 GB) with a much smaller `trip_update_state` UPSERT-only table (~180K rows/day, 1-3 GB), plus a compressed JSONL → parquet → Backblaze B2 archive for cold raw evidence. Derivation reads final-state directly from `trip_update_state`.

**Architecture:** The collector keeps writing to `trip_update_snapshots` during dual-write/validation, then UPSERTs each (trip_id, stop_sequence) into `trip_update_state`, AND appends every raw row to a daily JSONL archive file. A nightly rotation job converts yesterday's JSONL to parquet and uploads to B2. A new derivation pipeline reads `trip_update_state` directly. After ≥ 7 days of validated parity with the old pipeline, cutover removes the old write path; after 14 more days clean, the old table and dead code are dropped.

**Tech Stack:** Python 3.11+, SQLAlchemy 2.x, PostgreSQL 16+, polars (parquet I/O), zstandard (streaming JSONL compression), boto3 (S3-compatible B2 API), pytest, uv.

**Spec:** `docs/superpowers/specs/2026-05-17-trip-update-state-refactor-design.md`

---

## Prerequisites (do before Task 1)

These are one-time setup steps. They are NOT TDD tasks — they prepare the environment.

### P1 — Verify branch state

- [ ] Confirm you are NOT on `main` or `docs/trip-update-state-refactor-spec`
- [ ] Create implementation branch off `main`:

```bash
git checkout main
git pull
git checkout -b feature/trip-update-state-refactor
```

### P2 — Backblaze B2 account + bucket

The cold archive lives in B2 (S3-compatible, $0.005/GB/month).

- [ ] Sign up at https://www.backblaze.com/cloud-storage
- [ ] Create a bucket. Suggested name: `wmata-archive-<short-suffix>` (must be globally unique). Set it to **private** and lifecycle "Keep all versions of the file."
- [ ] Create an Application Key scoped to this bucket with read+write permission. Save the key ID and application key — you can't view the secret again.
- [ ] Note your bucket's S3 endpoint URL from the bucket details page (looks like `https://s3.us-east-005.backblazeb2.com`).
- [ ] Append to `.env` (do NOT commit this file):

```bash
B2_ACCESS_KEY_ID=<your-key-id>
B2_SECRET_ACCESS_KEY=<your-application-key>
B2_ENDPOINT_URL=https://s3.us-east-005.backblazeb2.com   # adjust to your region
B2_ARCHIVE_BUCKET=wmata-archive-<your-suffix>
```

- [ ] Smoke test bucket connectivity:

```bash
uv run python -c "
import os, boto3
from dotenv import load_dotenv
load_dotenv()
s3 = boto3.client('s3',
    endpoint_url=os.environ['B2_ENDPOINT_URL'],
    aws_access_key_id=os.environ['B2_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['B2_SECRET_ACCESS_KEY'])
print(s3.list_buckets())
"
```

Expected: A `Buckets` list containing your bucket name.

### P3 — Python dependencies

- [ ] Add new dependencies via `uv`:

```bash
uv add zstandard boto3
```

- [ ] Confirm they resolve and lockfile is updated:

```bash
uv sync --extra dev
```

### P4 — Verify Postgres is reachable for tests

Some tests in this plan need real Postgres semantics (UPSERT with conditional CASE). The repo's default test fixture uses SQLite in-memory, which does NOT support our exact UPSERT SQL.

- [ ] Confirm a local Postgres dev database exists:

```bash
psql -d wmata_dashboard -c "SELECT version();"
```

Expected: Postgres version string.

- [ ] We will use `@pytest.mark.integration` for the Postgres-only UPSERT tests. Confirm the marker is registered:

```bash
grep -A4 "markers" pyproject.toml | grep "integration"
```

Expected: `"integration: Integration tests requiring database",`

---

## Task 1: Phase A — Preserve existing `trip_update_snapshots` to parquet

**Files:**
- Run: `pipelines/archive_trip_update_snapshots.py` (existing, no modifications)
- Verify: `archive/trip_update_snapshots/*.parquet`

**Context:** Before any structural changes, push all 14 days of accumulated raw data to compressed parquet. Uses the existing tool (no new code).

- [ ] **Step 1: Dry-run the archive to confirm scope**

Run:

```bash
uv run python pipelines/archive_trip_update_snapshots.py --retention-days 0 --dry-run
```

Expected: Lists every date from 2026-05-03 through (today - 0 days), showing row counts per date and total. Does NOT write files.

- [ ] **Step 2: Run the archive for real**

Run (this is the long-running step — can take ~1-2 hours):

```bash
uv run python pipelines/archive_trip_update_snapshots.py --retention-days 0
```

Expected: For each date, "archived YYYY-MM-DD: N rows → archive/trip_update_snapshots/YYYY-MM-DD.parquet (X MB), deleted N from trip_update_snapshots". Final line: "VACUUM complete."

- [ ] **Step 3: Verify parquet files exist for every collection date**

Run:

```bash
ls -la archive/trip_update_snapshots/ | tail -20
echo "---"
uv run python -c "
import polars as pl
from pathlib import Path
total = 0
for f in sorted(Path('archive/trip_update_snapshots').glob('*.parquet')):
    n = pl.scan_parquet(f).select(pl.len()).collect().item()
    total += n
    print(f'{f.name}: {n:,} rows')
print(f'Total: {total:,} rows')
"
```

Expected: One parquet file per date 2026-05-03 → (today-1). Total row count matches sum of the original `trip_update_snapshots` table.

- [ ] **Step 4: Confirm DB has reclaimed space**

Run:

```bash
psql -d wmata_dashboard -c "SELECT pg_size_pretty(pg_database_size('wmata_dashboard'));"
psql -d wmata_dashboard -c "SELECT COUNT(*) FROM trip_update_snapshots;"
```

Expected: DB size dramatically smaller (down from ~146 GB to ~20-30 GB after VACUUM). Table row count: 0 (or only today's rows if collector ran during the archive).

- [ ] **Step 5: Commit progress** (no code changes, but tag the milestone with a no-op commit message in the plan repo)

There is no code change to commit for Task 1. Skip the commit step and proceed.

---

## Task 2: Add `TripUpdateState` SQLAlchemy model

**Files:**
- Modify: `src/models.py` (add new `TripUpdateState` model)
- Test: `tests/test_models.py` (add test for the new model)

**Context:** Define the SQLAlchemy ORM model for the new table. Schema matches the spec exactly.

- [ ] **Step 1: Write a failing test for the model schema**

Open `tests/test_models.py` and add this test at the end of the file:

```python
def test_trip_update_state_schema(db_session):
    """TripUpdateState has the columns the refactor design requires."""
    from src.models import TripUpdateState

    columns = {c.name for c in TripUpdateState.__table__.columns}
    expected = {
        "trip_id",
        "stop_sequence",
        "stop_id",
        "vehicle_id",
        "final_snapshot_ts",
        "final_schedule_relationship",
        "last_pred_snapshot_ts",
        "last_predicted_arrival_ts",
        "derived_at",
    }
    assert columns == expected, f"unexpected columns: {columns ^ expected}"

    # Composite PK on (trip_id, stop_sequence)
    pk_cols = {c.name for c in TripUpdateState.__table__.primary_key.columns}
    assert pk_cols == {"trip_id", "stop_sequence"}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_models.py::test_trip_update_state_schema -v
```

Expected: FAIL with `ImportError: cannot import name 'TripUpdateState' from 'src.models'`.

- [ ] **Step 3: Add the model to `src/models.py`**

In `src/models.py`, add this class. Place it near the existing `TripUpdateSnapshot` class so similar concepts cluster:

```python
class TripUpdateState(Base):
    """Final-state-only mirror of WMATA TripUpdate predictions per (trip, stop).

    Unlike the append-only ``trip_update_snapshots``, this table holds
    exactly one row per ``(trip_id, stop_sequence)``: the final state
    observed before the (trip, stop) drops out of WMATA's feed. The
    collector UPSERTs into this table on every poll. The derivation
    pipeline reads it directly, avoiding the ~21M-row/day snapshot scan.

    Lifecycle:
        1. Trip starts -> rows inserted for upcoming stops.
        2. Bus moves -> rows update as predictions refine.
        3. Bus passes -> row's final state captured.
        4. End of service day -> ``derive_stop_events_from_state.py``
           materializes the corresponding ``stop_event`` and sets
           ``derived_at``.
        5. Cleanup cron deletes derived rows >2 days old, and any rows
           (derived or not) >7 days old, so the table can't grow
           unbounded.
    """

    __tablename__ = "trip_update_state"

    trip_id = Column(String, primary_key=True)
    stop_sequence = Column(Integer, primary_key=True)

    stop_id = Column(String, nullable=False)
    vehicle_id = Column(String, nullable=True)

    final_snapshot_ts = Column(DateTime, nullable=False)
    final_schedule_relationship = Column(String, nullable=True)

    last_pred_snapshot_ts = Column(DateTime, nullable=True)
    last_predicted_arrival_ts = Column(DateTime, nullable=True)

    derived_at = Column(DateTime, nullable=True)
```

Make sure `Column`, `String`, `Integer`, and `DateTime` are imported at the top of `src/models.py` (they should already be).

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
uv run pytest tests/test_models.py::test_trip_update_state_schema -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/models.py tests/test_models.py
git commit -m "feat: add TripUpdateState model for refactor"
```

---

## Task 3: Database migration for `trip_update_state`

**Files:**
- Create: `scripts/migrate_create_trip_update_state.py`
- Test: `tests/test_migrate_trip_update_state.py`

**Context:** A one-shot migration script that creates the new table with its indexes. Follows the pattern of existing migrations in `scripts/` (e.g., `scripts/migrate_runs_stops_observable.py`).

- [ ] **Step 1: Write a failing test for the migration's idempotency**

Create `tests/test_migrate_trip_update_state.py`:

```python
"""Tests for scripts/migrate_create_trip_update_state.py."""

import pytest
from sqlalchemy import inspect, text


@pytest.mark.integration
def test_migration_creates_table_and_indexes(db_session):
    """Migration creates the table with the spec'd indexes; re-running is a no-op."""
    from scripts.migrate_create_trip_update_state import run_migration

    inspector = inspect(db_session.bind)

    # Drop the table first to ensure a clean slate (idempotent test).
    db_session.execute(text("DROP TABLE IF EXISTS trip_update_state"))
    db_session.commit()
    assert "trip_update_state" not in inspector.get_table_names()

    # First run creates everything.
    run_migration(db_session.bind)
    inspector = inspect(db_session.bind)
    assert "trip_update_state" in inspector.get_table_names()
    indexes = {idx["name"] for idx in inspector.get_indexes("trip_update_state")}
    assert "idx_tus_final_snapshot_ts" in indexes
    assert "idx_tus_trip_id" in indexes

    # Second run is a no-op (idempotent).
    run_migration(db_session.bind)  # Must not raise.
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_migrate_trip_update_state.py -v -m integration
```

Expected: FAIL with `ModuleNotFoundError: No module named 'scripts.migrate_create_trip_update_state'`.

- [ ] **Step 3: Create the migration script**

Create `scripts/migrate_create_trip_update_state.py`:

```python
"""Create the ``trip_update_state`` table and its indexes.

Idempotent: ``CREATE TABLE IF NOT EXISTS`` + ``CREATE INDEX IF NOT EXISTS``,
so re-running is safe.

Usage:
    uv run python scripts/migrate_create_trip_update_state.py
"""

import sys

from dotenv import load_dotenv
from sqlalchemy import text

from src.database import get_engine


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trip_update_state (
    trip_id                     VARCHAR    NOT NULL,
    stop_sequence               INTEGER    NOT NULL,
    stop_id                     VARCHAR    NOT NULL,
    vehicle_id                  VARCHAR,
    final_snapshot_ts           TIMESTAMP  NOT NULL,
    final_schedule_relationship VARCHAR,
    last_pred_snapshot_ts       TIMESTAMP,
    last_predicted_arrival_ts   TIMESTAMP,
    derived_at                  TIMESTAMP,
    PRIMARY KEY (trip_id, stop_sequence)
);
"""

CREATE_INDEX_FINAL_SNAPSHOT_TS = """
CREATE INDEX IF NOT EXISTS idx_tus_final_snapshot_ts
    ON trip_update_state (final_snapshot_ts);
"""

CREATE_INDEX_TRIP_ID = """
CREATE INDEX IF NOT EXISTS idx_tus_trip_id
    ON trip_update_state (trip_id);
"""


def run_migration(engine) -> None:
    """Apply the migration. Safe to re-run."""
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.execute(text(CREATE_INDEX_FINAL_SNAPSHOT_TS))
        conn.execute(text(CREATE_INDEX_TRIP_ID))


def main() -> int:
    """CLI entry point."""
    load_dotenv()
    engine = get_engine()
    print("Creating trip_update_state table + indexes...")
    run_migration(engine)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
uv run pytest tests/test_migrate_trip_update_state.py -v -m integration
```

Expected: PASS.

- [ ] **Step 5: Run the migration against the dev DB**

Run:

```bash
uv run python scripts/migrate_create_trip_update_state.py
```

Expected: "Creating trip_update_state table + indexes... Done."

Verify with:

```bash
psql -d wmata_dashboard -c "\d trip_update_state"
```

Expected: Table definition with 9 columns and 2 indexes (plus the PK).

- [ ] **Step 6: Commit**

```bash
git add scripts/migrate_create_trip_update_state.py tests/test_migrate_trip_update_state.py
git commit -m "feat: migration creating trip_update_state table"
```

---

## Task 4: UPSERT helper with conditional `last_pred_*` logic

**Files:**
- Modify: `src/upsert_helpers.py` (add `upsert_trip_update_state` function)
- Test: `tests/test_upsert_trip_update_state.py`

**Context:** The new UPSERT has subtle semantics that differ from the generic `upsert_rows`. `final_*` fields always overwrite, but `last_pred_*` fields only overwrite when the incoming `predicted_arrival_ts` is non-null (this preserves the existing algorithm — WMATA sometimes nullifies predictions right at arrival, and we want to retain the last meaningful estimate).

- [ ] **Step 1: Write failing tests covering all UPSERT semantics**

Create `tests/test_upsert_trip_update_state.py`:

```python
"""Tests for src.upsert_helpers.upsert_trip_update_state."""

from datetime import datetime, timedelta

import pytest
from sqlalchemy import select

from src.models import TripUpdateState


def _make_row(
    trip_id: str = "T1",
    stop_sequence: int = 1,
    stop_id: str = "S1",
    vehicle_id: str | None = "V1",
    snapshot_ts: datetime | None = None,
    predicted_arrival_ts: datetime | None = None,
    schedule_relationship: str | None = "SCHEDULED",
) -> dict:
    """Build a row dict in the shape upsert_trip_update_state expects."""
    return {
        "trip_id": trip_id,
        "stop_sequence": stop_sequence,
        "stop_id": stop_id,
        "vehicle_id": vehicle_id,
        "snapshot_ts": snapshot_ts or datetime(2026, 5, 17, 14, 0, 0),
        "predicted_arrival_ts": predicted_arrival_ts,
        "schedule_relationship": schedule_relationship,
    }


@pytest.mark.integration
def test_first_insert_creates_row(db_session):
    """An UPSERT against an empty table inserts a new row."""
    from src.upsert_helpers import upsert_trip_update_state

    pred = datetime(2026, 5, 17, 14, 5, 0)
    upsert_trip_update_state(db_session, [_make_row(predicted_arrival_ts=pred)])
    db_session.commit()

    row = db_session.execute(select(TripUpdateState)).scalar_one()
    assert row.trip_id == "T1"
    assert row.stop_sequence == 1
    assert row.final_snapshot_ts == datetime(2026, 5, 17, 14, 0, 0)
    assert row.last_pred_snapshot_ts == datetime(2026, 5, 17, 14, 0, 0)
    assert row.last_predicted_arrival_ts == pred


@pytest.mark.integration
def test_upsert_overwrites_final_fields_always(db_session):
    """final_* fields always reflect the most recent snapshot."""
    from src.upsert_helpers import upsert_trip_update_state

    t1 = datetime(2026, 5, 17, 14, 0, 0)
    t2 = t1 + timedelta(minutes=1)

    upsert_trip_update_state(db_session, [_make_row(snapshot_ts=t1)])
    upsert_trip_update_state(
        db_session,
        [_make_row(snapshot_ts=t2, schedule_relationship="SKIPPED")],
    )
    db_session.commit()

    row = db_session.execute(select(TripUpdateState)).scalar_one()
    assert row.final_snapshot_ts == t2
    assert row.final_schedule_relationship == "SKIPPED"


@pytest.mark.integration
def test_last_pred_updates_only_when_prediction_is_non_null(db_session):
    """last_pred_* fields stick on the most recent NON-NULL prediction."""
    from src.upsert_helpers import upsert_trip_update_state

    t1 = datetime(2026, 5, 17, 14, 0, 0)
    t2 = t1 + timedelta(minutes=1)
    pred1 = datetime(2026, 5, 17, 14, 5, 0)

    upsert_trip_update_state(
        db_session, [_make_row(snapshot_ts=t1, predicted_arrival_ts=pred1)]
    )
    # Second snapshot has a NULL prediction — should NOT overwrite last_pred_*.
    upsert_trip_update_state(
        db_session, [_make_row(snapshot_ts=t2, predicted_arrival_ts=None)]
    )
    db_session.commit()

    row = db_session.execute(select(TripUpdateState)).scalar_one()
    assert row.final_snapshot_ts == t2  # final_ moved forward
    assert row.last_pred_snapshot_ts == t1  # but last_pred_ stuck on t1
    assert row.last_predicted_arrival_ts == pred1


@pytest.mark.integration
def test_vehicle_id_coalesces_to_latest_non_null(db_session):
    """vehicle_id keeps the last non-null value (can come and go in WMATA feed)."""
    from src.upsert_helpers import upsert_trip_update_state

    t1 = datetime(2026, 5, 17, 14, 0, 0)
    t2 = t1 + timedelta(minutes=1)

    upsert_trip_update_state(db_session, [_make_row(snapshot_ts=t1, vehicle_id="V1")])
    upsert_trip_update_state(db_session, [_make_row(snapshot_ts=t2, vehicle_id=None)])
    db_session.commit()

    row = db_session.execute(select(TripUpdateState)).scalar_one()
    assert row.vehicle_id == "V1"  # preserved from the earlier snapshot
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_upsert_trip_update_state.py -v -m integration
```

Expected: All four FAIL with `ImportError: cannot import name 'upsert_trip_update_state' from 'src.upsert_helpers'`.

- [ ] **Step 3: Implement `upsert_trip_update_state` in `src/upsert_helpers.py`**

Append this function to `src/upsert_helpers.py`:

```python
def upsert_trip_update_state(db, rows: list[dict]) -> int:
    """UPSERT trip_update_state rows with conditional last_pred_* semantics.

    ``rows`` is a list of dicts shaped like::

        {
            "trip_id": str,
            "stop_sequence": int,
            "stop_id": str,
            "vehicle_id": str | None,
            "snapshot_ts": datetime,
            "predicted_arrival_ts": datetime | None,
            "schedule_relationship": str | None,
        }

    Semantics on conflict (trip_id, stop_sequence):
        - final_snapshot_ts, final_schedule_relationship: always overwrite.
        - last_pred_snapshot_ts, last_predicted_arrival_ts: overwrite ONLY
          when the incoming predicted_arrival_ts is non-null. WMATA
          sometimes nullifies predictions right at arrival; we want to
          keep the last meaningful estimate (matching the existing
          derivation algorithm at derive_stop_events_trip_updates.py:90).
        - vehicle_id: COALESCE(new, existing) — keep last non-null.
        - stop_id: overwrite (should be stable across snapshots for a
          given (trip, stop_sequence), but defensively keep latest).
        - derived_at: never touched by this function (only the derivation
          pipeline writes it).

    Postgres-only by construction: uses pg_insert with conditional
    excluded.* logic in the ON CONFLICT DO UPDATE clause. SQLite cannot
    represent this UPSERT, so callers in test contexts must use a real
    Postgres connection (mark tests with ``@pytest.mark.integration``).

    Returns the number of rows passed in (Postgres doesn't reliably
    return inserted-vs-updated counts on ON CONFLICT).
    """
    if not rows:
        return 0

    from sqlalchemy import case
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from src.models import TripUpdateState

    # Build the INSERT statement.
    payload = [
        {
            "trip_id": r["trip_id"],
            "stop_sequence": r["stop_sequence"],
            "stop_id": r["stop_id"],
            "vehicle_id": r["vehicle_id"],
            "final_snapshot_ts": r["snapshot_ts"],
            "final_schedule_relationship": r["schedule_relationship"],
            # On first insert, last_pred_* is just the incoming prediction
            # (or NULL if no prediction yet).
            "last_pred_snapshot_ts": r["snapshot_ts"]
            if r["predicted_arrival_ts"] is not None
            else None,
            "last_predicted_arrival_ts": r["predicted_arrival_ts"],
        }
        for r in rows
    ]

    stmt = pg_insert(TripUpdateState).values(payload)

    # ON CONFLICT DO UPDATE — referencing EXCLUDED (proposed-new) vs the
    # existing-table values.
    excluded = stmt.excluded
    table = TripUpdateState.__table__
    stmt = stmt.on_conflict_do_update(
        index_elements=["trip_id", "stop_sequence"],
        set_={
            "stop_id": excluded.stop_id,
            # COALESCE keeps last non-null.
            "vehicle_id": case(
                (excluded.vehicle_id.is_(None), table.c.vehicle_id),
                else_=excluded.vehicle_id,
            ),
            "final_snapshot_ts": excluded.final_snapshot_ts,
            "final_schedule_relationship": excluded.final_schedule_relationship,
            # Conditional: keep existing last_pred_* unless incoming has
            # a non-null prediction.
            "last_pred_snapshot_ts": case(
                (excluded.last_predicted_arrival_ts.is_(None), table.c.last_pred_snapshot_ts),
                else_=excluded.last_pred_snapshot_ts,
            ),
            "last_predicted_arrival_ts": case(
                (excluded.last_predicted_arrival_ts.is_(None), table.c.last_predicted_arrival_ts),
                else_=excluded.last_predicted_arrival_ts,
            ),
            # derived_at intentionally not in set_ — leave whatever was
            # already there (NULL until derivation runs).
        },
    )

    db.execute(stmt)
    return len(rows)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/test_upsert_trip_update_state.py -v -m integration
```

Expected: All four PASS.

- [ ] **Step 5: Commit**

```bash
git add src/upsert_helpers.py tests/test_upsert_trip_update_state.py
git commit -m "feat: upsert_trip_update_state with conditional last_pred semantics"
```

---

## Task 5: Modify `_save_trip_updates` to dual-write into `trip_update_state`

**Files:**
- Modify: `src/wmata_collector.py:565-616` (the `_save_trip_updates` method)
- Test: `tests/test_collector_dual_write.py`

**Context:** The existing collector at `src/wmata_collector.py:565` writes to `trip_update_snapshots` with internal state-change dedup. We add a parallel write path that UPSERTs each row's final-state representation into `trip_update_state`. The old write path is untouched. After cutover (Task 13), the old path is removed.

- [ ] **Step 1: Write a failing test for dual-write behavior**

Create `tests/test_collector_dual_write.py`:

```python
"""Tests that _save_trip_updates writes to both tables."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from src.models import TripUpdateSnapshot, TripUpdateState


@pytest.mark.integration
def test_save_trip_updates_writes_to_both_tables(db_session):
    """Each call to _save_trip_updates writes the row to BOTH tables."""
    from src.wmata_collector import WMATACollector

    # Construct a collector with a real db_session. The API key is unused
    # in this test path.
    collector = WMATACollector(api_key="unused", db_session=db_session)

    rows = [
        {
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 1,
            "route_id": "R1",
            "vehicle_id": "V1",
            "snapshot_ts": datetime(2026, 5, 17, 14, 0, 0),
            "predicted_arrival_ts": datetime(2026, 5, 17, 14, 5, 0),
            "predicted_departure_ts": datetime(2026, 5, 17, 14, 5, 30),
            "schedule_relationship": "SCHEDULED",
            "collected_at": datetime(2026, 5, 17, 14, 0, 5),
        }
    ]
    collector._save_trip_updates(rows)

    # Old path: row exists in trip_update_snapshots.
    snapshot = db_session.execute(select(TripUpdateSnapshot)).scalar_one()
    assert snapshot.trip_id == "T1"

    # New path: row exists in trip_update_state with the right final-state.
    state = db_session.execute(select(TripUpdateState)).scalar_one()
    assert state.trip_id == "T1"
    assert state.stop_sequence == 1
    assert state.final_snapshot_ts == datetime(2026, 5, 17, 14, 0, 0)
    assert state.last_predicted_arrival_ts == datetime(2026, 5, 17, 14, 5, 0)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_collector_dual_write.py -v -m integration
```

Expected: FAIL — only `TripUpdateSnapshot` row exists, `TripUpdateState` row missing (AssertionError on `state` query).

- [ ] **Step 3: Modify `_save_trip_updates` to dual-write**

Edit `src/wmata_collector.py`. Find the `_save_trip_updates` method (line 565). Replace its body so the final-write section (currently lines 608-616) becomes:

```python
        if new_objects:
            self.db.bulk_save_objects(new_objects)
            self.db.commit()

        # NEW: dual-write to trip_update_state. We UPSERT one row per
        # (trip_id, stop_sequence) holding the latest predictions. The
        # derivation pipeline reads this table directly after cutover.
        # We pass ALL rows (not just state-changed ones) so the
        # final_snapshot_ts always reflects the latest poll, even when
        # state hasn't changed.
        from src.upsert_helpers import upsert_trip_update_state

        upsert_payload = [
            {
                "trip_id": r["trip_id"],
                "stop_sequence": r["stop_sequence"],
                "stop_id": r["stop_id"],
                "vehicle_id": r.get("vehicle_id"),
                "snapshot_ts": r["snapshot_ts"],
                "predicted_arrival_ts": r.get("predicted_arrival_ts"),
                "schedule_relationship": r.get("schedule_relationship"),
            }
            for r in rows
            if r.get("stop_sequence") is not None  # skip rows missing the PK part
        ]
        if upsert_payload:
            upsert_trip_update_state(self.db, upsert_payload)
            self.db.commit()

        print(
            f"  Saved {len(new_objects)} of {len(rows)} trip update rows "
            f"(cache={len(self._tu_dedup_cache):,}); "
            f"upserted {len(upsert_payload)} into trip_update_state"
        )
        return len(new_objects)
```

The `import` is intentionally function-local: keeps the existing module import block stable and isolates the change. If you prefer top-of-file, move the import there.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
uv run pytest tests/test_collector_dual_write.py -v -m integration
```

Expected: PASS.

- [ ] **Step 5: Run existing smoke + collector-adjacent tests to verify no regression**

Run:

```bash
uv run pytest -m smoke -v
```

Expected: All smoke tests PASS (the dual-write doesn't change any behavior of existing tests).

- [ ] **Step 6: Commit**

```bash
git add src/wmata_collector.py tests/test_collector_dual_write.py
git commit -m "feat: collector dual-writes to trip_update_state"
```

---

## Task 6: JSONL archive writer in the collector

**Files:**
- Create: `src/archive_writer.py`
- Modify: `src/wmata_collector.py` (instantiate writer; call from `_save_trip_updates`)
- Test: `tests/test_archive_writer.py`

**Context:** A streaming JSONL writer that appends one row per (trip, stop) per snapshot to a daily ZSTD-compressed file rotated at UTC midnight. The compressed file is the source of truth for the cold-storage parquet archive (Task 7). Writes happen per-snapshot so a collector crash loses at most one snapshot's worth of evidence.

- [ ] **Step 1: Write failing tests for the writer**

Create `tests/test_archive_writer.py`:

```python
"""Tests for src.archive_writer.JsonlArchiveWriter."""

from datetime import datetime
from pathlib import Path

import zstandard as zstd


def test_writer_creates_daily_file(tmp_path: Path):
    """append() creates a daily JSONL file under archive_dir."""
    from src.archive_writer import JsonlArchiveWriter

    writer = JsonlArchiveWriter(archive_dir=tmp_path)
    writer.append(
        {"trip_id": "T1", "stop_id": "S1"},
        snapshot_ts=datetime(2026, 5, 17, 14, 0, 0),
    )
    writer.close()

    expected = tmp_path / "2026-05-17.jsonl.zst"
    assert expected.exists()


def test_writer_appends_multiple_rows(tmp_path: Path):
    """Multiple append() calls add separate lines to the same daily file."""
    from src.archive_writer import JsonlArchiveWriter

    writer = JsonlArchiveWriter(archive_dir=tmp_path)
    for i in range(3):
        writer.append(
            {"trip_id": f"T{i}", "stop_id": "S1"},
            snapshot_ts=datetime(2026, 5, 17, 14, 0, 0),
        )
    writer.close()

    path = tmp_path / "2026-05-17.jsonl.zst"
    dctx = zstd.ZstdDecompressor()
    with open(path, "rb") as f:
        decompressed = dctx.decompress(f.read()).decode()
    lines = [line for line in decompressed.splitlines() if line]
    assert len(lines) == 3


def test_writer_rotates_at_utc_midnight(tmp_path: Path):
    """Crossing a UTC date boundary writes to a new file."""
    from src.archive_writer import JsonlArchiveWriter

    writer = JsonlArchiveWriter(archive_dir=tmp_path)
    writer.append({"trip_id": "T1"}, snapshot_ts=datetime(2026, 5, 17, 23, 59, 59))
    writer.append({"trip_id": "T2"}, snapshot_ts=datetime(2026, 5, 18, 0, 0, 1))
    writer.close()

    assert (tmp_path / "2026-05-17.jsonl.zst").exists()
    assert (tmp_path / "2026-05-18.jsonl.zst").exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_archive_writer.py -v
```

Expected: All three FAIL with `ModuleNotFoundError: No module named 'src.archive_writer'`.

- [ ] **Step 3: Create `src/archive_writer.py`**

```python
"""Streaming JSONL writer for raw WMATA TripUpdate snapshots.

The writer appends one JSON line per (trip, stop) per snapshot to a
ZSTD-compressed file named by UTC date. Files rotate automatically when
the snapshot timestamp crosses a UTC midnight boundary.

Designed for the cold-archive path: writes are append-only and crash-safe
(each line is flushed independently; a crash loses at most the in-progress
line, not earlier lines).
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import zstandard as zstd


class JsonlArchiveWriter:
    """Append rows to daily ZSTD-compressed JSONL files.

    Rotation rule: a new file is opened when the snapshot_ts UTC date
    changes vs the currently-open file's date. The collector calls
    ``close()`` on shutdown to flush the active file.
    """

    def __init__(self, archive_dir: Path | str):
        """Create or reopen an archive directory; no file is opened yet.

        The first call to ``append()`` opens the file for that snapshot's
        UTC date.
        """
        self._archive_dir = Path(archive_dir)
        self._archive_dir.mkdir(parents=True, exist_ok=True)
        self._open_date: date | None = None
        self._open_fh = None
        self._raw_fh = None
        self._compressor = None

    def append(self, row: dict[str, Any], snapshot_ts: datetime) -> None:
        """Write one JSON line for ``row`` to the file for snapshot_ts's UTC date.

        ``snapshot_ts`` MUST be naive UTC (project-wide convention; see
        CLAUDE.md). Rotates the open file if the date has changed.
        """
        target_date = snapshot_ts.date()
        if self._open_date != target_date:
            self._rotate_to(target_date)

        line = json.dumps(row, default=str) + "\n"
        self._open_fh.write(line.encode("utf-8"))

    def _rotate_to(self, target_date: date) -> None:
        """Close any open file and open the file for ``target_date``."""
        self.close()
        path = self._archive_dir / f"{target_date.isoformat()}.jsonl.zst"
        # Open in append-binary mode so reopening today's file preserves
        # earlier rows (collector restart mid-day).
        self._raw_fh = open(path, "ab")
        # zstd streaming compressor; level 3 = good balance of
        # compression ratio vs CPU on a steady ingest workload.
        self._compressor = zstd.ZstdCompressor(level=3)
        self._open_fh = self._compressor.stream_writer(self._raw_fh)
        self._open_date = target_date

    def close(self) -> None:
        """Flush the zstd footer and close both the writer and the underlying file.

        Two separate handles to close: the stream_writer flushes the zstd
        compression footer; the underlying raw file holds the OS handle.
        Closing the stream_writer alone leaks the raw handle.
        """
        if self._open_fh is not None:
            self._open_fh.close()  # flushes zstd footer to self._raw_fh
            self._open_fh = None
        if self._raw_fh is not None:
            self._raw_fh.close()
            self._raw_fh = None
        self._open_date = None
        self._compressor = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/test_archive_writer.py -v
```

Expected: All three PASS.

- [ ] **Step 5: Wire writer into the collector**

Edit `src/wmata_collector.py`:

5a) Add an import at the top of the file (near other `from src.` imports):

```python
from src.archive_writer import JsonlArchiveWriter
```

5b) In `__init__` (around line 30, where `_tu_dedup_cache` is initialized), add:

```python
        # Cold archive: raw rows go to compressed JSONL daily files.
        # Path matches the existing archive_trip_update_snapshots.py
        # convention (REPO_ROOT / "archive" / ...).
        from pathlib import Path
        archive_root = Path(__file__).resolve().parent.parent / "archive" / "raw_snapshots"
        self._archive_writer = JsonlArchiveWriter(archive_dir=archive_root)
```

5c) In `_save_trip_updates`, append each raw row to the archive BEFORE the dedup loop. Modify the for-loop body so the first thing inside it is the archive call:

```python
        new_objects = []
        seen_keys: set[tuple[str, str]] = set()
        for row in rows:
            # Archive EVERY raw row, even ones the dedup will skip — the
            # archive is the complete evidence trail.
            self._archive_writer.append(row, snapshot_ts=row["snapshot_ts"])

            key = (row["trip_id"], row["stop_id"])
            # ... rest of existing loop unchanged ...
```

- [ ] **Step 6: Add a smoke test confirming the writer is exercised by the collector**

Append to `tests/test_collector_dual_write.py`:

```python
def test_collector_writes_jsonl_archive(db_session, tmp_path, monkeypatch):
    """_save_trip_updates appends rows to the JSONL archive."""
    from src.wmata_collector import WMATACollector

    collector = WMATACollector(api_key="unused", db_session=db_session)
    # Redirect the archive to a tmpdir.
    from src.archive_writer import JsonlArchiveWriter
    collector._archive_writer = JsonlArchiveWriter(archive_dir=tmp_path)

    rows = [
        {
            "trip_id": "T1",
            "stop_id": "S1",
            "stop_sequence": 1,
            "route_id": "R1",
            "vehicle_id": "V1",
            "snapshot_ts": datetime(2026, 5, 17, 14, 0, 0),
            "predicted_arrival_ts": datetime(2026, 5, 17, 14, 5, 0),
            "predicted_departure_ts": datetime(2026, 5, 17, 14, 5, 30),
            "schedule_relationship": "SCHEDULED",
            "collected_at": datetime(2026, 5, 17, 14, 0, 5),
        }
    ]
    collector._save_trip_updates(rows)
    collector._archive_writer.close()

    assert (tmp_path / "2026-05-17.jsonl.zst").exists()
```

Mark this test `@pytest.mark.integration` like the others. Then re-run:

```bash
uv run pytest tests/test_collector_dual_write.py -v -m integration
```

Expected: All PASS.

- [ ] **Step 7: Commit**

```bash
git add src/archive_writer.py src/wmata_collector.py tests/test_archive_writer.py tests/test_collector_dual_write.py
git commit -m "feat: JSONL archive writer wired into collector"
```

---

## Task 7: Nightly rotation — JSONL → parquet → B2 upload

**Files:**
- Create: `pipelines/rotate_archive.py`
- Test: `tests/test_rotate_archive.py`

**Context:** A nightly job (run via systemd timer or cron) that:
1. Reads yesterday's `.jsonl.zst` archive.
2. Converts it to a parquet file (better compression + columnar layout).
3. Uploads the parquet to B2.
4. Verifies the upload object size matches local.
5. Deletes local JSONL + parquet after verification.

- [ ] **Step 1: Write failing tests for the rotation pipeline**

Create `tests/test_rotate_archive.py`:

```python
"""Tests for pipelines.rotate_archive."""

import json
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import zstandard as zstd


def _write_jsonl_zst(path: Path, rows: list[dict]) -> None:
    """Helper: write rows as ZSTD-compressed JSONL."""
    data = "".join(json.dumps(r, default=str) + "\n" for r in rows).encode()
    cctx = zstd.ZstdCompressor(level=3)
    path.write_bytes(cctx.compress(data))


def test_jsonl_to_parquet_round_trip(tmp_path: Path):
    """jsonl_to_parquet preserves all rows and columns."""
    from pipelines.rotate_archive import jsonl_to_parquet

    jsonl_path = tmp_path / "2026-05-17.jsonl.zst"
    parquet_path = tmp_path / "2026-05-17.parquet"
    _write_jsonl_zst(
        jsonl_path,
        [
            {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1},
            {"trip_id": "T2", "stop_id": "S2", "stop_sequence": 2},
        ],
    )

    row_count = jsonl_to_parquet(jsonl_path, parquet_path)
    assert row_count == 2

    df = pl.read_parquet(parquet_path)
    assert df.height == 2
    assert df["trip_id"].to_list() == ["T1", "T2"]


def test_rotate_uploads_and_cleans_up(tmp_path: Path):
    """End-to-end: rotate reads JSONL, uploads parquet, deletes both on success."""
    from pipelines import rotate_archive

    jsonl_path = tmp_path / "2026-05-17.jsonl.zst"
    _write_jsonl_zst(
        jsonl_path,
        [{"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1}],
    )

    fake_s3 = MagicMock()
    # head_object returns object size matching local — verification passes.
    fake_s3.head_object.return_value = {"ContentLength": 9999}  # patched below

    with patch.object(rotate_archive, "_make_s3_client", return_value=fake_s3):
        with patch("pathlib.Path.stat") as stat_mock:
            # Force the "local size" reported to match head_object's ContentLength.
            stat_mock.return_value = MagicMock(st_size=9999)
            rotate_archive.rotate_one_day(
                archive_dir=tmp_path,
                target_date=date(2026, 5, 17),
                bucket="test-bucket",
                key_prefix="raw_snapshots",
            )

    # Upload happened.
    fake_s3.upload_file.assert_called_once()
    args, kwargs = fake_s3.upload_file.call_args
    assert kwargs.get("Bucket") == "test-bucket" or args[1] == "test-bucket"

    # Local files deleted after successful upload.
    assert not jsonl_path.exists()
    assert not (tmp_path / "2026-05-17.parquet").exists()


def test_rotate_keeps_local_on_upload_failure(tmp_path: Path):
    """If upload fails (head_object size mismatch), local files are NOT deleted."""
    from pipelines import rotate_archive

    jsonl_path = tmp_path / "2026-05-17.jsonl.zst"
    _write_jsonl_zst(
        jsonl_path,
        [{"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1}],
    )

    fake_s3 = MagicMock()
    fake_s3.head_object.return_value = {"ContentLength": 1}  # mismatch

    with patch.object(rotate_archive, "_make_s3_client", return_value=fake_s3):
        with pytest.raises(RuntimeError, match="size mismatch"):
            rotate_archive.rotate_one_day(
                archive_dir=tmp_path,
                target_date=date(2026, 5, 17),
                bucket="test-bucket",
                key_prefix="raw_snapshots",
            )

    # Files retained for retry.
    assert jsonl_path.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_rotate_archive.py -v
```

Expected: All FAIL with `ModuleNotFoundError: No module named 'pipelines.rotate_archive'`.

- [ ] **Step 3: Create `pipelines/rotate_archive.py`**

```python
"""Nightly rotation: yesterday's JSONL archive -> parquet -> B2.

Reads ``archive/raw_snapshots/<date>.jsonl.zst``, converts to parquet
with zstd compression, uploads to the configured B2 bucket, verifies the
upload object size, and deletes local files only after a successful
verification.

Designed for idempotent re-runs: if the parquet has already been
uploaded for a date, re-running uploads again (overwriting) and deletes
local files (no-op if already gone). If the JSONL is missing for the
target date, exits 0 — nothing to do.

Usage:
    uv run python pipelines/rotate_archive.py --date 2026-05-17
    uv run python pipelines/rotate_archive.py            # defaults to yesterday UTC
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
import polars as pl
import zstandard as zstd
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ARCHIVE_DIR = REPO_ROOT / "archive" / "raw_snapshots"


def _make_s3_client():
    """Build a boto3 S3 client pointed at B2.

    Reads B2_ACCESS_KEY_ID / B2_SECRET_ACCESS_KEY / B2_ENDPOINT_URL from
    environment (loaded via dotenv at module import or main()).
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ["B2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["B2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["B2_SECRET_ACCESS_KEY"],
    )


def jsonl_to_parquet(jsonl_path: Path, parquet_path: Path) -> int:
    """Read ZSTD-compressed JSONL, write zstd-compressed parquet.

    Returns the row count written. Uses polars for the actual conversion
    so the schema is inferred from the JSONL contents.
    """
    dctx = zstd.ZstdDecompressor()
    with open(jsonl_path, "rb") as f:
        decompressed = dctx.decompress(f.read()).decode("utf-8")
    df = pl.read_ndjson(decompressed.encode("utf-8"))
    df.write_parquet(parquet_path, compression="zstd")
    return df.height


def rotate_one_day(
    archive_dir: Path,
    target_date: date,
    bucket: str,
    key_prefix: str,
) -> None:
    """Convert one day's JSONL to parquet, upload, verify, clean up.

    Raises ``RuntimeError`` if the uploaded object size doesn't match
    local — caller can retry without losing data.
    """
    jsonl_path = archive_dir / f"{target_date.isoformat()}.jsonl.zst"
    parquet_path = archive_dir / f"{target_date.isoformat()}.parquet"

    if not jsonl_path.exists():
        print(f"No JSONL archive for {target_date.isoformat()}, nothing to rotate.")
        return

    rows = jsonl_to_parquet(jsonl_path, parquet_path)
    local_size = parquet_path.stat().st_size
    print(f"Wrote {parquet_path.name}: {rows:,} rows, {local_size:,} bytes")

    s3 = _make_s3_client()
    key = f"{key_prefix}/{target_date.isoformat()}.parquet"
    s3.upload_file(str(parquet_path), bucket, key)
    print(f"Uploaded to s3://{bucket}/{key}")

    head = s3.head_object(Bucket=bucket, Key=key)
    remote_size = head["ContentLength"]
    if remote_size != local_size:
        raise RuntimeError(
            f"Upload size mismatch for {key}: local={local_size}, remote={remote_size}"
        )

    # Verified — delete local files.
    jsonl_path.unlink()
    parquet_path.unlink()
    print(f"Cleaned up local files for {target_date.isoformat()}")


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description=(
            "Rotate yesterday's JSONL archive to parquet and upload to B2."
        )
    )
    parser.add_argument(
        "--date",
        help="UTC date to rotate (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=DEFAULT_ARCHIVE_DIR,
        help=f"Path to archive directory (default: {DEFAULT_ARCHIVE_DIR})",
    )
    args = parser.parse_args()

    load_dotenv()
    bucket = os.environ["B2_ARCHIVE_BUCKET"]

    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target = (datetime.utcnow() - timedelta(days=1)).date()

    rotate_one_day(
        archive_dir=args.archive_dir,
        target_date=target,
        bucket=bucket,
        key_prefix="raw_snapshots",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/test_rotate_archive.py -v
```

Expected: All three PASS.

- [ ] **Step 5: Commit**

```bash
git add pipelines/rotate_archive.py tests/test_rotate_archive.py
git commit -m "feat: rotate_archive — JSONL to parquet to B2"
```

---

## Task 8: Cleanup cron for `trip_update_state`

**Files:**
- Create: `pipelines/cleanup_trip_update_state.py`
- Test: `tests/test_cleanup_trip_update_state.py`

**Context:** Two-pass cleanup as per spec:
1. Delete rows with `derived_at < NOW() - 2 days` (normal lifecycle).
2. Delete rows with `final_snapshot_ts < NOW() - 7 days` (safety net for un-derivable trips, e.g., trips with no `vehicle_position` to anchor service_date).

- [ ] **Step 1: Write failing tests**

Create `tests/test_cleanup_trip_update_state.py`:

```python
"""Tests for pipelines.cleanup_trip_update_state."""

from datetime import datetime, timedelta

import pytest
from sqlalchemy import select

from src.models import TripUpdateState


def _make_state_row(
    trip_id: str,
    stop_sequence: int,
    *,
    final_snapshot_ts: datetime,
    derived_at: datetime | None = None,
) -> TripUpdateState:
    return TripUpdateState(
        trip_id=trip_id,
        stop_sequence=stop_sequence,
        stop_id="S1",
        vehicle_id="V1",
        final_snapshot_ts=final_snapshot_ts,
        final_schedule_relationship="SCHEDULED",
        last_pred_snapshot_ts=final_snapshot_ts,
        last_predicted_arrival_ts=final_snapshot_ts + timedelta(minutes=5),
        derived_at=derived_at,
    )


@pytest.mark.integration
def test_cleanup_deletes_derived_rows_older_than_two_days(db_session):
    """Derived rows with derived_at older than 2 days are deleted."""
    from pipelines.cleanup_trip_update_state import run_cleanup

    now = datetime.utcnow()
    db_session.add_all([
        _make_state_row(
            "T_old", 1, final_snapshot_ts=now - timedelta(days=4),
            derived_at=now - timedelta(days=3),
        ),
        _make_state_row(
            "T_recent", 1, final_snapshot_ts=now - timedelta(hours=12),
            derived_at=now - timedelta(hours=1),
        ),
        _make_state_row("T_unfinished", 1, final_snapshot_ts=now - timedelta(hours=1)),
    ])
    db_session.commit()

    run_cleanup(db_session)
    db_session.commit()

    remaining = {r.trip_id for r in db_session.execute(select(TripUpdateState)).scalars()}
    assert remaining == {"T_recent", "T_unfinished"}  # T_old gone


@pytest.mark.integration
def test_cleanup_safety_net_deletes_undisrived_rows_older_than_seven_days(db_session):
    """Un-derived rows older than 7 days are deleted as safety net."""
    from pipelines.cleanup_trip_update_state import run_cleanup

    now = datetime.utcnow()
    db_session.add_all([
        _make_state_row("T_stale", 1, final_snapshot_ts=now - timedelta(days=8)),
        _make_state_row("T_fresh", 1, final_snapshot_ts=now - timedelta(days=1)),
    ])
    db_session.commit()

    run_cleanup(db_session)
    db_session.commit()

    remaining = {r.trip_id for r in db_session.execute(select(TripUpdateState)).scalars()}
    assert remaining == {"T_fresh"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_cleanup_trip_update_state.py -v -m integration
```

Expected: All FAIL with `ModuleNotFoundError: No module named 'pipelines.cleanup_trip_update_state'`.

- [ ] **Step 3: Create the cleanup pipeline**

Create `pipelines/cleanup_trip_update_state.py`:

```python
"""Cleanup cron for trip_update_state.

Runs daily. Two passes:

1. Normal lifecycle: rows whose stop_events were materialized
   (``derived_at IS NOT NULL``) and that materialization happened more
   than 2 days ago.
2. Safety net: rows that were NEVER derived but whose final_snapshot_ts
   is older than 7 days. Catches un-derivable trips (e.g., trips with no
   ``vehicle_position`` to anchor service_date) so the table can't grow
   unbounded.

The 2-day window provides a re-derivation buffer without requiring the
parquet archive. Beyond that, re-derivation falls back to parquet.

Usage:
    uv run python pipelines/cleanup_trip_update_state.py
    uv run python pipelines/cleanup_trip_update_state.py --dry-run
"""

import argparse
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import delete
from sqlalchemy.orm import Session

from src.database import get_session
from src.models import TripUpdateState


def run_cleanup(db: Session) -> dict:
    """Run both cleanup passes against the given session.

    Returns a dict with row counts deleted by each pass.
    """
    now = datetime.utcnow()
    derived_cutoff = now - timedelta(days=2)
    safety_cutoff = now - timedelta(days=7)

    derived_stmt = delete(TripUpdateState).where(
        TripUpdateState.derived_at.is_not(None),
        TripUpdateState.derived_at < derived_cutoff,
    )
    derived_result = db.execute(derived_stmt)

    safety_stmt = delete(TripUpdateState).where(
        TripUpdateState.final_snapshot_ts < safety_cutoff,
    )
    safety_result = db.execute(safety_stmt)

    return {
        "derived_deleted": derived_result.rowcount or 0,
        "safety_deleted": safety_result.rowcount or 0,
    }


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Delete aged trip_update_state rows."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute counts without deleting (rolls back).",
    )
    args = parser.parse_args()

    load_dotenv()
    db = get_session()
    try:
        counts = run_cleanup(db)
        if args.dry_run:
            db.rollback()
            print(f"DRY-RUN: would delete {counts}")
        else:
            db.commit()
            print(f"Cleanup complete: {counts}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/test_cleanup_trip_update_state.py -v -m integration
```

Expected: Both PASS.

- [ ] **Step 5: Commit**

```bash
git add pipelines/cleanup_trip_update_state.py tests/test_cleanup_trip_update_state.py
git commit -m "feat: cleanup pipeline for trip_update_state"
```

---

## Task 9: New derivation pipeline reading from `trip_update_state`

**Files:**
- Create: `pipelines/derive_stop_events_from_state.py`
- Test: `tests/test_derive_stop_events_from_state.py`

**Context:** The new derivation reads `trip_update_state` (one row per (trip, stop_sequence)) instead of scanning `trip_update_snapshots` (millions of rows per day). The algorithm is simpler: the table IS the final state, so no per-(trip, stop) reduction is needed. Output schema (`stop_events`) is identical to the existing pipeline. Per the validation strategy (spec Phase D), this pipeline writes to a side table (`stop_events_v2`) by default.

- [ ] **Step 1: Write a failing test for the derivation against curated state rows**

Create `tests/test_derive_stop_events_from_state.py`:

```python
"""Tests for pipelines.derive_stop_events_from_state."""

from datetime import datetime

import pytest
from sqlalchemy import select

from src.models import StopEvent, StopTime, Trip, TripUpdateState, VehiclePosition


def _seed_minimal_route(db_session, *, route_id="R1", trip_id="T1"):
    """Seed the minimum DB state for a single-stop derivation test."""
    db_session.add_all([
        Trip(trip_id=trip_id, route_id=route_id, direction_id=0, is_current=True),
        StopTime(
            trip_id=trip_id, stop_sequence=1, stop_id="S1",
            arrival_time="14:05:00", departure_time="14:05:30", is_current=True,
        ),
        VehiclePosition(
            trip_id=trip_id, route_id=route_id, trip_start_date="20260517",
            latitude=0, longitude=0, timestamp=datetime(2026, 5, 17, 14, 0, 0),
        ),
    ])


@pytest.mark.integration
def test_derive_produces_stop_event_with_correct_observed_arrival(db_session):
    """A trip_update_state row produces a stop_event with last_predicted_arrival_ts as observed."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    _seed_minimal_route(db_session)
    pred = datetime(2026, 5, 17, 14, 6, 30)  # 90s late vs 14:05 schedule
    db_session.add(TripUpdateState(
        trip_id="T1", stop_sequence=1, stop_id="S1", vehicle_id="V1",
        final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
        final_schedule_relationship="SCHEDULED",
        last_pred_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
        last_predicted_arrival_ts=pred,
    ))
    db_session.commit()

    derive_for_route_date(
        db_session, route_id="R1",
        service_date=__import__("datetime").date(2026, 5, 17),
        target_table_name="stop_events",
    )
    db_session.commit()

    event = db_session.execute(select(StopEvent)).scalar_one()
    assert event.trip_id == "T1"
    assert event.observed_arrival_ts == pred
    assert event.schedule_relationship == "SCHEDULED"
    assert event.source == "trip_update"


@pytest.mark.integration
def test_derive_emits_skipped_stops(db_session):
    """A SKIPPED final_schedule_relationship produces a stop_event with observed_arrival_ts=None."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    _seed_minimal_route(db_session)
    db_session.add(TripUpdateState(
        trip_id="T1", stop_sequence=1, stop_id="S1", vehicle_id="V1",
        final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
        final_schedule_relationship="SKIPPED",
        last_pred_snapshot_ts=None,
        last_predicted_arrival_ts=None,
    ))
    db_session.commit()

    derive_for_route_date(
        db_session, route_id="R1",
        service_date=__import__("datetime").date(2026, 5, 17),
        target_table_name="stop_events",
    )
    db_session.commit()

    event = db_session.execute(select(StopEvent)).scalar_one()
    assert event.schedule_relationship == "SKIPPED"
    assert event.observed_arrival_ts is None


@pytest.mark.integration
def test_derive_sets_derived_at_on_state_rows(db_session):
    """After derivation, the source state rows have derived_at set."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    _seed_minimal_route(db_session)
    db_session.add(TripUpdateState(
        trip_id="T1", stop_sequence=1, stop_id="S1", vehicle_id="V1",
        final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
        final_schedule_relationship="SCHEDULED",
        last_pred_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
        last_predicted_arrival_ts=datetime(2026, 5, 17, 14, 6, 30),
    ))
    db_session.commit()

    derive_for_route_date(
        db_session, route_id="R1",
        service_date=__import__("datetime").date(2026, 5, 17),
        target_table_name="stop_events",
    )
    db_session.commit()

    row = db_session.execute(select(TripUpdateState)).scalar_one()
    assert row.derived_at is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_derive_stop_events_from_state.py -v -m integration
```

Expected: All FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Create the new derivation pipeline**

Create `pipelines/derive_stop_events_from_state.py`:

```python
"""Derive stop_events from trip_update_state (the refactored pipeline).

Replaces the old ``derive_stop_events_trip_updates.py``. Reads
``trip_update_state`` directly — one row per (trip, stop_sequence)
already in final-state — eliminating the ~21M-row/day snapshot scan.

The output schema and semantics MUST be byte-for-byte identical to the
old pipeline (validated during Phase D against ``stop_events_v2``); see
the design doc for the parity criteria.

Service-date attribution: as before, vehicle_positions for the same
trip_id on the target service_date is the authoritative anchor — trip
updates themselves don't record trip_start_date.

Usage:
    uv run python pipelines/derive_stop_events_from_state.py --route C51 --date 2026-05-03
    uv run python pipelines/derive_stop_events_from_state.py --all-routes --date 2026-05-03
"""

import argparse
import time
from datetime import date as date_type
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import update
from sqlalchemy.orm import Session

from pipelines.stop_events_common import parse_gtfs_time_to_dt
from src.batch_iterator import run_route_date_grid
from src.database import get_session
from src.models import Route, StopTime, Trip, TripUpdateState, VehiclePosition
from src.timezones import eastern_today, utcnow_naive
from src.upsert_helpers import upsert_rows


def derive_for_route_date(
    db: Session,
    route_id: str,
    service_date: date_type,
    target_table_name: str = "stop_events",
) -> dict:
    """Materialize stop_events for one (route, service_date) from trip_update_state.

    ``target_table_name`` is "stop_events" for production and
    "stop_events_v2" during Phase D side-by-side validation. The target
    table must already exist with the StopEvent schema.

    Returns a counters dict identical in shape to the old pipeline's
    output, for parity comparison.
    """
    start_ts = time.time()
    service_date_str = service_date.isoformat()
    trip_start_date_str = service_date.strftime("%Y%m%d")

    trips = db.query(Trip).filter(Trip.route_id == route_id, Trip.is_current).all()
    trip_direction = {t.trip_id: t.direction_id for t in trips}
    if not trip_direction:
        return _empty(route_id, service_date_str, start_ts, "No current trips for route")

    # Service-date attribution: a trip ran today iff a vehicle_position
    # with matching trip_start_date exists.
    vp_trip_ids = {
        row[0]
        for row in db.query(VehiclePosition.trip_id)
        .filter(
            VehiclePosition.route_id == route_id,
            VehiclePosition.trip_start_date == trip_start_date_str,
        )
        .distinct()
        .all()
    }
    active_trip_ids = vp_trip_ids & set(trip_direction.keys())
    if not active_trip_ids:
        return _empty(
            route_id, service_date_str, start_ts,
            "No vehicle_positions for any current trip on this service_date",
        )

    # Schedule lookup (trip_id, stop_sequence) -> stop_id + scheduled times.
    schedule_index: dict[tuple[str, int], dict] = {}
    for st in (
        db.query(StopTime)
        .filter(StopTime.trip_id.in_(active_trip_ids), StopTime.is_current)
        .all()
    ):
        schedule_index[(st.trip_id, st.stop_sequence)] = {
            "stop_id": st.stop_id,
            "arrival_time": st.arrival_time,
            "departure_time": st.departure_time,
        }
    if not schedule_index:
        return _empty(route_id, service_date_str, start_ts, "No stop_times for active trips")

    # Read state directly — one row per (trip, stop_sequence). No scan.
    state_rows = (
        db.query(TripUpdateState)
        .filter(TripUpdateState.trip_id.in_(active_trip_ids))
        .all()
    )

    rows = []
    derived_at = utcnow_naive()
    skipped_count = 0
    no_prediction_count = 0
    derived_keys: list[tuple[str, int]] = []

    # Use the StopEvent model only when target is "stop_events"; for
    # validation we write to a side table with identical schema.
    from src.models import StopEvent
    target_model = StopEvent if target_table_name == "stop_events" else _resolve_side_table(target_table_name)

    for state in state_rows:
        sched = schedule_index.get((state.trip_id, state.stop_sequence))
        if sched is None:
            continue  # ADDED trip or stale GTFS; skip.

        scheduled_arrival_ts = (
            parse_gtfs_time_to_dt(sched["arrival_time"], service_date)
            if sched["arrival_time"] else None
        )
        scheduled_departure_ts = (
            parse_gtfs_time_to_dt(sched["departure_time"], service_date)
            if sched["departure_time"] else None
        )

        is_skipped = state.final_schedule_relationship == "SKIPPED"
        if is_skipped:
            schedule_relationship = "SKIPPED"
            observed_arrival_ts = None
            skipped_count += 1
        else:
            observed_arrival_ts = state.last_predicted_arrival_ts
            if observed_arrival_ts is None:
                no_prediction_count += 1
                continue
            schedule_relationship = "SCHEDULED"

        deviation_sec = None
        if observed_arrival_ts is not None and scheduled_arrival_ts is not None:
            deviation_sec = int((observed_arrival_ts - scheduled_arrival_ts).total_seconds())

        rows.append({
            "service_date": service_date_str,
            "trip_id": state.trip_id,
            "route_id": route_id,
            "direction_id": trip_direction[state.trip_id],
            "vehicle_id": state.vehicle_id,
            "stop_id": sched["stop_id"],
            "stop_sequence": state.stop_sequence,
            "scheduled_arrival_ts": scheduled_arrival_ts,
            "scheduled_departure_ts": scheduled_departure_ts,
            "observed_arrival_ts": observed_arrival_ts,
            "deviation_sec": deviation_sec,
            "source": "trip_update",
            "schedule_relationship": schedule_relationship,
            "match_distance_m": None,
            "derived_at": derived_at,
        })
        derived_keys.append((state.trip_id, state.stop_sequence))

    rows_written = 0
    if rows:
        upsert_rows(
            db, target_model, rows,
            constraint_name="uq_stop_events_run_stop_source",
            update_cols=[
                "route_id", "direction_id", "vehicle_id", "stop_id",
                "scheduled_arrival_ts", "scheduled_departure_ts",
                "observed_arrival_ts", "deviation_sec",
                "schedule_relationship", "match_distance_m", "derived_at",
            ],
        )
        rows_written = len(rows)

        # Mark source state rows as derived so the cleanup cron can age them out.
        db.execute(
            update(TripUpdateState)
            .where(
                TripUpdateState.trip_id.in_({k[0] for k in derived_keys}),
                TripUpdateState.stop_sequence.in_({k[1] for k in derived_keys}),
            )
            .values(derived_at=derived_at)
        )

    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "active_trips": len(active_trip_ids),
        "state_rows_scanned": len(state_rows),
        "skipped_emitted": skipped_count,
        "dropped_no_prediction": no_prediction_count,
        "rows_written": rows_written,
        "elapsed_sec": round(time.time() - start_ts, 2),
    }


def _resolve_side_table(name: str):
    """Return a model bound to a side-table (same schema as StopEvent).

    Used for Phase D validation where we write to ``stop_events_v2``.
    The side table must already exist with identical schema.
    """
    from src.models import StopEvent
    if name == "stop_events_v2":
        # Lazy import + reflect: produce a dynamic model bound to the side table.
        # For simplicity in this plan, the side table uses StopEvent's columns
        # but a different __tablename__. The recommended implementation is:
        #   1. Create stop_events_v2 with `CREATE TABLE stop_events_v2 (LIKE stop_events INCLUDING ALL);`
        #   2. Use raw SQL INSERT ... ON CONFLICT for writes to the side table.
        # See pipelines/run_daily_batch.py Task 11 for how this is configured.
        raise NotImplementedError(
            "Side table writes use raw SQL in run_daily_batch — call this "
            "function only with target_table_name='stop_events'."
        )
    raise ValueError(f"Unknown target table: {name}")


def _empty(route_id, service_date_str, start_ts, note):
    return {
        "route_id": route_id,
        "service_date": service_date_str,
        "active_trips": 0,
        "state_rows_scanned": 0,
        "skipped_emitted": 0,
        "dropped_no_prediction": 0,
        "rows_written": 0,
        "elapsed_sec": round(time.time() - start_ts, 2),
        "note": note,
    }


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Derive stop_events from trip_update_state."
    )
    parser.add_argument("--route", help="Single route_id")
    parser.add_argument("--all-routes", action="store_true")
    parser.add_argument("--date", help="YYYY-MM-DD; defaults to today (Eastern)")
    args = parser.parse_args()

    if not args.route and not args.all_routes:
        parser.error("pass --route or --all-routes")

    load_dotenv()
    service_date = (
        datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else eastern_today()
    )
    db = get_session()
    try:
        if args.route:
            route_ids = [args.route]
        else:
            route_ids = [r.route_id for r in db.query(Route).filter(Route.is_current).all()]
        results = run_route_date_grid(
            derive_for_route_date, db, route_ids, [service_date], verbose=True,
        )
        for r in results:
            print(r)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

Note the imports at the top — add `import sys` if it isn't already there.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/test_derive_stop_events_from_state.py -v -m integration
```

Expected: All three PASS.

- [ ] **Step 5: Commit**

```bash
git add pipelines/derive_stop_events_from_state.py tests/test_derive_stop_events_from_state.py
git commit -m "feat: derive_stop_events_from_state reads state table directly"
```

---

## Task 10: Side table `stop_events_v2` + run new derivation alongside the old

**Files:**
- Create: `scripts/migrate_create_stop_events_v2.py`
- Modify: `pipelines/derive_stop_events_from_state.py` (raw-SQL write path for side table)
- Modify: `pipelines/run_daily_batch.py` (add new derivation to nightly run)
- Test: `tests/test_migrate_stop_events_v2.py`

**Context:** During the validation period (Phase D), we run BOTH derivation pipelines nightly. The old one writes to `stop_events` (production). The new one writes to `stop_events_v2` (side table). The comparison script (Task 11) diffs them daily.

- [ ] **Step 1: Write a failing test for the side-table migration**

Create `tests/test_migrate_stop_events_v2.py`:

```python
"""Test scripts/migrate_create_stop_events_v2.py."""

import pytest
from sqlalchemy import inspect, text


@pytest.mark.integration
def test_v2_table_has_same_columns_as_stop_events(db_session):
    """stop_events_v2 has identical columns to stop_events."""
    from scripts.migrate_create_stop_events_v2 import run_migration

    db_session.execute(text("DROP TABLE IF EXISTS stop_events_v2"))
    db_session.commit()

    run_migration(db_session.bind)

    inspector = inspect(db_session.bind)
    cols_v1 = {c["name"] for c in inspector.get_columns("stop_events")}
    cols_v2 = {c["name"] for c in inspector.get_columns("stop_events_v2")}
    assert cols_v1 == cols_v2
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest tests/test_migrate_stop_events_v2.py -v -m integration
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Create the side-table migration**

Create `scripts/migrate_create_stop_events_v2.py`:

```python
"""Create stop_events_v2 as a structural clone of stop_events.

Used during Phase D side-by-side validation: the new derivation pipeline
writes here, the comparison script diffs it against the production
``stop_events``. Dropped after Phase E cutover.

Usage:
    uv run python scripts/migrate_create_stop_events_v2.py
"""

import sys

from dotenv import load_dotenv
from sqlalchemy import text

from src.database import get_engine


CLONE_SQL = """
CREATE TABLE IF NOT EXISTS stop_events_v2
    (LIKE stop_events INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
"""


def run_migration(engine) -> None:
    """Create the side table (idempotent)."""
    with engine.begin() as conn:
        conn.execute(text(CLONE_SQL))


def main() -> int:
    """CLI entry point."""
    load_dotenv()
    run_migration(get_engine())
    print("stop_events_v2 ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Verify the test passes**

Run:

```bash
uv run pytest tests/test_migrate_stop_events_v2.py -v -m integration
```

Expected: PASS.

- [ ] **Step 5: Run the migration**

```bash
uv run python scripts/migrate_create_stop_events_v2.py
```

Expected: "stop_events_v2 ready."

- [ ] **Step 6: Modify the side-table write path in the new derivation**

Find the `_resolve_side_table` function in `pipelines/derive_stop_events_from_state.py` (created in Task 9) and replace it with a working implementation. Replace the entire function body:

```python
def _resolve_side_table(name: str):
    """Return a model bound to the side table (same schema as StopEvent).

    Used for Phase D validation where we write to ``stop_events_v2``.
    The side table must already exist with identical schema (see
    scripts/migrate_create_stop_events_v2.py).
    """
    if name != "stop_events_v2":
        raise ValueError(f"Unknown target table: {name}")

    # Dynamically declare a model bound to stop_events_v2 with the same
    # columns as StopEvent. SQLAlchemy supports this via __table_args__
    # but the simplest path is automap-style: copy StopEvent.__table__'s
    # column definitions onto a new Table object bound to the side name.
    from sqlalchemy import Table

    from src.models import Base, StopEvent

    side_name = "stop_events_v2"
    if side_name in Base.metadata.tables:
        return _SideStopEvent

    side_table = StopEvent.__table__.tometadata(Base.metadata, name=side_name)

    # Create a minimal class wrapping the side table so upsert_rows works.
    class _SideStopEvent:
        __table__ = side_table
        __tablename__ = side_name

    globals()["_SideStopEvent"] = _SideStopEvent
    return _SideStopEvent
```

- [ ] **Step 7: Modify `pipelines/run_daily_batch.py` to run BOTH derivations**

Open `pipelines/run_daily_batch.py`. Find the call to the old derivation (search for `derive_stop_events_trip_updates`). Right after that call, add a parallel call to the new derivation against `stop_events_v2`:

```python
# Phase D side-by-side validation: run the new derivation against
# stop_events_v2 so the comparison script (compare_old_vs_new_derivation.py)
# can diff outputs nightly. Remove after Phase E cutover.
from pipelines.derive_stop_events_from_state import derive_for_route_date as derive_v2
for route_id in route_ids:
    derive_v2(db, route_id=route_id, service_date=service_date, target_table_name="stop_events_v2")
```

Adjust variable names to match the actual run_daily_batch.py locals (the script likely already has `route_ids`, `service_date`, and `db` in scope).

- [ ] **Step 8: Verify the daily batch still runs end-to-end (smoke)**

Run:

```bash
uv run python pipelines/run_daily_batch.py --date 2026-05-17
```

Expected: completes without error. Both `stop_events` and `stop_events_v2` have rows for the date.

- [ ] **Step 9: Commit**

```bash
git add scripts/migrate_create_stop_events_v2.py tests/test_migrate_stop_events_v2.py pipelines/derive_stop_events_from_state.py pipelines/run_daily_batch.py
git commit -m "feat: stop_events_v2 side table + dual derivation for validation"
```

---

## Task 11: Validation comparison script

**Files:**
- Create: `pipelines/compare_old_vs_new_derivation.py`
- Test: `tests/test_compare_derivations.py`

**Context:** Runs nightly during Phase D. For each (route_id, service_date) in the last N days:
- Compare row counts between `stop_events` and `stop_events_v2`.
- For matching (trip_id, stop_sequence), compare `observed_arrival_ts`, `schedule_relationship`, `deviation_sec`.
- Report aggregate agreement %. Flag any per-route disagreement > 1%.

- [ ] **Step 1: Write a failing test for the comparison logic**

Create `tests/test_compare_derivations.py`:

```python
"""Tests for pipelines.compare_old_vs_new_derivation."""

from datetime import date, datetime

import pytest
from sqlalchemy import text

from src.models import StopEvent


def _make_event(**kwargs):
    """Build a StopEvent row with sensible defaults."""
    defaults = dict(
        service_date="2026-05-17",
        trip_id="T1",
        route_id="R1",
        direction_id=0,
        stop_id="S1",
        stop_sequence=1,
        scheduled_arrival_ts=datetime(2026, 5, 17, 14, 5, 0),
        scheduled_departure_ts=datetime(2026, 5, 17, 14, 5, 30),
        observed_arrival_ts=datetime(2026, 5, 17, 14, 6, 30),
        deviation_sec=90,
        source="trip_update",
        schedule_relationship="SCHEDULED",
        match_distance_m=None,
        derived_at=datetime(2026, 5, 17, 14, 10, 0),
    )
    defaults.update(kwargs)
    return StopEvent(**defaults)


@pytest.mark.integration
def test_perfect_match_reports_100_percent_agreement(db_session):
    """Identical rows in both tables yield 100% agreement."""
    from pipelines.compare_old_vs_new_derivation import compare_one_day

    # Ensure side table exists for the test session.
    db_session.execute(text("CREATE TABLE IF NOT EXISTS stop_events_v2 (LIKE stop_events INCLUDING ALL)"))
    db_session.add(_make_event())
    db_session.execute(text("""
        INSERT INTO stop_events_v2 SELECT * FROM stop_events
    """))
    db_session.commit()

    result = compare_one_day(db_session, target_date=date(2026, 5, 17))
    assert result["agreement_pct"] == 100.0
    assert result["diverging_routes"] == []


@pytest.mark.integration
def test_observed_arrival_mismatch_lowers_agreement(db_session):
    """Different observed_arrival_ts in v2 lowers agreement below 100%."""
    from pipelines.compare_old_vs_new_derivation import compare_one_day

    db_session.execute(text("CREATE TABLE IF NOT EXISTS stop_events_v2 (LIKE stop_events INCLUDING ALL)"))
    db_session.add(_make_event())
    db_session.commit()
    db_session.execute(text("""
        INSERT INTO stop_events_v2 (service_date, trip_id, route_id, direction_id,
            stop_id, stop_sequence, scheduled_arrival_ts, scheduled_departure_ts,
            observed_arrival_ts, deviation_sec, source, schedule_relationship,
            match_distance_m, derived_at)
        VALUES ('2026-05-17', 'T1', 'R1', 0, 'S1', 1,
            '2026-05-17 14:05:00', '2026-05-17 14:05:30',
            '2026-05-17 14:08:00', 180, 'trip_update', 'SCHEDULED', NULL, NOW())
    """))
    db_session.commit()

    result = compare_one_day(db_session, target_date=date(2026, 5, 17))
    assert result["agreement_pct"] < 100.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
uv run pytest tests/test_compare_derivations.py -v -m integration
```

Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Create the comparison pipeline**

Create `pipelines/compare_old_vs_new_derivation.py`:

```python
"""Phase D validation: diff stop_events vs stop_events_v2.

For each (route_id, service_date) in the requested window, compute:
  - Row counts in both tables
  - For matching (trip_id, stop_sequence), agreement on
    (observed_arrival_ts, schedule_relationship, deviation_sec)
  - Per-route disagreement %; flag any > 1%.

Phase E cutover requires agreement_pct >= 99.5 for at least 7 consecutive
days including one full weekend (see design doc).

Usage:
    uv run python pipelines/compare_old_vs_new_derivation.py --date 2026-05-17
    uv run python pipelines/compare_old_vs_new_derivation.py --days-back 7
"""

import argparse
import sys
from datetime import date as date_type
from datetime import datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database import get_session


COMPARE_SQL = """
WITH joined AS (
    SELECT
        old.route_id,
        old.trip_id,
        old.stop_sequence,
        old.observed_arrival_ts  AS old_ts,
        v2.observed_arrival_ts   AS new_ts,
        old.schedule_relationship AS old_sr,
        v2.schedule_relationship  AS new_sr,
        old.deviation_sec         AS old_dev,
        v2.deviation_sec          AS new_dev
    FROM stop_events old
    LEFT JOIN stop_events_v2 v2
        ON v2.trip_id = old.trip_id
       AND v2.stop_sequence = old.stop_sequence
       AND v2.service_date = old.service_date
    WHERE old.service_date = :service_date
      AND old.source = 'trip_update'
)
SELECT
    route_id,
    COUNT(*) AS total,
    SUM(CASE WHEN new_ts IS NULL THEN 1 ELSE 0 END) AS missing_in_v2,
    SUM(CASE WHEN old_ts = new_ts AND old_sr = new_sr AND old_dev IS NOT DISTINCT FROM new_dev THEN 1 ELSE 0 END) AS matched
FROM joined
GROUP BY route_id
"""


def compare_one_day(db: Session, target_date: date_type) -> dict:
    """Compute agreement metrics for one service date.

    Returns a dict with:
        agreement_pct: float (0-100)
        diverging_routes: list[dict] for routes with > 1% disagreement
        per_route: dict[route_id -> {total, matched, missing}]
    """
    rows = db.execute(
        text(COMPARE_SQL),
        {"service_date": target_date.isoformat()},
    ).all()

    per_route = {}
    total_all = 0
    matched_all = 0
    diverging = []
    for r in rows:
        route_id, total, missing, matched = r
        total_all += total or 0
        matched_all += matched or 0
        agreement = (matched / total * 100) if total else 100.0
        per_route[route_id] = {
            "total": total or 0,
            "matched": matched or 0,
            "missing_in_v2": missing or 0,
            "agreement_pct": round(agreement, 2),
        }
        if total and (total - matched) / total > 0.01:
            diverging.append({"route_id": route_id, **per_route[route_id]})

    overall = (matched_all / total_all * 100) if total_all else 100.0
    return {
        "service_date": target_date.isoformat(),
        "total_rows": total_all,
        "matched_rows": matched_all,
        "agreement_pct": round(overall, 2),
        "diverging_routes": diverging,
        "per_route": per_route,
    }


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Compare stop_events vs stop_events_v2."
    )
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--days-back", type=int, default=1,
                        help="Compare the last N days (default: 1)")
    args = parser.parse_args()

    load_dotenv()
    db = get_session()
    try:
        if args.date:
            dates = [datetime.strptime(args.date, "%Y-%m-%d").date()]
        else:
            today = datetime.utcnow().date()
            dates = [today - timedelta(days=i) for i in range(1, args.days_back + 1)]
        for d in dates:
            result = compare_one_day(db, d)
            print(
                f"{result['service_date']}: "
                f"{result['agreement_pct']}% agreement "
                f"({result['matched_rows']:,}/{result['total_rows']:,}), "
                f"{len(result['diverging_routes'])} routes with >1% disagreement"
            )
            for d_route in result["diverging_routes"]:
                print(f"  ! {d_route}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
uv run pytest tests/test_compare_derivations.py -v -m integration
```

Expected: Both PASS.

- [ ] **Step 5: Commit**

```bash
git add pipelines/compare_old_vs_new_derivation.py tests/test_compare_derivations.py
git commit -m "feat: comparison script for Phase D validation"
```

---

## Task 12: Phase D — Operate validation period (≥ 7 days)

**Files:** None modified. This is an operational task.

**Context:** With dual-write live (Task 5-6) and dual-derivation live (Task 10), the system collects evidence for at least 7 consecutive days, ideally including a full weekend.

**This is not a TDD task.** It's a watch-and-record activity. The implementer / operator runs comparisons and waits.

- [ ] **Step 1: After dual-write has been running for 24 hours, run the comparison for the first day**

```bash
uv run python pipelines/compare_old_vs_new_derivation.py --days-back 1
```

Record the output. Expected: ≥ 99.5% agreement.

- [ ] **Step 2: Schedule the comparison via the daily batch**

Modify `pipelines/run_daily_batch.py` to run the comparison after derivation:

```python
# Phase D — log nightly comparison alongside the derivation.
from pipelines.compare_old_vs_new_derivation import compare_one_day
result = compare_one_day(db, target_date=service_date)
print(f"Phase D comparison: {result['agreement_pct']}% agreement, "
      f"{len(result['diverging_routes'])} diverging routes")
```

- [ ] **Step 3: Observe daily reports for ≥ 7 days**

Each morning, read the previous night's batch log for the comparison summary. Record agreement %.

**Pass criteria for moving to Task 13 (cutover):**
- Agreement % ≥ 99.5 on at least 7 consecutive days.
- At least one full weekend (Saturday + Sunday) included.
- Any specific route with > 1% disagreement is investigated. Either:
  - It's a known semantic difference (document in spec under "Open questions").
  - It's a bug — fix in `derive_stop_events_from_state.py`, re-run comparison.

- [ ] **Step 4: After pass criteria met, commit the comparison-logging change and proceed to Task 13**

```bash
git add pipelines/run_daily_batch.py
git commit -m "ops: log Phase D comparison in daily batch"
```

---

## Task 13: Phase E — Cutover

**Files:**
- Modify: `src/wmata_collector.py:565+` (remove writes to `trip_update_snapshots`)
- Modify: `pipelines/run_daily_batch.py` (remove old derivation, redirect new derivation to `stop_events`)
- Drop: `stop_events_v2` (after successful cutover)

**Context:** Switching production over to the new pipeline. Phase D validation must be passing.

- [ ] **Step 1: Stop the collector**

```bash
sudo systemctl stop wmata-collector   # or however the collector is started
# Or: kill the running process if launched manually
```

- [ ] **Step 2: Modify `_save_trip_updates` to stop writing to `trip_update_snapshots`**

Edit `src/wmata_collector.py`. In `_save_trip_updates`, REMOVE these lines:

```python
        if new_objects:
            self.db.bulk_save_objects(new_objects)
            self.db.commit()
```

And remove the `new_objects.append(TripUpdateSnapshot(**row))` inside the loop. Also remove the dedup-cache logic if `_tu_dedup_cache` is no longer used (verify with `grep -rn "_tu_dedup_cache" src/`).

Keep:
- The archive writer append (Task 6).
- The UPSERT to `trip_update_state` (Task 5).

- [ ] **Step 3: Reconfigure new derivation to write to `stop_events`**

Edit `pipelines/run_daily_batch.py`:

3a) Remove the call to the old derivation pipeline (the `derive_stop_events_trip_updates` import + invocation).

3b) Change the new derivation invocation to target `stop_events`:

```python
# Production derivation: write to stop_events directly.
from pipelines.derive_stop_events_from_state import derive_for_route_date
for route_id in route_ids:
    derive_for_route_date(db, route_id=route_id, service_date=service_date,
                          target_table_name="stop_events")
```

3c) Remove the Phase D comparison call (Task 12 Step 2) and the v2 side-table call.

- [ ] **Step 4: Run a manual derivation as a smoke test**

```bash
uv run python pipelines/derive_stop_events_from_state.py --all-routes --date $(date -u +%Y-%m-%d)
```

Expected: Completes without error. Reasonable row counts logged per route.

- [ ] **Step 5: Verify API still works**

```bash
uv run uvicorn api.main:app --reload &
sleep 5
curl -s http://localhost:8000/api/routes | head -c 500
kill %1
```

Expected: A JSON response with route data, including reasonable metrics for recent days.

- [ ] **Step 6: Restart the collector**

```bash
sudo systemctl start wmata-collector
# Or your usual startup command.
```

Verify it's writing:

```bash
sleep 90
psql -d wmata_dashboard -c "SELECT MAX(final_snapshot_ts) FROM trip_update_state;"
```

Expected: Recent timestamp (within the last 2 minutes).

- [ ] **Step 7: Drop the side table**

```bash
psql -d wmata_dashboard -c "DROP TABLE stop_events_v2;"
```

- [ ] **Step 8: Commit**

```bash
git add src/wmata_collector.py pipelines/run_daily_batch.py
git commit -m "feat: Phase E cutover — new pipeline is production, side table dropped"
```

---

## Task 14: Phase F — Retirement (after 14 days clean operation)

**Files:**
- Delete: `pipelines/derive_stop_events_trip_updates.py`
- Delete: `pipelines/archive_trip_update_snapshots.py`
- Drop: `trip_update_snapshots` table
- Update: `CLAUDE.md`
- Update: `NOTES.md`

**Context:** After Phase E has been running cleanly for 14 days, retire dead code and free the 129 GB.

- [ ] **Step 1: Verify 14 days of clean operation**

Confirm:

```bash
git log --since='14 days ago' --grep='Phase E' --oneline
# Should show the cutover commit.

uv run pytest -m smoke -v
# Should pass.

psql -d wmata_dashboard -c "
SELECT MAX(derived_at) AS latest_derive,
       COUNT(*) AS rows_in_state
FROM trip_update_state;"
# latest_derive should be from last night; rows_in_state should be ~180K-360K (current 2-day cleanup window).
```

- [ ] **Step 2: Drop the old table**

```bash
psql -d wmata_dashboard -c "DROP TABLE trip_update_snapshots;"
```

Verify size reclaimed:

```bash
psql -d wmata_dashboard -c "SELECT pg_size_pretty(pg_database_size('wmata_dashboard'));"
```

Expected: DB shrinks by ~129 GB → final DB size <10 GB.

- [ ] **Step 3: Delete dead code**

```bash
git rm pipelines/derive_stop_events_trip_updates.py
git rm pipelines/archive_trip_update_snapshots.py
```

Also `grep -rn "trip_update_snapshots\|TripUpdateSnapshot\|archive_trip_update_snapshots\|derive_stop_events_trip_updates" src/ pipelines/ scripts/ api/` and remove any remaining references:
- `src/wmata_collector.py`: remove unused imports (TripUpdateSnapshot, `_tu_dedup_cache` if dead)
- `src/models.py`: remove `class TripUpdateSnapshot`
- `scripts/`: remove probe scripts that target the old table (`probe_dropped_tu_trips.py`, `probe_trip_updates*.py`, `add_trip_update_trip_snap_index.py`) if they reference the old table

- [ ] **Step 4: Update `CLAUDE.md`**

In `CLAUDE.md`, find the section that documents the architecture (look for "stop_events / runs are the architectural foundation" and "trip_update_snapshots" references). Replace references to `trip_update_snapshots` with `trip_update_state`, and add a note explaining the new architecture:

```markdown
- **Trip update state is mirrored, not appended.** The collector
  UPSERTs each (trip_id, stop_sequence) into `trip_update_state`
  (one row per pair, ~180K rows/day total). The old append-only
  `trip_update_snapshots` table was retired in NOTES-XX (link to
  PR). Raw evidence is preserved in Backblaze B2 as
  `s3://<bucket>/raw_snapshots/<YYYY-MM-DD>.parquet`.
```

Replace XX with the appropriate NOTES item number once created.

- [ ] **Step 5: Update `NOTES.md`**

Use the `update-notes-in-pr` skill to fold any NOTES.md edits into this final PR.

Specifically:
- Close NOTES-21 reference (archive job is retired).
- Add a brief reference to the completed refactor at the top of NOTES.md.

- [ ] **Step 6: Run full test suite**

```bash
uv run pytest
uv run ruff check src/ scripts/ api/ pipelines/ tests/
cd frontend && npm run lint && npm test
```

Expected: All pass.

- [ ] **Step 7: Commit and open a PR**

```bash
git add -u
git commit -m "feat: Phase F retirement — drop trip_update_snapshots, remove dead code"
git push -u origin feature/trip-update-state-refactor
gh pr create --title "feat: trip_update_state refactor — final retirement" --body "$(cat <<'EOF'
## Summary

Completes the trip_update_state refactor (spec:
`docs/superpowers/specs/2026-05-17-trip-update-state-refactor-design.md`).

After 14 days of clean Phase E operation:

- Drops the `trip_update_snapshots` table (frees ~129 GB).
- Removes the old derivation pipeline (`derive_stop_events_trip_updates.py`)
  and its archive job (`archive_trip_update_snapshots.py`).
- Updates `CLAUDE.md` and `NOTES.md` to reflect the new architecture.

DB size: ~146 GB → <10 GB.

## Test plan

- [ ] `uv run pytest`
- [ ] `uv run pytest -m integration`
- [ ] `uv run ruff check src/ scripts/ api/ pipelines/ tests/`
- [ ] `cd frontend && npm run lint && npm test`
- [ ] Collector logs show normal operation
- [ ] API smoke: `/api/routes` returns valid data with deltas

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-review checklist

Run through this after marking all tasks complete:

- [ ] Every task has a test before implementation (TDD discipline).
- [ ] No "TBD" / "TODO" / "implement later" tokens (search: `grep -nE 'TBD|TODO|FIXME|fill in|XXX' docs/superpowers/plans/2026-05-17-trip-update-state-refactor.md`).
- [ ] Every file path is absolute or repo-relative — no `path/to/file.py` placeholders.
- [ ] Function signatures match between definition (Task 4, 6, 7, 8, 9) and call sites (Task 5, 10, 13).
- [ ] Each commit message reflects what changed (no "WIP" / "fixes" / "stuff").
- [ ] The plan stops at Phase F retirement — no creep into cloud migration.

If anything is missing, fix inline and re-run the checklist.
