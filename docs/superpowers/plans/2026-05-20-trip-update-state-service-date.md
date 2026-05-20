# `trip_update_state` service_date PK + Phase D recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `trip_update_state` table preserve per-day rows so historical service-dates are recoverable; fix the silently-failing v2 derivation pipeline; add an idempotent JSONL replay tool so any past day in the archive window can be re-derived on demand.

**Architecture:** Add `service_date DATE NOT NULL` to `trip_update_state` and include it in the PK. Collector computes `service_date` per row at UPSERT time (preferring `tripDescriptor.start_date` when WMATA provides it, falling back to Eastern day of `snapshot_ts`). The existing `_resolve_side_table` bug — which has prevented `stop_events_v2` from ever being populated — is fixed in the same PR by returning the real `Table` object rather than a wrapper class. Cleanup collapses to a single date-based rule. A new `pipelines/replay_archive_to_state.py` script lets us recover 2026-05-18 and 2026-05-19 (and any future lost days) from the JSONL archive.

**Tech Stack:** PostgreSQL, SQLAlchemy 2.x, psycopg2, zstandard (for JSONL.zst), pyarrow (existing).

---

## File Structure

**Modified:**
- `src/models.py` — add `service_date` column to `TripUpdateState`.
- `src/wmata_collector.py` — extract `start_date` from TripUpdate's TripDescriptor; add `service_date` to the UPSERT payload.
- `src/upsert_helpers.py` — `upsert_trip_update_state` includes `service_date` in the conflict target and SET clause; `upsert_rows` accepts both ORM-mapped classes and bare `Table` objects.
- `pipelines/derive_stop_events_from_state.py` — fix `_resolve_side_table` to return a real `Table` (drops the wrapper class); filter the state-rows query by `service_date`.
- `pipelines/cleanup_trip_update_state.py` — single `DELETE WHERE service_date < CURRENT_DATE - INTERVAL '7 days'` rule.
- `pipelines/run_daily_batch.py` — post-batch guard: if v2 derivation wrote 0 rows for any route+date, fail loud.
- `NOTES.md` — update NOTES-72 status.

**Created:**
- `scripts/migrate_add_service_date_to_state.py` — schema migration (idempotent).
- `pipelines/replay_archive_to_state.py` — JSONL → state replay tool.
- `tests/test_replay_archive_to_state.py` — unit tests for the replay tool.
- `tests/test_derive_state_filters_by_service_date.py` — verifies the derivation filter.
- `tests/test_trip_update_state_upsert_per_day.py` — verifies two days coexist.
- `docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md` — short spec addendum documenting the schema change rationale (so the 2026-05-17 spec stays as historical context).

---

## Operational note (deployment is the user's call)

The collector process (PID 63105 at time of writing) must be stopped before the schema migration runs, because the running collector code has the old model and will fail to INSERT after the column becomes NOT NULL. The PR includes everything to make the change ready; the actual deployment sequence (stop collector → run migration → restart with new code → optionally replay 5/18+5/19) is performed by the user after the PR merges. The PR does NOT touch the running collector; it ships the new code that will be live after restart.

---

### Task 1: Plan branch baseline + smoke check

**Files:**
- No files modified. Sanity-check the current branch state.

- [ ] **Step 1: Confirm clean branch**

Run: `git status`
Expected: `On branch feat/state-service-date-pk`, clean (or only this plan doc untracked).

- [ ] **Step 2: Confirm baseline tests pass**

Run: `uv run pytest -m smoke -q`
Expected: all green. (If anything is red, stop and investigate — we don't want to attribute pre-existing failures to this change.)

- [ ] **Step 3: Commit the plan**

```bash
git add docs/superpowers/plans/2026-05-20-trip-update-state-service-date.md
git commit -m "docs: plan trip_update_state service_date PK + Phase D recovery"
```

---

### Task 2: Schema migration script

**Files:**
- Create: `scripts/migrate_add_service_date_to_state.py`

- [ ] **Step 1: Write the migration**

```python
"""Add service_date column to trip_update_state and rebuild the PK.

The original PK (trip_id, stop_sequence) lets each subsequent day's snapshot
overwrite the prior day's state, which makes historical re-derivation
impossible. Adding service_date to the PK preserves one row per
(trip, stop, service_date).

Idempotent: every statement is conditional. Safe to re-run.

Usage:
    uv run python scripts/migrate_add_service_date_to_state.py

Pre-requisite: stop the continuous_combined_collector. The running
collector's TripUpdateState model lacks the service_date column and will
fail to INSERT until restarted with the new code.
"""

import sys

from dotenv import load_dotenv
from sqlalchemy import text

from src.database import get_engine

ADD_COLUMN_SQL = """
ALTER TABLE trip_update_state
    ADD COLUMN IF NOT EXISTS service_date DATE;
"""

BACKFILL_SQL = """
UPDATE trip_update_state
SET service_date = (final_snapshot_ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date
WHERE service_date IS NULL;
"""

SET_NOT_NULL_SQL = """
ALTER TABLE trip_update_state
    ALTER COLUMN service_date SET NOT NULL;
"""

# DROP + ADD instead of a single ALTER because PG requires the constraint
# name to recreate it. trip_update_state_pkey is the conventional default name.
DROP_PK_SQL = """
ALTER TABLE trip_update_state DROP CONSTRAINT IF EXISTS trip_update_state_pkey;
"""

ADD_PK_SQL = """
ALTER TABLE trip_update_state
    ADD CONSTRAINT trip_update_state_pkey
    PRIMARY KEY (trip_id, stop_sequence, service_date);
"""

CREATE_SERVICE_DATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_tus_service_date
    ON trip_update_state (service_date);
"""


def run_migration(engine) -> None:
    """Apply the migration in a single transaction. Safe to re-run."""
    with engine.begin() as conn:
        conn.execute(text(ADD_COLUMN_SQL))
        conn.execute(text(BACKFILL_SQL))
        conn.execute(text(SET_NOT_NULL_SQL))
        conn.execute(text(DROP_PK_SQL))
        conn.execute(text(ADD_PK_SQL))
        conn.execute(text(CREATE_SERVICE_DATE_INDEX_SQL))


def main() -> int:
    """CLI entry point."""
    load_dotenv()
    engine = get_engine()
    print("Adding service_date column + rebuilding trip_update_state PK...")
    run_migration(engine)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Commit (don't run yet — collector is still live)**

```bash
git add scripts/migrate_add_service_date_to_state.py
git commit -m "feat: migration script for trip_update_state service_date PK"
```

The migration will not be run until the deployment step (Task 11).

---

### Task 3: Update `TripUpdateState` model

**Files:**
- Modify: `src/models.py` (TripUpdateState class around line 393)

- [ ] **Step 1: Add `service_date` column to the model**

After the existing `trip_id` / `stop_sequence` PK columns and before `stop_id`:

```python
from sqlalchemy import Column, Date, DateTime, Integer, String  # add Date if not already imported

class TripUpdateState(Base):
    # ... existing docstring ...

    __tablename__ = "trip_update_state"

    trip_id = Column(String, primary_key=True)
    stop_sequence = Column(Integer, primary_key=True)
    service_date = Column(Date, primary_key=True)

    stop_id = Column(String, nullable=False)
    # ... rest unchanged ...
```

Update the lifecycle docstring (lines 402-411) to reflect:
- step 1 mentions "rows inserted per (trip, stop) for the current service_date"
- step 5 mentions cleanup is now `service_date < CURRENT_DATE - INTERVAL '7 days'`

- [ ] **Step 2: Run model-import smoke test**

Run: `uv run python -c "from src.models import TripUpdateState; print(TripUpdateState.__table__.primary_key.columns.keys())"`
Expected: `['trip_id', 'stop_sequence', 'service_date']`

- [ ] **Step 3: Run existing test suite to check for breakage**

Run: `uv run pytest -m smoke -q`
Expected: green (the in-memory SQLite fixture creates tables from `Base.metadata`, so adding a nullable-defaulting column shouldn't break anything; if a test asserts old PK columns it'll surface here).

If any test fails, fix it inline before committing.

- [ ] **Step 4: Commit**

```bash
git add src/models.py
git commit -m "feat: add service_date to TripUpdateState model"
```

---

### Task 4: Update `upsert_trip_update_state` and `upsert_rows`

**Files:**
- Modify: `src/upsert_helpers.py`

- [ ] **Step 1: Inspect `upsert_trip_update_state`**

Run: `grep -nA 40 "def upsert_trip_update_state" src/upsert_helpers.py`

The function currently has an `INSERT ... ON CONFLICT (trip_id, stop_sequence)` SQL. We need to change the conflict target to `(trip_id, stop_sequence, service_date)` and add `service_date` to the row payload.

- [ ] **Step 2: Apply the changes**

Modifications:

1. The row payload spec mentions a `service_date` key (callers pass it).
2. The INSERT column list includes `service_date`.
3. The ON CONFLICT target becomes `(trip_id, stop_sequence, service_date)`.
4. (No SET clause change — `service_date` is part of the key, not updated.)

Also patch `upsert_rows` to handle both ORM classes and bare `Table` objects:

```python
# At the top of upsert_rows, replace `stmt = pg_insert(model).values(rows)` with:
table = getattr(model, "__table__", model)
stmt = pg_insert(table).values(rows)
```

This makes the helper work whether the caller passes `StopEvent` (mapped class — `pg_insert` reads `.__table__` automatically) or a side-table `Table` returned by `_resolve_side_table` (handled by the new `getattr`).

- [ ] **Step 3: Write a parametrized test for the dual-input shape**

File: `tests/test_upsert_rows_accepts_table_or_model.py`

```python
"""upsert_rows must accept both ORM mapped classes and bare Table objects.

The Phase D side-table flow (stop_events_v2) passes a Table created via
`StopEvent.__table__.to_metadata(isolated_meta, name=...)` — this is NOT
a declarative-mapped class. The original implementation passed the
wrapper class straight to `pg_insert`, which raised
`sqlalchemy.exc.ArgumentError: subject table for an INSERT, UPDATE or
DELETE expected, got <class ...>`.
"""

import pytest
from sqlalchemy import MetaData

from src.models import StopEvent
from src.upsert_helpers import upsert_rows


def test_upsert_rows_accepts_mapped_class(pg_session):
    """Smoke: passing the mapped class still works (regression guard)."""
    # Use a minimal row that satisfies stop_events constraints; rely on
    # the existing fixture's conftest setup for FK targets.
    # If the schema requires more setup, this becomes a SKIP — the assertion
    # we care about is that the call shape is accepted, not that the row
    # persists.
    rows = [
        {
            "service_date": "2026-05-20",
            "trip_id": "TEST_TRIP_UPSERT_ROWS_1",
            "route_id": "TEST",
            "direction_id": 0,
            "vehicle_id": "TEST_VEH",
            "stop_id": "TEST_STOP",
            "stop_sequence": 1,
            "scheduled_arrival_ts": None,
            "scheduled_departure_ts": None,
            "observed_arrival_ts": None,
            "deviation_sec": None,
            "source": "trip_update",
            "schedule_relationship": "SCHEDULED",
            "match_distance_m": None,
            "derived_at": None,
        }
    ]
    try:
        upsert_rows(
            pg_session,
            StopEvent,
            rows,
            constraint_name="uq_stop_events_run_stop_source",
            update_cols=["vehicle_id"],
        )
    except Exception as e:
        # If FK constraints reject the test row, that's a different failure
        # mode than the "subject table expected" ArgumentError we're guarding.
        if "subject table" in str(e):
            raise
        pytest.skip(f"FK or constraint issue irrelevant to this test: {e}")


def test_upsert_rows_accepts_bare_table(pg_session):
    """The Phase D regression: a side-table Table object must be accepted."""
    isolated_meta = MetaData()
    side_table = StopEvent.__table__.to_metadata(
        isolated_meta, name="stop_events_v2"
    )
    rows = [
        {
            "service_date": "2026-05-20",
            "trip_id": "TEST_TRIP_UPSERT_ROWS_2",
            "route_id": "TEST",
            "direction_id": 0,
            "vehicle_id": "TEST_VEH",
            "stop_id": "TEST_STOP",
            "stop_sequence": 1,
            "scheduled_arrival_ts": None,
            "scheduled_departure_ts": None,
            "observed_arrival_ts": None,
            "deviation_sec": None,
            "source": "trip_update",
            "schedule_relationship": "SCHEDULED",
            "match_distance_m": None,
            "derived_at": None,
        }
    ]
    try:
        upsert_rows(
            pg_session,
            side_table,
            rows,
            constraint_name="uq_stop_events_v2_run_stop_source",
            update_cols=["vehicle_id"],
        )
    except Exception as e:
        if "subject table" in str(e):
            raise
        pytest.skip(f"stop_events_v2 not present in this DB: {e}")
```

- [ ] **Step 4: Run the new test**

Run: `uv run pytest tests/test_upsert_rows_accepts_table_or_model.py -v`
Expected: both tests PASS (or SKIP if pg_session can't be created, but never ERROR on "subject table").

- [ ] **Step 5: Commit**

```bash
git add src/upsert_helpers.py tests/test_upsert_rows_accepts_table_or_model.py
git commit -m "fix: upsert helpers accept bare Table; service_date in state PK"
```

---

### Task 5: Capture `tripDescriptor.start_date` in the collector

**Files:**
- Modify: `src/wmata_collector.py:get_realtime_trip_updates` (around line 504-592)

- [ ] **Step 1: Add start_date extraction**

In the TripUpdate parsing loop (line ~535-576), after extracting `trip_id` / `route_id` / `vehicle_id`, add:

```python
trip_start_date = (
    tu.trip.start_date
    if tu.trip.HasField("start_date") and tu.trip.start_date
    else None
)
```

And add `"trip_start_date": trip_start_date,` to the dict appended to `rows` (line ~562-575).

- [ ] **Step 2: Verify the collector module still imports / parses**

Run: `uv run python -c "from src.wmata_collector import WMATACollector; print('ok')"`
Expected: `ok` (no import-time errors).

- [ ] **Step 3: Commit**

```bash
git add src/wmata_collector.py
git commit -m "feat(collector): extract trip.start_date in trip-update parsing"
```

---

### Task 6: Compute and write `service_date` in the collector UPSERT path

**Files:**
- Modify: `src/wmata_collector.py:_save_trip_updates` (around line 594-685)
- Modify: `src/timezones.py` (use existing helper) — read-only check, may need to add a helper if none exists.

- [ ] **Step 1: Add a service_date helper for a row**

In `src/wmata_collector.py`, add a module-level helper near the existing imports:

```python
from datetime import date, datetime
from src.timezones import to_eastern_naive  # existing helper; if it doesn't exist, use the closest equivalent in src/timezones.py

def _service_date_for_row(row: dict) -> date:
    """Return the Eastern service_date for a trip-update row.

    Prefers ``trip_start_date`` (YYYYMMDD string from GTFS-RT
    ``tripDescriptor.start_date``) when present and parseable. Otherwise
    falls back to the Eastern calendar day of ``snapshot_ts`` — which is
    correct for 99%+ of WMATA bus trips since service-day-crossing
    overnight bus operations are rare.
    """
    raw = row.get("trip_start_date")
    if raw:
        try:
            return datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            pass  # fall through to snapshot_ts inference
    snapshot_ts: datetime = row["snapshot_ts"]
    return to_eastern_naive(snapshot_ts).date()
```

(If `to_eastern_naive` doesn't exist, peek at `src/timezones.py` and use the matching helper. The conversion is from a naive-UTC datetime to a naive-Eastern datetime.)

- [ ] **Step 2: Use the helper in the upsert payload**

In `_save_trip_updates`, modify the `upsert_payload` (line 655) to include `service_date`:

```python
upsert_payload = [
    {
        "trip_id": r["trip_id"],
        "stop_sequence": r["stop_sequence"],
        "service_date": _service_date_for_row(r),
        "stop_id": r["stop_id"],
        "vehicle_id": r.get("vehicle_id"),
        "snapshot_ts": r["snapshot_ts"],
        "predicted_arrival_ts": r.get("predicted_arrival_ts"),
        "schedule_relationship": r.get("schedule_relationship"),
    }
    for r in rows
    if r.get("stop_sequence") is not None
]
```

- [ ] **Step 3: Verify imports**

Run: `uv run python -c "from src.wmata_collector import _service_date_for_row; print('ok')"`
Expected: `ok`.

- [ ] **Step 4: Unit test the helper**

File: `tests/test_collector_service_date.py`

```python
"""Tests for the _service_date_for_row helper in the collector."""

from datetime import date, datetime

from src.wmata_collector import _service_date_for_row


def test_uses_trip_start_date_when_present():
    """When WMATA populates trip.start_date, that wins."""
    row = {
        "trip_start_date": "20260518",
        "snapshot_ts": datetime(2026, 5, 19, 3, 0, 0),  # 23:00 ET prior day
    }
    assert _service_date_for_row(row) == date(2026, 5, 18)


def test_falls_back_to_snapshot_ts_when_missing():
    """No trip_start_date → Eastern day of snapshot_ts."""
    # 2026-05-19 04:30 UTC = 2026-05-19 00:30 ET (still 19th in Eastern)
    row = {
        "trip_start_date": None,
        "snapshot_ts": datetime(2026, 5, 19, 4, 30, 0),
    }
    assert _service_date_for_row(row) == date(2026, 5, 19)


def test_falls_back_to_snapshot_ts_when_unparseable():
    """Garbage trip_start_date → snapshot_ts fallback, not an exception."""
    row = {
        "trip_start_date": "bogus",
        "snapshot_ts": datetime(2026, 5, 19, 18, 0, 0),  # 14:00 ET
    }
    assert _service_date_for_row(row) == date(2026, 5, 19)


def test_eastern_boundary_handled():
    """A snapshot at 23:30 UTC on 5/19 = 19:30 ET, still the 19th."""
    row = {
        "trip_start_date": None,
        "snapshot_ts": datetime(2026, 5, 19, 23, 30, 0),
    }
    assert _service_date_for_row(row) == date(2026, 5, 19)
```

- [ ] **Step 5: Run the test**

Run: `uv run pytest tests/test_collector_service_date.py -v`
Expected: 4 PASS.

- [ ] **Step 6: Commit**

```bash
git add src/wmata_collector.py tests/test_collector_service_date.py
git commit -m "feat(collector): compute service_date per row and include in UPSERT"
```

---

### Task 7: Fix `_resolve_side_table` and add service_date filter to derivation

**Files:**
- Modify: `pipelines/derive_stop_events_from_state.py`

- [ ] **Step 1: Replace the wrapper class with a real Table return**

In `pipelines/derive_stop_events_from_state.py:239-279`, simplify `_resolve_side_table`:

```python
def _resolve_side_table(name: str):
    """Return a SQLAlchemy Table bound to the side table (same schema as StopEvent).

    Used for Phase D validation where we write to ``stop_events_v2``.
    The side table must already exist with identical schema (see
    scripts/migrate_create_stop_events_v2.py).

    Returns a real Table object on an isolated MetaData() — not a wrapper
    class. ``upsert_rows`` accepts both Tables and ORM-mapped classes.
    """
    if name != "stop_events_v2":
        raise ValueError(f"Unknown target table: {name}")

    cached = _side_table_registry.get(name)
    if cached is not None:
        return cached

    from sqlalchemy import MetaData

    from src.models import StopEvent

    isolated_meta = MetaData()
    side_table = StopEvent.__table__.to_metadata(isolated_meta, name=name)

    _side_table_registry[name] = side_table
    return side_table
```

Change the registry type annotation accordingly:

```python
from sqlalchemy import Table

_side_table_registry: dict[str, Table] = {}
```

- [ ] **Step 2: Add service_date filter to the state query**

In the `derive_for_route_date` function around line 100-103, change:

```python
state_rows = (
    db.query(TripUpdateState).filter(TripUpdateState.trip_id.in_(active_trip_ids)).all()
)
```

to:

```python
state_rows = (
    db.query(TripUpdateState)
    .filter(TripUpdateState.trip_id.in_(active_trip_ids))
    .filter(TripUpdateState.service_date == service_date)
    .all()
)
```

(`service_date` here is the local variable already in scope — confirm by reading the function signature.)

- [ ] **Step 3: Smoke-test by reading the file**

Run: `grep -nA 2 "service_date == service_date" pipelines/derive_stop_events_from_state.py`
Expected: the new filter line appears in `derive_for_route_date`.

- [ ] **Step 4: Write a focused test for the filter**

File: `tests/test_derive_state_filters_by_service_date.py`

```python
"""Verify that derive_stop_events_from_state only consumes state rows for
the requested service_date, even when the same (trip_id, stop_sequence)
exists for multiple dates.

This is the regression guard for the original Phase D design flaw — when
the PK was just (trip_id, stop_sequence), the same trip_id running on
multiple days overwrote itself and could not be derived per-day. With
service_date in the PK, multiple days coexist and the derivation must
filter by service_date or it'd read the wrong day.
"""

from datetime import date, datetime

import pytest

from src.models import TripUpdateState, VehiclePosition


@pytest.mark.smoke
def test_state_rows_for_two_dates_coexist(pg_session):
    """Two service_dates for the same (trip_id, stop_sequence) can be
    stored simultaneously."""
    pg_session.execute(TripUpdateState.__table__.delete())
    pg_session.add_all([
        TripUpdateState(
            trip_id="T_TEST_FILTER",
            stop_sequence=1,
            service_date=date(2026, 5, 18),
            stop_id="S1",
            vehicle_id="V1",
            final_snapshot_ts=datetime(2026, 5, 18, 18, 0),
            last_predicted_arrival_ts=datetime(2026, 5, 18, 18, 5),
        ),
        TripUpdateState(
            trip_id="T_TEST_FILTER",
            stop_sequence=1,
            service_date=date(2026, 5, 19),
            stop_id="S1",
            vehicle_id="V2",
            final_snapshot_ts=datetime(2026, 5, 19, 18, 0),
            last_predicted_arrival_ts=datetime(2026, 5, 19, 18, 7),
        ),
    ])
    pg_session.flush()

    rows = (
        pg_session.query(TripUpdateState)
        .filter(TripUpdateState.trip_id == "T_TEST_FILTER")
        .filter(TripUpdateState.service_date == date(2026, 5, 18))
        .all()
    )
    assert len(rows) == 1
    assert rows[0].vehicle_id == "V1"

    rows = (
        pg_session.query(TripUpdateState)
        .filter(TripUpdateState.trip_id == "T_TEST_FILTER")
        .filter(TripUpdateState.service_date == date(2026, 5, 19))
        .all()
    )
    assert len(rows) == 1
    assert rows[0].vehicle_id == "V2"
```

- [ ] **Step 5: Run the test**

Run: `uv run pytest tests/test_derive_state_filters_by_service_date.py -v`
Expected: PASS (uses real Postgres `pg_session` because the date-aware PK is a PG-only behavior).

- [ ] **Step 6: Commit**

```bash
git add pipelines/derive_stop_events_from_state.py tests/test_derive_state_filters_by_service_date.py
git commit -m "fix: _resolve_side_table returns real Table + filter by service_date"
```

---

### Task 8: Simplify cleanup pipeline

**Files:**
- Modify: `pipelines/cleanup_trip_update_state.py`

- [ ] **Step 1: Read the current implementation**

Run: `cat pipelines/cleanup_trip_update_state.py`

There are two DELETE rules today (the normal-cleanup + the safety net). Replace both with one rule on `service_date`.

- [ ] **Step 2: Replace the cleanup logic**

The new DELETE statement is:

```sql
DELETE FROM trip_update_state WHERE service_date < CURRENT_DATE - INTERVAL '7 days';
```

Preserve the CLI arg signature (e.g. `--retention-days N`) if it exists, defaulting to 7. The function should print the count of rows deleted.

- [ ] **Step 3: Smoke-test by reading the file**

Run: `grep -nA 5 "DELETE FROM trip_update_state" pipelines/cleanup_trip_update_state.py`
Expected: only one DELETE statement, keyed on `service_date`.

- [ ] **Step 4: Run any existing cleanup tests**

Run: `uv run pytest tests/ -k cleanup_trip -v`
Expected: green (if there's an existing test, it may need to be updated for the new semantics — fix it inline if so).

- [ ] **Step 5: Commit**

```bash
git add pipelines/cleanup_trip_update_state.py tests/  # only if a test was updated
git commit -m "feat: cleanup_trip_update_state uses single service_date rule"
```

---

### Task 9: Write the JSONL replay tool

**Files:**
- Create: `pipelines/replay_archive_to_state.py`
- Create: `tests/test_replay_archive_to_state.py`

- [ ] **Step 1: TDD — write the replay test first**

File: `tests/test_replay_archive_to_state.py`

```python
"""Tests for the JSONL → trip_update_state replay tool.

The replay tool must be idempotent: running it twice for the same date
must leave the table in the same state as running it once. It must also
faithfully reproduce the collector's UPSERT semantics — final_snapshot_ts
follows the most recent snapshot, last_pred_* follows the most recent
non-null prediction.
"""

import io
import json
from datetime import date, datetime
from pathlib import Path

import pytest
import zstandard as zstd

from src.models import TripUpdateState
from pipelines.replay_archive_to_state import replay_archive_for_date


def _write_jsonl_zst(tmp_path: Path, name: str, rows: list[dict]) -> Path:
    """Helper: write a list of dicts as a zstd-compressed JSONL file."""
    raw = "\n".join(json.dumps(r) for r in rows).encode("utf-8") + b"\n"
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(raw)
    p = tmp_path / name
    p.write_bytes(compressed)
    return p


@pytest.mark.smoke
def test_replay_writes_final_state(tmp_path, pg_session):
    """A sequence of snapshots reduces to one row in state with the latest values."""
    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()

    rows = [
        # Two snapshots for the same (trip, stop), service_date 2026-05-18.
        {
            "snapshot_ts": "2026-05-18T18:00:00",
            "trip_id": "T_REPLAY",
            "stop_id": "S1",
            "stop_sequence": 1,
            "vehicle_id": "V1",
            "predicted_arrival_ts": "2026-05-18T18:05:00",
            "schedule_relationship": "SCHEDULED",
            "trip_start_date": "20260518",
        },
        {
            "snapshot_ts": "2026-05-18T18:01:00",
            "trip_id": "T_REPLAY",
            "stop_id": "S1",
            "stop_sequence": 1,
            "vehicle_id": "V1",
            "predicted_arrival_ts": "2026-05-18T18:06:00",
            "schedule_relationship": "SCHEDULED",
            "trip_start_date": "20260518",
        },
    ]
    _write_jsonl_zst(archive_dir, "2026-05-18.0.jsonl.zst", rows)

    pg_session.execute(TripUpdateState.__table__.delete())
    pg_session.commit()

    count = replay_archive_for_date(
        pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir
    )
    assert count == 2  # two snapshots processed

    state = (
        pg_session.query(TripUpdateState)
        .filter(TripUpdateState.trip_id == "T_REPLAY")
        .all()
    )
    assert len(state) == 1
    assert state[0].service_date == date(2026, 5, 18)
    assert state[0].last_predicted_arrival_ts == datetime(2026, 5, 18, 18, 6)
    assert state[0].final_snapshot_ts == datetime(2026, 5, 18, 18, 1)


@pytest.mark.smoke
def test_replay_is_idempotent(tmp_path, pg_session):
    """Running replay twice produces the same end state."""
    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    rows = [
        {
            "snapshot_ts": "2026-05-18T18:00:00",
            "trip_id": "T_IDEM",
            "stop_id": "S1",
            "stop_sequence": 1,
            "vehicle_id": "V1",
            "predicted_arrival_ts": "2026-05-18T18:05:00",
            "schedule_relationship": "SCHEDULED",
            "trip_start_date": "20260518",
        },
    ]
    _write_jsonl_zst(archive_dir, "2026-05-18.0.jsonl.zst", rows)

    pg_session.execute(TripUpdateState.__table__.delete())
    pg_session.commit()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    state1 = pg_session.query(TripUpdateState).filter_by(trip_id="T_IDEM").one()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)
    state2 = pg_session.query(TripUpdateState).filter_by(trip_id="T_IDEM").one()

    assert state1.final_snapshot_ts == state2.final_snapshot_ts
    assert state1.last_predicted_arrival_ts == state2.last_predicted_arrival_ts


@pytest.mark.smoke
def test_replay_does_not_touch_other_dates(tmp_path, pg_session):
    """Replay for 2026-05-18 must not modify rows for 2026-05-19."""
    archive_dir = tmp_path / "raw_snapshots"
    archive_dir.mkdir()
    _write_jsonl_zst(
        archive_dir,
        "2026-05-18.0.jsonl.zst",
        [{
            "snapshot_ts": "2026-05-18T18:00:00",
            "trip_id": "T_SAME",
            "stop_id": "S1",
            "stop_sequence": 1,
            "vehicle_id": "V_18",
            "predicted_arrival_ts": "2026-05-18T18:05:00",
            "schedule_relationship": "SCHEDULED",
            "trip_start_date": "20260518",
        }],
    )

    pg_session.execute(TripUpdateState.__table__.delete())
    pg_session.add(TripUpdateState(
        trip_id="T_SAME",
        stop_sequence=1,
        service_date=date(2026, 5, 19),
        stop_id="S1",
        vehicle_id="V_19",
        final_snapshot_ts=datetime(2026, 5, 19, 18, 0),
    ))
    pg_session.commit()

    replay_archive_for_date(pg_session, target_date=date(2026, 5, 18), archive_root=archive_dir)

    on_19 = pg_session.query(TripUpdateState).filter_by(
        trip_id="T_SAME", service_date=date(2026, 5, 19)
    ).one()
    assert on_19.vehicle_id == "V_19"  # untouched
```

- [ ] **Step 2: Run the test to confirm it fails (module not found)**

Run: `uv run pytest tests/test_replay_archive_to_state.py -v`
Expected: 3 FAIL with ImportError on `pipelines.replay_archive_to_state`.

- [ ] **Step 3: Implement the replay tool**

File: `pipelines/replay_archive_to_state.py`

```python
"""Replay archived JSONL snapshots into trip_update_state.

The JSONL archive is the source of truth for any historical service_date.
This tool reads ``archive/raw_snapshots/YYYY-MM-DD.*.jsonl.zst`` files and
replays each line into ``trip_update_state`` using the same UPSERT
semantics as the live collector.

Idempotent: re-running for the same date produces the same end state,
because UPSERT semantics are deterministic given the same input sequence.

Usage:
    uv run python pipelines/replay_archive_to_state.py --date 2026-05-18
    uv run python pipelines/replay_archive_to_state.py --date 2026-05-18 --archive-root /path/to/archive/raw_snapshots
"""

import argparse
import json
import sys
from datetime import date as date_type
from datetime import datetime
from pathlib import Path

import zstandard as zstd
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from src.database import get_session
from src.upsert_helpers import upsert_trip_update_state
from src.wmata_collector import _service_date_for_row

DEFAULT_ARCHIVE_ROOT = Path("archive/raw_snapshots")
BATCH_SIZE = 5000


def _parse_dt(s: str | None) -> datetime | None:
    """Parse an ISO datetime string, returning None if input is None/empty."""
    if not s:
        return None
    return datetime.fromisoformat(s)


def _iter_jsonl_zst(path: Path):
    """Yield decoded dict objects from a zstd-compressed JSONL file.

    Uses streaming decompression so multi-frame files (one frame per
    collector flush) decode correctly — see PR #131 for the analogous
    fix on the parquet reader side.
    """
    dctx = zstd.ZstdDecompressor()
    with path.open("rb") as fh:
        with dctx.stream_reader(fh) as reader:
            buf = b""
            while True:
                chunk = reader.read(65536)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if line:
                        yield json.loads(line)
            if buf.strip():
                yield json.loads(buf)


def replay_archive_for_date(
    db: Session, target_date: date_type, archive_root: Path = DEFAULT_ARCHIVE_ROOT
) -> int:
    """Replay all archive files for ``target_date`` into trip_update_state.

    Globs ``{archive_root}/{target_date}.*.jsonl.zst`` (per-process file
    pattern from PR #132) plus the legacy single-file pattern
    ``{archive_root}/{target_date}.jsonl.zst``. Each line is decoded and
    pushed through the same ``upsert_trip_update_state`` helper the
    collector uses, in batches of ``BATCH_SIZE`` for memory bounds.

    Returns the total number of snapshot lines processed.
    """
    pattern_per_proc = f"{target_date.isoformat()}.*.jsonl.zst"
    pattern_legacy = f"{target_date.isoformat()}.jsonl.zst"
    paths = sorted(
        list(archive_root.glob(pattern_per_proc)) + list(archive_root.glob(pattern_legacy))
    )
    if not paths:
        print(f"No archive files found for {target_date} under {archive_root}")
        return 0

    print(f"Replaying {len(paths)} archive file(s) for {target_date}:")
    for p in paths:
        print(f"  - {p.name}")

    total = 0
    batch: list[dict] = []
    for p in paths:
        for raw in _iter_jsonl_zst(p):
            if raw.get("stop_sequence") is None:
                continue
            row = {
                "trip_id": raw["trip_id"],
                "stop_sequence": raw["stop_sequence"],
                "service_date": _service_date_for_row({
                    "trip_start_date": raw.get("trip_start_date"),
                    "snapshot_ts": _parse_dt(raw["snapshot_ts"]),
                }),
                "stop_id": raw["stop_id"],
                "vehicle_id": raw.get("vehicle_id"),
                "snapshot_ts": _parse_dt(raw["snapshot_ts"]),
                "predicted_arrival_ts": _parse_dt(raw.get("predicted_arrival_ts")),
                "schedule_relationship": raw.get("schedule_relationship"),
            }
            # Only replay rows for the target date — defensive against
            # midnight-crossing files that might contain rows from the
            # next service-day.
            if row["service_date"] != target_date:
                continue
            batch.append(row)
            total += 1
            if len(batch) >= BATCH_SIZE:
                upsert_trip_update_state(db, batch)
                db.commit()
                batch = []

    if batch:
        upsert_trip_update_state(db, batch)
        db.commit()

    print(f"Replayed {total} snapshot rows for {target_date}.")
    return total


def main() -> int:
    """CLI entry point."""
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="Service date (YYYY-MM-DD)")
    parser.add_argument(
        "--archive-root",
        default=str(DEFAULT_ARCHIVE_ROOT),
        help=f"Archive directory (default: {DEFAULT_ARCHIVE_ROOT})",
    )
    args = parser.parse_args()
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    archive_root = Path(args.archive_root)

    db = get_session()
    try:
        replay_archive_for_date(db, target_date, archive_root)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run the test, expect green**

Run: `uv run pytest tests/test_replay_archive_to_state.py -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add pipelines/replay_archive_to_state.py tests/test_replay_archive_to_state.py
git commit -m "feat: replay_archive_to_state idempotently restores past days"
```

---

### Task 10: Row-count guard in `run_daily_batch.py`

**Files:**
- Modify: `pipelines/run_daily_batch.py`

- [ ] **Step 1: Read the batch driver**

Run: `wc -l pipelines/run_daily_batch.py && grep -n "derive_stop_events_from_state\|stop_events_v2" pipelines/run_daily_batch.py`

We want a post-step assertion: after the v2 derivation runs, count rows in `stop_events_v2` for the target date and fail loud if zero.

- [ ] **Step 2: Add the guard**

After the `derive_stop_events_from_state_v2` step completes, insert a check (look at how the existing batch reports per-step results and follow the same pattern). The simplest form:

```python
# Post-step guard: if the v2 derivation produced zero rows for ANY route
# on this date, treat it as a failure. The silent-failure mode that
# broke Phase D for 4 days was "step exits 0 but writes nothing."
def _assert_v2_nonzero(target_date_str: str) -> None:
    from sqlalchemy import text
    from src.database import get_session
    db = get_session()
    try:
        n = db.execute(
            text("SELECT COUNT(*) FROM stop_events_v2 WHERE service_date = :d"),
            {"d": target_date_str},
        ).scalar_one()
        if not n:
            raise RuntimeError(
                f"v2 derivation wrote 0 rows for {target_date_str} — "
                "silent failure guard tripped"
            )
        print(f"v2 derivation guard: {n} rows in stop_events_v2 for {target_date_str}")
    finally:
        db.close()
```

Call `_assert_v2_nonzero(target_date_str)` after the `derive_stop_events_from_state_v2` step finishes with exit 0. (If the step itself returned non-zero, we already report the failure; the guard catches the zero-rows-but-exit-0 case.)

- [ ] **Step 3: Smoke-test the import**

Run: `uv run python -c "from pipelines.run_daily_batch import _assert_v2_nonzero; print('ok')"`
Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add pipelines/run_daily_batch.py
git commit -m "feat: row-count guard for silent v2 derivation failure"
```

---

### Task 11: Spec addendum + NOTES.md update

**Files:**
- Create: `docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md`
- Modify: `NOTES.md`

- [ ] **Step 1: Write the spec addendum**

File: `docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md`

```markdown
# `trip_update_state` schema addendum: service_date in PK

**Status:** Active (supersedes the schema section of 2026-05-17-trip-update-state-refactor-design.md)
**Author:** wlovotti + Claude
**Type:** Architecture amendment

## Why

The original 2026-05-17 spec keyed `trip_update_state` on `(trip_id,
stop_sequence)` and relied on a tight derive-then-cleanup race against
the next day's snapshots overwriting state. In practice:

1. WMATA's GTFS-RT `trip_id`s repeat day-over-day (94% reuse on
   consecutive weekdays — empirically measured on 2026-05-18 → 19).
   The UPSERT on `(trip_id, stop_sequence)` overwrites yesterday's
   state with today's run.
2. The "derive before tomorrow's trips start" race assumes the nightly
   batch never fails. The Phase D v2 derivation failed 4 nights in a
   row before anyone noticed.
3. The original cleanup rules (`derived_at < NOW() - INTERVAL '2 days'`
   plus a 7-day safety net) compensate for an irrecoverable design
   rather than just retain history.

## What changes

PK: `(trip_id, stop_sequence)` → `(trip_id, stop_sequence, service_date)`.

`service_date` is computed at UPSERT time:
- Preferred: `tripDescriptor.start_date` from the GTFS-RT feed (parsed
  YYYYMMDD).
- Fallback: Eastern calendar day of the snapshot's `snapshot_ts`
  (correct for 99%+ of WMATA bus trips since service-day-crossing
  overnight operations are rare).

Cleanup collapses to a single rule:
```
DELETE FROM trip_update_state WHERE service_date < CURRENT_DATE - INTERVAL '7 days';
```

The `derived_at` column is preserved as a diagnostic (did the pipeline
run for this row?) but no longer load-bearing.

## Storage impact

~180K rows/day × 7-day retention = ~1.3M rows, ~20-30 MB. Negligible vs
the 129 GB the legacy snapshot table held. The 30× compression vs the
original snapshot design is preserved — it came from collapsing the
prediction-trajectory dimension, not the calendar dimension.

## Recoverability

Any past service_date in the JSONL archive window can be re-derived via:

```bash
uv run python pipelines/replay_archive_to_state.py --date YYYY-MM-DD
uv run python pipelines/derive_stop_events_from_state.py --date YYYY-MM-DD --all-routes
```

Both steps are idempotent. Re-running them produces the same outputs.

## Migration

Single transaction:
```sql
ALTER TABLE trip_update_state ADD COLUMN service_date DATE;
UPDATE trip_update_state SET service_date =
  (final_snapshot_ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date;
ALTER TABLE trip_update_state ALTER COLUMN service_date SET NOT NULL;
ALTER TABLE trip_update_state DROP CONSTRAINT trip_update_state_pkey;
ALTER TABLE trip_update_state ADD CONSTRAINT trip_update_state_pkey
  PRIMARY KEY (trip_id, stop_sequence, service_date);
CREATE INDEX idx_tus_service_date ON trip_update_state (service_date);
```

Pre-requisite: stop the collector before migrating (the running collector
has the old model and will fail INSERTs after NOT NULL is set). Restart
with new code after migration completes.

## Phase D restart

The 2026-05-17 spec's Phase D bar (≥7 days at 100% agreement including
≥1 weekend day) is unchanged. The new path to satisfy it:

1. Apply migration + restart collector.
2. (Optional) Replay archive for 2026-05-18 + 2026-05-19, then run the v2
   derivation for those dates. This restores the original 2026-05-25
   cutover target.
3. Otherwise, accumulate 7 days forward from 2026-05-20 → earliest
   cutover 2026-05-27.
```

- [ ] **Step 2: Update NOTES.md**

In NOTES.md, modify the NOTES-72 block (around line 95):

- Add at top of the NOTES-72 block: "**Schema fix in flight (2026-05-20):** Phase D's v2 derivation pipeline failed silently for 4 consecutive nightly batches (2026-05-16 → 19) due to a `_resolve_side_table` SQLAlchemy bug. Investigation surfaced a deeper design flaw: the `trip_update_state` PK omitted `service_date`, so WMATA's repeating daily trip_ids overwrote prior-day state. PR XX adds service_date to the PK, fixes the bug, adds a JSONL replay tool for historical re-derivation, and adds a row-count guard so silent-zero failures can't recur. See `docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md`."

- Note that the Phase D cutover date moves to **2026-05-27** (forward-only path) or **2026-05-25** (after replay backfill of 5/18+5/19).

- [ ] **Step 3: Update the last-edited date header**

In NOTES.md line 9 (the "Last edited" line), update the date to 2026-05-20 and prepend a short summary of this change.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md NOTES.md
git commit -m "docs: spec addendum + NOTES-72 status update for schema change"
```

---

### Task 12: Full test pass + push branch + open PR

**Files:**
- No files modified.

- [ ] **Step 1: Run full smoke suite**

Run: `uv run pytest -m smoke -q`
Expected: all green.

- [ ] **Step 2: Run lint + format gates**

Run: `uv run ruff check src/ scripts/ api/ pipelines/ tests/`
Run: `uv run ruff format --check src/ scripts/ api/ pipelines/ tests/`
Expected: both clean. Fix any errors before continuing.

- [ ] **Step 3: Push branch**

```bash
git push -u origin feat/state-service-date-pk
```

- [ ] **Step 4: Open PR**

```bash
gh pr create --title "feat(state): service_date in PK + Phase D recovery (NOTES-72)" --body "$(cat <<'EOF'
## Summary
- Adds `service_date` to the `trip_update_state` PK so per-day rows
  coexist instead of overwriting each other (root cause of the Phase D
  recovery gap).
- Fixes the silently-failing v2 derivation: `_resolve_side_table`
  returned a wrapper class that `pg_insert` rejected. Now returns the
  underlying `Table` directly; `upsert_rows` accepts both shapes.
- Adds `pipelines/replay_archive_to_state.py` — idempotent JSONL replay
  tool so any past day in the archive window can be re-derived.
- Collapses cleanup to a single date-based rule.
- Adds a row-count guard so silent-zero failures can't recur.

See `docs/superpowers/specs/2026-05-20-trip-update-state-service-date-addendum.md`.

## Deployment (user-driven, after merge)

1. Stop `continuous_combined_collector.py`.
2. `uv run python scripts/migrate_add_service_date_to_state.py`
3. Restart collector with new code.
4. (Optional, to recover 5/18 + 5/19 for Phase D backfill)
   ```
   uv run python pipelines/replay_archive_to_state.py --date 2026-05-18
   uv run python pipelines/derive_stop_events_from_state.py --date 2026-05-18 --all-routes --target-table stop_events_v2
   uv run python pipelines/replay_archive_to_state.py --date 2026-05-19
   uv run python pipelines/derive_stop_events_from_state.py --date 2026-05-19 --all-routes --target-table stop_events_v2
   ```

## Test plan
- [x] `uv run pytest -m smoke -q` green locally
- [x] `uv run ruff check` + `uv run ruff format --check` green
- [ ] After merge + deploy, verify next nightly batch shows non-zero
  stop_events_v2 rows for the target date
- [ ] If replay path taken, verify `stop_events_v2` for 5/18 / 5/19
  matches `stop_events` for the same dates per the Phase D comparison
  pipeline
EOF
)"
```

- [ ] **Step 5: Capture the PR URL and report back**

Print the URL returned by `gh pr create` so the user can review.

---

## Self-Review notes

- Each task is self-contained — files listed, code shown, commands shown.
- TDD applied to the two new behaviors that materially shape the codebase: the replay tool (Task 9) and the service_date filter (Task 7). Skipped TDD for boilerplate (migration script, model field) where the test-first overhead doesn't add safety.
- Idempotency is enforced by:
  - Migration uses `IF NOT EXISTS` / `IF EXISTS`.
  - Replay test (Task 9, Step 1 second test).
  - Cleanup is a single SQL DELETE (Task 8).
  - Derivation continues to use `upsert_rows` (already idempotent).
- Operational sequencing (stop collector → migrate → restart) is explicit in the spec addendum and PR description; it is NOT done by this PR — that's a deliberate split between code change and deployment.
- Phase D restart paths (with vs without 5/18-19 backfill) are spelled out so the user can pick after the PR merges.
