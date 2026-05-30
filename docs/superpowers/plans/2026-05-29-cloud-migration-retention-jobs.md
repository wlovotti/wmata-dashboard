# Cloud Migration Phase 1 — Retention Jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Amendments (recorded after this plan was executed — the shipped code in
> `pipelines/archive_vehicle_positions.py` is authoritative over the snippets below):**
> 1. **`archive_day_to_parquet` must NOT use `conn.execution_options(stream_results=True, ...)`.**
>    `Connection.execution_options()` mutates the Connection *in place* (returns `self`)
>    in SQLAlchemy 2.0, so the option persists and breaks the subsequent `DELETE` /
>    `ROLLBACK TO SAVEPOINT` on the same connection. The shipped version uses a psycopg2
>    named server-side cursor on `conn.connection` (the raw DBAPI connection) — matching
>    `archive_trip_update_snapshots.py`, still bounded-memory, still inside the caller's
>    transaction (so the savepoint test cleanup works). Use that pattern, not Task 3 Step 2's snippet.
> 2. **The orchestrator's per-day upload `except` is `except Exception`, not `except RuntimeError`.**
>    A botocore `ClientError` (auth, throttling, missing bucket, network) must degrade that
>    day to "failed, continue" — never crash the whole run. The narrow `RuntimeError` catch
>    in Task 4 Step 3's snippet would let those escape.
> 3. **Built as a single commit, not split Task 2/3/4.** Splitting left `argparse`/`sys`/
>    `load_dotenv`/`get_engine` imported-but-unused (F401) until the orchestrator landed.
>    The whole tier-3 module + tests ship in one commit so every import is used at commit time.
> 4. **Extra tests added beyond the snippets:** an empty-DB tier-2 test (documents the
>    `rowcount or 0` guard) and a multi-day tier-3 orchestrator test (day 1 succeeds, day 2
>    raises a non-`RuntimeError` → proves per-day-commit independence AND the broadened catch).

**Goal:** Build the two net-new, locally-testable retention jobs from the NOTES-48 cloud-migration spec (§3.5): a tier-3 job that archives aged `vehicle_positions` to S3 parquet then deletes them, and a tier-2 job that windows `stop_events`/`runs` to a 365-day rolling window.

**Architecture:** Two standalone pipeline CLIs (`pipelines/`), siblings to the existing `cleanup_trip_update_state.py` and `archive_trip_update_snapshots.py`. Each is a pure function + a thin `main()`. The tier-2 job is pure-ORM (`delete(Model).where(...)`) and runs on SQLite for fast smoke tests. The tier-3 job streams rows server-side to a local zstd parquet, uploads to AWS S3, verifies, then deletes — Postgres-only, so its DB-touching tests are integration tests scoped to a sentinel far-past day inside the `pg_session` savepoint so they never contaminate a populated dev DB. They are deployed later as nightly systemd timers on the VM (spec §5 — out of scope for this plan).

**Tech Stack:** Python 3.12, SQLAlchemy 2.x, psycopg2, pyarrow (parquet + zstd), boto3 (S3), pytest. All four libraries are already declared in `pyproject.toml` (`pyarrow>=24.0.0`, `boto3>=1.43.9`, `zstandard>=0.25.0`, `psycopg2-binary>=2.9.0`) — no dependency changes.

---

## Context the engineer needs (read before starting)

You are implementing against `feature/notes-48-cloud-migration-phase1`, already rebased onto `main` (it contains PR #152 + #153). Key facts, each load-bearing:

1. **PR #152 pruned the `VehiclePosition` model to 14 persisted columns.** The archive must carry exactly those 14 — `id, vehicle_id, route_id, trip_id, latitude, longitude, speed, current_stop_sequence, stop_id, current_status, direction_id, trip_start_date, timestamp, collected_at` — and must NOT reference the 5 dropped fields (`vehicle_label, bearing, trip_start_time, schedule_relationship, occupancy_status`). They still exist physically in the dev DB (non-destructive prune), but the model and any new code treat them as gone.

2. **Datetime storage is naive UTC** (CLAUDE.md). `vehicle_positions.timestamp` is naive UTC. Use `src.timezones.utcnow_naive()` for the tier-3 cutoff and `DATE(timestamp)` for UTC-date partitioning. Never `datetime.now()`/`datetime.utcnow()`.

3. **`stop_events.service_date` and `runs.service_date` are `String` columns** in `YYYY-MM-DD` form — NOT `Date` (unlike `trip_update_state.service_date`). ISO `YYYY-MM-DD` strings sort lexicographically == chronologically, so compare against an `.isoformat()` **string** cutoff. This keeps the tier-2 DELETE pure-ORM and portable to SQLite. Use `src.timezones.eastern_today()` for the cutoff (service_date is an Eastern operational day).

4. **`pg_session` defaults to the LIVE dev DB** (`postgresql:///wmata_dashboard`) unless `PG_TEST_DATABASE_URL` is set (conftest.py). Its savepoint rollback only protects writes made *through the session's own connection*. Therefore: every DB-touching tier-3 test reads/writes through `pg_session.connection()` (one shared transaction) and uses a **sentinel far-past day (2020-01-01)** with no real rows. Never open a second connection (`engine.raw_connection()` / `engine.connect()`) against inserted-but-uncommitted rows — it can't see them, and any commit it makes escapes the savepoint and pollutes the dev DB.

5. **S3 is AWS S3, not B2** (spec §3.4). The existing `rotate_archive.py` uploads to Backblaze B2 via `endpoint_url` + `B2_*` env vars — that flow is separate and unchanged. The new tier-3 job uses the standard boto3 AWS credential chain and `S3_ARCHIVE_BUCKET`, with an optional `S3_ENDPOINT_URL` override for local/S3-compatible testing.

6. **Tests mock S3 with a `MagicMock`** patched in via `patch.object(module, "_make_s3_client", ...)` — the exact pattern in `tests/test_rotate_archive.py`. No `moto`, no real AWS.

7. **CLAUDE.md conventions:** docstrings on every function; run `ruff check` AND `ruff format --check` over `src/ scripts/ api/ pipelines/ tests/` before committing (two separate CI gates); every commit message ends with the trailer `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. The plan's `git commit -m` steps omit the trailer for brevity — add it.

8. **Do NOT run the real jobs against the dev DB.** The dev DB has a long >30-day tail (data back to 2025-10-12), so running the real tier-3 30-day archiver would try to archive months of real data. Tests use the sentinel day only. The PR body documents the operational run for the user (memory: don't ask subagents to run heavy backfills).

---

## File Structure

| File | Responsibility |
|---|---|
| `pipelines/window_derived_tables.py` (create) | Tier-2: delete `stop_events`/`runs` rows older than the 365-day window. Pure-ORM. |
| `tests/test_window_derived_tables.py` (create) | Tier-2 tests — SQLite `db_session`, `@pytest.mark.smoke`. |
| `pipelines/archive_vehicle_positions.py` (create) | Tier-3: stage day→parquet, upload to S3, verify, delete, vacuum. |
| `tests/test_archive_vehicle_positions.py` (create) | Tier-3 tests — smoke (helpers + fake-S3 + mocked orchestrator) and `@pytest.mark.integration` (pg sentinel-day round-trip). |

Two independent jobs in two files; tests alongside. No existing files are modified (systemd wiring and `run_daily_batch` integration are the spec §5 runbook, out of scope).

---

## Task 1: Tier-2 windowing job (`window_derived_tables.py`)

Simplest first — pure-ORM, SQLite-testable. Mirrors `pipelines/cleanup_trip_update_state.py`.

**Files:**
- Create: `pipelines/window_derived_tables.py`
- Test: `tests/test_window_derived_tables.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_window_derived_tables.py`:

```python
"""Tests for pipelines.window_derived_tables (tier-2 365-day windowing, NOTES-48).

`stop_events.service_date` / `runs.service_date` are STRING ("YYYY-MM-DD")
columns, so the windowing DELETE is pure-ORM and runs on SQLite — these are
fast smoke tests on the in-memory `db_session` (fresh and empty per test, so
exact-count assertions are safe).
"""

from datetime import timedelta

import pytest

from src.models import Run, StopEvent
from src.timezones import eastern_today


def _stop_event(service_date: str, trip_id: str) -> StopEvent:
    """Build a minimal StopEvent (only NOT NULL columns) for windowing tests."""
    return StopEvent(
        service_date=service_date,
        trip_id=trip_id,
        route_id="R1",
        direction_id=0,
        stop_id="S1",
        stop_sequence=1,
        source="trip_update",
    )


def _run(service_date: str, trip_id: str) -> Run:
    """Build a minimal Run (only NOT NULL columns) for windowing tests."""
    return Run(
        service_date=service_date,
        trip_id=trip_id,
        route_id="R1",
        direction_id=0,
        source="trip_update",
    )


@pytest.mark.smoke
def test_window_deletes_rows_older_than_365_days(db_session):
    """stop_events/runs older than the cutoff are deleted; newer rows stay."""
    from pipelines.window_derived_tables import run_window

    today = eastern_today()
    old = (today - timedelta(days=400)).isoformat()
    recent = (today - timedelta(days=10)).isoformat()
    db_session.add_all(
        [
            _stop_event(old, "T_old"),
            _stop_event(recent, "T_recent"),
            _run(old, "T_old"),
            _run(recent, "T_recent"),
        ]
    )
    db_session.commit()

    counts = run_window(db_session)
    db_session.commit()

    assert counts == {"stop_events": 1, "runs": 1}
    assert {r.service_date for r in db_session.query(StopEvent).all()} == {recent}
    assert {r.service_date for r in db_session.query(Run).all()} == {recent}


@pytest.mark.smoke
def test_window_respects_explicit_retention_days(db_session):
    """A tighter window deletes more rows."""
    from pipelines.window_derived_tables import run_window

    today = eastern_today()
    d100 = (today - timedelta(days=100)).isoformat()
    d10 = (today - timedelta(days=10)).isoformat()
    db_session.add_all([_stop_event(d100, "T100"), _stop_event(d10, "T10")])
    db_session.commit()

    counts = run_window(db_session, retention_days=30)
    db_session.commit()

    assert counts["stop_events"] == 1
    assert {r.service_date for r in db_session.query(StopEvent).all()} == {d10}


@pytest.mark.smoke
def test_window_boundary_is_exclusive(db_session):
    """A row exactly on the cutoff date is KEPT — the cutoff is a strict `<`."""
    from pipelines.window_derived_tables import compute_cutoff_str, run_window

    cutoff = compute_cutoff_str(365)
    db_session.add(_stop_event(cutoff, "T_boundary"))
    db_session.commit()

    run_window(db_session)
    db_session.commit()

    assert {r.trip_id for r in db_session.query(StopEvent).all()} == {"T_boundary"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_window_derived_tables.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipelines.window_derived_tables'`.

- [ ] **Step 3: Write the implementation**

Create `pipelines/window_derived_tables.py`:

```python
"""Tier-2 retention: 365-day rolling window for `stop_events` and `runs` (NOTES-48 §3.5).

These are the granular *derived* tables. Every metric is computed from them and
the answer lands in a tiny tier-1 rollup, so the granular rows are intermediate,
not authoritative — and they are recoverable by re-derivation from the raw
archives (`replay_archive_to_state.py` rebuilds `trip_update_state`; the tier-3
positions parquet restores `vehicle_positions`; `derive_stop_events*` then
regenerates them). A nightly job deletes rows whose Eastern `service_date` is
older than a 365-day window — generous on purpose during active metric
development; tighten toward 90 days once the metric set stabilizes.

Why a STRING comparison: `stop_events.service_date` and `runs.service_date` are
`String` columns in `YYYY-MM-DD` form (NOT `Date`, unlike `trip_update_state`).
Zero-padded ISO dates sort lexicographically == chronologically, so comparing
against `(eastern_today() - retention_days).isoformat()` is correct and keeps
the DELETE pure-ORM (and SQLite-portable for tests).

Usage:
    uv run python pipelines/window_derived_tables.py
    uv run python pipelines/window_derived_tables.py --retention-days 365
    uv run python pipelines/window_derived_tables.py --dry-run
"""

import argparse
import sys
from datetime import timedelta

from dotenv import load_dotenv
from sqlalchemy import delete
from sqlalchemy.orm import Session

from src.database import get_session
from src.models import Run, StopEvent
from src.timezones import eastern_today

# Tables windowed by this job. Both carry a STRING `service_date`. There is no
# FK between them, so deletion order is not load-bearing — listed stop_events
# first only for log readability.
WINDOWED_MODELS = (StopEvent, Run)

DEFAULT_RETENTION_DAYS = 365


def compute_cutoff_str(retention_days: int = DEFAULT_RETENTION_DAYS) -> str:
    """Return the inclusive cutoff as an ISO `YYYY-MM-DD` string.

    Rows with `service_date < cutoff` are expired. The cutoff is
    `eastern_today() - retention_days`, formatted via `date.isoformat()` so it
    compares correctly against the string `service_date` columns.
    """
    return (eastern_today() - timedelta(days=retention_days)).isoformat()


def run_window(db: Session, retention_days: int = DEFAULT_RETENTION_DAYS) -> dict[str, int]:
    """Delete `stop_events` / `runs` rows older than the retention window.

    Args:
        db: Active SQLAlchemy session. The caller is responsible for committing
            or rolling back after this returns.
        retention_days: Eastern days of granular derived data to retain.
            Default 365 (spec §3.5).

    Returns:
        Per-table deleted-row counts keyed by table name, e.g.
        ``{"stop_events": 1234, "runs": 56}``.
    """
    cutoff = compute_cutoff_str(retention_days)
    deleted: dict[str, int] = {}
    for model in WINDOWED_MODELS:
        result = db.execute(delete(model).where(model.service_date < cutoff))
        deleted[model.__tablename__] = result.rowcount or 0
    return deleted


def main() -> int:
    """CLI entry point: parse args, open a session, run the window, commit."""
    parser = argparse.ArgumentParser(
        description="Delete stop_events/runs rows older than the 365-day retention window."
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Eastern days of granular derived data to retain (default: {DEFAULT_RETENTION_DAYS}).",
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
        counts = run_window(db, retention_days=args.retention_days)
        if args.dry_run:
            db.rollback()
            print(f"DRY-RUN: would delete {counts}")
        else:
            db.commit()
            print(f"Window cleanup complete: {counts}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_window_derived_tables.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Lint and format**

Run: `uv run ruff check pipelines/window_derived_tables.py tests/test_window_derived_tables.py && uv run ruff format --check pipelines/window_derived_tables.py tests/test_window_derived_tables.py`
Expected: clean. If format fails, run `uv run ruff format pipelines/window_derived_tables.py tests/test_window_derived_tables.py` and re-check.

- [ ] **Step 6: Commit**

```bash
git add pipelines/window_derived_tables.py tests/test_window_derived_tables.py
git commit -m "feat: tier-2 365-day windowing for stop_events/runs (NOTES-48)"
```

---

## Task 2: Tier-3 pure helpers — schema, cutoff, parquet verify, S3 upload

Build the non-DB pieces of `archive_vehicle_positions.py` first, with fast smoke tests (no Postgres). Mirrors `archive_trip_update_snapshots.py` (parquet schema/verify) and `rotate_archive.py` (S3 client/upload-verify).

**Files:**
- Create: `pipelines/archive_vehicle_positions.py`
- Test: `tests/test_archive_vehicle_positions.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_archive_vehicle_positions.py`:

```python
"""Tests for pipelines.archive_vehicle_positions (tier-3 S3 archival, NOTES-48 §3.5).

Three layers:
  * smoke — pure helpers (cutoff, parquet round-trip verify) and the S3
    upload/verify with a MagicMock S3 client (no real AWS, no DB).
  * smoke — orchestrator control-flow with every I/O function monkeypatched,
    asserting the safety invariant: rows are never DELETEd unless archive +
    verify + S3 upload all succeeded.
  * integration — the Postgres streaming round-trip on a SENTINEL far-past day
    (2020-01-01) read/written through `pg_session.connection()` so the
    savepoint rollback cleans up and the live dev DB is never touched.
"""

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.models import VehiclePosition
from src.timezones import utcnow_naive


# --------------------------------------------------------------------------
# Pure helpers (smoke)
# --------------------------------------------------------------------------


@pytest.mark.smoke
def test_compute_cutoff_is_n_days_before_now():
    """compute_cutoff(30) is ~30 days before the naive-UTC now."""
    from pipelines.archive_vehicle_positions import compute_cutoff

    delta = utcnow_naive() - compute_cutoff(30)
    assert timedelta(days=29, hours=23) < delta < timedelta(days=30, hours=1)


@pytest.mark.smoke
def test_verify_parquet_roundtrip(tmp_path):
    """verify_parquet reports (True, n) when the row count matches, (False, n) otherwise."""
    from pipelines.archive_vehicle_positions import ARCHIVE_SCHEMA, verify_parquet

    path = tmp_path / "x.parquet"
    table = pa.table(
        {f.name: pa.array([None, None], type=f.type) for f in ARCHIVE_SCHEMA},
        schema=ARCHIVE_SCHEMA,
    )
    pq.write_table(table, path, compression="zstd")

    assert verify_parquet(path, 2) == (True, 2)
    assert verify_parquet(path, 5) == (False, 2)


@pytest.mark.smoke
def test_upload_and_verify_success(tmp_path):
    """upload_and_verify uploads then passes when remote size matches local."""
    from pipelines.archive_vehicle_positions import upload_and_verify

    path = tmp_path / "x.parquet"
    path.write_bytes(b"hello-parquet")
    fake_s3 = MagicMock()
    fake_s3.head_object.return_value = {"ContentLength": path.stat().st_size}

    upload_and_verify(fake_s3, path, "bucket", "vehicle_positions/2020-01-01.parquet")

    fake_s3.upload_file.assert_called_once_with(
        str(path), "bucket", "vehicle_positions/2020-01-01.parquet"
    )


@pytest.mark.smoke
def test_upload_and_verify_size_mismatch_raises(tmp_path):
    """A remote/local size mismatch raises — the caller must then skip the DELETE."""
    from pipelines.archive_vehicle_positions import upload_and_verify

    path = tmp_path / "x.parquet"
    path.write_bytes(b"hello-parquet")
    fake_s3 = MagicMock()
    fake_s3.head_object.return_value = {"ContentLength": 1}  # wrong

    with pytest.raises(RuntimeError, match="size mismatch"):
        upload_and_verify(fake_s3, path, "bucket", "k")


# --------------------------------------------------------------------------
# Integration round-trip (Postgres, sentinel day, savepoint-safe)
# --------------------------------------------------------------------------


def _vp(ts: datetime, vehicle_id: str, seq: int) -> VehiclePosition:
    """Build a minimal VehiclePosition (only NOT NULL columns) for archival tests."""
    return VehiclePosition(
        vehicle_id=vehicle_id,
        route_id="R1",
        trip_id=f"T{seq}",
        latitude=38.9,
        longitude=-77.0,
        timestamp=ts,
    )


@pytest.mark.integration
def test_archive_then_delete_day_roundtrip(pg_session, tmp_path):
    """archive_day_to_parquet → verify_parquet → delete_day on a sentinel day.

    All DB work goes through `pg_session.connection()` (one transaction) on a
    far-past day with no real rows, so the savepoint rollback undoes everything
    and a populated dev DB is never affected.
    """
    from pipelines.archive_vehicle_positions import (
        archive_day_to_parquet,
        count_rows_for_date,
        delete_day,
        verify_parquet,
    )

    conn = pg_session.connection()
    day = date(2020, 1, 1)
    pg_session.add_all([_vp(datetime(2020, 1, 1, 12, 0, i), "BUS_SENTINEL", i) for i in range(3)])
    pg_session.flush()  # make the rows visible to `conn` within the same txn

    assert count_rows_for_date(conn, day) == 3

    path = tmp_path / "2020-01-01.parquet"
    written = archive_day_to_parquet(conn, day, path)
    assert written == 3
    assert verify_parquet(path, 3) == (True, 3)

    assert delete_day(conn, day) == 3
    assert count_rows_for_date(conn, day) == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_archive_vehicle_positions.py -v -m smoke`
Expected: FAIL — `ModuleNotFoundError: No module named 'pipelines.archive_vehicle_positions'`.

- [ ] **Step 3: Write the module skeleton with the helpers under test**

Create `pipelines/archive_vehicle_positions.py` with the imports, constants, and the pure + S3 helpers (the DB-streaming functions land in Task 3, the orchestrator in Task 4):

```python
"""Tier-3 retention: archive aged `vehicle_positions` to S3 parquet, then DELETE (NOTES-48 §3.5).

`vehicle_positions` is the raw GPS feed and the project's largest table. It is
the only tier with a SHORT in-DB window (30 days) because its single live
consumer — the nightly proximity derivation (`pipelines/derive_stop_events.py`)
— runs within ~1 day of collection. Older rows live only as compressed parquet
in S3 and are reloaded only for re-derivation.

Flow, per UTC date older than the cutoff (oldest first):
  1. Stage the day's rows to a local zstd parquet via a server-side cursor
     (bounded memory — the streaming pattern from PR #131 / the
     trip_update_snapshots archiver; never polars row-by-row inference).
  2. Verify the parquet row count == the queried count.
  3. Upload to S3 and verify the uploaded object size == local size.
  4. ONLY THEN DELETE the day's rows from Postgres.
  5. Delete the local staging parquet.
After all days, run a regular (non-FULL) VACUUM so the collector's concurrent
writes are never blocked.

Columns: only the 14 the pruned model persists (PR #152). The five dropped
GTFS-RT fields are intentionally absent from the archive.

S3, not B2: spec §3.4 chose AWS S3 for one-bill/one-console consistency. This
job uses the standard boto3 AWS credential chain (env / shared config /
instance profile) and `S3_ARCHIVE_BUCKET`. `S3_ENDPOINT_URL`, if set, overrides
the endpoint (point at a local S3-compatible store in development); unset means
real AWS. The existing `rotate_archive.py` B2 flow is separate and unchanged.

Partitioning is by UTC date (`DATE(timestamp)`), matching the
trip_update_snapshots archiver. An Eastern service day therefore spans two
UTC-date parquet files; re-derivation reads both — an accepted trade for
pattern consistency.

Usage:
    uv run python pipelines/archive_vehicle_positions.py --dry-run
    uv run python pipelines/archive_vehicle_positions.py
    uv run python pipelines/archive_vehicle_positions.py --retention-days 30
"""

import argparse
import os
import sys
from datetime import date as date_type
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.database import get_engine
from src.timezones import utcnow_naive

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_STAGING_DIR = REPO_ROOT / "archive" / "vehicle_positions"
TABLE_NAME = "vehicle_positions"
TS_COLUMN = "timestamp"
KEY_PREFIX = "vehicle_positions"

# The 14 columns the pruned VehiclePosition model persists (PR #152). The five
# dropped GTFS-RT fields are intentionally excluded.
ARCHIVE_COLUMNS = [
    "id",
    "vehicle_id",
    "route_id",
    "trip_id",
    "latitude",
    "longitude",
    "speed",
    "current_stop_sequence",
    "stop_id",
    "current_status",
    "direction_id",
    "trip_start_date",
    "timestamp",
    "collected_at",
]

# Explicit pyarrow schema mirroring the Postgres column types. Declared, not
# inferred, to avoid the polars row-by-row inference failure (NULL columns
# reconciled against later non-null values) that motivated PR #131. `id` is
# int64: at ~900K rows/day the int32 ceiling (~2.1B) would be reached in ~6
# years, so int64 is the safe choice for an append-only archive.
ARCHIVE_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("vehicle_id", pa.string()),
        ("route_id", pa.string()),
        ("trip_id", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("speed", pa.float64()),
        ("current_stop_sequence", pa.int32()),
        ("stop_id", pa.string()),
        ("current_status", pa.int32()),
        ("direction_id", pa.int32()),
        ("trip_start_date", pa.string()),
        ("timestamp", pa.timestamp("us")),
        ("collected_at", pa.timestamp("us")),
    ]
)

# Rows per server-side fetch batch. ~50K rows × 14 mostly-narrow columns lands
# well under the laptop's memory-pressure threshold while amortising per-batch
# overhead.
STREAM_BATCH_SIZE = 50_000


def compute_cutoff(retention_days: int = 30) -> datetime:
    """Return the naive-UTC cutoff: rows with `timestamp < cutoff` are expired.

    Naive UTC matches the project-wide storage convention — every `DateTime`
    column in the DB holds naive UTC (CLAUDE.md).
    """
    return utcnow_naive() - timedelta(days=retention_days)


def verify_parquet(parquet_path: Path, expected_rows: int) -> tuple[bool, int]:
    """Confirm a parquet file's row count matches ``expected_rows``.

    Reads only the ParquetFile metadata, so no row data is materialised.
    Returns ``(matches, actual_row_count)``.
    """
    actual = pq.ParquetFile(parquet_path).metadata.num_rows
    return actual == expected_rows, int(actual)


def _make_s3_client():
    """Build a boto3 S3 client for AWS S3 (spec §3.4).

    Uses the standard AWS credential chain (env vars / shared config /
    instance profile on the VM). If ``S3_ENDPOINT_URL`` is set it overrides the
    endpoint — useful to target a local S3-compatible store in development;
    unset means real AWS S3.
    """
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    kwargs = {"endpoint_url": endpoint} if endpoint else {}
    return boto3.client("s3", **kwargs)


def upload_and_verify(s3, parquet_path: Path, bucket: str, key: str) -> None:
    """Upload ``parquet_path`` to ``s3://bucket/key`` and verify the object size.

    Raises ``RuntimeError`` if the uploaded object's ``ContentLength`` does not
    match the local file size. The caller MUST treat this as "do not DELETE" —
    the rows stay in Postgres for the next run.
    """
    local_size = parquet_path.stat().st_size
    s3.upload_file(str(parquet_path), bucket, key)
    remote_size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if remote_size != local_size:
        raise RuntimeError(
            f"Upload size mismatch for s3://{bucket}/{key}: "
            f"local={local_size}, remote={remote_size}"
        )
```

- [ ] **Step 4: Run the smoke tests to verify they pass**

Run: `uv run pytest tests/test_archive_vehicle_positions.py -v -m smoke`
Expected: the 4 helper smoke tests PASS (`test_compute_cutoff_*`, `test_verify_parquet_*`, `test_upload_and_verify_*`). The integration test is not collected under `-m smoke`.

- [ ] **Step 5: Lint and format**

Run: `uv run ruff check pipelines/archive_vehicle_positions.py tests/test_archive_vehicle_positions.py && uv run ruff format --check pipelines/archive_vehicle_positions.py tests/test_archive_vehicle_positions.py`
Expected: clean (run `ruff format` on the two files if the format gate fails). Note: `Connection`, `date_type`, `text`, `get_engine` are imported now but used in Task 3 — if `ruff` flags unused imports (F401) at this commit, proceed to Task 3 in the same working session before committing, OR temporarily commit Task 2+3 together. Recommended: do Steps of Task 3 before this commit so no unused-import window exists.

- [ ] **Step 6: Commit** (after Task 3's implementation lands — see note above)

```bash
git add pipelines/archive_vehicle_positions.py tests/test_archive_vehicle_positions.py
git commit -m "feat: tier-3 vehicle_positions archival helpers + S3 upload (NOTES-48)"
```

---

## Task 3: Tier-3 Postgres streaming — count, archive, delete, vacuum

Add the DB-touching functions to `pipelines/archive_vehicle_positions.py`. The integration round-trip test written in Task 2 now passes.

**Files:**
- Modify: `pipelines/archive_vehicle_positions.py`
- Test: `tests/test_archive_vehicle_positions.py` (the `@pytest.mark.integration` test already written)

- [ ] **Step 1: Confirm the integration test currently fails**

Run: `bin/test-with-pg` is heavy; instead run just this test against a Postgres DB. With a scratch DB:
```bash
PG_TEST_DATABASE_URL=postgresql:///wmata_test_local \
  uv run pytest tests/test_archive_vehicle_positions.py::test_archive_then_delete_day_roundtrip -v
```
(Requires `wmata_test_local` to exist with the schema; `bin/test-with-pg` provisions it. If you have not run `bin/test-with-pg` yet, run it once to create + migrate the scratch DB, then Ctrl-C after it starts the suite, or just let it run.)
Expected: FAIL — `ImportError: cannot import name 'archive_day_to_parquet'`.

- [ ] **Step 2: Add the streaming + delete + count functions**

Insert these functions into `pipelines/archive_vehicle_positions.py` after `upload_and_verify`:

```python
def count_rows_before(conn: Connection, cutoff: datetime) -> int:
    """Return the count of rows with `timestamp < cutoff` on ``conn``'s transaction."""
    result = conn.execute(
        text(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {TS_COLUMN} < :cutoff"),
        {"cutoff": cutoff},
    ).scalar()
    return int(result or 0)


def list_expired_dates(conn: Connection, cutoff: datetime) -> list[date_type]:
    """Return distinct UTC dates (`DATE(timestamp)`) with ≥1 row older than ``cutoff``.

    `timestamp` is naive UTC, so `DATE(timestamp)` is the UTC calendar date with
    no timezone conversion. Ascending order so the archive loop processes the
    oldest day first.
    """
    rows = conn.execute(
        text(
            f"SELECT DISTINCT DATE({TS_COLUMN}) AS d FROM {TABLE_NAME} "
            f"WHERE {TS_COLUMN} < :cutoff ORDER BY d"
        ),
        {"cutoff": cutoff},
    ).all()
    return [r[0] for r in rows]


def count_rows_for_date(conn: Connection, day: date_type) -> int:
    """Return the count of rows whose `timestamp` falls on `day` (UTC)."""
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    result = conn.execute(
        text(
            f"SELECT COUNT(*) FROM {TABLE_NAME} "
            f"WHERE {TS_COLUMN} >= :start AND {TS_COLUMN} < :end"
        ),
        {"start": start, "end": end},
    ).scalar()
    return int(result or 0)


def archive_day_to_parquet(conn: Connection, day: date_type, parquet_path: Path) -> int:
    """Stream all rows for `day` (UTC) on ``conn`` to ``parquet_path`` as zstd parquet.

    Uses a server-side cursor (`stream_results=True`) so memory is bounded by
    ``STREAM_BATCH_SIZE`` rather than the whole day — the same bounded-memory
    streaming as PR #131. Running through the caller's `Connection` (rather than
    a fresh `engine.raw_connection()`) is deliberate: tests pass
    `pg_session.connection()` so the read shares the session's transaction and
    the savepoint rollback cleans up. Rows are written with the explicit
    ``ARCHIVE_SCHEMA`` (no inference). Returns the row count written.
    """
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    columns_sql = ", ".join(ARCHIVE_COLUMNS)
    stmt = text(
        f"SELECT {columns_sql} FROM {TABLE_NAME} "
        f"WHERE {TS_COLUMN} >= :start AND {TS_COLUMN} < :end "
        f"ORDER BY {TS_COLUMN}, id"
    )
    rows_written = 0
    result = conn.execution_options(
        stream_results=True, max_row_buffer=STREAM_BATCH_SIZE
    ).execute(stmt, {"start": start, "end": end})
    with pq.ParquetWriter(parquet_path, ARCHIVE_SCHEMA, compression="zstd") as writer:
        while True:
            rows = result.fetchmany(STREAM_BATCH_SIZE)
            if not rows:
                break
            # Transpose row tuples to per-column lists, coerce to the explicit
            # schema (None → null, naive datetime → timestamp[us]).
            columns_data = list(zip(*rows, strict=True))
            arrays = [
                pa.array(col, type=ARCHIVE_SCHEMA.field(i).type)
                for i, col in enumerate(columns_data)
            ]
            writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=ARCHIVE_SCHEMA))
            rows_written += len(rows)
    return rows_written


def delete_day(conn: Connection, day: date_type) -> int:
    """DELETE all rows whose `timestamp` falls on `day` (UTC) on ``conn``.

    The caller MUST have archived + verified + uploaded the day first — this is
    the irreversible step. Does NOT commit; the caller (the orchestrator)
    commits per day so a later failure cannot lose already-archived days.
    Returns the number of rows deleted.
    """
    start = datetime.combine(day, datetime.min.time())
    end = start + timedelta(days=1)
    result = conn.execute(
        text(f"DELETE FROM {TABLE_NAME} WHERE {TS_COLUMN} >= :start AND {TS_COLUMN} < :end"),
        {"start": start, "end": end},
    )
    return int(result.rowcount or 0)


def vacuum_table(engine) -> None:
    """Run a regular `VACUUM vehicle_positions` (NOT FULL).

    Reclaims the just-deleted rows for reuse and refreshes statistics under a
    SHARE UPDATE EXCLUSIVE lock that does not block the collector's concurrent
    INSERTs. VACUUM cannot run inside a transaction, so it uses AUTOCOMMIT —
    the same pattern as the trip_update_snapshots archiver.
    """
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"VACUUM {TABLE_NAME}"))
```

- [ ] **Step 3: Run the integration test to verify it passes**

Run:
```bash
PG_TEST_DATABASE_URL=postgresql:///wmata_test_local \
  uv run pytest tests/test_archive_vehicle_positions.py::test_archive_then_delete_day_roundtrip -v
```
Expected: PASS. (Sentinel-day rows are inserted, archived to parquet, verified, deleted — all inside the savepoint, then rolled back.)

- [ ] **Step 4: Run the full file (smoke + integration)**

Run: `PG_TEST_DATABASE_URL=postgresql:///wmata_test_local uv run pytest tests/test_archive_vehicle_positions.py -v`
Expected: all PASS.

- [ ] **Step 5: Lint and format**

Run: `uv run ruff check pipelines/archive_vehicle_positions.py tests/test_archive_vehicle_positions.py && uv run ruff format --check pipelines/archive_vehicle_positions.py tests/test_archive_vehicle_positions.py`
Expected: clean (no unused imports now). Run `ruff format` on the files if the format gate fails.

- [ ] **Step 6: Commit** (this is the Task 2 + Task 3 commit — see Task 2 Step 6 note)

```bash
git add pipelines/archive_vehicle_positions.py tests/test_archive_vehicle_positions.py
git commit -m "feat: tier-3 vehicle_positions archival — streaming + S3 + delete (NOTES-48)"
```

---

## Task 4: Tier-3 orchestrator + CLI

Wire the pieces into `archive_and_delete` and a `main()`, with mocked control-flow tests proving the safety invariant.

**Files:**
- Modify: `pipelines/archive_vehicle_positions.py`
- Test: `tests/test_archive_vehicle_positions.py`

- [ ] **Step 1: Write the failing orchestrator tests**

Append to `tests/test_archive_vehicle_positions.py`:

```python
# --------------------------------------------------------------------------
# Orchestrator control flow (smoke — every I/O function monkeypatched)
# --------------------------------------------------------------------------


def _patch_orchestrator_io(monkeypatch, *, verify_result, upload_raises):
    """Monkeypatch every I/O call in archive_and_delete; return a record dict.

    Stubs the DB/engine/S3 calls so the orchestrator's control flow runs with
    no real Postgres or AWS. `record["deleted"]` accumulates the days passed to
    delete_day, so a test can assert DELETE was or wasn't reached.
    """
    import pipelines.archive_vehicle_positions as mod

    record: dict[str, list] = {"deleted": [], "uploaded": []}
    monkeypatch.setattr(mod, "get_engine", lambda: MagicMock())
    monkeypatch.setattr(mod, "count_rows_before", lambda conn, cutoff: 3)
    monkeypatch.setattr(mod, "list_expired_dates", lambda conn, cutoff: [date(2020, 1, 1)])
    monkeypatch.setattr(mod, "count_rows_for_date", lambda conn, day: 3)
    monkeypatch.setattr(mod, "archive_day_to_parquet", lambda conn, day, path: 3)
    monkeypatch.setattr(mod, "verify_parquet", lambda path, expected: verify_result)
    monkeypatch.setattr(mod, "_make_s3_client", lambda: MagicMock())
    monkeypatch.setattr(mod, "vacuum_table", lambda engine: None)

    def _upload(s3, path, bucket, key):
        record["uploaded"].append(key)
        if upload_raises:
            raise RuntimeError("size mismatch (simulated)")

    def _delete(conn, day):
        record["deleted"].append(day)
        return 3

    monkeypatch.setattr(mod, "upload_and_verify", _upload)
    monkeypatch.setattr(mod, "delete_day", _delete)
    return mod, record


@pytest.mark.smoke
def test_orchestrator_happy_path_deletes_after_upload(monkeypatch, tmp_path):
    """When archive + verify + upload all succeed, the day is uploaded then deleted; rc=0."""
    mod, record = _patch_orchestrator_io(monkeypatch, verify_result=(True, 3), upload_raises=False)
    rc = mod.archive_and_delete(retention_days=30, staging_dir=tmp_path, bucket="b")
    assert rc == 0
    assert record["uploaded"] == ["vehicle_positions/2020-01-01.parquet"]
    assert record["deleted"] == [date(2020, 1, 1)]


@pytest.mark.smoke
def test_orchestrator_skips_delete_when_verify_fails(monkeypatch, tmp_path):
    """A parquet verify mismatch skips upload AND delete; rc=1 (failure)."""
    mod, record = _patch_orchestrator_io(monkeypatch, verify_result=(False, 2), upload_raises=False)
    rc = mod.archive_and_delete(retention_days=30, staging_dir=tmp_path, bucket="b")
    assert rc == 1
    assert record["uploaded"] == []
    assert record["deleted"] == []


@pytest.mark.smoke
def test_orchestrator_skips_delete_when_upload_fails(monkeypatch, tmp_path):
    """An S3 upload/verify failure skips the delete; rc=1 (failure)."""
    mod, record = _patch_orchestrator_io(monkeypatch, verify_result=(True, 3), upload_raises=True)
    rc = mod.archive_and_delete(retention_days=30, staging_dir=tmp_path, bucket="b")
    assert rc == 1
    assert record["deleted"] == []


@pytest.mark.smoke
def test_orchestrator_dry_run_does_nothing(monkeypatch, tmp_path):
    """dry_run uploads/deletes nothing and returns 0."""
    mod, record = _patch_orchestrator_io(monkeypatch, verify_result=(True, 3), upload_raises=False)
    rc = mod.archive_and_delete(retention_days=30, staging_dir=tmp_path, bucket="b", dry_run=True)
    assert rc == 0
    assert record["uploaded"] == []
    assert record["deleted"] == []
```

- [ ] **Step 2: Run the orchestrator tests to verify they fail**

Run: `uv run pytest tests/test_archive_vehicle_positions.py -v -m smoke -k orchestrator`
Expected: FAIL — `AttributeError: ... has no attribute 'archive_and_delete'`.

- [ ] **Step 3: Add `archive_and_delete` and `main`**

Append to `pipelines/archive_vehicle_positions.py`:

```python
def _format_size(path: Path) -> str:
    """Return a human-readable MB size string for ``path``."""
    return f"{path.stat().st_size / (1024 * 1024):,.1f} MB"


def archive_and_delete(
    retention_days: int = 30,
    staging_dir: Path = DEFAULT_STAGING_DIR,
    bucket: str | None = None,
    dry_run: bool = False,
) -> int:
    """Drive one archive→verify→upload→delete cycle. Returns 0 on success, 1 on any failure.

    For each UTC date older than the cutoff (oldest first): stage to local
    parquet, verify the row count, upload to S3 and verify the object size, then
    DELETE the day and remove the local staging file. A day that fails archive,
    verify, or upload is left intact (rows stay in Postgres) and counts as a
    failure. After all successful deletes, a regular VACUUM runs.

    Args:
        retention_days: Rows with `timestamp` older than this are archived
            (default 30, spec §3.5).
        staging_dir: Local directory for the transient parquet files (deleted
            after a successful upload).
        bucket: S3 bucket name; falls back to ``S3_ARCHIVE_BUCKET`` when None.
        dry_run: Print the plan without writing parquet, uploading, or deleting.

    Returns:
        0 if every expired day was archived+deleted (or there was nothing to
        do, or dry-run); 1 if any day failed verification or upload.
    """
    cutoff = compute_cutoff(retention_days)
    bucket = bucket or os.environ.get("S3_ARCHIVE_BUCKET")
    print(
        f"archive_vehicle_positions: retention={retention_days}d, "
        f"cutoff={cutoff.isoformat()}, staging_dir={staging_dir}, bucket={bucket}"
    )
    if dry_run:
        print("(dry-run mode — no parquet written, no uploads, no rows deleted)")
    if not dry_run and not bucket:
        print("ERROR: no S3 bucket (set --bucket or S3_ARCHIVE_BUCKET).", file=sys.stderr)
        return 1

    engine = get_engine()
    failures = 0
    archived_dates: list[date_type] = []

    with engine.connect() as conn:
        total = count_rows_before(conn, cutoff)
        if total == 0:
            print("No rows older than cutoff. Nothing to do.")
            return 0
        print(f"Total expired rows: {total:,}")
        expired_dates = list_expired_dates(conn, cutoff)
        print(f"Distinct expired UTC dates: {len(expired_dates)}")

        if not dry_run:
            staging_dir.mkdir(parents=True, exist_ok=True)
            s3 = _make_s3_client()

        for day in expired_dates:
            expected = count_rows_for_date(conn, day)
            parquet_path = staging_dir / f"{day.isoformat()}.parquet"
            key = f"{KEY_PREFIX}/{day.isoformat()}.parquet"

            if dry_run:
                print(
                    f"DRY-RUN {day.isoformat()}: would archive {expected:,} rows "
                    f"→ s3://{bucket}/{key}, then DELETE from {TABLE_NAME}"
                )
                continue

            written = archive_day_to_parquet(conn, day, parquet_path)
            ok, actual = verify_parquet(parquet_path, expected)
            if written != expected or not ok:
                print(
                    f"FAIL {day.isoformat()}: wrote {written:,}, parquet has {actual:,}, "
                    f"expected {expected:,}. Refusing to upload/DELETE.",
                    file=sys.stderr,
                )
                failures += 1
                parquet_path.unlink(missing_ok=True)
                continue

            try:
                upload_and_verify(s3, parquet_path, bucket, key)
            except RuntimeError as exc:
                print(f"FAIL {day.isoformat()}: {exc}. Refusing to DELETE.", file=sys.stderr)
                failures += 1
                parquet_path.unlink(missing_ok=True)
                continue

            deleted = delete_day(conn, day)
            conn.commit()  # commit per day: a later failure cannot lose this day
            archived_dates.append(day)
            print(
                f"archived {day.isoformat()}: {written:,} rows → s3://{bucket}/{key} "
                f"({_format_size(parquet_path)}), deleted {deleted:,} from {TABLE_NAME}"
            )
            parquet_path.unlink(missing_ok=True)

    if archived_dates and not dry_run:
        print(f"running VACUUM {TABLE_NAME}...")
        vacuum_table(engine)
        print("VACUUM complete.")

    if failures:
        print(f"{failures} day(s) failed — see errors above", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description=(
            "Archive expired vehicle_positions rows to S3 parquet, then DELETE. "
            "Verifies the parquet round-trip and the S3 object size before any "
            "destructive operation."
        )
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=30,
        help="Rows with timestamp older than this many days are archived (default: 30).",
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=DEFAULT_STAGING_DIR,
        help=f"Local staging directory for transient parquet (default: {DEFAULT_STAGING_DIR}).",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="S3 bucket (overrides S3_ARCHIVE_BUCKET).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan without writing parquet, uploading, or deleting.",
    )
    args = parser.parse_args()

    load_dotenv()
    return archive_and_delete(
        retention_days=args.retention_days,
        staging_dir=args.staging_dir,
        bucket=args.bucket,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run the orchestrator tests to verify they pass**

Run: `uv run pytest tests/test_archive_vehicle_positions.py -v -m smoke`
Expected: all 8 smoke tests PASS (4 helpers + 4 orchestrator).

- [ ] **Step 5: Lint and format**

Run: `uv run ruff check pipelines/archive_vehicle_positions.py tests/test_archive_vehicle_positions.py && uv run ruff format --check pipelines/archive_vehicle_positions.py tests/test_archive_vehicle_positions.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add pipelines/archive_vehicle_positions.py tests/test_archive_vehicle_positions.py
git commit -m "feat: tier-3 archival orchestrator + CLI (NOTES-48)"
```

---

## Task 5: Full verification + PR

**Files:** none (verification + PR only).

- [ ] **Step 1: Full lint + format gate (mirror CI exactly)**

Run:
```bash
uv run ruff check src/ scripts/ api/ pipelines/ tests/
uv run ruff format --check src/ scripts/ api/ pipelines/ tests/
```
Expected: both clean. Fix any issues and re-run.

- [ ] **Step 2: Smoke suite**

Run: `uv run pytest -m smoke`
Expected: all PASS (includes the new tier-2 tests and the tier-3 helper/orchestrator smoke tests).

- [ ] **Step 3: Full suite with Postgres (mirror CI)**

Run: `bin/test-with-pg`
Expected: all PASS — this provisions the scratch DB `wmata_test_local`, migrates it, and runs the whole suite including the tier-3 `@pytest.mark.integration` round-trip on the scratch DB.

- [ ] **Step 4: Push and open the PR**

```bash
git push -u origin feature/notes-48-cloud-migration-phase1
gh pr create --title "feat: tier-2/tier-3 retention jobs for cloud migration (NOTES-48)" --body "$(cat <<'EOF'
## Summary

Implements the two net-new retention jobs from the NOTES-48 cloud-migration spec (§3.5), the locally-testable code on the "archival fast-follow" path:

- **Tier-2 — `pipelines/window_derived_tables.py`:** deletes `stop_events`/`runs` rows older than a 365-day rolling window. Pure-ORM; the granular derived rows are recoverable by re-derivation, so this is a storage/latency trade, not data loss.
- **Tier-3 — `pipelines/archive_vehicle_positions.py`:** archives `vehicle_positions` older than 30 days to compressed parquet in **AWS S3** (spec §3.4), verifies the parquet row count and the uploaded object size, then DELETEs — archive-then-verify-then-delete, never a bare DELETE. Streams server-side (bounded memory, PR #131 pattern). Archives only the 14 columns the pruned model persists (PR #152).

Also refreshes the spec's footprint numbers for PR #152 (the pruning is ~6% in-DB, not ~26%, because `vehicle_positions` is index-dominated) and adds the post-restore `DROP COLUMN` reclaim step to the §5 runbook.

## Testing

- Tier-2: SQLite smoke tests (fresh in-memory DB → exact-count assertions).
- Tier-3: smoke tests for pure helpers, a `MagicMock` S3 client (no real AWS), and the orchestrator's safety invariant (rows are never DELETEd unless archive + verify + upload all succeed); a Postgres `@pytest.mark.integration` round-trip on a **sentinel far-past day (2020-01-01)** read/written through `pg_session.connection()` so the savepoint rollback cleans up and a populated dev DB is never touched.
- `bin/test-with-pg` green (scratch DB).

## Operating the jobs (for the user to run — not run here)

These are nightly jobs; deploy as systemd timers on the VM per spec §5. They issue DELETEs, so per CLAUDE.md they are NOT run from this PR. To exercise safely first:

```bash
# Tier-2 — preview, then run:
uv run python pipelines/window_derived_tables.py --dry-run
uv run python pipelines/window_derived_tables.py

# Tier-3 — set the bucket + AWS creds in .env, preview, then run:
uv run python pipelines/archive_vehicle_positions.py --dry-run
uv run python pipelines/archive_vehicle_positions.py
```

Tier-3 needs `S3_ARCHIVE_BUCKET` and AWS credentials (env / shared config / instance profile); `S3_ENDPOINT_URL` optionally points at an S3-compatible store. Heads-up: against the current dev DB the tier-3 job would archive the full >30-day tail (data back to 2025-10-12) on first run — expected, but run it where that's intended (i.e., post-migration on the VM).

## Out of scope (spec §5 runbook)

VM provisioning, SSH hardening, the `pg_dump | pg_restore` cutover, systemd units/timers, and the weekly `pg_dump → S3` backup. This PR is the retention *code* only.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Report the PR URL** and stop. CI watch + merge follow the normal review flow.

---

## Self-Review (completed during plan authoring)

**Spec coverage (§3.5):** tier-2 `stop_events`/`runs` 365-day window → Task 1 ✓; tier-3 `vehicle_positions` → 30-day window + S3 parquet archive + archive-then-DELETE → Tasks 2–4 ✓; `trip_update_state` retention → already exists (`cleanup_trip_update_state.py`), no new work, correctly out of scope ✓; tier-1 rollups kept forever → no job needed ✓. Provisioning/cutover (§5) → intentionally out of scope (separate runbook). The spec-number refresh (§2/§3.3/§5) landed in the earlier commit `87031e7`.

**Placeholder scan:** no TBD/TODO; every code step shows complete, runnable code; every test step shows the exact `pytest` command and expected outcome.

**Type/name consistency:** `compute_cutoff_str` (tier-2, string) vs `compute_cutoff` (tier-3, datetime) — deliberately distinct because the columns differ (String `service_date` vs naive-UTC `timestamp`). `ARCHIVE_COLUMNS`/`ARCHIVE_SCHEMA` field order matches the SELECT in `archive_day_to_parquet`. Per-day functions consistently take a `Connection` named `conn`; `vacuum_table` takes an `engine` (VACUUM needs autocommit). `delete_day` does not commit (caller commits) — matching the `run_cleanup`/`run_window` "caller commits" convention. Model NOT NULL columns verified against `main`'s `src/models.py`.

**Known sequencing note:** Task 2 and Task 3 share one commit (Task 3 Step 6) to avoid an unused-import (`Connection`/`text`/`get_engine`/`date_type`) window between them — called out explicitly in Task 2 Step 5/6.
