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


@pytest.mark.smoke
def test_orchestrator_continues_after_one_day_fails(monkeypatch, tmp_path):
    """Per-day commit: day 1 is archived+deleted; day 2's upload fails; the loop continues.

    Covers both robustness fixes at once: (1) the per-day-commit claim — a later
    day's failure must NOT cost an already-archived earlier day; (2) a NON-
    RuntimeError S3 error (as botocore raises) is caught gracefully rather than
    crashing the run. Day 2 raises a ValueError, which the old `except
    RuntimeError` would have let escape. Expect day 1 deleted, day 2 not, rc=1.
    """
    import pipelines.archive_vehicle_positions as mod

    day1, day2 = date(2020, 1, 1), date(2020, 1, 2)
    record: dict[str, list] = {"deleted": [], "uploaded": []}
    monkeypatch.setattr(mod, "get_engine", lambda: MagicMock())
    monkeypatch.setattr(mod, "count_rows_before", lambda conn, cutoff: 6)
    monkeypatch.setattr(mod, "list_expired_dates", lambda conn, cutoff: [day1, day2])
    monkeypatch.setattr(mod, "count_rows_for_date", lambda conn, day: 3)
    monkeypatch.setattr(mod, "archive_day_to_parquet", lambda conn, day, path: 3)
    monkeypatch.setattr(mod, "verify_parquet", lambda path, expected: (True, 3))
    monkeypatch.setattr(mod, "_make_s3_client", lambda: MagicMock())
    monkeypatch.setattr(mod, "vacuum_table", lambda engine: None)

    def _upload(s3, path, bucket, key):
        record["uploaded"].append(key)
        if "2020-01-02" in key:
            # Non-RuntimeError, as a botocore ClientError would be — must still
            # be caught so the run degrades gracefully instead of crashing.
            raise ValueError("simulated botocore ClientError, day 2")

    def _delete(conn, day):
        record["deleted"].append(day)
        return 3

    monkeypatch.setattr(mod, "upload_and_verify", _upload)
    monkeypatch.setattr(mod, "delete_day", _delete)

    rc = mod.archive_and_delete(retention_days=30, staging_dir=tmp_path, bucket="b")

    assert rc == 1  # day 2 failed → overall failure
    assert record["deleted"] == [day1]  # day 1 committed; day 2 never deleted
    assert record["uploaded"] == [
        "vehicle_positions/2020-01-01.parquet",
        "vehicle_positions/2020-01-02.parquet",
    ]  # loop continued to day 2 after day 1
