"""
Tests for ``src/data_completeness.py``.

Uses ``pg_session`` because the helper relies on Postgres ``date_trunc``
and the SAVEPOINT rollback semantics in the conftest fixture keep test
writes from leaking into the dev DB. All tests target a far-future
service date (year 2099) so they don't collide with real ingest rows.
"""

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from src.data_completeness import (
    MIN_COVERAGE_FOR_MATERIALIZATION,
    coverage_pct_for_date,
    expected_minutes_for_date,
    is_date_sufficiently_complete,
)
from src.timezones import eastern_day_bounds_utc

TEST_DATE = date(2099, 1, 15)


def _insert_snapshot_minutes(db, service_date: date, minute_count: int, offset_minutes: int = 0):
    """Insert ``minute_count`` trip_update_snapshot rows, one per consecutive minute.

    Each row carries a unique ``snapshot_ts`` at minute granularity inside the
    Eastern-day UTC window, starting ``offset_minutes`` minutes after the
    Eastern midnight UTC bound. One distinct-minute row is enough to register
    the bucket — the helper counts distinct buckets, not rows.
    """
    start_utc, _ = eastern_day_bounds_utc(service_date)
    for i in range(minute_count):
        ts = start_utc + timedelta(minutes=offset_minutes + i)
        db.execute(
            text(
                "INSERT INTO trip_update_snapshots (snapshot_ts, trip_id, stop_id) "
                "VALUES (:ts, :tid, :sid)"
            ),
            {"ts": ts, "tid": f"trip-{i}", "sid": f"stop-{i}"},
        )
    db.flush()


def _insert_vehicle_position_minutes(
    db, service_date: date, minute_count: int, offset_minutes: int = 0
):
    """Insert one vehicle_positions row per consecutive minute, like the helper above."""
    start_utc, _ = eastern_day_bounds_utc(service_date)
    for i in range(minute_count):
        ts = start_utc + timedelta(minutes=offset_minutes + i)
        db.execute(
            text(
                "INSERT INTO vehicle_positions "
                "(vehicle_id, latitude, longitude, timestamp) "
                "VALUES (:v, :lat, :lon, :ts)"
            ),
            {"v": f"v-{i}", "lat": 38.9, "lon": -77.0, "ts": ts},
        )
    db.flush()


@pytest.mark.smoke
def test_expected_minutes_for_normal_day():
    """A non-DST day has exactly 1,440 clock-minutes."""
    assert expected_minutes_for_date(TEST_DATE) == 1440


def test_coverage_zero_when_no_ingest(pg_session):
    """An untouched service date scores 0.0 coverage."""
    assert coverage_pct_for_date(pg_session, TEST_DATE) == 0.0
    assert is_date_sufficiently_complete(pg_session, TEST_DATE) is False


def test_coverage_full_day_from_snapshots(pg_session):
    """1,440 distinct snapshot minutes → 100% coverage → sufficient."""
    _insert_snapshot_minutes(pg_session, TEST_DATE, minute_count=1440)
    assert coverage_pct_for_date(pg_session, TEST_DATE) == pytest.approx(1.0)
    assert is_date_sufficiently_complete(pg_session, TEST_DATE) is True


def test_coverage_partial_day_below_threshold(pg_session):
    """A half-day's worth of snapshots falls under the 80% threshold."""
    _insert_snapshot_minutes(pg_session, TEST_DATE, minute_count=720)
    pct = coverage_pct_for_date(pg_session, TEST_DATE)
    assert pct == pytest.approx(720 / 1440)
    assert pct < MIN_COVERAGE_FOR_MATERIALIZATION
    assert is_date_sufficiently_complete(pg_session, TEST_DATE) is False


def test_coverage_just_above_threshold(pg_session):
    """An 80%-coverage day passes the guard at the default threshold."""
    _insert_snapshot_minutes(pg_session, TEST_DATE, minute_count=1152)  # 1440 * 0.80
    assert is_date_sufficiently_complete(pg_session, TEST_DATE) is True


def test_coverage_unions_snapshots_and_positions(pg_session):
    """Coverage is the UNION across both ingest tables — partial snapshots
    plus disjoint positions still register the broader signal.

    Insert 600 snapshot-minutes (0..599) and 600 vehicle_positions-minutes
    starting at offset 720 (720..1319). The union covers 1,200 distinct
    minutes → 1200/1440 = 83.3% → above threshold.
    """
    _insert_snapshot_minutes(pg_session, TEST_DATE, minute_count=600, offset_minutes=0)
    _insert_vehicle_position_minutes(pg_session, TEST_DATE, minute_count=600, offset_minutes=720)
    pct = coverage_pct_for_date(pg_session, TEST_DATE)
    assert pct == pytest.approx(1200 / 1440)
    assert is_date_sufficiently_complete(pg_session, TEST_DATE) is True


def test_threshold_override(pg_session):
    """The threshold argument lets callers tighten or relax the gate."""
    _insert_snapshot_minutes(pg_session, TEST_DATE, minute_count=720)  # 50%
    assert is_date_sufficiently_complete(pg_session, TEST_DATE, threshold=0.30) is True
    assert is_date_sufficiently_complete(pg_session, TEST_DATE, threshold=0.90) is False
