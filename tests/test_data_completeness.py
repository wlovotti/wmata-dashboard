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

from src.agency_config import load_agency_config
from src.data_completeness import (
    MIN_COVERAGE_FOR_MATERIALIZATION,
    agency_coverage_threshold,
    coverage_pct_for_date,
    expected_minutes_for_date,
    is_date_sufficiently_complete,
)
from src.timezones import eastern_day_bounds_utc

TEST_DATE = date(2099, 1, 15)


def _insert_heartbeat_minutes(db, service_date: date, minute_count: int, offset_minutes: int = 0):
    """Insert ``minute_count`` collector_heartbeats rows, one per consecutive minute.

    Each row carries a unique ``ts`` at minute granularity inside the
    Eastern-day UTC window, starting ``offset_minutes`` minutes after the
    Eastern midnight UTC bound. One distinct-minute row is enough to register
    the bucket — the helper counts distinct buckets, not rows.
    """
    start_utc, _ = eastern_day_bounds_utc(service_date)
    for i in range(minute_count):
        ts = start_utc + timedelta(minutes=offset_minutes + i)
        db.execute(
            text("INSERT INTO collector_heartbeats (ts, collector_name) VALUES (:ts, 'combined')"),
            {"ts": ts},
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


def test_coverage_full_day_from_heartbeats(pg_session):
    """1,440 distinct heartbeat minutes → 100% coverage → sufficient."""
    _insert_heartbeat_minutes(pg_session, TEST_DATE, minute_count=1440)
    assert coverage_pct_for_date(pg_session, TEST_DATE) == pytest.approx(1.0)
    assert is_date_sufficiently_complete(pg_session, TEST_DATE) is True


def test_coverage_partial_day_below_threshold(pg_session):
    """A half-day's worth of heartbeats falls under the 80% threshold."""
    _insert_heartbeat_minutes(pg_session, TEST_DATE, minute_count=720)
    pct = coverage_pct_for_date(pg_session, TEST_DATE)
    assert pct == pytest.approx(720 / 1440)
    assert pct < MIN_COVERAGE_FOR_MATERIALIZATION
    assert is_date_sufficiently_complete(pg_session, TEST_DATE) is False


def test_coverage_just_above_threshold(pg_session):
    """An 80%-coverage day passes the guard at the default threshold."""
    _insert_heartbeat_minutes(pg_session, TEST_DATE, minute_count=1152)  # 1440 * 0.80
    assert is_date_sufficiently_complete(pg_session, TEST_DATE) is True


def test_coverage_unions_heartbeats_and_positions(pg_session):
    """Coverage is the UNION across both ingest tables — partial heartbeats
    plus disjoint positions still register the broader signal.

    Insert 600 heartbeat-minutes (0..599) and 600 vehicle_positions-minutes
    starting at offset 720 (720..1319). The union covers 1,200 distinct
    minutes → 1200/1440 = 83.3% → above threshold.
    """
    _insert_heartbeat_minutes(pg_session, TEST_DATE, minute_count=600, offset_minutes=0)
    _insert_vehicle_position_minutes(pg_session, TEST_DATE, minute_count=600, offset_minutes=720)
    pct = coverage_pct_for_date(pg_session, TEST_DATE)
    assert pct == pytest.approx(1200 / 1440)
    assert is_date_sufficiently_complete(pg_session, TEST_DATE) is True


def test_threshold_override(pg_session):
    """The threshold argument lets callers tighten or relax the gate."""
    _insert_heartbeat_minutes(pg_session, TEST_DATE, minute_count=720)  # 50%
    assert is_date_sufficiently_complete(pg_session, TEST_DATE, threshold=0.30) is True
    assert is_date_sufficiently_complete(pg_session, TEST_DATE, threshold=0.90) is False


@pytest.mark.smoke
def test_expected_minutes_honors_tz_name_on_dst_transition():
    """NOTES-100: a non-Eastern tz_name changes which day is short/long.

    2026-03-08 is Eastern's spring-forward day (23h, 1380 min) but an
    ordinary 24h day everywhere that doesn't spring forward on that date
    (e.g. Pacific also transitions the same US day in practice, so use a
    fixed-offset zone with no DST to prove the parameter is actually
    threaded through, not just defaulted).
    """
    assert expected_minutes_for_date(TEST_DATE, tz_name="America/New_York") == 1440
    assert expected_minutes_for_date(TEST_DATE, tz_name="Pacific/Honolulu") == 1440
    # Eastern's spring-forward day is short (23h); a no-DST zone's isn't.
    dst_day = date(2026, 3, 8)
    assert expected_minutes_for_date(dst_day, tz_name="America/New_York") == 1380
    assert expected_minutes_for_date(dst_day, tz_name="Pacific/Honolulu") == 1440


def test_coverage_pct_uses_tz_name_window_not_eastern(pg_session):
    """A heartbeat placed one minute before the *next* Pacific midnight
    (23:59 Pacific on TEST_DATE) falls inside the Pacific TEST_DATE window
    but past the end of the Eastern TEST_DATE window (which closes at
    05:00 UTC, 3 hours before Pacific midnight even starts) -- i.e.
    tz_name genuinely changes which rows are counted, not just cosmetic
    labeling.
    """
    from datetime import timedelta

    from src.timezones import local_midnight_as_utc

    late_pacific_ts = local_midnight_as_utc(
        TEST_DATE + timedelta(days=1), "America/Los_Angeles"
    ) - timedelta(minutes=1)
    pg_session.execute(
        text("INSERT INTO collector_heartbeats (ts, collector_name) VALUES (:ts, 'combined')"),
        {"ts": late_pacific_ts},
    )
    pg_session.flush()

    pacific_pct = coverage_pct_for_date(pg_session, TEST_DATE, tz_name="America/Los_Angeles")
    eastern_pct = coverage_pct_for_date(pg_session, TEST_DATE, tz_name="America/New_York")
    assert pacific_pct == pytest.approx(1 / 1440)
    assert eastern_pct == 0.0


@pytest.mark.smoke
def test_agency_coverage_threshold_wmata_matches_flat_constant():
    """WMATA polls TripUpdates every tick (trip_updates_every_ticks=1), so
    every tick is 'active' and the theoretical coverage ceiling is 100% --
    the agency-aware threshold must equal today's flat 0.80 constant
    exactly, or every existing WMATA day's completeness classification
    would silently shift.
    """
    cfg = load_agency_config("wmata")
    assert agency_coverage_threshold(cfg) == pytest.approx(MIN_COVERAGE_FOR_MATERIALIZATION)


@pytest.mark.smoke
def test_agency_coverage_threshold_sfmta_is_cadence_capped():
    """SFMTA polls TripUpdates every 2nd tick and VehiclePositions every 3rd
    tick (both configured in config/agencies/sfmta.yaml) -- neither feed is
    polled on every tick, so a heartbeat that only fires on ticks where
    something was actually polled can never reach 100% minute-coverage
    even under perfect collection. Over the LCM(2, 3) = 6-tick cycle, ticks
    2, 3, 4, 6 are active (TU: 2,4,6; VP: 3,6) -- 4/6 = 66.7% ceiling.
    The flat 0.80 threshold is literally unreachable for this agency; the
    cadence-aware threshold must be well below it.
    """
    cfg = load_agency_config("sfmta")
    threshold = agency_coverage_threshold(cfg)
    assert threshold == pytest.approx(0.80 * (4 / 6))
    assert threshold < MIN_COVERAGE_FOR_MATERIALIZATION


@pytest.mark.smoke
def test_agency_coverage_threshold_keeps_relative_safety_margin():
    """The agency-aware threshold is MIN_COVERAGE_FOR_MATERIALIZATION scaled
    by the agency's own cadence ceiling -- same "80% of achievable" safety
    margin philosophy as the original flat constant, just relative to what
    THIS agency's cadence can actually deliver instead of assuming 100%."""
    from src.agency_config import AgencyConfig

    # A hypothetical agency polling both feeds on every 4th tick: ceiling
    # is exactly 1/4 (only tick 4 of every 4-tick cycle is active).
    cfg = AgencyConfig(
        name="hypothetical",
        display_name="Hypothetical Transit",
        timezone="America/New_York",
        api_key_env="X",
        auth_style="header",
        trip_updates_url="",
        vehicle_positions_url="",
        extra_params={},
        static_gtfs_url="",
        static_gtfs_params={},
        tick_sec=60,
        trip_updates_every_ticks=4,
        vehicle_positions_every_ticks=4,
        archive_dir="",
        pid_file="",
        heartbeat_name="",
        database_url_env="X_DATABASE_URL",
        healthcheck_url_env="X_HEALTHCHECK_URL",
    )
    assert agency_coverage_threshold(cfg) == pytest.approx(MIN_COVERAGE_FOR_MATERIALIZATION * 0.25)
