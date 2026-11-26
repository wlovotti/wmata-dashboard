"""
Tests for ``src/data_completeness.py``.

Uses ``pg_session`` because the helper relies on Postgres ``date_trunc``
and the SAVEPOINT rollback semantics in the conftest fixture keep test
writes from leaking into the dev DB. All tests target a far-future
service date (year 2099) so they don't collide with real ingest rows.
"""

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from src.agency_config import CompletenessPolicy, load_agency_config
from src.data_completeness import (
    MIN_COVERAGE_FOR_MATERIALIZATION,
    agency_coverage_threshold,
    assess_data_quality,
    coverage_pct_for_date,
    expected_minutes_for_date,
    has_trip_update_signal,
    is_date_sufficiently_complete,
    longest_gap,
    resolve_data_quality,
    service_window_utc,
)
from src.timezones import eastern_day_bounds_utc, local_day_bounds_utc

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
    db,
    service_date: date,
    minute_count: int,
    offset_minutes: int = 0,
    step_minutes: int = 1,
    tz_name: str = "America/New_York",
    skip: tuple[int, int] | None = None,
):
    """Insert one vehicle_positions row every ``step_minutes`` minutes.

    ``minute_count`` rows land at ``offset_minutes + i * step_minutes``
    after local midnight in ``tz_name``; ``step_minutes=3`` models SFMTA's
    VehiclePositions cadence (every 3rd 60 s tick). ``collected_at`` (the
    poll time the #270 gap leg reads) equals ``timestamp``. ``skip`` is a
    ``(first, last)`` minute-offset range (inclusive) left empty — a feed
    outage.
    """
    start_utc, _ = local_day_bounds_utc(service_date, tz_name)
    for i in range(minute_count):
        minute = offset_minutes + i * step_minutes
        if skip is not None and skip[0] <= minute <= skip[1]:
            continue
        ts = start_utc + timedelta(minutes=minute)
        db.execute(
            text(
                "INSERT INTO vehicle_positions "
                "(vehicle_id, latitude, longitude, timestamp, collected_at) "
                "VALUES (:v, :lat, :lon, :ts, :ts)"
            ),
            {"v": f"v-{i}", "lat": 38.9, "lon": -77.0, "ts": ts},
        )
    db.flush()


def _insert_trip_update_state_minutes(
    db,
    service_date: date,
    minute_count: int,
    offset_minutes: int = 0,
    step_minutes: int = 1,
    tz_name: str = "America/New_York",
    state_service_date: date | None = None,
    skip: tuple[int, int] | None = None,
):
    """Insert one trip_update_state row every ``step_minutes`` minutes.

    Each row's ``final_snapshot_ts`` lands at ``offset_minutes + i *
    step_minutes`` after local midnight in ``tz_name`` (NOTES-104: the
    poll-time signal available in every data-arrival regime).
    ``state_service_date`` overrides the row's ``service_date`` column
    (default: ``service_date``) so tests can show the coverage query keys
    on the snapshot timestamp alone, not the row's service date. ``skip``
    is a ``(first, last)`` minute-offset range (inclusive) left empty.
    """
    start_utc, _ = local_day_bounds_utc(service_date, tz_name)
    sd = state_service_date or service_date
    for i in range(minute_count):
        minute = offset_minutes + i * step_minutes
        if skip is not None and skip[0] <= minute <= skip[1]:
            continue
        ts = start_utc + timedelta(minutes=minute)
        db.execute(
            text(
                "INSERT INTO trip_update_state "
                "(trip_id, stop_sequence, service_date, stop_id, final_snapshot_ts) "
                "VALUES (:t, 1, :sd, 's-1', :ts)"
            ),
            {"t": f"t-{sd.isoformat()}-{offset_minutes}-{i}", "sd": sd, "ts": ts},
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


def test_coverage_full_day_from_trip_update_state_alone(pg_session):
    """NOTES-104 replayed-only regime: a date that arrived purely via
    ``replay_archive_to_state.py`` has no heartbeats and no positions, only
    ``trip_update_state`` rows. Their ``final_snapshot_ts`` minute-buckets
    alone must be able to certify the day complete.
    """
    _insert_trip_update_state_minutes(pg_session, TEST_DATE, minute_count=1440)
    assert coverage_pct_for_date(pg_session, TEST_DATE) == pytest.approx(1.0)
    assert is_date_sufficiently_complete(pg_session, TEST_DATE) is True


def test_coverage_unions_all_three_signals(pg_session):
    """Three disjoint 480-minute stretches, one per signal, union to a full day."""
    _insert_heartbeat_minutes(pg_session, TEST_DATE, minute_count=480, offset_minutes=0)
    _insert_vehicle_position_minutes(pg_session, TEST_DATE, minute_count=480, offset_minutes=480)
    _insert_trip_update_state_minutes(pg_session, TEST_DATE, minute_count=480, offset_minutes=960)
    assert coverage_pct_for_date(pg_session, TEST_DATE) == pytest.approx(1.0)


def test_trip_update_state_leg_keys_on_snapshot_time_not_service_date(pg_session):
    """The coverage leg is a plain range scan on ``final_snapshot_ts``
    (indexed): a past-midnight owl snapshot stamped with the previous
    service date still counts toward the clock-day it landed in. The
    *signal* test, by contrast, keys on the row's own service date
    (issue #272) — rows that belong to another date do not make this date's
    trip-update signal "present".
    """
    _insert_trip_update_state_minutes(
        pg_session,
        TEST_DATE,
        minute_count=100,
        state_service_date=TEST_DATE - timedelta(days=1),
    )
    assert coverage_pct_for_date(pg_session, TEST_DATE) == pytest.approx(100 / 1440)
    assert has_trip_update_signal(pg_session, TEST_DATE) is False
    assert has_trip_update_signal(pg_session, TEST_DATE - timedelta(days=1)) is True


def test_has_trip_update_signal_ignores_next_dates_rows_in_this_window(pg_session):
    """Issue #272: SFMTA 9/29 was demoted because the 9/30 replay left a few
    rows (service_date 9/30) polled inside 9/29's evening window after 9/29's
    own state had been deleted. Those rows must not count as 9/29's signal;
    a row whose service_date IS the date does, wherever it was polled."""
    _insert_trip_update_state_minutes(
        pg_session,
        TEST_DATE,
        minute_count=5,
        offset_minutes=1430,
        state_service_date=TEST_DATE + timedelta(days=1),
    )
    assert has_trip_update_signal(pg_session, TEST_DATE) is False
    _insert_trip_update_state_minutes(
        pg_session, TEST_DATE + timedelta(days=2), minute_count=1, state_service_date=TEST_DATE
    )
    assert has_trip_update_signal(pg_session, TEST_DATE) is True


def test_has_trip_update_signal_false_when_no_rows_in_window(pg_session):
    """No trip_update_state rows for the date → no signal, even if
    heartbeats / positions are present."""
    _insert_heartbeat_minutes(pg_session, TEST_DATE, minute_count=1440)
    assert has_trip_update_signal(pg_session, TEST_DATE) is False


def test_resolve_keeps_earned_complete_when_tu_signal_pruned(pg_session):
    """Retention rule (NOTES-104): a date previously stamped complete, now
    measuring below threshold ONLY because its trip_update_state rows are
    gone, keeps the prior stamp and prior coverage and reports ``kept``."""
    cfg = load_agency_config("sfmta")
    tz = "America/Los_Angeles"
    _insert_vehicle_position_minutes(
        pg_session, TEST_DATE, minute_count=480, step_minutes=3, tz_name=tz
    )

    quality, pct, kept = resolve_data_quality(
        pg_session,
        TEST_DATE,
        threshold=agency_coverage_threshold(cfg),
        tz_name=tz,
        prior=("complete", 0.731),
    )
    assert (quality, pct, kept) == ("complete", 0.731, True)

    # A prior with NULL stored coverage stays NULL: the keep path never
    # substitutes a freshly measured, signal-less number.
    assert resolve_data_quality(
        pg_session,
        TEST_DATE,
        threshold=agency_coverage_threshold(cfg),
        tz_name=tz,
        prior=("complete", None),
    ) == ("complete", None, True)


def test_resolve_demotes_when_tu_signal_present_but_thin(pg_session):
    """A genuine outage: trip_update_state rows exist but cover little of
    the day. The prior 'complete' does not protect it; with every input
    present the coverage is re-measured and both legs name the cause."""
    cfg = load_agency_config("sfmta")
    tz = "America/Los_Angeles"
    _insert_vehicle_position_minutes(
        pg_session, TEST_DATE, minute_count=480, step_minutes=3, tz_name=tz
    )
    _insert_trip_update_state_minutes(pg_session, TEST_DATE, minute_count=120, tz_name=tz)

    decision = assess_data_quality(
        pg_session,
        TEST_DATE,
        threshold=agency_coverage_threshold(cfg),
        tz_name=tz,
        prior=("complete", 0.731),
        gap_policy=cfg.completeness,
    )
    assert decision.data_quality == "partial" and not decision.kept
    assert decision.coverage_measured
    assert decision.coverage_pct == pytest.approx((480 + 80) / 1440)
    assert any(r.startswith("coverage") for r in decision.reasons)
    assert any(r.startswith("trip_update gap") for r in decision.reasons)


def test_resolve_never_promotes_via_prior_and_ignores_prior_when_complete(pg_session):
    """A prior 'partial' cannot be upgraded while an input is missing (here
    VP), however good the trip-update leg looks; once every leg is
    assessable and passes, the day is complete regardless of the prior."""
    cfg = load_agency_config("sfmta")
    tz = "America/Los_Angeles"
    thr = agency_coverage_threshold(cfg)
    assert resolve_data_quality(pg_session, TEST_DATE, thr, tz, prior=("partial", 0.3)) == (
        "partial",
        0.3,
        True,
    )

    _insert_trip_update_state_minutes(pg_session, TEST_DATE, minute_count=1440, tz_name=tz)
    assert resolve_data_quality(pg_session, TEST_DATE, thr, tz, prior=("partial", 0.3)) == (
        "partial",
        0.3,
        True,
    )

    _insert_vehicle_position_minutes(
        pg_session, TEST_DATE, minute_count=480, step_minutes=3, tz_name=tz
    )
    quality, pct, kept = resolve_data_quality(
        pg_session, TEST_DATE, thr, tz, prior=("partial", 0.3)
    )
    assert (quality, kept) == ("complete", False)
    assert pct == pytest.approx(1.0)


def test_sfmta_cadence_day_clears_threshold_only_with_trip_update_signal(pg_session):
    """The NOTES-104 addendum case: a healthy SFMTA day on the laptop.

    VehiclePositions every 3rd minute alone covers exactly 1/3 of the day,
    below SFMTA's cadence-aware threshold (0.8 × 4/6 ≈ 0.533) — which is
    why every SFMTA date was stamped 'partial' before the trip_update_state
    leg existed. Adding TripUpdates every 2nd minute lifts the union to the
    minutes divisible by 2 or 3 → 2/3, clearing the threshold. Uses the
    Pacific day window so the test exercises the same tz path production
    does.
    """
    cfg = load_agency_config("sfmta")
    threshold = agency_coverage_threshold(cfg)
    tz = "America/Los_Angeles"

    _insert_vehicle_position_minutes(
        pg_session, TEST_DATE, minute_count=480, step_minutes=3, tz_name=tz
    )
    vp_only = coverage_pct_for_date(pg_session, TEST_DATE, tz_name=tz)
    assert vp_only == pytest.approx(1 / 3)
    assert is_date_sufficiently_complete(pg_session, TEST_DATE, threshold, tz_name=tz) is False

    _insert_trip_update_state_minutes(
        pg_session, TEST_DATE, minute_count=720, step_minutes=2, tz_name=tz
    )
    with_tu = coverage_pct_for_date(pg_session, TEST_DATE, tz_name=tz)
    assert with_tu == pytest.approx(2 / 3)
    assert is_date_sufficiently_complete(pg_session, TEST_DATE, threshold, tz_name=tz) is True


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
        vp_archive_dir="",
        pid_file="",
        heartbeat_name="",
        database_url_env="X_DATABASE_URL",
        healthcheck_url_env="X_HEALTHCHECK_URL",
        s3_bucket="",
        s3_tu_prefix="",
        s3_vp_prefix="",
    )
    assert agency_coverage_threshold(cfg) == pytest.approx(MIN_COVERAGE_FOR_MATERIALIZATION * 0.25)


# ---------------------------------------------------------------------------
# Issue #270: per-feed service-hours gap leg
# ---------------------------------------------------------------------------

WMATA = load_agency_config("wmata")
SFMTA = load_agency_config("sfmta")
EASTERN_TZ = "America/New_York"
PACIFIC_TZ = "America/Los_Angeles"


def _dst_days(year: int, tz_name: str) -> tuple[date, date]:
    """Return (spring-forward, fall-back) local dates for ``year`` in ``tz_name``."""
    zone = ZoneInfo(tz_name)
    found = []
    d = date(year, 1, 1)
    while d.year == year:
        a = datetime.combine(d, time(0), tzinfo=zone).utcoffset()
        b = datetime.combine(d + timedelta(days=1), time(0), tzinfo=zone).utcoffset()
        if a != b:
            found.append(d)
        d += timedelta(days=1)
    return found[0], found[1]


@pytest.mark.smoke
def test_longest_gap_counts_window_edges_and_ignores_outside_points():
    """The edges count (a feed that stops for good shows a gap to the window
    end); points outside the window are ignored; no points = whole window."""
    s = datetime(2099, 1, 15, 10, 0)
    e = s + timedelta(hours=10)
    pts = [s + timedelta(minutes=m) for m in (5, 10, 100)] + [s - timedelta(hours=1)]
    gap, g0, g1 = longest_gap(pts, s, e)
    assert gap == pytest.approx(500.0)
    assert (g0, g1) == (s + timedelta(minutes=100), e)
    assert longest_gap([], s, e)[0] == pytest.approx(600.0)
    # SQLite raw SQL hands timestamps back as strings.
    assert longest_gap([(s + timedelta(minutes=300)).isoformat(sep=" ")], s, e)[0] == 300.0


@pytest.mark.smoke
def test_service_window_is_agency_local_and_dst_safe():
    """05:00–24:00 local maps to the right UTC instants in each zone, on an
    ordinary day and on both DST transition days (the 02:00 transition lies
    outside the default window, so it stays 19 real hours)."""
    start, end = service_window_utc(TEST_DATE, PACIFIC_TZ)
    assert (start, end) == (datetime(2099, 1, 15, 13, 0), datetime(2099, 1, 16, 8, 0))
    start, end = service_window_utc(TEST_DATE, EASTERN_TZ)
    assert (start, end) == (datetime(2099, 1, 15, 10, 0), datetime(2099, 1, 16, 5, 0))
    for tz in (EASTERN_TZ, PACIFIC_TZ):
        spring, fall = _dst_days(2099, tz)
        for d in (spring, fall):
            start, end = service_window_utc(d, tz)
            assert end - start == timedelta(hours=19)
        # A whole-day window shows the DST day's real length.
        whole = CompletenessPolicy(service_start_hour=0, service_end_hour=24)
        s0, e0 = service_window_utc(spring, tz, whole)
        s1, e1 = service_window_utc(fall, tz, whole)
        assert (e0 - s0, e1 - s1) == (timedelta(hours=23), timedelta(hours=25))


def test_single_trip_update_outage_partial_although_coverage_passes(pg_session):
    """SFMTA 2026-10-21 shape: trip updates silent 10:50–16:42 local while
    VP keeps polling every 3 minutes. Coverage still clears SFMTA's 0.533
    (the flaw #270 describes), but the trip-update gap leg stamps the date
    partial and says why."""
    thr = agency_coverage_threshold(SFMTA)
    _insert_vehicle_position_minutes(
        pg_session, TEST_DATE, minute_count=480, step_minutes=3, tz_name=PACIFIC_TZ
    )
    _insert_trip_update_state_minutes(
        pg_session, TEST_DATE, minute_count=1440, tz_name=PACIFIC_TZ, skip=(650, 1001)
    )

    decision = assess_data_quality(pg_session, TEST_DATE, thr, PACIFIC_TZ, None, SFMTA.completeness)

    assert decision.measured_coverage >= thr
    assert decision.data_quality == "partial"
    assert decision.trip_update_gap.longest_gap_min == pytest.approx(353.0)
    assert decision.vehicle_position_gap.longest_gap_min == pytest.approx(3.0)
    assert len(decision.reasons) == 1 and decision.reasons[0].startswith("trip_update gap 353")
    assert "partial because" in decision.describe()


def test_single_vehicle_position_outage_partial_at_wmata(pg_session):
    """At WMATA either feed alone reads ~1.00 coverage, so a VP-only outage
    was invisible; the VP gap leg catches it."""
    _insert_trip_update_state_minutes(pg_session, TEST_DATE, minute_count=1440)
    _insert_vehicle_position_minutes(pg_session, TEST_DATE, minute_count=1440, skip=(600, 719))

    decision = assess_data_quality(
        pg_session, TEST_DATE, 0.80, EASTERN_TZ, None, WMATA.completeness
    )

    assert decision.measured_coverage == pytest.approx(1.0)
    assert decision.data_quality == "partial"
    assert decision.vehicle_position_gap.longest_gap_min == pytest.approx(121.0)
    assert decision.reasons[0].startswith("vehicle_position gap 121")


def test_gap_limit_is_strict_and_per_agency_configurable(pg_session):
    """A gap of exactly the limit passes; the limit comes from the policy."""
    _insert_trip_update_state_minutes(pg_session, TEST_DATE, minute_count=1440)
    _insert_vehicle_position_minutes(pg_session, TEST_DATE, minute_count=1440, skip=(600, 688))

    ninety = assess_data_quality(pg_session, TEST_DATE, 0.80, EASTERN_TZ, None, WMATA.completeness)
    assert ninety.vehicle_position_gap.longest_gap_min == pytest.approx(90.0)
    assert ninety.data_quality == "complete" and not ninety.reasons

    sixty = assess_data_quality(
        pg_session, TEST_DATE, 0.80, EASTERN_TZ, None, CompletenessPolicy(feed_gap_max_minutes=60)
    )
    assert sixty.data_quality == "partial"


def test_gap_outside_service_hours_is_ignored(pg_session):
    """A 4-hour overnight silence (00:00–04:00 local) is outside 05–24 and
    does not stamp the date partial."""
    _insert_trip_update_state_minutes(pg_session, TEST_DATE, minute_count=1440, skip=(0, 239))
    _insert_vehicle_position_minutes(pg_session, TEST_DATE, minute_count=1440, skip=(0, 239))

    decision = assess_data_quality(pg_session, TEST_DATE, 0.80, EASTERN_TZ, None)

    assert decision.measured_coverage == pytest.approx(1200 / 1440)  # still >= 0.80
    assert decision.data_quality == "complete" and not decision.reasons
    assert decision.trip_update_gap.longest_gap_min <= 1.0
    assert decision.vehicle_position_gap.longest_gap_min <= 1.0


def test_dst_day_with_full_data_is_complete(pg_session):
    """A spring-forward day (23 h local) with both feeds polling every minute
    shows no phantom gap from the transition."""
    spring, _ = _dst_days(2099, EASTERN_TZ)
    _insert_trip_update_state_minutes(pg_session, spring, minute_count=1380)
    _insert_vehicle_position_minutes(pg_session, spring, minute_count=1380)

    decision = assess_data_quality(pg_session, spring, 0.80, EASTERN_TZ, None)

    assert decision.measured_coverage == pytest.approx(1.0)
    assert decision.data_quality == "complete"
    assert decision.trip_update_gap.longest_gap_min <= 1.0
    assert decision.vehicle_position_gap.longest_gap_min <= 1.0


def test_vp_leg_not_assessable_when_a_utc_day_is_missing(pg_session):
    """VP is pruned in whole UTC days. If one UTC day the window overlaps has
    no rows, the leg is not assessable: it neither stamps partial nor lets a
    prior partial be upgraded, and the decision says so."""
    # Rows only for the first local hours (UTC TEST_DATE); the window's
    # second UTC day (TEST_DATE + 1, from 19:00 local) has none.
    _insert_trip_update_state_minutes(pg_session, TEST_DATE, minute_count=1440)
    _insert_vehicle_position_minutes(pg_session, TEST_DATE, minute_count=600)

    fresh = assess_data_quality(pg_session, TEST_DATE, 0.80, EASTERN_TZ, None)
    assert not fresh.vehicle_position_gap.assessable
    assert fresh.data_quality == "complete"  # first stamp: decided by assessed legs
    assert any(n.startswith("vehicle_position") for n in fresh.not_assessed)

    kept = assess_data_quality(pg_session, TEST_DATE, 0.80, EASTERN_TZ, ("partial", 0.42))
    assert (kept.data_quality, kept.coverage_pct, kept.kept) == ("partial", 0.42, True)
    assert not kept.coverage_measured


def test_assessable_vp_gap_demotes_even_when_tu_pruned_but_keeps_coverage(pg_session):
    """A real VP gap is evidence even after the trip-update rows are gone:
    the date is demoted, but its stored coverage is carried over rather than
    replaced by a number measured without the trip-update leg."""
    _insert_vehicle_position_minutes(pg_session, TEST_DATE, minute_count=1440, skip=(600, 799))

    decision = assess_data_quality(
        pg_session, TEST_DATE, 0.80, EASTERN_TZ, ("complete", 0.97), WMATA.completeness
    )

    assert decision.data_quality == "partial" and not decision.kept
    assert not decision.coverage_measured and decision.coverage_pct == 0.97
    assert decision.reasons[0].startswith("vehicle_position gap")
    assert resolve_data_quality(
        pg_session, TEST_DATE, 0.80, EASTERN_TZ, ("complete", 0.97), WMATA.completeness
    ) == ("partial", 0.97, True)


# ---------------------------------------------------------------------------
# Issue #272: a two-run backfill must not re-stamp the boundary date
# ---------------------------------------------------------------------------

BOUNDARY = date(2020, 3, 10)  # older than any cleanup cutoff, predates real data


def test_two_run_backfill_leaves_boundary_stamp_and_coverage_unchanged(pg_session):
    """Run 1 derives BOUNDARY and stamps it complete; its end-of-run cleanup
    (the real ``run_cleanup``, 7-day window) deletes BOUNDARY's state. Run 2
    starts at BOUNDARY+1: the replay leaves a few BOUNDARY+1 rows polled in
    BOUNDARY's evening window, and the derive re-aggregates BOUNDARY as D-1.
    Its stamp and stored coverage must not change (SFMTA 9/29, issue #272).
    """
    from pipelines.cleanup_trip_update_state import run_cleanup
    from src.models import SystemMetricsDaily
    from src.system_metrics import upsert_system_metrics_for_date

    thr = agency_coverage_threshold(SFMTA)
    kwargs = {
        "tz_name": PACIFIC_TZ,
        "completeness_threshold": thr,
        "agency": "sfmta",
        "gap_policy": SFMTA.completeness,
    }

    # Run 1: VP (every 3 min, two days so both UTC days are present) + state.
    for d in (BOUNDARY, BOUNDARY + timedelta(days=1)):
        _insert_vehicle_position_minutes(
            pg_session, d, minute_count=480, step_minutes=3, tz_name=PACIFIC_TZ
        )
    _insert_trip_update_state_minutes(
        pg_session, BOUNDARY, minute_count=720, step_minutes=2, tz_name=PACIFIC_TZ
    )
    first = upsert_system_metrics_for_date(pg_session, BOUNDARY, **kwargs)
    assert first["data_quality"] == "complete"
    row = pg_session.get(SystemMetricsDaily, BOUNDARY.isoformat())
    stored = (row.data_quality, row.coverage_pct)
    assert stored[1] == pytest.approx(2 / 3)

    # End of run 1: the 7-day cleanup removes BOUNDARY's state.
    run_cleanup(pg_session, retention_days=7, tz_name=PACIFIC_TZ)
    assert has_trip_update_signal(pg_session, BOUNDARY) is False

    # Run 2: BOUNDARY+1's replay, with a few rows inside BOUNDARY's evening.
    _insert_trip_update_state_minutes(
        pg_session,
        BOUNDARY,
        minute_count=10,
        offset_minutes=1430,
        tz_name=PACIFIC_TZ,
        state_service_date=BOUNDARY + timedelta(days=1),
    )
    _insert_trip_update_state_minutes(
        pg_session,
        BOUNDARY + timedelta(days=1),
        minute_count=720,
        step_minutes=2,
        tz_name=PACIFIC_TZ,
    )
    # Without the fix this is exactly the demotion: thin, "signal present".
    assert coverage_pct_for_date(pg_session, BOUNDARY, PACIFIC_TZ) < thr

    upsert_system_metrics_for_date(pg_session, BOUNDARY, **kwargs)  # D-1 re-aggregation

    row = pg_session.get(SystemMetricsDaily, BOUNDARY.isoformat())
    assert (row.data_quality, row.coverage_pct) == stored
