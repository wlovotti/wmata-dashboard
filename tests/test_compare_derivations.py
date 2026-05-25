"""Tests for pipelines.compare_old_vs_new_derivation."""

from datetime import date, datetime

import pytest
from sqlalchemy import text

from src.models import StopEvent


def _make_event(**kwargs):
    """Build a StopEvent row with sensible defaults.

    Uses a synthetic service_date (2099-05-17) that will never exist in
    production data, so that ``compare_one_day`` comparisons are not
    contaminated by pre-existing rows in a populated dev DB.
    """
    defaults = {
        "service_date": "2099-05-17",
        "trip_id": "T1",
        "route_id": "R1",
        "direction_id": 0,
        "stop_id": "S1",
        "stop_sequence": 1,
        "scheduled_arrival_ts": datetime(2099, 5, 17, 14, 5, 0),
        "scheduled_departure_ts": datetime(2099, 5, 17, 14, 5, 30),
        "observed_arrival_ts": datetime(2099, 5, 17, 14, 6, 30),
        "deviation_sec": 90,
        "source": "trip_update",
        "schedule_relationship": "SCHEDULED",
        "match_distance_m": None,
        "derived_at": datetime(2099, 5, 17, 14, 10, 0),
    }
    defaults.update(kwargs)
    return StopEvent(**defaults)


@pytest.mark.integration
def test_perfect_match_reports_100_percent_agreement(pg_session):
    """Identical rows in both tables yield 100% agreement."""
    from pipelines.compare_old_vs_new_derivation import compare_one_day

    # Ensure side table exists (idempotent CREATE).
    pg_session.execute(
        text("CREATE TABLE IF NOT EXISTS stop_events_v2 (LIKE stop_events INCLUDING ALL)")
    )
    pg_session.add(_make_event())
    # Explicit flush: Session.execute(text(...)) does NOT autoflush, so the
    # raw INSERT...SELECT below would not see the pending stop_events row.
    pg_session.flush()
    pg_session.execute(
        text("INSERT INTO stop_events_v2 SELECT * FROM stop_events WHERE trip_id = 'T1'")
    )
    pg_session.commit()

    result = compare_one_day(pg_session, target_date=date(2099, 5, 17))
    assert result["agreement_pct"] == 100.0
    assert result["diverging_routes"] == []


@pytest.mark.integration
def test_observed_arrival_mismatch_lowers_agreement(pg_session):
    """Different observed_arrival_ts in v2 lowers agreement below 100%."""
    from pipelines.compare_old_vs_new_derivation import compare_one_day

    pg_session.execute(
        text("CREATE TABLE IF NOT EXISTS stop_events_v2 (LIKE stop_events INCLUDING ALL)")
    )
    pg_session.add(_make_event())
    pg_session.commit()
    pg_session.execute(
        text("""
        INSERT INTO stop_events_v2 (service_date, trip_id, route_id, direction_id,
            stop_id, stop_sequence, scheduled_arrival_ts, scheduled_departure_ts,
            observed_arrival_ts, deviation_sec, source, schedule_relationship,
            match_distance_m, derived_at)
        VALUES ('2099-05-17', 'T1', 'R1', 0, 'S1', 1,
            '2099-05-17 14:05:00', '2099-05-17 14:05:30',
            '2099-05-17 14:08:00', 180, 'trip_update', 'SCHEDULED', NULL, NOW())
    """)
    )
    pg_session.commit()

    result = compare_one_day(pg_session, target_date=date(2099, 5, 17))
    assert result["agreement_pct"] < 100.0


@pytest.mark.integration
def test_skipped_stops_with_agreeing_nulls_report_match(pg_session):
    """SKIPPED rows (observed_arrival_ts=NULL, deviation_sec=NULL) in both
    tables must be counted as matched. Bare `=` comparison on NULL columns
    would silently misreport these as mismatched, capping agreement at
    ~91% even when the pipelines fully agree."""
    from pipelines.compare_old_vs_new_derivation import compare_one_day

    pg_session.execute(
        text("CREATE TABLE IF NOT EXISTS stop_events_v2 (LIKE stop_events INCLUDING ALL)")
    )
    pg_session.add(
        _make_event(
            schedule_relationship="SKIPPED",
            observed_arrival_ts=None,
            deviation_sec=None,
        )
    )
    # Explicit flush: Session.execute(text(...)) does NOT autoflush.
    pg_session.flush()
    pg_session.execute(
        text("INSERT INTO stop_events_v2 SELECT * FROM stop_events WHERE trip_id = 'T1'")
    )
    pg_session.commit()

    result = compare_one_day(pg_session, target_date=date(2099, 5, 17))
    assert result["agreement_pct"] == 100.0
    assert result["diverging_routes"] == []
    assert result["v2_only_rows"] == 0
