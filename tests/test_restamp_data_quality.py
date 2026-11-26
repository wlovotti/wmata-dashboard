"""
Tests for ``pipelines/restamp_data_quality.py`` (NOTES-104 re-stamp tool).

Uses ``pg_session`` for the same reason ``test_data_completeness.py`` does:
the coverage query relies on Postgres ``date_trunc``. Rows target a
far-future date so they can't collide with real materialized history, and
the fixture's SAVEPOINT rollback discards them.
"""

from datetime import date, timedelta

import pytest
from sqlalchemy import text

from pipelines.restamp_data_quality import apply_restamp, plan_restamp
from src.agency_config import load_agency_config
from src.models import RouteMetricsDailyOverlay, SystemMetricsDaily
from src.timezones import local_day_bounds_utc

TEST_DATE = date(2099, 3, 10)
SFMTA = load_agency_config("sfmta")


def _seed_partial_day(db, service_date: date, overlay_routes: int = 3) -> None:
    """Materialize one system row + ``overlay_routes`` overlay rows stamped 'partial'."""
    iso = service_date.isoformat()
    db.add(
        SystemMetricsDaily(
            service_date=iso, otp_percentage=70.0, data_quality="partial", coverage_pct=0.33
        )
    )
    for i in range(overlay_routes):
        db.add(
            RouteMetricsDailyOverlay(
                route_id=f"R{i}",
                service_date=iso,
                day_type="weekday",
                data_quality="partial",
                coverage_pct=0.33,
            )
        )
    db.flush()


def _seed_trip_update_minutes(db, service_date: date, minute_count: int, tz_name: str) -> None:
    """One trip_update_state row per minute from local midnight in ``tz_name``."""
    start_utc, _ = local_day_bounds_utc(service_date, tz_name)
    for i in range(minute_count):
        db.execute(
            text(
                "INSERT INTO trip_update_state "
                "(trip_id, stop_sequence, service_date, stop_id, final_snapshot_ts) "
                "VALUES (:t, 1, :sd, 's', :ts)"
            ),
            {"t": f"restamp-{i}", "sd": service_date, "ts": start_utc + timedelta(minutes=i)},
        )
    db.flush()


def _seed_vehicle_positions(
    db, service_date: date, tz_name: str, skip: tuple[int, int] | None = None
) -> None:
    """One VP row every 3 minutes (SFMTA cadence) over the local day and the next.

    Two local days so every UTC day the service window overlaps has rows
    (the #270 VP gap leg is otherwise not assessable). ``skip`` is a
    ``(first, last)`` minute-offset range of ``service_date`` left empty.
    """
    start_utc, _ = local_day_bounds_utc(service_date, tz_name)
    for minute in range(0, 2 * 1440, 3):
        if skip is not None and skip[0] <= minute <= skip[1]:
            continue
        ts = start_utc + timedelta(minutes=minute)
        db.execute(
            text(
                "INSERT INTO vehicle_positions "
                "(vehicle_id, latitude, longitude, timestamp, collected_at) "
                "VALUES ('v', 37.7, -122.4, :ts, :ts)"
            ),
            {"ts": ts},
        )
    db.flush()


def _stamps(db, iso: str) -> tuple[tuple[str, float | None], set[tuple[str, float | None]]]:
    """Return (system stamp, set of overlay stamps) for ``iso``."""
    sysrow = db.get(SystemMetricsDaily, iso)
    overlay = (
        db.query(RouteMetricsDailyOverlay)
        .filter(RouteMetricsDailyOverlay.service_date == iso)
        .all()
    )
    return (sysrow.data_quality, sysrow.coverage_pct), {
        (r.data_quality, r.coverage_pct) for r in overlay
    }


def test_plan_reports_flip_and_absent_without_writing(pg_session):
    """A partial-stamped date with full trip-update coverage plans a FLIP to
    complete; a date with no system row is reported absent. Planning writes
    nothing."""
    _seed_partial_day(pg_session, TEST_DATE)
    _seed_trip_update_minutes(pg_session, TEST_DATE, 1440, SFMTA.timezone)
    _seed_vehicle_positions(pg_session, TEST_DATE, SFMTA.timezone)

    rows = plan_restamp(pg_session, SFMTA, TEST_DATE, TEST_DATE + timedelta(days=1))

    assert [r.service_date for r in rows] == [TEST_DATE, TEST_DATE + timedelta(days=1)]
    flip, absent = rows
    assert flip.old_quality == "partial" and flip.new_quality == "complete" and flip.changed
    assert flip.new_coverage == pytest.approx(1.0)
    assert flip.overlay_rows == 3
    assert absent.absent and not absent.changed

    # Dry-run: nothing touched.
    system_stamp, overlay_stamps = _stamps(pg_session, TEST_DATE.isoformat())
    assert system_stamp == ("partial", 0.33)
    assert overlay_stamps == {("partial", 0.33)}


def test_apply_rewrites_both_tables_and_skips_absent(pg_session):
    """Applying writes the new stamp to the system row and every overlay row
    for the date, leaves metric columns alone, and skips absent dates."""
    _seed_partial_day(pg_session, TEST_DATE)
    _seed_trip_update_minutes(pg_session, TEST_DATE, 1440, SFMTA.timezone)
    _seed_vehicle_positions(pg_session, TEST_DATE, SFMTA.timezone)
    rows = plan_restamp(pg_session, SFMTA, TEST_DATE, TEST_DATE + timedelta(days=1))

    written = apply_restamp(pg_session, rows)

    assert written == 1
    system_stamp, overlay_stamps = _stamps(pg_session, TEST_DATE.isoformat())
    assert system_stamp == ("complete", pytest.approx(1.0))
    assert len(overlay_stamps) == 1
    ((overlay_quality, overlay_coverage),) = overlay_stamps
    assert overlay_quality == "complete"
    assert overlay_coverage == pytest.approx(1.0)
    assert pg_session.get(SystemMetricsDaily, TEST_DATE.isoformat()).otp_percentage == 70.0
    assert pg_session.get(SystemMetricsDaily, (TEST_DATE + timedelta(days=1)).isoformat()) is None


def test_plan_keeps_earned_complete_when_tu_signal_pruned(pg_session):
    """Retention rule: a complete-stamped date whose trip_update_state rows
    are gone measures VP-only (or zero) but is reported ``kept``, not
    demoted, and apply leaves it untouched."""
    iso = TEST_DATE.isoformat()
    pg_session.add(SystemMetricsDaily(service_date=iso, data_quality="complete", coverage_pct=0.99))
    pg_session.add(
        RouteMetricsDailyOverlay(
            route_id="R0",
            service_date=iso,
            day_type="weekday",
            data_quality="complete",
            coverage_pct=0.99,
        )
    )
    pg_session.flush()

    (row,) = plan_restamp(pg_session, SFMTA, TEST_DATE, TEST_DATE)

    assert row.kept and not row.changed
    assert (row.new_quality, row.new_coverage) == ("complete", 0.99)
    assert apply_restamp(pg_session, [row]) == 0  # kept dates are not written
    assert _stamps(pg_session, iso) == (("complete", 0.99), {("complete", 0.99)})


def test_kept_row_with_null_coverage_is_left_null(pg_session):
    """Rows that predate the coverage_pct column store NULL. A kept stamp
    must not have a freshly measured (signal-less) number written over
    that NULL — PR #244 review round 2."""
    iso = TEST_DATE.isoformat()
    pg_session.add(SystemMetricsDaily(service_date=iso, data_quality="complete", coverage_pct=None))
    pg_session.flush()

    (row,) = plan_restamp(pg_session, SFMTA, TEST_DATE, TEST_DATE)

    assert row.kept and row.new_coverage is None
    assert apply_restamp(pg_session, [row]) == 0
    assert pg_session.get(SystemMetricsDaily, iso).coverage_pct is None


def test_plan_and_apply_handle_overlay_only_dates(pg_session):
    """A date with overlay rows but no system row (system pipeline failed,
    overlay succeeded) is not 'absent': its overlay rows are re-stamped."""
    iso = TEST_DATE.isoformat()
    pg_session.add(
        RouteMetricsDailyOverlay(
            route_id="R0",
            service_date=iso,
            day_type="weekday",
            data_quality="partial",
            coverage_pct=0.33,
        )
    )
    pg_session.flush()
    _seed_trip_update_minutes(pg_session, TEST_DATE, 1440, SFMTA.timezone)
    _seed_vehicle_positions(pg_session, TEST_DATE, SFMTA.timezone)

    (row,) = plan_restamp(pg_session, SFMTA, TEST_DATE, TEST_DATE)

    assert not row.absent and not row.has_system_row and row.overlay_rows == 1
    assert row.changed and row.new_quality == "complete"
    assert apply_restamp(pg_session, [row]) == 1
    overlay = pg_session.query(RouteMetricsDailyOverlay).filter_by(service_date=iso).one()
    assert overlay.data_quality == "complete"
    assert pg_session.get(SystemMetricsDaily, iso) is None


def test_plan_keeps_partial_when_coverage_stays_low(pg_session):
    """A genuinely thin day (trip-update rows present but sparse) stays
    partial -- the tool re-evaluates, it does not blanket-promote."""
    _seed_partial_day(pg_session, TEST_DATE, overlay_routes=1)
    _seed_trip_update_minutes(pg_session, TEST_DATE, 200, SFMTA.timezone)  # ~14% < 53%

    (row,) = plan_restamp(pg_session, SFMTA, TEST_DATE, TEST_DATE)

    assert row.new_quality == "partial" and not row.changed


def test_apply_never_rewrites_coverage_of_an_unchanged_stamp(pg_session):
    """Issue #272: a date whose stamp does not flip is not written at all,
    even when every input is present and the re-measured coverage differs
    from the stored one."""
    _seed_partial_day(pg_session, TEST_DATE, overlay_routes=2)
    _seed_trip_update_minutes(pg_session, TEST_DATE, 200, SFMTA.timezone)
    _seed_vehicle_positions(pg_session, TEST_DATE, SFMTA.timezone)

    (row,) = plan_restamp(pg_session, SFMTA, TEST_DATE, TEST_DATE)
    assert row.new_quality == "partial" and not row.changed
    assert row.coverage_measured and row.new_coverage != pytest.approx(0.33)

    assert apply_restamp(pg_session, [row]) == 0
    assert _stamps(pg_session, TEST_DATE.isoformat()) == (
        ("partial", 0.33),
        {("partial", 0.33)},
    )


def test_pruned_boundary_date_is_kept_not_zeroed(pg_session):
    """SFMTA 9/29 shape: the date's own state and VP are gone, only a few of
    the next date's rows sit in its evening window. The plan keeps the
    stored stamp and apply writes nothing — the stored coverage is never
    replaced by the ~0 measured now (issue #272)."""
    iso = TEST_DATE.isoformat()
    pg_session.add(SystemMetricsDaily(service_date=iso, data_quality="partial", coverage_pct=0.336))
    pg_session.flush()
    start_utc, end_utc = local_day_bounds_utc(TEST_DATE, SFMTA.timezone)
    for i in range(5):
        pg_session.execute(
            text(
                "INSERT INTO trip_update_state "
                "(trip_id, stop_sequence, service_date, stop_id, final_snapshot_ts) "
                "VALUES (:t, 1, :sd, 's', :ts)"
            ),
            {
                "t": f"next-{i}",
                "sd": TEST_DATE + timedelta(days=1),
                "ts": end_utc - timedelta(minutes=5 - i),
            },
        )
    pg_session.flush()

    (row,) = plan_restamp(pg_session, SFMTA, TEST_DATE, TEST_DATE)

    assert row.kept and not row.changed and not row.coverage_measured
    assert (row.new_quality, row.new_coverage) == ("partial", 0.336)
    assert row.decision.measured_coverage < 0.05
    assert apply_restamp(pg_session, [row]) == 0
    assert pg_session.get(SystemMetricsDaily, iso).coverage_pct == 0.336


def test_gap_demotion_with_pruned_tu_writes_stamp_but_keeps_coverage(pg_session):
    """A complete date whose VP (still present) shows a 3-hour service-hours
    gap flips to partial (#270) even though its trip-update rows are gone,
    but only ``data_quality`` is written: the coverage could not be
    re-measured without the trip-update leg."""
    iso = TEST_DATE.isoformat()
    pg_session.add(SystemMetricsDaily(service_date=iso, data_quality="complete", coverage_pct=0.66))
    pg_session.flush()
    _seed_vehicle_positions(pg_session, TEST_DATE, SFMTA.timezone, skip=(600, 780))

    (row,) = plan_restamp(pg_session, SFMTA, TEST_DATE, TEST_DATE)

    assert row.changed and row.new_quality == "partial" and not row.coverage_measured
    assert any(r.startswith("vehicle_position gap") for r in row.decision.reasons)
    assert apply_restamp(pg_session, [row]) == 1
    assert _stamps(pg_session, iso)[0] == ("partial", 0.66)
