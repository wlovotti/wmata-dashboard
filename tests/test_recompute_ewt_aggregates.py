"""Tests for scripts/recompute_ewt_aggregates.py (issue #269 EWT-only recompute).

Uses ``pg_session`` (scratch Postgres, rolled back per test): the write path
goes through ``src.upsert_helpers.upsert_rows`` (``INSERT ... ON CONFLICT``),
which SQLite cannot run. The metric computes are injected fakes, so the tests
exercise the planning, refusal and write logic without seeding GTFS.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from scripts import recompute_ewt_aggregates as tool
from src.agency_config import load_agency_config
from src.models import RouteMetricsDailyOverlay, StopEvent, SystemMetricsDaily

D = date(2026, 10, 14)
ISO = D.isoformat()
OLD_AT = datetime(2026, 10, 15, 3, 0, 0)
WMATA = load_agency_config("wmata")

STORED_EWT = {
    "ewt_obs_sum_h": 10_000.0,
    "ewt_obs_sum_h_sq": 9.0e8,  # includes one huge pair
    "ewt_n_observed_headways": 20,
    "ewt_sched_sum_h": 9_000.0,
    "ewt_sched_sum_h_sq": 5.0e6,
    "ewt_n_scheduled_headways": 18,
}
NEW_EWT = {
    "ewt_obs_sum_h": 9_500.0,
    "ewt_obs_sum_h_sq": 6.0e6,
    "ewt_n_observed_headways": 19,
    "ewt_sched_sum_h": 9_000.0,
    "ewt_sched_sum_h_sq": 5.0e6,
    "ewt_n_scheduled_headways": 18,
}
OTHER = {
    "otp_origin_early": 1,
    "otp_origin_on_time": 2,
    "otp_origin_late": 3,
    "otp_destination_early": 4,
    "otp_destination_on_time": 5,
    "otp_destination_late": 6,
    "otp_all_early": 7,
    "otp_all_on_time": 8,
    "otp_all_late": 9,
    "scheduled_trips": 100,
    "delivered_trips": 92,
    "bunching_count": 3,
    "bunching_total_headways": 40,
}


def _seed(db) -> None:
    """One date: a system row, two overlay rows, and one trip_update stop_event."""
    db.add(
        SystemMetricsDaily(
            service_date=ISO,
            otp_percentage=71.5,
            service_delivered_ratio=0.92032,
            ewt_seconds=220.2,
            swt_seconds=None,
            bunching_rate=0.11,
            data_quality="complete",
            coverage_pct=0.666,
            computed_at=OLD_AT,
        )
    )
    for rid in ("D50", "D10"):
        db.add(
            RouteMetricsDailyOverlay(
                route_id=rid,
                service_date=ISO,
                day_type="weekday",
                **OTHER,
                **STORED_EWT,
                data_quality="complete",
                coverage_pct=0.666,
                computed_at=OLD_AT,
            )
        )
    db.add(
        StopEvent(
            service_date=ISO,
            trip_id="T1",
            route_id="D50",
            direction_id=0,
            stop_id="S1",
            stop_sequence=1,
            source="trip_update",
            schedule_relationship="SCHEDULED",
        )
    )
    db.flush()


def _fake_overlay(routes=("D50", "D10"), other_override=None):
    """Fake overlay compute: new EWT sums, other columns as stored unless overridden."""

    def _compute(db, service_date, gtfs_snapshot_id=None, tz_name=None, agency=None):
        """Return one recomputed overlay row per route."""
        other = {**OTHER, **(other_override or {})}
        return [
            {"route_id": r, "service_date": ISO, "day_type": "weekday", **other, **NEW_EWT}
            for r in routes
        ]

    return _compute


def _fake_system(ewt=141.1, swt=439.9, sd=0.92032):
    """Fake system compute returning the given EWT/SWT and stored-equal other metrics."""

    def _compute(db, service_date, gtfs_snapshot_id=None, tz_name=None, agency=None):
        """Return the recomputed system metrics dict."""
        return {
            "otp_percentage": 71.5,
            "service_delivered_ratio": sd,
            "ewt_seconds": ewt,
            "swt_seconds": swt,
            "bunching_rate": 0.11,
        }

    return _compute


def _snapshot(db) -> tuple[dict, dict]:
    """Every column of the system row and of each overlay row, for before/after comparison."""
    db.expire_all()
    system = tool._row_dict(db.get(SystemMetricsDaily, ISO))
    overlay = {
        r.route_id: tool._row_dict(r)
        for r in db.query(RouteMetricsDailyOverlay)
        .filter(RouteMetricsDailyOverlay.service_date == ISO)
        .all()
    }
    return system, overlay


@pytest.mark.integration
def test_dry_run_plans_but_writes_nothing(pg_session):
    """Planning reports the EWT change and other-column drift, and leaves every row as stored."""
    _seed(pg_session)
    before = _snapshot(pg_session)
    plans = tool.plan_recompute(
        pg_session,
        WMATA,
        D,
        D,
        compute_overlay=_fake_overlay(other_override={"delivered_trips": 93}),
        compute_system=_fake_system(sd=0.92508),
    )
    assert [p.status for p in plans] == ["write"]
    p = plans[0]
    assert p.ewt_changes == {
        "ewt_obs_sum_h": 2,
        "ewt_obs_sum_h_sq": 2,
        "ewt_n_observed_headways": 2,
        "ewt_seconds": 1,
        "swt_seconds": 1,
    }
    # Drift outside the EWT set is reported, labelled as not written.
    assert p.other_changes == {"delivered_trips": 2, "service_delivered_ratio": 1}
    assert "NOT written" in tool.format_plan(p)
    assert _snapshot(pg_session) == before


@pytest.mark.integration
def test_apply_writes_only_ewt_columns_and_computed_at(pg_session):
    """Every non-EWT column (stamps included) stays byte-identical; EWT and computed_at move."""
    _seed(pg_session)
    sys_before, ov_before = _snapshot(pg_session)
    (plan,) = tool.plan_recompute(
        pg_session,
        WMATA,
        D,
        D,
        compute_overlay=_fake_overlay(other_override={"delivered_trips": 93}),
        compute_system=_fake_system(sd=0.92508),
    )
    now = datetime(2026, 11, 27, 12, 0, 0)
    tool.apply_plan(pg_session, plan, now=now)
    sys_after, ov_after = _snapshot(pg_session)

    written_sys = {"ewt_seconds", "swt_seconds", "computed_at"}
    assert {k: v for k, v in sys_after.items() if k not in written_sys} == {
        k: v for k, v in sys_before.items() if k not in written_sys
    }
    assert sys_after["ewt_seconds"] == pytest.approx(141.1)
    assert sys_after["swt_seconds"] == pytest.approx(439.9)
    assert sys_after["computed_at"] == now

    written_ov = set(tool.EWT_OVERLAY_COLUMNS) | {"computed_at"}
    for rid in ("D50", "D10"):
        assert {k: v for k, v in ov_after[rid].items() if k not in written_ov} == {
            k: v for k, v in ov_before[rid].items() if k not in written_ov
        }
        assert {c: ov_after[rid][c] for c in tool.EWT_OVERLAY_COLUMNS} == NEW_EWT
        assert ov_after[rid]["computed_at"] == now
        # The drifted delivered_trips was NOT written.
        assert ov_after[rid]["delivered_trips"] == 92


@pytest.mark.integration
def test_route_set_mismatch_is_refused_and_not_writable(pg_session):
    """A recompute that would add a route (needing a stamp) is refused, and apply refuses it."""
    _seed(pg_session)
    (plan,) = tool.plan_recompute(
        pg_session,
        WMATA,
        D,
        D,
        compute_overlay=_fake_overlay(routes=("D50", "D10", "NEW1")),
        compute_system=_fake_system(),
    )
    assert plan.status == "refused"
    assert "no stored row" in plan.reason
    assert tool.format_plan(plan).startswith("REFUSED")
    with pytest.raises(ValueError):
        tool.apply_plan(pg_session, plan)


@pytest.mark.integration
def test_nulling_stored_ewt_is_refused(pg_session):
    """A recompute that would null a stored system EWT is refused."""
    _seed(pg_session)
    (plan,) = tool.plan_recompute(
        pg_session,
        WMATA,
        D,
        D,
        compute_overlay=_fake_overlay(),
        compute_system=_fake_system(ewt=None),
    )
    assert plan.status == "refused"
    assert "null" in plan.reason


@pytest.mark.integration
def test_compute_failure_is_reported_as_failed_not_refused(pg_session):
    """An exception becomes a FAILED plan (distinct label), and the run continues."""
    _seed(pg_session)

    def _boom(*args, **kwargs):
        """Simulate a compute crash."""
        raise RuntimeError("boom")

    (plan,) = tool.plan_recompute(
        pg_session, WMATA, D, D, compute_overlay=_boom, compute_system=_fake_system()
    )
    assert plan.status == "failed"
    assert tool.format_plan(plan).startswith("FAILED")
    assert "failed 1" in tool.summarize([plan])[0]


@pytest.mark.integration
def test_dates_without_trip_update_rows_and_excluded_dates_are_skipped(pg_session):
    """Only dates with trip_update rows are candidates; --exclude-date drops one."""
    _seed(pg_session)
    pg_session.add(SystemMetricsDaily(service_date="2026-10-13", data_quality="complete"))
    pg_session.flush()
    assert tool.candidate_dates(pg_session, date(2026, 10, 13), D) == [D]
    assert (
        tool.plan_recompute(
            pg_session,
            WMATA,
            date(2026, 10, 13),
            D,
            exclude=frozenset({D}),
            compute_overlay=_fake_overlay(),
            compute_system=_fake_system(),
        )
        == []
    )
