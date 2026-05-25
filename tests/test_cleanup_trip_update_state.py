"""Tests for pipelines.cleanup_trip_update_state.

Cleanup is a single date-based rule now: rows whose ``service_date``
is older than the retention window are deleted. The previous two-pass
implementation (derived rows older than 2 days, safety net for
underived rows older than 7 days) collapsed because ``service_date``
is in the PK — there's no need to reason about whether derivation
already ran.
"""

from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import select

from src.models import TripUpdateState
from src.timezones import eastern_today


def _make_state_row(
    trip_id: str,
    stop_sequence: int,
    *,
    service_date: date,
    derived_at: datetime | None = None,
) -> TripUpdateState:
    """Build a TripUpdateState row for cleanup tests.

    Args:
        trip_id: GTFS trip identifier.
        stop_sequence: Stop sequence number within the trip.
        service_date: Eastern service date this row belongs to. Drives
            the cleanup cutoff.
        derived_at: When stop_events were materialized; ``None`` means
            the row has not yet been derived. Preserved as a diagnostic;
            no longer load-bearing for cleanup.

    Returns:
        An unsaved :class:`~src.models.TripUpdateState` instance.
    """
    final_ts = datetime.combine(service_date, datetime.min.time()) + timedelta(hours=18)
    return TripUpdateState(
        trip_id=trip_id,
        stop_sequence=stop_sequence,
        service_date=service_date,
        stop_id="S1",
        vehicle_id="V1",
        final_snapshot_ts=final_ts,
        final_schedule_relationship="SCHEDULED",
        last_pred_snapshot_ts=final_ts,
        last_predicted_arrival_ts=final_ts + timedelta(minutes=5),
        derived_at=derived_at,
    )


@pytest.mark.integration
def test_cleanup_deletes_rows_older_than_retention_window(pg_session):
    """Rows with service_date older than the cutoff are deleted; newer rows stay.

    Cutoff is ``eastern_today() - retention_days``. With the default 7-day
    retention, a row dated 10 days ago is past the cutoff; 3 days ago is
    inside it.
    """
    from pipelines.cleanup_trip_update_state import run_cleanup

    today = eastern_today()
    pg_session.add_all(
        [
            _make_state_row("T_old", 1, service_date=today - timedelta(days=10)),
            _make_state_row("T_recent", 1, service_date=today - timedelta(days=3)),
            _make_state_row("T_today", 1, service_date=today),
        ]
    )
    pg_session.commit()

    counts = run_cleanup(pg_session)
    pg_session.commit()

    # On a populated dev DB, run_cleanup deletes all visible old rows (not just
    # the test's rows). Assert at-least-N to stay portable across populated DBs.
    assert counts["deleted"] >= 1
    remaining = {
        r.trip_id
        for r in pg_session.execute(
            select(TripUpdateState).where(
                TripUpdateState.trip_id.in_(["T_old", "T_recent", "T_today"])
            )
        ).scalars()
    }
    assert remaining == {"T_recent", "T_today"}


@pytest.mark.integration
def test_cleanup_respects_explicit_retention_days(pg_session):
    """A tighter retention window deletes more rows.

    Same fixture as above but called with ``retention_days=2``: anything
    older than 2 days gets deleted, so both T_old and T_recent (3 days
    out) are removed.
    """
    from pipelines.cleanup_trip_update_state import run_cleanup

    today = eastern_today()
    pg_session.add_all(
        [
            _make_state_row("T_old", 1, service_date=today - timedelta(days=10)),
            _make_state_row("T_recent", 1, service_date=today - timedelta(days=3)),
            _make_state_row("T_today", 1, service_date=today),
        ]
    )
    pg_session.commit()

    counts = run_cleanup(pg_session, retention_days=2)
    pg_session.commit()

    # On a populated dev DB, run_cleanup deletes all visible old rows (not just
    # the test's rows). Assert at-least-N to stay portable across populated DBs.
    assert counts["deleted"] >= 2
    remaining = {
        r.trip_id
        for r in pg_session.execute(
            select(TripUpdateState).where(
                TripUpdateState.trip_id.in_(["T_old", "T_recent", "T_today"])
            )
        ).scalars()
    }
    assert remaining == {"T_today"}


@pytest.mark.integration
def test_cleanup_ignores_derived_at(pg_session):
    """A derived row inside the retention window stays; that's the whole point
    of moving the cleanup rule onto service_date.

    Previously, ``derived_at`` set + 2-day window would have deleted a
    recent row. Now only ``service_date`` matters.
    """
    from pipelines.cleanup_trip_update_state import run_cleanup

    today = eastern_today()
    pg_session.add(
        _make_state_row(
            "T_derived_recent",
            1,
            service_date=today - timedelta(days=3),
            derived_at=datetime.now() - timedelta(days=3),
        )
    )
    pg_session.commit()

    counts = run_cleanup(pg_session)
    pg_session.commit()

    # The test verifies that T_derived_recent (inside the retention window) was NOT
    # deleted. On a populated dev DB, run_cleanup also deletes pre-existing old rows
    # so we can't assert an exact count — the remaining-set check is authoritative.
    assert "deleted" in counts
    remaining = {
        r.trip_id
        for r in pg_session.execute(
            select(TripUpdateState).where(TripUpdateState.trip_id == "T_derived_recent")
        ).scalars()
    }
    assert remaining == {"T_derived_recent"}
