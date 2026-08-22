"""The --max-age-days gate: reload only when the newest snapshot is stale."""

from datetime import timedelta

from src.models import GTFSSnapshot
from src.timezones import utcnow_naive


def test_reload_due_when_no_snapshot(db_session):
    """No gtfs_snapshots row at all always means a reload is due."""
    from scripts.run_gtfs_reload import reload_due

    assert reload_due(db_session, max_age_days=7) is True


def test_reload_not_due_for_fresh_snapshot(db_session):
    """A snapshot younger than max_age_days does not trigger a reload."""
    from scripts.run_gtfs_reload import reload_due

    db_session.add(GTFSSnapshot(snapshot_date=utcnow_naive() - timedelta(days=2)))
    db_session.commit()
    assert reload_due(db_session, max_age_days=7) is False


def test_reload_due_for_stale_snapshot(db_session):
    """A snapshot older than max_age_days triggers a reload."""
    from scripts.run_gtfs_reload import reload_due

    db_session.add(GTFSSnapshot(snapshot_date=utcnow_naive() - timedelta(days=8)))
    db_session.commit()
    assert reload_due(db_session, max_age_days=7) is True
