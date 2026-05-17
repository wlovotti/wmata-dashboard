"""Tests for scripts/migrate_create_stop_events_v2.py."""

import pytest
from sqlalchemy import inspect, text


@pytest.mark.integration
def test_v2_table_has_same_columns_as_stop_events(pg_session):
    """stop_events_v2 has identical columns to stop_events and the v2 constraint.

    The migration is idempotent (CREATE TABLE IF NOT EXISTS + IF NOT EXISTS
    constraint check), so this exercises whichever path applies — fresh CREATE
    on a clean DB, or no-op on an already-migrated DB. We deliberately don't
    DROP stop_events_v2 first: holding an ACCESS EXCLUSIVE lock inside the
    test's outer transaction would deadlock the CREATE that run_migration
    issues on a separate connection.
    """
    from scripts.migrate_create_stop_events_v2 import run_migration

    engine = pg_session.bind.engine
    run_migration(engine)

    inspector = inspect(engine)
    cols_v1 = {c["name"] for c in inspector.get_columns("stop_events")}
    cols_v2 = {c["name"] for c in inspector.get_columns("stop_events_v2")}
    assert cols_v1 == cols_v2

    # Verify the v2-specific UNIQUE constraint exists with the expected name.
    constraints = pg_session.execute(
        text(
            "SELECT conname FROM pg_constraint WHERE conname = 'uq_stop_events_v2_run_stop_source'"
        )
    ).scalar()
    assert constraints == "uq_stop_events_v2_run_stop_source"
