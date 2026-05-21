"""Tests for scripts/migrate_create_trip_update_state.py."""

import pytest
from sqlalchemy import inspect, text


@pytest.mark.integration
def test_migration_creates_table_and_indexes(db_session):
    """Migration creates the table with the spec'd indexes; re-running is a no-op."""
    from scripts.migrate_create_trip_update_state import run_migration

    # db_session.bind is a Connection inside an outer transaction; we want the
    # underlying Engine so run_migration can open its own transaction via engine.begin().
    engine = db_session.bind.engine

    inspector = inspect(engine)

    # Drop the table first to ensure a clean slate (idempotent test).
    db_session.execute(text("DROP TABLE IF EXISTS trip_update_state"))
    db_session.commit()
    assert "trip_update_state" not in inspector.get_table_names()

    # First run creates everything.
    run_migration(engine)
    inspector = inspect(engine)
    assert "trip_update_state" in inspector.get_table_names()
    indexes = {idx["name"] for idx in inspector.get_indexes("trip_update_state")}
    assert "idx_tus_final_snapshot_ts" in indexes
    assert "idx_tus_trip_id" in indexes
    assert "idx_tus_service_date" in indexes

    # Fresh-install schema includes service_date in the PK (2026-05-20
    # addendum). Verify the column exists so a regression that drops it
    # from the CREATE TABLE SQL doesn't go unnoticed.
    cols = {c["name"] for c in inspector.get_columns("trip_update_state")}
    assert "service_date" in cols

    # Second run is a no-op (idempotent).
    run_migration(engine)  # Must not raise.
