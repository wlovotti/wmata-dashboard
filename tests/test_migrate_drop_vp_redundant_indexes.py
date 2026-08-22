"""Tests for scripts/migrate_drop_vp_redundant_indexes.py."""

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, inspect, text
from sqlalchemy.pool import StaticPool


def _fresh_engine():
    """Return a fresh, throw-away in-memory SQLite engine.

    Isolated from the session-scoped ``test_engine`` fixture so DDL here
    (creating/dropping ``vehicle_positions`` and its indexes) can't leak
    into other tests.
    """
    return create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


@pytest.mark.integration
def test_target_indexes_covers_both_investigation_batches():
    """TARGET_INDEXES holds the three single-column (NOTES-82) plus the two
    composite (NOTES-129) index names, and excludes the retained
    idx_route_timestamp.
    """
    from scripts.migrate_drop_vp_redundant_indexes import TARGET_INDEXES

    assert set(TARGET_INDEXES) == {
        "ix_vehicle_positions_vehicle_id",
        "ix_vehicle_positions_route_id",
        "ix_vehicle_positions_trip_id",
        "idx_vehicle_timestamp",
        "idx_trip_timestamp",
    }
    assert "idx_route_timestamp" not in TARGET_INDEXES


@pytest.mark.integration
def test_precheck_reports_absent_when_table_missing():
    """precheck() returns all-False when vehicle_positions doesn't exist yet."""
    from scripts.migrate_drop_vp_redundant_indexes import TARGET_INDEXES, precheck

    engine = _fresh_engine()
    found = precheck(engine)
    assert found == dict.fromkeys(TARGET_INDEXES, False)
    engine.dispose()


@pytest.mark.integration
def test_run_migration_only_drops_indexes_found_on_target_table():
    """Execute mode drops exactly the indexes precheck found on vehicle_positions.

    Regression guard for the review finding that the execute loop must not
    iterate the raw TARGET_INDEXES list unconditionally: it should only act
    on names precheck actually reported as present (scoped to
    vehicle_positions), skipping anything reported absent.
    """
    from scripts.migrate_drop_vp_redundant_indexes import (
        TABLE_NAME,
        TARGET_INDEXES,
        run_migration,
    )

    engine = _fresh_engine()
    metadata = MetaData()
    Table(
        TABLE_NAME,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("vehicle_id", String),
        Column("trip_id", String),
        Column("route_id", String),
        Column("timestamp", String),
    )
    metadata.create_all(engine)

    # Only create a subset of TARGET_INDEXES, so the run must not assume
    # all of them exist.
    with engine.begin() as conn:
        conn.execute(
            text("CREATE INDEX ix_vehicle_positions_vehicle_id ON vehicle_positions (vehicle_id)")
        )
        conn.execute(
            text("CREATE INDEX idx_trip_timestamp ON vehicle_positions (trip_id, timestamp)")
        )

    found_before = run_migration(engine, confirm=True)
    assert found_before == {
        "ix_vehicle_positions_vehicle_id": True,
        "ix_vehicle_positions_route_id": False,
        "ix_vehicle_positions_trip_id": False,
        "idx_vehicle_timestamp": False,
        "idx_trip_timestamp": True,
    }

    inspector = inspect(engine)
    remaining = {idx["name"] for idx in inspector.get_indexes(TABLE_NAME)}
    assert remaining.isdisjoint(TARGET_INDEXES)

    # Re-running is a no-op and must not raise.
    run_migration(engine, confirm=True)
    engine.dispose()
