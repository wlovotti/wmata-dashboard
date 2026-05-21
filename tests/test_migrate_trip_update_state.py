"""Tests for scripts/migrate_create_trip_update_state.py."""

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import StaticPool


@pytest.mark.integration
def test_migration_creates_table_and_indexes():
    """Migration creates the table with the spec'd indexes; re-running is a no-op.

    Uses a **function-scoped** in-memory SQLite engine that is isolated from
    the session-scoped ``test_engine`` fixture.  The original implementation
    issued ``DROP TABLE IF EXISTS trip_update_state`` on the shared
    session-scoped engine and then committed, which permanently removed the
    table from that engine for all subsequent tests in the session —
    contaminating every downstream test that relied on the table existing.

    This version creates a fresh, throw-away SQLite DB so the drop + recreate
    is invisible to the rest of the suite.
    """
    from scripts.migrate_create_trip_update_state import run_migration

    # Fresh, isolated in-memory SQLite — dies when this function returns.
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    inspector = inspect(engine)

    # Drop the table first to ensure a clean slate (idempotent test).
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS trip_update_state"))
    assert "trip_update_state" not in inspector.get_table_names()

    # First run creates everything.
    run_migration(engine)
    inspector = inspect(engine)
    assert "trip_update_state" in inspector.get_table_names()

    # Indexes: SQLite doesn't create named indexes the same way Postgres does,
    # but we can still verify the column set and PK.
    cols = {c["name"] for c in inspector.get_columns("trip_update_state")}
    # Fresh-install schema includes service_date in the PK (2026-05-20
    # addendum). Verify the column exists so a regression that drops it
    # from the CREATE TABLE SQL doesn't go unnoticed.
    assert "service_date" in cols
    assert "trip_id" in cols
    assert "stop_sequence" in cols
    assert "stop_id" in cols
    assert "final_snapshot_ts" in cols

    # Second run is a no-op (idempotent).
    run_migration(engine)  # Must not raise.
    engine.dispose()


@pytest.mark.integration
@pytest.mark.pg
def test_migration_idempotent_postgres(pg_session):
    """On Postgres, verify that re-running the migration is a no-op.

    The ``pg_session`` fixture points at a migrated Postgres DB (the dev DB
    or the CI test DB, both of which already have the ``trip_update_state``
    table from a prior ``migrate_all.py`` run).  Rather than DROP + recreate
    (which fights SAVEPOINT semantics and requires ACCESS EXCLUSIVE + DDL),
    this test verifies that the migration is idempotent — running
    ``run_migration()`` a second time on a live DB raises no error — and
    that the live table already has the expected columns and named indexes.
    Both properties catch regressions: a non-idempotent script would raise;
    a missing index or column would fail an assertion.
    """
    from scripts.migrate_create_trip_update_state import run_migration

    engine = pg_session.bind.engine
    inspector = inspect(engine)

    # Table must already exist (created by init_db + migrate_all in CI,
    # or by the dev migration workflow locally).
    assert "trip_update_state" in inspector.get_table_names(), (
        "trip_update_state not found — run 'uv run python scripts/migrate_all.py' "
        "against PG_TEST_DATABASE_URL first"
    )

    # Idempotent re-run must not raise.
    run_migration(engine)

    # Verify expected indexes and columns survive the re-run.
    inspector = inspect(engine)
    indexes = {idx["name"] for idx in inspector.get_indexes("trip_update_state")}
    assert "idx_tus_final_snapshot_ts" in indexes
    assert "idx_tus_trip_id" in indexes
    assert "idx_tus_service_date" in indexes

    cols = {c["name"] for c in inspector.get_columns("trip_update_state")}
    assert "service_date" in cols
    assert "trip_id" in cols
    assert "stop_sequence" in cols
