"""
Shared upsert helper for PostgreSQL pipeline pipelines.

All four per-route pipelines (derive_stop_events, derive_stop_events_trip_updates,
aggregate_runs, compute_bunching) share the identical boilerplate:

    stmt = pg_insert(Model).values(rows)
    update_cols = {c: stmt.excluded[c] for c in (...)}
    stmt = stmt.on_conflict_do_update(constraint=..., set_=update_cols)
    db.execute(stmt)
    db.commit()

`upsert_rows` collapses all four lines into one call, keeps the constraint
name and update-column list close to each call site (not buried in the
function), and provides a single seam for future batching and error handling.

This module is **PostgreSQL-only** — it imports
``sqlalchemy.dialects.postgresql.insert``, which is not available on SQLite.
Call it only from pipelines that already require PostgreSQL.
"""

from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import DeclarativeBase, Session


def upsert_rows(
    db: Session,
    model: type[DeclarativeBase],
    rows: list[dict[str, Any]],
    constraint_name: str,
    update_cols: list[str],
) -> None:
    """Upsert a list of row dicts into a PostgreSQL table via ON CONFLICT DO UPDATE.

    Builds and executes a single ``INSERT … ON CONFLICT DO UPDATE`` statement.
    All rows in *rows* must share the same column set (as produced by the
    pipeline's list-comprehension assemblers).  The session is committed after
    the statement executes.

    Parameters
    ----------
    db:
        Active SQLAlchemy session bound to a PostgreSQL database.
    model:
        The SQLAlchemy ORM model class whose mapped table is the target.
    rows:
        Non-empty list of dicts mapping column names to values.  Callers are
        responsible for ensuring *rows* is non-empty before calling (the
        function raises ``ValueError`` on an empty list to catch bugs early).
    constraint_name:
        The name of the unique constraint that drives conflict detection, e.g.
        ``"uq_stop_events_run_stop_source"``.
    update_cols:
        Column names to overwrite when a conflict is detected.  All names must
        be present in *rows*.

    Raises
    ------
    ValueError
        If *rows* is empty (callers should guard with ``if rows:`` before
        calling; an empty list most likely indicates a logic error upstream).
    """
    if not rows:
        raise ValueError("upsert_rows called with an empty rows list")

    stmt = pg_insert(model).values(rows)
    set_ = {c: stmt.excluded[c] for c in update_cols}
    stmt = stmt.on_conflict_do_update(constraint=constraint_name, set_=set_)
    db.execute(stmt)
    db.commit()
