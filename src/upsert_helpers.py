"""
Shared upsert helper for PostgreSQL pipeline pipelines.

All four per-route pipelines (derive_stop_events, derive_stop_events_from_state,
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

from sqlalchemy import case
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

    # Accept both ORM-mapped classes (which expose __table__) and bare
    # Table objects (e.g. side tables produced by
    # ``_resolve_side_table`` in pipelines/derive_stop_events_from_state.py).
    # pg_insert rejects arbitrary classes; passing a Table works for both
    # cases because mapped classes route through their __table__ anyway.
    table = getattr(model, "__table__", model)
    stmt = pg_insert(table).values(rows)
    set_ = {c: stmt.excluded[c] for c in update_cols}
    stmt = stmt.on_conflict_do_update(constraint=constraint_name, set_=set_)
    db.execute(stmt)
    db.commit()


def upsert_trip_update_state(db: Session, rows: list[dict[str, Any]]) -> int:
    """UPSERT trip_update_state rows with conditional last_pred_* semantics.

    ``rows`` is a list of dicts shaped like::

        {
            "trip_id": str,
            "stop_sequence": int,
            "service_date": date,
            "stop_id": str,
            "vehicle_id": str | None,
            "snapshot_ts": datetime,
            "predicted_arrival_ts": datetime | None,
            "schedule_relationship": str | None,
        }

    Semantics on conflict (trip_id, stop_sequence, service_date):
        - final_snapshot_ts, final_schedule_relationship: always overwrite.
        - last_pred_snapshot_ts, last_predicted_arrival_ts: overwrite ONLY
          when the incoming predicted_arrival_ts is non-null. WMATA
          sometimes nullifies predictions right at arrival; we want to
          keep the last meaningful estimate (matching the existing
          derivation algorithm in derive_stop_events_from_state.py).
        - vehicle_id: COALESCE(new, existing) — keep last non-null.
        - stop_id: overwrite (should be stable across snapshots for a
          given (trip, stop_sequence), but defensively keep latest).
        - derived_at: never touched by this function (only the derivation
          pipeline writes it).

    Postgres-only by construction: uses pg_insert with conditional
    excluded.* logic in the ON CONFLICT DO UPDATE clause. SQLite cannot
    represent this UPSERT, so callers in test contexts must use a real
    Postgres connection (mark tests with ``@pytest.mark.integration``).

    Parameters
    ----------
    db:
        Active SQLAlchemy session bound to a PostgreSQL database.
    rows:
        List of row dicts.  Empty list is a no-op (returns 0).

    Returns
    -------
    int
        The number of rows passed in (Postgres doesn't reliably return
        inserted-vs-updated counts on ON CONFLICT).
    """
    if not rows:
        return 0

    from src.models import TripUpdateState

    payload = [
        {
            "trip_id": r["trip_id"],
            "stop_sequence": r["stop_sequence"],
            "service_date": r["service_date"],
            "stop_id": r["stop_id"],
            "vehicle_id": r["vehicle_id"],
            "final_snapshot_ts": r["snapshot_ts"],
            "final_schedule_relationship": r["schedule_relationship"],
            "last_pred_snapshot_ts": r["snapshot_ts"]
            if r["predicted_arrival_ts"] is not None
            else None,
            "last_predicted_arrival_ts": r["predicted_arrival_ts"],
        }
        for r in rows
    ]

    stmt = pg_insert(TripUpdateState).values(payload)

    excluded = stmt.excluded
    table = TripUpdateState.__table__
    stmt = stmt.on_conflict_do_update(
        index_elements=["trip_id", "stop_sequence", "service_date"],
        set_={
            "stop_id": excluded.stop_id,
            "vehicle_id": case(
                (excluded.vehicle_id.is_(None), table.c.vehicle_id),
                else_=excluded.vehicle_id,
            ),
            "final_snapshot_ts": excluded.final_snapshot_ts,
            "final_schedule_relationship": excluded.final_schedule_relationship,
            "last_pred_snapshot_ts": case(
                (
                    excluded.last_predicted_arrival_ts.is_(None),
                    table.c.last_pred_snapshot_ts,
                ),
                else_=excluded.last_pred_snapshot_ts,
            ),
            "last_predicted_arrival_ts": case(
                (
                    excluded.last_predicted_arrival_ts.is_(None),
                    table.c.last_predicted_arrival_ts,
                ),
                else_=excluded.last_predicted_arrival_ts,
            ),
        },
    )

    db.execute(stmt)
    return len(rows)
