"""Drop the three redundant single-column ``vehicle_positions`` indexes (NOTES-82).

IMPORTANT — THIS IS A DESTRUCTIVE MANUAL ACTION
================================================
This script is NOT run automatically by ``scripts/migrate_all.py``'s CI usage
in a way that touches production — ``migrate_all.py`` auto-discovers and
invokes every ``migrate_*.py`` sibling's ``main()``, which for this script
means the **dry-run** path (see below) unless the operator has set
``--yes`` via direct invocation. There is no automatic execution path.

``ix_vehicle_positions_vehicle_id``, ``ix_vehicle_positions_route_id``, and
``ix_vehicle_positions_trip_id`` were single-column btree indexes created via
``index=True`` on the SQLAlchemy model. PR #220 (NOTES-82) removed
``index=True`` from the model; this script performs the corresponding
``DROP INDEX`` on a live database. Each is shadowed by a composite index
sharing the same leading column (``idx_vehicle_timestamp``,
``idx_route_timestamp``, ``idx_trip_timestamp`` respectively), so the
composite already serves any query that filters on the single column alone.
Measurement on the primary DB (2026-08-22, ~34-day accumulation window)
showed all three at ``idx_scan = 0`` (route_id showed 3, still negligible
next to its composite's 14,853); the same pattern held on the SFMTA sidecar.
See PR #220's body for the full measurement evidence.

Per ``docs/MIGRATIONS.md``: back up first (and test against a restored copy)
before running with ``--yes`` against a system-of-record database.

This script is idempotent — safe to re-run, including after
``bin/refresh-dev-db.sh`` restores an older dump that re-introduces the
indexes. It targets exactly one database per invocation; run it once per
database that carries this schema (see ``--agency`` below).

Usage
-----
    # Preview (safe — no DB changes), against DATABASE_URL (primary):
    uv run python scripts/migrate_drop_vp_redundant_indexes.py

    # Preview against the SFMTA sidecar:
    uv run python scripts/migrate_drop_vp_redundant_indexes.py --agency sfmta

    # Execute the drops (IRREVERSIBLE) against the primary:
    uv run python scripts/migrate_drop_vp_redundant_indexes.py --yes

    # Execute against the SFMTA sidecar:
    uv run python scripts/migrate_drop_vp_redundant_indexes.py --agency sfmta --yes
"""

import argparse
import sys

from dotenv import load_dotenv
from sqlalchemy import inspect, text

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_engine

# Exactly the three single-column indexes made redundant by their
# same-leading-column composite counterpart (NOTES-82). Nothing else is
# touched by this script.
TARGET_INDEXES = (
    "ix_vehicle_positions_vehicle_id",
    "ix_vehicle_positions_route_id",
    "ix_vehicle_positions_trip_id",
)

TABLE_NAME = "vehicle_positions"


def precheck(engine) -> dict[str, bool]:
    """Report which of :data:`TARGET_INDEXES` currently exist on ``engine``.

    Uses SQLAlchemy's reflection API (dialect-neutral) rather than a raw
    ``pg_indexes`` query so this also works against a SQLite test engine.

    Args:
        engine: SQLAlchemy engine connected to the target database.

    Returns:
        Mapping of index name to whether it currently exists. If
        ``vehicle_positions`` itself does not exist, every entry is False.
    """
    inspector = inspect(engine)
    if TABLE_NAME not in inspector.get_table_names():
        return dict.fromkeys(TARGET_INDEXES, False)

    existing_index_names = {idx["name"] for idx in inspector.get_indexes(TABLE_NAME)}
    return {name: name in existing_index_names for name in TARGET_INDEXES}


def run_migration(engine, confirm: bool = False) -> dict[str, bool]:
    """Drop the redundant indexes, or print the plan in dry-run mode.

    Idempotent: each statement is ``DROP INDEX IF EXISTS``, so re-running
    after some or all indexes are already gone (or re-running against a
    freshly restored dump that has them again) is always safe.

    Args:
        engine: SQLAlchemy engine connected to the target database.
        confirm: If False (the default), print the precheck + plan and
            return without making changes. If True, execute the
            transaction-wrapped ``DROP INDEX IF EXISTS`` statements.

    Returns:
        The precheck mapping (index name -> existed before this call),
        captured before any drops.
    """
    found = precheck(engine)

    print(f"Precheck — {TABLE_NAME} redundant single-column indexes:")
    for name, exists in found.items():
        print(f"  {'EXISTS' if exists else 'absent'}: {name}")

    if not confirm:
        print(
            "\nDRY-RUN: no changes made. Re-run with --yes to execute the "
            "irreversible DROP INDEX statements."
        )
        return found

    if not any(found.values()):
        print("\nNothing to do — none of the target indexes exist.")
        return found

    print("\nExecuting drops (IRREVERSIBLE)...")
    with engine.begin() as conn:
        for name in TARGET_INDEXES:
            conn.execute(text(f"DROP INDEX IF EXISTS {name}"))
            print(f"  Dropped (or already absent): {name}")

    print("Done.")
    return found


def main() -> int:
    """CLI entry point.

    Resolves the target database from ``--agency`` (default ``wmata``, i.e.
    ``DATABASE_URL``) via ``src.agency_config``, matching the pattern used by
    other agency-aware scripts (e.g. ``pipelines/cleanup_trip_update_state.py``).
    Run once per database this schema exists on — currently the primary
    (``wmata``) and the SFMTA sidecar (``sfmta``).

    Returns:
        0 on success, 1 on unexpected error.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Drop the three redundant vehicle_positions single-column "
            "indexes (ix_vehicle_positions_vehicle_id, _route_id, "
            "_trip_id) made obsolete by NOTES-82 / PR #220. "
            "IRREVERSIBLE — requires --yes."
        )
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help=(
            "Confirm you have read the module docstring (backup taken per "
            "docs/MIGRATIONS.md) and want to execute the irreversible "
            "DROP INDEX statements. Without this flag the script prints "
            "the precheck + plan and exits without touching the DB."
        ),
    )
    parser.add_argument(
        "--agency",
        default="wmata",
        help=(
            "Agency name matching config/agencies/<agency>.yaml (default: "
            "'wmata', i.e. DATABASE_URL). Pass 'sfmta' to target the SFMTA "
            "sidecar database instead. Run once per database."
        ),
    )
    args = parser.parse_args()

    load_dotenv()
    cfg = load_agency_config(args.agency)
    engine = get_engine(resolve_agency_db_url(cfg))

    try:
        run_migration(engine, confirm=args.yes)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
