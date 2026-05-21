"""
Fail loudly if the live database is missing tables or columns the
SQLAlchemy models declare.

Walks every table in `Base.metadata.tables`, asks SQLAlchemy's `inspect()`
for the live columns of that table, and asserts every model column name
is present. Live extras (legacy columns, manual additions) are tolerated
— this check is one-directional. The point is to catch the case where a
model column ships without a matching `scripts/migrate_*.py`, or where a
migration exists but never gets run against the live Postgres.

Also validates each ``scripts/migrate_create_*.py`` script's hardcoded
``CREATE TABLE`` SQL against the model, catching the case where a column
is added to the model but the companion create-migration is not updated.
This is a static check — no database connection required for this half.

Origin: PR #67 (closes NOTES-31) added `Run.stops_observable`, shipped
`scripts/migrate_runs_stops_observable.py`, merged with green CI, and
500'd the Recent Runs API endpoint immediately because the migration
hadn't been run against the live Postgres. The SQLite test lane rebuilds
the schema from `Base.metadata` every run, so model and test DB are
tautologically aligned and a missing migration is invisible.

PR #135's first push had 12 CI failures — 8 traced to
``migrate_create_trip_update_state.py`` having a hardcoded CREATE TABLE
that fell out of sync with the model when ``service_date`` was added. The
migrate-script check below would have caught that gap before any test ran.

Run after `scripts/migrate_all.py` in CI's Postgres lane. Safe to run by
hand against a live database to sanity-check schema state.

Usage:
  uv run python scripts/check_schema_drift.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import inspect

from src.database import get_engine
from src.models import Base

# Map from table name (as it appears in a CREATE TABLE statement) to the
# corresponding SQLAlchemy model table object.  Populated lazily from Base.
_TABLE_NAME_TO_MODEL_TABLE: dict[str, object] | None = None


def _model_table_map() -> dict:
    """Return {table_name: Table} for every table in Base.metadata."""
    global _TABLE_NAME_TO_MODEL_TABLE
    if _TABLE_NAME_TO_MODEL_TABLE is None:
        _TABLE_NAME_TO_MODEL_TABLE = dict(Base.metadata.tables)
    return _TABLE_NAME_TO_MODEL_TABLE


def _extract_create_table_name(sql: str) -> str | None:
    """Return the table name from a CREATE TABLE [IF NOT EXISTS] statement.

    Matches the first occurrence of CREATE TABLE (with optional IF NOT
    EXISTS) and extracts the bare table name (no schema prefix).  Returns
    None if no match is found.
    """
    m = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
        sql,
        re.IGNORECASE,
    )
    return m.group(1) if m else None


def _extract_column_names_from_create(sql: str) -> set[str]:
    """Extract column names from the column-definition block of a CREATE TABLE.

    Parses the parenthesised body of the first CREATE TABLE statement and
    returns every token that looks like a column name (first identifier on
    each comma-separated line).  Constraint keywords (PRIMARY, UNIQUE,
    CHECK, FOREIGN, CONSTRAINT, LIKE) are skipped so they don't pollute
    the column set.

    This is intentionally simple — no full SQL grammar.  The migrate_create
    scripts in this repo use straightforward, unquoted column names, so a
    regex approach is sufficient.
    """
    # Extract the parenthesised body after CREATE TABLE ... (
    m = re.search(r"CREATE\s+TABLE[^(]*\((.+)\)", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return set()

    body = m.group(1)
    # Split on commas that are not inside nested parens (e.g. CHECK (...)).
    # We do a simple depth-counting split.
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in body:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())

    _CONSTRAINT_KEYWORDS = {
        "PRIMARY",
        "UNIQUE",
        "CHECK",
        "FOREIGN",
        "CONSTRAINT",
        "LIKE",
    }
    columns: set[str] = set()
    for part in parts:
        part = part.strip()
        if not part:
            continue
        # First word on the line is either a column name or a constraint keyword.
        first = part.split()[0].upper()
        if first in _CONSTRAINT_KEYWORDS:
            continue
        columns.add(first.lower())

    return columns


def check_migrate_create_scripts() -> list[str]:
    """Validate each migrate_create_*.py script against its model table.

    For every ``scripts/migrate_create_*.py`` that contains a ``CREATE TABLE``
    statement targeting a table known to the SQLAlchemy model, assert that
    every model column is listed in the SQL.  Returns a list of error
    strings; empty list means all scripts are in sync.
    """
    scripts_dir = Path(__file__).resolve().parent
    model_tables = _model_table_map()
    errors: list[str] = []

    for script_path in sorted(scripts_dir.glob("migrate_create_*.py")):
        source = script_path.read_text()
        table_name = _extract_create_table_name(source)
        if table_name is None:
            # Script uses LIKE or dynamic SQL — skip static check.
            continue
        if table_name not in model_tables:
            # Side tables (e.g. stop_events_v2) not in the model — skip.
            continue

        model_table = model_tables[table_name]
        model_cols = {c.name for c in model_table.columns}
        script_cols = _extract_column_names_from_create(source)

        missing = sorted(model_cols - script_cols)
        if missing:
            errors.append(
                f"  {script_path.name}: CREATE TABLE for '{table_name}' is missing "
                f"column(s) declared in the model: {missing}"
            )

    return errors


def main() -> None:
    """Compare model schema against live DB and migrate_create scripts; exit non-zero on drift."""
    load_dotenv()

    # --- Static check: migrate_create_*.py scripts vs. model columns ---
    script_errors = check_migrate_create_scripts()
    if script_errors:
        print(
            "migrate_create script drift detected — hardcoded CREATE TABLE SQL "
            "is missing column(s) that the SQLAlchemy model declares:",
            file=sys.stderr,
        )
        for err in script_errors:
            print(err, file=sys.stderr)
        print(
            "\nUpdate the relevant migrate_create_*.py script(s) to include the "
            "missing column(s) before merging.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    # --- Live check: model columns vs. live Postgres ---
    engine = get_engine()
    inspector = inspect(engine)

    live_tables = set(inspector.get_table_names())
    model_tables = dict(Base.metadata.tables)

    missing_tables: list[str] = []
    column_drift: list[tuple[str, list[str]]] = []

    for table_name, table in model_tables.items():
        if table_name not in live_tables:
            missing_tables.append(table_name)
            continue
        live_cols = {c["name"] for c in inspector.get_columns(table_name)}
        model_cols = {c.name for c in table.columns}
        missing = sorted(model_cols - live_cols)
        if missing:
            column_drift.append((table_name, missing))

    if not missing_tables and not column_drift:
        print(
            f"Schema drift check passed: {len(model_tables)} model table(s) "
            "all present with every declared column."
        )
        return

    print("Schema drift detected:", file=sys.stderr)
    if missing_tables:
        print("  Missing tables:", file=sys.stderr)
        for name in sorted(missing_tables):
            print(f"    - {name}", file=sys.stderr)
    if column_drift:
        print("  Tables with missing columns:", file=sys.stderr)
        for table_name, missing in sorted(column_drift):
            print(f"    - {table_name}: {missing}", file=sys.stderr)
    print(
        "\nThis means the live Postgres is missing schema declared in src/models.py. "
        "Either a migration script was not run, or a model column was added without "
        "a corresponding scripts/migrate_*.py.",
        file=sys.stderr,
    )
    raise SystemExit(1)


if __name__ == "__main__":
    main()
