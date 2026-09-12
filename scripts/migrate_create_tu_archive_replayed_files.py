"""Create the ``tu_archive_replayed_files`` manifest table (TU archive replay, NOTES-145).

Idempotent (CREATE TABLE IF NOT EXISTS). Run once per database:

    uv run python scripts/migrate_create_tu_archive_replayed_files.py
    uv run python scripts/migrate_create_tu_archive_replayed_files.py --agency sfmta
"""

import argparse
import sys

from dotenv import load_dotenv
from sqlalchemy import text

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_engine

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tu_archive_replayed_files (
    filename             VARCHAR    NOT NULL,
    target_service_date  DATE       NOT NULL,
    row_count            INTEGER    NOT NULL,
    replayed_at          TIMESTAMP  NOT NULL,
    PRIMARY KEY (filename, target_service_date)
);
"""


def run_migration(engine) -> None:
    """Apply the migration. Safe to re-run.

    Creates the ``tu_archive_replayed_files`` table with ``(filename,
    target_service_date)`` as its primary key, matching ``TuArchiveReplayedFile`` in ``src/models.py``.
    """
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))


def main(argv=None) -> int:
    """CLI entry point; ``argv`` is explicit so migrate_all.py can pass []."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", default="wmata", choices=("wmata", "sfmta"))
    args = parser.parse_args(argv)
    load_dotenv()
    cfg = load_agency_config(args.agency)
    engine = get_engine(resolve_agency_db_url(cfg))
    # Print the resolved host/dbname (never the password, via the URL
    # object's own .host/.database attrs rather than stringifying the
    # whole URL) so a per-agency invocation is self-verifying — the reader
    # can confirm this is really about to hit the database they intended
    # before it runs.
    print(
        f"Creating tu_archive_replayed_files in the {args.agency} database "
        f"({engine.url.host or 'local'}/{engine.url.database})..."
    )
    run_migration(engine)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
