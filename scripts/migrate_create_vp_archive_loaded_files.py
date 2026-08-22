"""Create the ``vp_archive_loaded_files`` manifest table (NOTES-95 VP loader).

Idempotent (CREATE TABLE IF NOT EXISTS). Run once per database:

    uv run python scripts/migrate_create_vp_archive_loaded_files.py
    uv run python scripts/migrate_create_vp_archive_loaded_files.py --agency sfmta
"""

import argparse
import sys

from dotenv import load_dotenv
from sqlalchemy import text

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_engine

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS vp_archive_loaded_files (
    filename       VARCHAR    PRIMARY KEY,
    row_count      INTEGER    NOT NULL,
    dropped_count  INTEGER    NOT NULL DEFAULT 0,
    loaded_at      TIMESTAMP  NOT NULL
);
"""


def run_migration(engine) -> None:
    """Apply the migration. Safe to re-run.

    Creates the ``vp_archive_loaded_files`` table with ``filename`` as its
    primary key, matching ``VpArchiveLoadedFile`` in ``src/models.py``.
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
        f"Creating vp_archive_loaded_files in the {args.agency} database "
        f"({engine.url.host or 'local'}/{engine.url.database})..."
    )
    run_migration(engine)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
