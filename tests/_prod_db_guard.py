"""Helpers for the production-DB pytest guard (issue #247 / NOTES-131).

Split out of `tests/conftest.py` so `tests/test_prod_db_guard.py` can import
these directly without going through a second, unregistered copy of the
`conftest` module (there is no `tests/__init__.py`, so `import
tests.conftest` and pytest's own auto-loaded `conftest` module are not the
same module object).
"""

import os
from urllib.parse import urlparse

# Database names that are the real system of record. A non-smoke test run
# pointed at either of these risks reading/writing production data or
# getting non-deterministic results from whatever happens to be in the DB
# that day.
PRODUCTION_DB_NAMES = {"wmata_dashboard", "sfmta_dashboard"}

# Env vars that can carry a Postgres URL into a test run.
DB_URL_ENV_VARS = ("DATABASE_URL", "SFMTA_DATABASE_URL", "PG_TEST_DATABASE_URL")

# The scratch DB name every safe invocation below points at.
SCRATCH_DB_NAME = "wmata_test_local"


def is_production_db_url(url: str | None) -> bool:
    """Return True when `url` names a real system-of-record Postgres DB.

    Handles ``postgresql:///name`` (no host), ``postgresql://user:pw@host:port/name``,
    and URLs with a trailing query string. Non-Postgres URLs (e.g. sqlite) are
    never production, and a missing/empty URL is never production.
    """
    if not url:
        return False
    parsed = urlparse(url)
    if not parsed.scheme.startswith("postgres"):
        return False
    db_name = parsed.path.lstrip("/")
    return db_name in PRODUCTION_DB_NAMES


def guard_message(offending: list[tuple[str, str]]) -> str:
    """Build the abort message naming every offending env var and DB.

    `offending` is a list of (env_var_name, url) pairs pointed at production.
    All three DB-URL env vars must point at the scratch DB in the printed
    invocation -- DATABASE_URL and SFMTA_DATABASE_URL both feed
    `src/database.py`'s `load_dotenv()`-populated defaults, so leaving either
    one unset lets it fall back to the real value in `.env` and re-trip this
    guard.
    """
    lines = ["Refusing to run non-smoke tests against a production database:"]
    for var_name, url in offending:
        lines.append(f"  {var_name}={url}")
    lines.append("")
    lines.append("Safe options:")
    lines.append(
        f"  PG_TEST_DATABASE_URL=postgresql:///{SCRATCH_DB_NAME} "
        f"DATABASE_URL=postgresql:///{SCRATCH_DB_NAME} "
        f"SFMTA_DATABASE_URL=postgresql:///{SCRATCH_DB_NAME} uv run pytest"
    )
    lines.append("  bin/test-with-pg")
    lines.append("  PYTEST_ALLOW_PROD_DB=1 uv run pytest   # deliberate override")
    return "\n".join(lines)


def find_offending_env_vars() -> list[tuple[str, str]]:
    """Return (env_var_name, url) pairs among `DB_URL_ENV_VARS` that point at production."""
    return [
        (var_name, url)
        for var_name in DB_URL_ENV_VARS
        if (url := os.environ.get(var_name)) and is_production_db_url(url)
    ]


def check_collection(items) -> str | None:
    """Return an abort message if `items` should not run against the current env, else None.

    Pure decision core for `pytest_collection_modifyitems` (kept in
    `tests/conftest.py`, which just calls this and calls `pytest.exit` on a
    non-None result). Kept here, separate from the hookimpl, so
    `tests/test_prod_db_guard.py` can exercise the decision logic directly
    without importing the registered `conftest` module a second time under
    a different module identity (there is no `tests/__init__.py`).

    `items` is any sequence of objects exposing `get_closest_marker(name)`
    (pytest's `Item` protocol). If at least one item is given and every item
    carries the `smoke` marker, the run is fast/DB-light by convention and
    is not blocked. An empty `items` sequence (e.g. every test in the target
    got deselected by `-m 'not smoke'`) is NOT treated as smoke-only -- it's
    still checked, since that's exactly the shape of an accidental
    non-smoke invocation against an all-smoke file. `PYTEST_ALLOW_PROD_DB=1`
    always overrides.
    """
    if items and all(item.get_closest_marker("smoke") for item in items):
        return None

    if os.environ.get("PYTEST_ALLOW_PROD_DB") == "1":
        return None

    offending = find_offending_env_vars()
    if offending:
        return guard_message(offending)
    return None
