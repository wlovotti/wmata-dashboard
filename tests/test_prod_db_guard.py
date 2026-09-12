"""Guard against a non-smoke pytest run silently reading the production DB.

Covers `tests/_prod_db_guard.py`: `is_production_db_url` (URL parsing) and
`check_collection` (the pure decision core behind `tests/conftest.py`'s
`pytest_collection_modifyitems` hookimpl, which aborts a non-smoke,
non-overridden session pointed at `wmata_dashboard` / `sfmta_dashboard`;
issue #247 / NOTES-131).

Everything here is imported from `tests/_prod_db_guard.py`, not
`tests/conftest.py`: there is no `tests/__init__.py`, so `import
tests.conftest` would load a second, unregistered copy of the `conftest`
module rather than the one pytest auto-loads and registers the hook from.
"""

import pytest

from tests._prod_db_guard import check_collection, is_production_db_url


class _FakeItem:
    """Minimal stand-in for a pytest `Item` exposing only what the hook needs."""

    def __init__(self, has_smoke_marker: bool):
        """Record whether this fake item carries the `smoke` marker."""
        self._has_smoke_marker = has_smoke_marker

    def get_closest_marker(self, name):
        """Return a truthy sentinel for `smoke` when configured, else None."""
        if name == "smoke" and self._has_smoke_marker:
            return object()
        return None


@pytest.mark.smoke
class TestIsProductionDbUrl:
    """Unit coverage for the URL-shape parsing helper."""

    def test_bare_postgres_url_wmata(self):
        """`postgresql:///wmata_dashboard` (no host) is production."""
        assert is_production_db_url("postgresql:///wmata_dashboard") is True

    def test_bare_postgres_url_sfmta(self):
        """`postgresql:///sfmta_dashboard` (no host) is production."""
        assert is_production_db_url("postgresql:///sfmta_dashboard") is True

    def test_full_url_with_user_host_port(self):
        """A fully qualified URL with credentials/host/port is still parsed."""
        assert is_production_db_url("postgresql://user:pw@localhost:5432/wmata_dashboard") is True

    def test_url_with_query_string(self):
        """A trailing query string doesn't get glued onto the DB name."""
        assert (
            is_production_db_url("postgresql://localhost:5432/wmata_dashboard?sslmode=disable")
            is True
        )

    def test_scratch_db_name_is_not_production(self):
        """A scratch DB name is not treated as production."""
        assert is_production_db_url("postgresql:///wmata_test_local") is False

    def test_sqlite_url_is_never_production(self):
        """Non-Postgres URLs (sqlite) are never production."""
        assert is_production_db_url("sqlite:///:memory:") is False

    def test_none_is_not_production(self):
        """A missing/None URL is not production."""
        assert is_production_db_url(None) is False

    def test_empty_string_is_not_production(self):
        """An empty URL string is not production."""
        assert is_production_db_url("") is False


@pytest.mark.smoke
class TestCheckCollection:
    """Coverage for `check_collection`, the hookimpl's pure decision core."""

    def test_prod_url_with_nonsmoke_item_aborts(self, monkeypatch):
        """A prod DATABASE_URL plus a non-smoke item aborts the session."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=False)]
        assert check_collection(items) is not None

    def test_prod_url_with_only_smoke_items_passes(self, monkeypatch):
        """A prod DATABASE_URL with only smoke-marked items is not blocked."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=True)]
        assert check_collection(items) is None

    def test_prod_url_with_override_env_passes(self, monkeypatch):
        """`PYTEST_ALLOW_PROD_DB=1` overrides the guard."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.setenv("PYTEST_ALLOW_PROD_DB", "1")

        items = [_FakeItem(has_smoke_marker=False)]
        assert check_collection(items) is None

    def test_scratch_url_passes(self, monkeypatch):
        """A scratch DATABASE_URL never triggers the guard."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_test_local")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=False)]
        assert check_collection(items) is None

    def test_sqlite_url_passes(self, monkeypatch):
        """A sqlite DATABASE_URL never triggers the guard."""
        monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=False)]
        assert check_collection(items) is None

    def test_prod_sfmta_url_aborts(self, monkeypatch):
        """A prod SFMTA_DATABASE_URL is caught even when DATABASE_URL is scratch."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_test_local")
        monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///sfmta_dashboard")
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=False)]
        assert check_collection(items) is not None

    def test_prod_pg_test_database_url_aborts(self, monkeypatch):
        """A prod PG_TEST_DATABASE_URL is caught too."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_test_local")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.setenv("PG_TEST_DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=False)]
        assert check_collection(items) is not None

    def test_no_items_selected_aborts(self, monkeypatch):
        """An empty item list (e.g. `-m 'not smoke'` on an all-smoke file)
        is not treated as smoke-only -- it's exactly the shape of an
        accidental non-smoke run, so the guard still fires.
        """
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        assert check_collection([]) is not None

    def test_message_names_offending_var_and_scratch_invocation(self, monkeypatch):
        """The abort message names the offending var and a working scratch invocation."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        message = check_collection([_FakeItem(has_smoke_marker=False)])
        assert message is not None
        assert "DATABASE_URL=postgresql:///wmata_dashboard" in message
        assert "PG_TEST_DATABASE_URL=postgresql:///wmata_test_local" in message
        assert "SFMTA_DATABASE_URL=postgresql:///wmata_test_local" in message
        assert "bin/test-with-pg" in message
        assert "PYTEST_ALLOW_PROD_DB=1" in message
