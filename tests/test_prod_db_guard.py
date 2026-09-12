"""Guard against a non-smoke pytest run silently reading the production DB.

Covers `tests/conftest.py::_is_production_db_url` (URL parsing) and the
`pytest_collection_modifyitems` hookimpl that aborts a non-smoke,
non-overridden session pointed at `wmata_dashboard` / `sfmta_dashboard`
(issue #247 / NOTES-131).
"""

import pytest

from tests.conftest import _is_production_db_url, pytest_collection_modifyitems


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


class _FakeConfig:
    """Stand-in for pytest's `Config`; the hook doesn't need real behavior from it."""


@pytest.mark.smoke
class TestIsProductionDbUrl:
    """Unit coverage for the URL-shape parsing helper."""

    def test_bare_postgres_url_wmata(self):
        """`postgresql:///wmata_dashboard` (no host) is production."""
        assert _is_production_db_url("postgresql:///wmata_dashboard") is True

    def test_bare_postgres_url_sfmta(self):
        """`postgresql:///sfmta_dashboard` (no host) is production."""
        assert _is_production_db_url("postgresql:///sfmta_dashboard") is True

    def test_full_url_with_user_host_port(self):
        """A fully qualified URL with credentials/host/port is still parsed."""
        assert _is_production_db_url("postgresql://user:pw@localhost:5432/wmata_dashboard") is True

    def test_url_with_query_string(self):
        """A trailing query string doesn't get glued onto the DB name."""
        assert (
            _is_production_db_url("postgresql://localhost:5432/wmata_dashboard?sslmode=disable")
            is True
        )

    def test_scratch_db_name_is_not_production(self):
        """A scratch DB name is not treated as production."""
        assert _is_production_db_url("postgresql:///wmata_test_local") is False

    def test_sqlite_url_is_never_production(self):
        """Non-Postgres URLs (sqlite) are never production."""
        assert _is_production_db_url("sqlite:///:memory:") is False

    def test_none_is_not_production(self):
        """A missing/None URL is not production."""
        assert _is_production_db_url(None) is False

    def test_empty_string_is_not_production(self):
        """An empty URL string is not production."""
        assert _is_production_db_url("") is False


@pytest.mark.smoke
class TestCollectionModifyitemsHook:
    """Coverage for the fail-fast `pytest_collection_modifyitems` hookimpl."""

    def test_prod_url_with_nonsmoke_item_aborts(self, monkeypatch):
        """A prod DATABASE_URL plus a non-smoke item aborts the session."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=False)]
        with pytest.raises(pytest.exit.Exception):
            pytest_collection_modifyitems(_FakeConfig(), items)

    def test_prod_url_with_only_smoke_items_passes(self, monkeypatch):
        """A prod DATABASE_URL with only smoke-marked items is not blocked."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=True)]
        pytest_collection_modifyitems(_FakeConfig(), items)  # must not raise

    def test_prod_url_with_override_env_passes(self, monkeypatch):
        """`PYTEST_ALLOW_PROD_DB=1` overrides the guard."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.setenv("PYTEST_ALLOW_PROD_DB", "1")

        items = [_FakeItem(has_smoke_marker=False)]
        pytest_collection_modifyitems(_FakeConfig(), items)  # must not raise

    def test_scratch_url_passes(self, monkeypatch):
        """A scratch DATABASE_URL never triggers the guard."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_test_local")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=False)]
        pytest_collection_modifyitems(_FakeConfig(), items)  # must not raise

    def test_sqlite_url_passes(self, monkeypatch):
        """A sqlite DATABASE_URL never triggers the guard."""
        monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=False)]
        pytest_collection_modifyitems(_FakeConfig(), items)  # must not raise

    def test_prod_sfmta_url_aborts(self, monkeypatch):
        """A prod SFMTA_DATABASE_URL is caught even when DATABASE_URL is scratch."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_test_local")
        monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///sfmta_dashboard")
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=False)]
        with pytest.raises(pytest.exit.Exception):
            pytest_collection_modifyitems(_FakeConfig(), items)

    def test_prod_pg_test_database_url_aborts(self, monkeypatch):
        """A prod PG_TEST_DATABASE_URL is caught too."""
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_test_local")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.setenv("PG_TEST_DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        items = [_FakeItem(has_smoke_marker=False)]
        with pytest.raises(pytest.exit.Exception):
            pytest_collection_modifyitems(_FakeConfig(), items)

    def test_no_items_selected_aborts(self, monkeypatch):
        """An empty item list (e.g. `-m 'not smoke'` on an all-smoke file)
        is not treated as smoke-only -- it's exactly the shape of an
        accidental non-smoke run, so the guard still fires.
        """
        monkeypatch.setenv("DATABASE_URL", "postgresql:///wmata_dashboard")
        monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
        monkeypatch.delenv("PG_TEST_DATABASE_URL", raising=False)
        monkeypatch.delenv("PYTEST_ALLOW_PROD_DB", raising=False)

        with pytest.raises(pytest.exit.Exception):
            pytest_collection_modifyitems(_FakeConfig(), [])
