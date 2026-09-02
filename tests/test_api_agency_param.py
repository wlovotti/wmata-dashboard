"""Tests for the `agency` query parameter added to every DB-backed endpoint (NOTES-139).

Exercises `api.main._session_for_agency` through the live app:

- The default `agency=wmata` reproduces pre-NOTES-139 behavior exactly.
- An unknown agency name 404s with the list of valid names.
- A known agency (sfmta) whose database URL env var isn't set 503s,
  naming the missing env var.
- `agency=sfmta` resolves its session via `SFMTA_DATABASE_URL` when set.

Uses a dedicated fixture rather than `conftest.py`'s shared `client`
fixture: that fixture's stub ignores whatever `db_url` it's called with
(it always returns one fixed SQLite session), which is fine for tests
that don't care about agency routing but can't demonstrate it. The
fixture here instead *records* the `db_url` argument `get_session()`
receives, so `test_sfmta_agency_uses_sfmta_database_url` can assert the
right env var drove the right call.
"""

import pytest
from fastapi.testclient import TestClient

import api.main
from api.main import app


@pytest.fixture
def agency_client(db_session, monkeypatch):
    """TestClient whose api.main.get_session calls are recorded.

    Returns `(client, calls)` where `calls` accumulates the `db_url`
    argument passed to `get_session()` on each invocation (`None` means
    "use DATABASE_URL", matching `get_session`'s own default). The stub
    always backs requests with the same in-memory SQLite `db_session`
    regardless of `db_url` -- these tests check *routing* (which env var
    drove the call, and the resulting status code), not per-agency data
    isolation.
    """
    calls: list[str | None] = []

    class _SessionProxy:
        """Thin proxy so handlers' `db.close()` doesn't tear down the shared test session."""

        def __init__(self, session):
            self._session = session

        def __getattr__(self, name):
            return getattr(self._session, name)

        def close(self):
            """No-op: the underlying `db_session` fixture owns its own teardown."""
            return None

    def _fake_get_session(db_url=None):
        """Record `db_url` and hand back the shared test session."""
        calls.append(db_url)
        return _SessionProxy(db_session)

    monkeypatch.setattr(api.main, "get_session", _fake_get_session)
    # The app's startup lifespan fires a background asyncio.to_thread task
    # that calls get_session() to warm the scorecard cache -- it races
    # with test requests on a separate thread, which would otherwise add
    # a nondeterministic extra `None` entry to `calls`. Neutralize it so
    # `calls` reflects only the requests each test makes.
    monkeypatch.setattr(api.main, "_warm_scorecard_cache_sync", lambda: None)
    with TestClient(app) as test_client:
        yield test_client, calls


@pytest.mark.api
def test_default_agency_matches_bare_wmata_behavior(agency_client):
    """No `agency` param and an explicit `agency=wmata` both succeed identically.

    `config/agencies/wmata.yaml`'s `database.url_env` is `DATABASE_URL`
    itself, so this is the exact pre-NOTES-139 code path -- just routed
    through `_session_for_agency` instead of a bare `get_session()`.
    """
    client, calls = agency_client

    implicit = client.get("/api/gtfs/freshness")
    explicit = client.get("/api/gtfs/freshness?agency=wmata")

    assert implicit.status_code == 200
    assert explicit.status_code == 200
    assert implicit.json() == explicit.json()
    # Both calls resolved through DATABASE_URL (no per-agency override).
    assert len(calls) == 2


@pytest.mark.api
def test_unknown_agency_returns_404(agency_client):
    """An agency with no config/agencies/<name>.yaml file 404s with the valid list."""
    client, calls = agency_client

    response = client.get("/api/gtfs/freshness?agency=cta")

    assert response.status_code == 404
    detail = response.json()["detail"]
    assert "cta" in detail
    assert "wmata" in detail
    assert "sfmta" in detail
    # The session-opening path never runs for a rejected agency name.
    assert calls == []


@pytest.mark.api
def test_known_agency_missing_env_var_returns_503(agency_client, monkeypatch):
    """sfmta is a valid agency, but with no SFMTA_DATABASE_URL set it 503s, not 500s."""
    monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
    client, calls = agency_client

    response = client.get("/api/gtfs/freshness?agency=sfmta")

    assert response.status_code == 503
    assert "SFMTA_DATABASE_URL" in response.json()["detail"]
    assert calls == []


@pytest.mark.api
def test_sfmta_agency_uses_sfmta_database_url(agency_client, monkeypatch):
    """With SFMTA_DATABASE_URL set, agency=sfmta opens a session against it."""
    monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///sfmta_test_fixture")
    client, calls = agency_client

    response = client.get("/api/gtfs/freshness?agency=sfmta")

    assert response.status_code == 200
    assert calls == ["postgresql:///sfmta_test_fixture"]


@pytest.mark.api
def test_agency_param_applied_across_endpoint_family(agency_client, monkeypatch):
    """Spot-check a second, differently-shaped endpoint routes agency the same way.

    `/api/gtfs/freshness` above covers the mechanics in detail; this
    confirms `/api/routes` (a route_id-less, days-windowed endpoint) was
    wired up too, guarding against a copy-paste miss across the ~20
    handlers NOTES-139 touched.
    """
    monkeypatch.delenv("SFMTA_DATABASE_URL", raising=False)
    client, _calls = agency_client

    ok = client.get("/api/routes")
    unknown = client.get("/api/routes?agency=cta")
    missing_env = client.get("/api/routes?agency=sfmta")

    assert ok.status_code == 200
    assert unknown.status_code == 404
    assert missing_env.status_code == 503
