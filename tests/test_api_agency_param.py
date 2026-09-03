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

import os

import pytest
from fastapi.testclient import TestClient

import api.main
from api.main import app


@pytest.fixture
def agency_client(db_session, monkeypatch):
    """TestClient whose api.main.get_session calls are recorded.

    Returns `(client, calls)` where `calls` accumulates the `db_url`
    argument passed to `get_session()` on each invocation. For the
    default `agency=wmata` this is whatever `os.environ["DATABASE_URL"]`
    resolves to (matching `get_session`'s own default of reading that
    env var) -- NOT `None`; `None` would only occur if `DATABASE_URL`
    itself were unset. The stub always backs requests with the same
    in-memory SQLite `db_session` regardless of `db_url` -- these tests
    check *routing* (which env var drove the call, and the resulting
    status code), not per-agency data isolation.
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
    # Both calls resolved through the literal DATABASE_URL value (no
    # per-agency override) -- not just "two calls happened".
    assert calls == [os.environ["DATABASE_URL"]] * 2


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
def test_path_traversal_agency_name_returns_404_not_500(agency_client):
    """`agency=../frequent_routes` 404s -- it must never reach the filesystem.

    Regression for NOTES-139 review finding 2: before the fix, `agency`
    flowed straight into `CONFIG_DIR / f"{agency}.yaml"` inside
    `load_agency_config`. `config/agencies/../frequent_routes.yaml`
    resolves to the real `config/frequent_routes.yaml` file, which has a
    completely different schema (no top-level `api` key), so the old code
    500'd with an unhandled `KeyError` -- and whether a request 404'd or
    500'd was effectively an oracle for which files exist elsewhere under
    `config/`. `_session_for_agency` now checks `agency` against
    `_valid_agency_names()` (a fixed, safe directory listing) before ever
    building a path from it.
    """
    client, calls = agency_client

    response = client.get("/api/gtfs/freshness?agency=../frequent_routes")

    assert response.status_code == 404
    # Never even opened a session -- the reject happens before any
    # filesystem access with the caller-controlled value.
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


@pytest.mark.api
def test_non_wmata_agency_uses_agency_aware_route_config(
    agency_client, db_session, monkeypatch, tmp_path
):
    """`agency=sfmta` gets a real agency-aware per-agency config (NOTES-143),
    replacing the PR #236 stopgap that unconditionally forced
    `is_frequent=False` / `targets=None` at the API layer.

    `config/frequent_routes.yaml` and `config/route_targets.yaml` are
    WMATA route_id-keyed, and route_ids overlap across agencies (SFMTA
    has its own "1", "9", "14", "90", ...) -- applying them unfiltered to
    a non-wmata request would silently classify e.g. SFMTA's own route
    "TEST1" using WMATA route TEST1's frequent/target data. Points the
    loaders at temp YAML fixtures (via their env-var overrides, see
    `src/frequent_routes.py` / `src/route_targets.py`) with `TEST1`
    classified high-frequency and given a per-route OTP override, then
    confirms: wmata sees both; sfmta sees neither the frequent
    designation nor the per-route override, but DOES still get the
    (agency-agnostic) system-default target rather than `None` --
    `src/route_targets.py`'s "system-default-only" behavior for
    non-wmata agencies, a real per-agency answer rather than the old
    all-or-nothing stub.
    """
    import src.frequent_routes as fr_mod
    import src.route_targets as rt_mod
    from src.models import Route

    freq_yaml = tmp_path / "frequent_routes.yaml"
    freq_yaml.write_text("high_freq:\n  - TEST1\nmedium_freq: []\n")
    targets_yaml = tmp_path / "route_targets.yaml"
    targets_yaml.write_text(
        "system_default:\n"
        "  otp: 75.0\n"
        "  service_delivered: 0.95\n"
        "  ewt_minutes: 3.0\n"
        "  bunching_pct: 0.04\n"
        "routes:\n"
        "  TEST1:\n"
        "    otp: 90.0\n"
    )
    monkeypatch.setenv("WMATA_FREQUENT_ROUTES_PATH", str(freq_yaml))
    monkeypatch.setenv("WMATA_ROUTE_TARGETS_PATH", str(targets_yaml))
    fr_mod.reset_cache_for_tests()
    rt_mod.reset_cache_for_tests()

    db_session.add(Route(route_id="TEST1", route_short_name="TEST1", route_type=3, is_current=True))
    db_session.commit()

    monkeypatch.setenv("SFMTA_DATABASE_URL", "postgresql:///unused-not-actually-opened")

    client, _calls = agency_client

    wmata_list = client.get("/api/routes?agency=wmata")
    sfmta_list = client.get("/api/routes?agency=sfmta")
    wmata_detail = client.get("/api/routes/TEST1?agency=wmata")
    sfmta_detail = client.get("/api/routes/TEST1?agency=sfmta")

    assert wmata_list.status_code == 200
    assert sfmta_list.status_code == 200
    assert wmata_detail.status_code == 200
    assert sfmta_detail.status_code == 200

    wmata_route = next(r for r in wmata_list.json()["routes"] if r["route_id"] == "TEST1")
    sfmta_route = next(r for r in sfmta_list.json()["routes"] if r["route_id"] == "TEST1")

    # wmata sees the real classification/per-route override...
    assert wmata_route["is_frequent"] is True
    assert wmata_route["targets"]["otp"] == 90.0
    assert wmata_detail.json()["is_frequent"] is True
    assert wmata_detail.json()["targets"]["otp"] == 90.0

    # ...sfmta gets no frequent designation and no per-route override,
    # but still a real (system-default) target -- not `None`.
    assert sfmta_route["is_frequent"] is False
    assert sfmta_route["targets"]["otp"] == 75.0
    assert sfmta_detail.json()["is_frequent"] is False
    assert sfmta_detail.json()["targets"]["otp"] == 75.0


@pytest.mark.api
def test_shapes_cache_is_isolated_per_agency(tmp_path, monkeypatch):
    """Cross-agency cache poisoning regression (NOTES-139 review finding 1).

    `/api/shapes` (`api.aggregations.get_system_shapes`) is one of eight
    process-wide caches that used to carry no database identity in their
    key -- `?agency=sfmta` and the default WMATA request shared a single
    `"system"` slot and could serve each other's shapes. This test backs
    two REAL, distinct SQLite files (not the shared in-memory `db_session`
    the other tests here use, which would give both "agencies" the same
    `_db_identity` and defeat the point) with different seeded routes, and
    asserts each agency's request only ever sees its own data -- in both
    call orders, so an entry written by either agency can't leak into the
    other's slot.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    import api.aggregations as agg
    from src.models import Base, Route, Shape, Trip

    def _seed(session, route_id: str, shape_id: str) -> None:
        """Insert one current route+trip with a 2-point shape."""
        session.add(
            Route(route_id=route_id, route_short_name=route_id, route_type=3, is_current=True)
        )
        session.add(
            Trip(trip_id=f"t-{route_id}", route_id=route_id, shape_id=shape_id, is_current=True)
        )
        session.add_all(
            [
                Shape(
                    shape_id=shape_id, shape_pt_lat=38.0, shape_pt_lon=-77.0, shape_pt_sequence=1
                ),
                Shape(
                    shape_id=shape_id, shape_pt_lat=38.1, shape_pt_lon=-77.1, shape_pt_sequence=2
                ),
            ]
        )
        session.commit()

    wmata_url = f"sqlite:///{tmp_path / 'wmata.db'}"
    sfmta_url = f"sqlite:///{tmp_path / 'sfmta.db'}"
    wmata_engine = create_engine(wmata_url)
    sfmta_engine = create_engine(sfmta_url)
    Base.metadata.create_all(bind=wmata_engine)
    Base.metadata.create_all(bind=sfmta_engine)
    wmata_session = sessionmaker(bind=wmata_engine)()
    sfmta_session = sessionmaker(bind=sfmta_engine)()
    _seed(wmata_session, "W1", "SHAPE-W")
    _seed(sfmta_session, "S1", "SHAPE-S")

    real_database_url = os.environ["DATABASE_URL"]
    monkeypatch.setenv("SFMTA_DATABASE_URL", sfmta_url)

    def _fake_get_session(db_url=None):
        """Route to whichever real session matches the resolved db_url."""
        if db_url == sfmta_url:
            return sfmta_session
        assert db_url == real_database_url
        return wmata_session

    monkeypatch.setattr(api.main, "get_session", _fake_get_session)
    monkeypatch.setattr(api.main, "_warm_scorecard_cache_sync", lambda: None)
    agg._shapes_cache.clear()

    try:
        with TestClient(app) as client:
            wmata_first = client.get("/api/shapes?agency=wmata")
            sfmta_first = client.get("/api/shapes?agency=sfmta")
            wmata_second = client.get("/api/shapes?agency=wmata")
            sfmta_second = client.get("/api/shapes?agency=sfmta")

        assert [r["route_id"] for r in wmata_first.json()["routes"]] == ["W1"]
        assert [r["route_id"] for r in sfmta_first.json()["routes"]] == ["S1"]
        # The other agency's intervening request must not have overwritten
        # the shared cache slot -- each agency's second call still sees
        # only its own data.
        assert wmata_second.json() == wmata_first.json()
        assert sfmta_second.json() == sfmta_first.json()
    finally:
        agg._shapes_cache.clear()
        wmata_session.close()
        sfmta_session.close()
