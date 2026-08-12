"""
Tests for get_agency_comparison_data — the agency comparison page's (PR #198)
core aggregation (WMATA vs SFMTA headline KPIs over the matched window).

Multi-agency is one physical database per agency (src/agency_config.py), so
this function takes a dict of already-open sessions rather than a single
`db: Session` like the rest of api/aggregations.py. Tests build one
in-memory SQLite session per simulated agency to exercise the cross-session
logic without a real second Postgres database.
"""

from datetime import timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import api.main
from api.aggregations import AGENCY_COMPARISON_WINDOW_START, get_agency_comparison_data
from src.models import Base, SystemMetricsDaily
from src.timezones import eastern_today


def _make_session():
    """Build a fresh in-memory SQLite session, mirroring conftest's test_engine."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def _seed_row(session, service_date, **kwargs):
    """Insert one system_metrics_daily row; kwargs override the defaults."""
    defaults = {
        "otp_percentage": 70.0,
        "service_delivered_ratio": 0.9,
        "ewt_seconds": 150.0,
        "bunching_rate": 0.03,
        "data_quality": "complete",
        "coverage_pct": 1.0,
    }
    defaults.update(kwargs)
    session.add(SystemMetricsDaily(service_date=service_date.isoformat(), **defaults))


class TestGetAgencyComparisonData:
    """Core aggregation tests — no HTTP layer involved."""

    def test_empty_sessions_envelope(self):
        """Both agencies present but with no rows: null means, empty window_end."""
        wmata_db = _make_session()
        sfmta_db = _make_session()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db, "sfmta": sfmta_db})
        finally:
            wmata_db.close()
            sfmta_db.close()

        assert result["window_start"] == AGENCY_COMPARISON_WINDOW_START.isoformat()
        assert result["window_end"] is None
        assert len(result["agencies"]) == 2
        agency_names = {a["agency"] for a in result["agencies"]}
        assert agency_names == {"wmata", "sfmta"}
        for agency in result["agencies"]:
            for metric in ("otp", "service_delivered", "ewt", "bunching"):
                assert agency["metrics"][metric]["window_mean"] is None
                assert agency["metrics"][metric]["wow_delta"] is None
                assert agency["metrics"][metric]["days_included"] == 0
                assert agency["metrics"][metric]["partial_days"] == 0
        assert len(result["caveats"]) >= 3

    def test_display_names_from_agency_config(self):
        """Each agency's display_name comes from config/agencies/<name>.yaml."""
        wmata_db = _make_session()
        sfmta_db = _make_session()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db, "sfmta": sfmta_db})
        finally:
            wmata_db.close()
            sfmta_db.close()

        by_agency = {a["agency"]: a["display_name"] for a in result["agencies"]}
        assert by_agency["wmata"] == "WMATA Metrobus"
        assert by_agency["sfmta"] == "SFMTA (Muni)"

    def test_missing_agency_omitted_from_output(self):
        """A sessions dict with only one agency yields a one-entry agencies list.

        Simulates a dev environment without SFMTA_DATABASE_URL configured --
        the endpoint should degrade to showing whatever agencies it could
        open a session for, not blow up.
        """
        wmata_db = _make_session()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db})
        finally:
            wmata_db.close()

        assert len(result["agencies"]) == 1
        assert result["agencies"][0]["agency"] == "wmata"

    def test_window_mean_skips_nulls_and_excludes_before_window_start(self):
        """window_mean averages only non-null values on/after window_start."""
        wmata_db = _make_session()
        # One row before the matched window (must be excluded).
        _seed_row(wmata_db, AGENCY_COMPARISON_WINDOW_START - timedelta(days=1), otp_percentage=10.0)
        # Two rows inside the window, one with a null OTP value.
        _seed_row(wmata_db, AGENCY_COMPARISON_WINDOW_START, otp_percentage=60.0)
        _seed_row(wmata_db, AGENCY_COMPARISON_WINDOW_START + timedelta(days=1), otp_percentage=None)
        wmata_db.commit()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db})
        finally:
            wmata_db.close()

        otp = result["agencies"][0]["metrics"]["otp"]
        assert otp["window_mean"] == 60.0
        assert otp["days_included"] == 1

    def test_partial_days_counted_but_still_included_in_mean(self):
        """Partial-quality rows still contribute their real value to window_mean.

        `collector_heartbeats` rows never reach the laptop database for
        SFMTA, leaving `vehicle_positions` as the only completeness-check
        numerator -- a ~33% ceiling regardless of collection health
        (NOTES-104) -- so every SFMTA row is flagged 'partial' even though
        the underlying metric is computed from real observations;
        excluding partial rows from the mean would zero out SFMTA entirely.
        """
        sfmta_db = _make_session()
        _seed_row(
            sfmta_db,
            AGENCY_COMPARISON_WINDOW_START,
            otp_percentage=65.0,
            data_quality="partial",
            coverage_pct=0.33,
        )
        _seed_row(
            sfmta_db,
            AGENCY_COMPARISON_WINDOW_START + timedelta(days=1),
            otp_percentage=67.0,
            data_quality="partial",
            coverage_pct=0.30,
        )
        sfmta_db.commit()
        try:
            result = get_agency_comparison_data({"sfmta": sfmta_db})
        finally:
            sfmta_db.close()

        otp = result["agencies"][0]["metrics"]["otp"]
        assert otp["window_mean"] == pytest.approx(66.0)
        assert otp["days_included"] == 2
        assert otp["partial_days"] == 2

    def test_week_over_week_delta_computed_from_shared_anchor(self):
        """wow_delta compares the trailing 7 days against the prior 7, per agency.

        Seeds 14 consecutive days ending at window_start + 13 with a known
        step function (30.0 for the first 7 days, 40.0 for the next 7) so
        the expected delta is unambiguous: mean(40.0 * 7) - mean(30.0 * 7)
        = 10.0.
        """
        wmata_db = _make_session()
        for i in range(7):
            _seed_row(
                wmata_db, AGENCY_COMPARISON_WINDOW_START + timedelta(days=i), otp_percentage=30.0
            )
        for i in range(7, 14):
            _seed_row(
                wmata_db, AGENCY_COMPARISON_WINDOW_START + timedelta(days=i), otp_percentage=40.0
            )
        wmata_db.commit()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db})
        finally:
            wmata_db.close()

        otp = result["agencies"][0]["metrics"]["otp"]
        assert otp["wow_delta"] == pytest.approx(10.0)
        assert (
            result["window_end"]
            == (AGENCY_COMPARISON_WINDOW_START + timedelta(days=13)).isoformat()
        )

    def test_week_over_week_delta_null_when_window_too_short(self):
        """Fewer than 14 days of history in the matched window: delta stays null.

        Matches the item's 'week-over-week deltas where the window allows'
        scope decision -- early in the matched window there isn't a full
        prior week to compare against yet.
        """
        wmata_db = _make_session()
        for i in range(5):
            _seed_row(
                wmata_db, AGENCY_COMPARISON_WINDOW_START + timedelta(days=i), otp_percentage=50.0
            )
        wmata_db.commit()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db})
        finally:
            wmata_db.close()

        otp = result["agencies"][0]["metrics"]["otp"]
        assert otp["wow_delta"] is None
        assert otp["window_mean"] == pytest.approx(50.0)

    def test_shared_anchor_is_min_of_agencies_latest_dates(self):
        """window_end (and each agency's wow slice) anchors on the earlier
        of the two agencies' latest available dates, so both columns
        compare the same calendar days.
        """
        wmata_db = _make_session()
        sfmta_db = _make_session()
        # WMATA has data through day 20; SFMTA only through day 15.
        for i in range(21):
            _seed_row(wmata_db, AGENCY_COMPARISON_WINDOW_START + timedelta(days=i))
        for i in range(16):
            _seed_row(sfmta_db, AGENCY_COMPARISON_WINDOW_START + timedelta(days=i))
        wmata_db.commit()
        sfmta_db.commit()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db, "sfmta": sfmta_db})
        finally:
            wmata_db.close()
            sfmta_db.close()

        assert (
            result["window_end"]
            == (AGENCY_COMPARISON_WINDOW_START + timedelta(days=15)).isoformat()
        )

    def test_window_mean_clipped_to_shared_anchor(self):
        """Every agency's window_mean/days_included/partial_days are clipped
        to the shared anchor, not just window_end.

        SFMTA has one extra day beyond the anchor (its own nightly batch
        ran a day ahead of WMATA, the laggard that sets the anchor) with a
        wildly different, partial-flagged value. That extra day must not
        leak into SFMTA's mean/counts -- otherwise the two columns would
        silently average different calendar day sets while the page
        header claims a matched window (the bug: window_end was clipped
        correctly but the per-metric aggregates iterated the agency's
        full unclipped row set).
        """
        wmata_db = _make_session()
        sfmta_db = _make_session()
        # WMATA has data through day 5 only -- this sets the anchor.
        for i in range(6):
            _seed_row(
                wmata_db, AGENCY_COMPARISON_WINDOW_START + timedelta(days=i), otp_percentage=70.0
            )
        # SFMTA has the same 6 days plus one day PAST the anchor with a
        # very different, partial value that must be excluded.
        for i in range(6):
            _seed_row(
                sfmta_db, AGENCY_COMPARISON_WINDOW_START + timedelta(days=i), otp_percentage=70.0
            )
        _seed_row(
            sfmta_db,
            AGENCY_COMPARISON_WINDOW_START + timedelta(days=6),
            otp_percentage=10.0,
            data_quality="partial",
        )
        wmata_db.commit()
        sfmta_db.commit()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db, "sfmta": sfmta_db})
        finally:
            wmata_db.close()
            sfmta_db.close()

        assert (
            result["window_end"] == (AGENCY_COMPARISON_WINDOW_START + timedelta(days=5)).isoformat()
        )
        sfmta = next(a for a in result["agencies"] if a["agency"] == "sfmta")
        otp = sfmta["metrics"]["otp"]
        assert otp["window_mean"] == pytest.approx(70.0)
        assert otp["days_included"] == 6
        assert otp["partial_days"] == 0

    def test_window_end_never_exceeds_today(self):
        """Rows seeded far in the future (bad clock, bad test data) don't
        push window_end past the real Eastern 'today'.
        """
        wmata_db = _make_session()
        _seed_row(wmata_db, eastern_today() + timedelta(days=30))
        wmata_db.commit()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db})
        finally:
            wmata_db.close()

        assert result["window_end"] is None or result["window_end"] <= eastern_today().isoformat()

    def test_caveats_mention_frequent_route_and_data_completeness(self):
        """Caveat footnotes cover the honest-comparability points from the item (PR #198):
        frequent-route designation, the OTP window, the 511.org duplicate
        stop_sequence artifact, and SFMTA's structurally-partial coverage
        (NOTES-104).
        """
        wmata_db = _make_session()
        try:
            result = get_agency_comparison_data({"wmata": wmata_db})
        finally:
            wmata_db.close()

        joined = " ".join(result["caveats"]).lower()
        assert "frequent" in joined
        assert "otp" in joined or "on-time" in joined
        assert "511.org" in joined or "stop_sequence" in joined
        assert "partial" in joined or "coverage" in joined


class TestAgencyComparisonEndpoint:
    """Route-wiring tests for GET /api/agency-comparison.

    api.main calls get_session() directly rather than via
    fastapi.Depends (see conftest's `client` fixture docstring), and this
    endpoint additionally opens a *second* session for SFMTA via
    `_open_agency_sessions` -- a real second Postgres database isn't
    available in the test environment, so these tests monkeypatch that
    helper directly rather than going through `get_session`/`DATABASE_URL`
    plumbing. The aggregation logic itself is covered by
    TestGetAgencyComparisonData above.
    """

    def test_returns_agency_comparison_envelope(self, client, db_session, monkeypatch):
        """Endpoint passes its opened sessions through to
        get_agency_comparison_data and returns the result verbatim.
        """
        monkeypatch.setattr(api.main, "_open_agency_sessions", lambda names: {"wmata": db_session})

        response = client.get("/api/agency-comparison")

        assert response.status_code == 200
        body = response.json()
        assert body["window_start"] == AGENCY_COMPARISON_WINDOW_START.isoformat()
        assert len(body["agencies"]) == 1
        assert body["agencies"][0]["agency"] == "wmata"
        assert len(body["caveats"]) >= 3

    def test_closes_every_opened_session(self, client, monkeypatch):
        """Every session _open_agency_sessions hands back gets closed,
        even though the endpoint doesn't touch it directly after
        delegating to get_agency_comparison_data.
        """
        closed = []

        class _FakeSession:
            def query(self, *args, **kwargs):
                class _EmptyQuery:
                    def filter(self, *a, **k):
                        return self

                    def all(self):
                        return []

                return _EmptyQuery()

            def close(self):
                closed.append(True)

        monkeypatch.setattr(
            api.main,
            "_open_agency_sessions",
            lambda names: {"wmata": _FakeSession(), "sfmta": _FakeSession()},
        )

        response = client.get("/api/agency-comparison")

        assert response.status_code == 200
        assert len(closed) == 2
