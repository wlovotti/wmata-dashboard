"""Period scorecard generator (issue #267): aggregation, exclusion and read-only rules.

All tests run against the in-memory SQLite ``db_session`` fixture; nothing
touches a real database.
"""

import json
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import event

import scripts.generate_scorecard as cli
from src import scorecard as sc
from src.models import (
    Calendar,
    CalendarDate,
    GTFSSnapshot,
    Route,
    RouteMetricsDailyOverlay,
    SystemMetricsDaily,
    Trip,
)

START = date(2026, 10, 1)  # Thursday
END = date(2026, 10, 4)  # Sunday
GENERATED = datetime(2026, 11, 25, 12, 0, 0)


def _sys_row(
    service_date: str, otp=None, sd=None, ewt=None, swt=None, bun=None, quality="complete"
):
    """Build a system_metrics_daily row."""
    return SystemMetricsDaily(
        service_date=service_date,
        otp_percentage=otp,
        service_delivered_ratio=sd,
        ewt_seconds=ewt,
        swt_seconds=swt,
        bunching_rate=bun,
        data_quality=quality,
        coverage_pct=1.0 if quality == "complete" else 0.4,
        computed_at=datetime(2026, 11, 2, 0, 0),
    )


def _overlay(
    route_id: str,
    service_date: str,
    *,
    early=0,
    on_time=0,
    late=0,
    sched=0,
    deliv=0,
    obs=(0.0, 0.0, 0),
    sch=(0.0, 0.0, 0),
    bun=(0, 0),
    quality="complete",
):
    """Build a route_metrics_daily_overlay row; obs/sch are (sum_h, sum_h_sq, n)."""
    return RouteMetricsDailyOverlay(
        route_id=route_id,
        service_date=service_date,
        day_type="weekday",
        otp_origin_early=0,
        otp_origin_on_time=0,
        otp_origin_late=0,
        otp_destination_early=0,
        otp_destination_on_time=0,
        otp_destination_late=0,
        otp_all_early=early,
        otp_all_on_time=on_time,
        otp_all_late=late,
        scheduled_trips=sched,
        delivered_trips=deliv,
        ewt_obs_sum_h=obs[0],
        ewt_obs_sum_h_sq=obs[1],
        ewt_n_observed_headways=obs[2],
        ewt_sched_sum_h=sch[0],
        ewt_sched_sum_h_sq=sch[1],
        ewt_n_scheduled_headways=sch[2],
        bunching_count=bun[0],
        bunching_total_headways=bun[1],
        data_quality=quality,
        computed_at=datetime(2026, 11, 2, 0, 0),
    )


def _bundle(early, on_time, late, sched=0, deliv=0, obs=(0.0, 0.0, 0), sch=(0.0, 0.0, 0)):
    """Hydrated overlay bundle for in-memory pooling tests."""
    from api.aggregations import _hydrate_overlay_row

    return _hydrate_overlay_row(
        _overlay(
            "R",
            "2026-10-01",
            early=early,
            on_time=on_time,
            late=late,
            sched=sched,
            deliv=deliv,
            obs=obs,
            sch=sch,
        )
    )


# ---------------------------------------------------------------------------
# Exclusion and mean-of-days rules
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_exclusion_reason_covers_missing_partial_null_and_included():
    """Missing row, non-complete stamp and null metric are each named; complete counts."""
    assert sc.exclusion_reason(None, "otp") == "missing"
    assert sc.exclusion_reason({"data_quality": "partial", "otp": 60.0}, "otp") == "partial"
    assert sc.exclusion_reason({"data_quality": "complete", "otp": None}, "otp") == "null"
    assert sc.exclusion_reason({"data_quality": "complete", "otp": 60.0}, "otp") is None


@pytest.mark.smoke
def test_summarize_is_mean_of_included_days_and_lists_exclusions():
    """Partial, missing and null days are excluded from the mean and listed with reasons."""
    dates = sc.period_dates(START, END)
    rows = {
        "2026-10-01": {"data_quality": "complete", "otp": 60.0},
        "2026-10-02": {"data_quality": "complete", "otp": 70.0},
        "2026-10-03": {"data_quality": "partial", "otp": 10.0},
        # 2026-10-04 missing entirely
    }
    s = sc.summarize_daily_metric(dates, rows, "otp")
    assert s["mean"] == pytest.approx(65.0)
    assert s["n_days"] == 4
    assert s["n_days_included"] == 2
    assert s["coverage"] == pytest.approx(0.5)
    assert s["excluded"] == [
        {"date": "2026-10-03", "reason": "partial"},
        {"date": "2026-10-04", "reason": "missing"},
    ]
    assert s["min"] == {"value": 60.0, "date": "2026-10-01"}
    assert s["max"] == {"value": 70.0, "date": "2026-10-02"}

    rows["2026-10-02"]["ewt"] = None
    rows["2026-10-01"]["ewt"] = 120.0
    e = sc.summarize_daily_metric(dates, rows, "ewt")
    assert e["mean"] == pytest.approx(120.0)
    assert {"date": "2026-10-02", "reason": "null"} in e["excluded"]


@pytest.mark.smoke
def test_summarize_with_no_included_days_is_all_none():
    """No eligible day -> every statistic None, not zero."""
    s = sc.summarize_daily_metric(sc.period_dates(START, START), {}, "otp")
    assert s["mean"] is None and s["median"] is None and s["min"] is None
    assert s["n_days_included"] == 0


@pytest.mark.smoke
def test_period_dates_rejects_reversed_period():
    """End before start is an error, not an empty period."""
    with pytest.raises(ValueError):
        sc.period_dates(END, START)


@pytest.mark.smoke
def test_day_type_split_uses_calendar_weekday():
    """Thu/Fri are weekday, Sat and Sun get their own means."""
    rows = {
        "2026-10-01": {"data_quality": "complete", "otp": 60.0},
        "2026-10-02": {"data_quality": "complete", "otp": 62.0},
        "2026-10-03": {"data_quality": "complete", "otp": 70.0},
        "2026-10-04": {"data_quality": "complete", "otp": 80.0},
    }
    split = sc.day_type_split(sc.period_dates(START, END), rows)
    assert split["weekday"]["otp"] == {"mean": 61.0, "n_days_included": 2, "n_days": 2}
    assert split["saturday"]["otp"]["mean"] == 70.0
    assert split["sunday"]["otp"]["mean"] == 80.0


@pytest.mark.smoke
def test_flag_outliers_flags_extreme_day_but_not_weekend_shift():
    """A one-day spike is flagged; a weekend level shift is not (grouped by day type)."""
    values = {f"2026-10-{d:02d}": 100.0 + (d % 3) for d in range(1, 11)}
    values["2026-10-05"] = 1000.0
    assert [f["date"] for f in sc.flag_outliers(values)] == ["2026-10-05"]

    shifted = {"a": 10.0, "b": 11.0, "c": 10.5, "d": 10.2, "s1": 50.0, "s2": 51.0}
    groups = {"a": "w", "b": "w", "c": "w", "d": "w", "s1": "s", "s2": "s"}
    assert sc.flag_outliers(shifted, groups) == []
    assert sc.flag_outliers({"a": 1.0, "b": 1.0, "c": 1.0}) == []  # MAD 0
    assert sc.flag_outliers({"a": 1.0, "b": 9.0}) == []  # too few values


# ---------------------------------------------------------------------------
# Pooling and rankings
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_route_figures_are_pooled_not_mean_of_days():
    """Day 1 1/10 on time, day 2 90/100 -> pooled 91/110 (82.7%), not mean 50%."""
    bundles = {
        "2026-10-01": {"R": _bundle(0, 1, 9, sched=10, deliv=5)},
        "2026-10-02": {"R": _bundle(0, 90, 10, sched=100, deliv=100)},
    }
    pooled = sc.pool_route_metrics(bundles)["R"]
    assert pooled["otp_all_pct"] == pytest.approx(91 * 100 / 110, abs=0.01)
    assert pooled["otp_n"] == 110
    assert pooled["service_delivered_ratio"] == pytest.approx(105 / 110, abs=1e-4)
    assert pooled["days_with_data"] == 2
    assert pooled["otp_median_day"] == pytest.approx(50.0)


@pytest.mark.smoke
def test_route_ewt_pooled_from_sufficient_statistics():
    """EWT pools sum(h) and sum(h^2) across days: AWT = sum h^2 / (2 sum h)."""
    # Day 1: two 600 s headways; day 2: one 1200 s headway. Scheduled 600 s x 3.
    bundles = {
        "2026-10-01": {"R": _bundle(0, 0, 0, obs=(1200.0, 720000.0, 2), sch=(1200.0, 720000.0, 2))},
        "2026-10-02": {"R": _bundle(0, 0, 0, obs=(1200.0, 1440000.0, 1), sch=(600.0, 360000.0, 1))},
    }
    pooled = sc.pool_route_metrics(bundles)["R"]
    awt = (720000.0 + 1440000.0) / (2 * 2400.0)
    swt = (720000.0 + 360000.0) / (2 * 1800.0)
    assert pooled["ewt_seconds"] == pytest.approx(awt - swt, abs=0.01)
    assert pooled["ewt_n_observed"] == 3


def _fields(otp=None, otp_n=0, deliv=0, ewt=None, ewt_n=0, cov=None):
    """Minimal pooled-route fields for ranking tests."""
    return {
        "otp_all_pct": otp,
        "otp_n": otp_n,
        "service_delivered_delivered": deliv,
        "service_delivered_scheduled": deliv,
        "service_delivered_ratio": 1.0 if deliv else None,
        "ewt_seconds": ewt,
        "ewt_n_observed": ewt_n,
        "ewt_coverage_ratio": cov,
        "days_with_data": 1,
        "otp_median_day": otp,
        "ewt_median_day": ewt,
    }


@pytest.mark.smoke
def test_rank_routes_applies_headline_rule_and_floors():
    """Frequent routes rank by EWT (low first), others by OTP (high first); thin routes aren't ranked."""
    big_otp = sc.ROUTE_OTP_MIN_OBSERVATIONS
    big_trips = sc.ROUTE_OTP_MIN_DELIVERED_TRIPS
    big_h = sc.ROUTE_EWT_MIN_OBSERVED_HEADWAYS
    metrics = {
        "F1": _fields(otp=50, otp_n=big_otp, deliv=big_trips, ewt=60, ewt_n=big_h, cov=0.9),
        "F2": _fields(otp=90, otp_n=big_otp, deliv=big_trips, ewt=300, ewt_n=big_h, cov=0.9),
        "F3": _fields(ewt=10, ewt_n=big_h, cov=0.2),  # coverage below floor
        "N1": _fields(otp=70, otp_n=big_otp, deliv=big_trips),
        "N2": _fields(otp=95, otp_n=big_otp, deliv=big_trips),
        "N3": _fields(otp=99, otp_n=10, deliv=5),  # thin: would top the list
    }
    names = {"F1": {"short_name": "F1", "long_name": "First"}}
    r = sc.rank_routes(metrics, frozenset({"F1", "F2", "F3", "F4"}), names, top_n=1)

    freq = r["frequent"]
    assert [e["route_id"] for e in freq["best"]] == ["F1"]
    assert [e["route_id"] for e in freq["worst"]] == ["F2"]
    assert freq["below_floor"] == ["F3"]
    assert freq["no_data"] == ["F4"]
    assert freq["best"][0]["route_long_name"] == "First"
    assert freq["worst"][0]["in_current_gtfs"] is False

    other = r["non_frequent"]
    assert [e["route_id"] for e in other["best"]] == ["N2"]
    assert [e["route_id"] for e in other["worst"]] == ["N1"]
    assert other["below_floor"] == ["N3"]


@pytest.mark.smoke
def test_best_and_worst_never_repeat_a_route():
    """With fewer eligible routes than 2 x top_n, the lists split instead of overlapping."""
    ranked = [{"route_id": x} for x in "ABC"]
    best, worst = sc._best_and_worst(ranked, 5)
    assert [e["route_id"] for e in best] == ["A", "B", "C"]
    assert worst == []
    best, worst = sc._best_and_worst(ranked, 2)
    assert [e["route_id"] for e in best] == ["A", "B"]
    assert [e["route_id"] for e in worst] == ["C"]


# ---------------------------------------------------------------------------
# End to end against SQLite
# ---------------------------------------------------------------------------


def _seed(session):
    """Seed 4 days of system rows, overlay rows, routes and one GTFS snapshot.

    10/03 is partial (excluded everywhere); 10/04 has a null EWT. Overlay
    rows are consistent with the stored system values on the complete dates.
    """
    session.add(GTFSSnapshot(snapshot_id=7, snapshot_date=datetime(2026, 9, 1), feed_version="FV7"))
    session.flush()
    session.add(
        Calendar(
            service_id="ALL",
            monday=1,
            tuesday=1,
            wednesday=1,
            thursday=1,
            friday=1,
            saturday=1,
            sunday=1,
            start_date="20260901",
            end_date="20261231",
            snapshot_id=7,
            valid_from=datetime(2026, 9, 1),
            is_current=True,
        )
    )
    session.add(
        CalendarDate(
            service_id="ALL",
            date="20261002",
            exception_type=2,
            snapshot_id=7,
            valid_from=datetime(2026, 9, 1),
            is_current=True,
        )
    )
    for rid in ("D50", "X99"):
        session.add(
            Route(
                route_id=rid,
                route_short_name=rid,
                route_long_name=f"{rid} long",
                route_type="3",
                snapshot_id=7,
                is_current=True,
            )
        )
    # System rows: OTP 60 / 80 complete, 10/03 partial, 10/04 complete with null EWT.
    session.add_all(
        [
            _sys_row("2026-10-01", otp=60.0, sd=0.5, ewt=0.0, swt=300.0, bun=0.1),
            _sys_row("2026-10-02", otp=80.0, sd=1.0, ewt=0.0, swt=300.0, bun=0.0),
            _sys_row(
                "2026-10-03", otp=5.0, sd=0.1, ewt=999.0, swt=300.0, bun=0.9, quality="partial"
            ),
            _sys_row("2026-10-04", otp=70.0, sd=0.75, ewt=None, swt=None, bun=None),
        ]
    )
    sched = (6000.0, 3600000.0, 10)  # ten 600 s scheduled headways -> SWT 300
    session.add_all(
        [
            # 10/01: 60/100 on time, 50/100 delivered, bunching 1/10.
            _overlay(
                "D50",
                "2026-10-01",
                on_time=30,
                late=20,
                sched=50,
                deliv=25,
                obs=sched,
                sch=sched,
                bun=(1, 5),
            ),
            _overlay(
                "X99",
                "2026-10-01",
                on_time=30,
                late=20,
                sched=50,
                deliv=25,
                obs=sched,
                sch=sched,
                bun=(0, 5),
            ),
            # 10/02: 80/100 on time, all delivered.
            _overlay(
                "D50",
                "2026-10-02",
                on_time=40,
                late=10,
                sched=50,
                deliv=50,
                obs=sched,
                sch=sched,
                bun=(0, 5),
            ),
            _overlay(
                "X99",
                "2026-10-02",
                on_time=40,
                late=10,
                sched=50,
                deliv=50,
                obs=sched,
                sch=sched,
                bun=(0, 5),
            ),
            # 10/03 partial date: must not reach any pooled figure.
            _overlay("D50", "2026-10-03", early=1000, sched=1000, quality="partial"),
            # 10/04: 70/100 on time, 75/100 delivered, no headways.
            _overlay("D50", "2026-10-04", on_time=35, late=15, sched=50, deliv=40),
            _overlay("X99", "2026-10-04", on_time=35, late=15, sched=50, deliv=35),
        ]
    )
    session.commit()


@pytest.mark.smoke
def test_build_scorecard_end_to_end(db_session):
    """Headline means exclude the partial day; pooled figures and rankings skip it too."""
    _seed(db_session)
    annotations = {
        "routes": [],
        "dates": [{"agency": "wmata", "date": "2026-10-02", "issue": "#999", "note": "test"}],
    }
    card = sc.build_scorecard(
        {"wmata": db_session, "sfmta": db_session},
        START,
        END,
        generated_at_utc=GENERATED,
        annotations=annotations,
        provenance={
            "git_commit": "abc123",
            "git_dirty_paths": [],
            "source_sha256": {"src/scorecard.py": "0" * 64},
            "source_digest": "1" * 64,
        },
    )
    json.dumps(card)  # fully JSON-serializable

    w = card["agencies"]["wmata"]
    assert card["peers"] == ["sfmta"]
    assert w["n_days_complete"] == 3
    assert w["metrics"]["otp"]["mean"] == pytest.approx(70.0)  # (60+80+70)/3, 10/03 out
    assert w["metrics"]["otp"]["excluded"] == [{"date": "2026-10-03", "reason": "partial"}]
    assert {"date": "2026-10-04", "reason": "null"} in w["metrics"]["ewt"]["excluded"]
    assert w["metrics"]["ewt"]["n_days_included"] == 2

    cc = w["pooled_crosscheck"]
    assert cc["pooled"]["otp"] == pytest.approx(210 * 100 / 300)
    assert cc["pooled"]["service_delivered"] == pytest.approx(225 / 300)
    assert cc["consistency"]["dates_checked"] == 3
    assert cc["consistency"]["dates_matching"] == 3, cc["consistency"]["mismatches"]

    assert w["gtfs_feed_versions"][0]["feed_version"] == "FV7"
    assert w["gtfs_feed_versions"][0]["dates"] == [
        "2026-10-01",
        "2026-10-02",
        "2026-10-03",
        "2026-10-04",
    ]
    # 10/02's only service is removed, yet service was observed: the feed did not
    # cover the date -> pattern unknown, listed, NOT holiday-type, split untouched.
    patterns = w["service_pattern_dates"]
    assert [(p["date"], p["kind"], p["holiday_type"]) for p in patterns] == [
        ("2026-10-02", "feed_not_covering", False)
    ]
    weekday = w["day_types"]["weekday"]
    assert weekday["holiday_type_dates"] == []
    assert weekday["excluding_holiday_type"] is None
    assert w["known_issues"][0]["issue"] == "#999"
    assert w["robustness"]["otp"]["median_day"] == pytest.approx(70.0)

    ranks = w["routes"]["rankings"]
    # D50 is on the real frequent list; with tiny test volumes it falls below the floor.
    assert "D50" in ranks["frequent"]["below_floor"]
    assert "X99" in ranks["non_frequent"]["below_floor"]
    assert "routes" not in card["agencies"]["sfmta"]  # peers are headline only
    assert card["caveats"]

    md = sc.render_markdown(card)
    assert "70.0% (median day 70.0%; 3 days)" in md
    assert "2026-10-03 (partial)" in md
    assert "feed_version FV7" in md
    assert "Comparability caveats" in md
    assert "2026-10-02 (weekday: schedule pattern unknown" in md
    assert "may not be reachable after a squash merge" in md
    assert "Known issue for 2026-10-02 (hand-entered annotation): test (#999)" in md
    assert "sha256 `1111111111111111` over 1 files" in md
    # Issue #269: the EWT long-gap rule is stated in the Method section.
    assert card["method"]["ewt_max_headway_sec"] == 7200
    assert "Any headway longer than 120 minutes, observed or scheduled" in md
    assert "keep values computed without it" in md
    # A scorecard JSON generated before the rule re-renders without claiming it.
    del card["method"]["ewt_max_headway_sec"]
    assert "Any headway longer than" not in sc.render_markdown(card)


@pytest.mark.smoke
def test_build_scorecard_issues_only_selects(db_session):
    """The generator never writes: every statement it issues is a SELECT."""
    _seed(db_session)
    statements: list[str] = []

    def _capture(conn, cursor, statement, parameters, context, executemany):
        """Record each SQL statement sent to the database."""
        statements.append(statement)

    engine = db_session.get_bind()
    event.listen(engine, "before_cursor_execute", _capture)
    try:
        sc.build_scorecard({"wmata": db_session}, START, END, generated_at_utc=GENERATED)
    finally:
        event.remove(engine, "before_cursor_execute", _capture)
    assert statements
    assert all(s.lstrip().upper().startswith("SELECT") for s in statements), [
        s for s in statements if not s.lstrip().upper().startswith("SELECT")
    ]
    assert not db_session.new and not db_session.dirty and not db_session.deleted


@pytest.mark.smoke
def test_cli_writes_markdown_and_json(tmp_path, monkeypatch):
    """The CLI writes both files; the JSON holds the headline figure the Markdown prints."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from src.models import Base

    db_file = tmp_path / "scorecard.db"
    url = f"sqlite:///{db_file}"
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    _seed(session)
    session.close()
    engine.dispose()

    monkeypatch.setenv("DATABASE_URL", url)
    out = tmp_path / "reports"
    rc = cli.main(
        ["--start", "2026-10-01", "--end", "2026-10-04", "--no-compare", "--out", str(out)]
    )
    assert rc == 0
    card = json.loads((out / "scorecard_wmata_2026-10-01_2026-10-04.json").read_text())
    md = (out / "scorecard_wmata_2026-10-01_2026-10-04.md").read_text()
    assert card["agencies"]["wmata"]["metrics"]["otp"]["mean"] == pytest.approx(70.0)
    assert card["peers"] == [] and card["caveats"] == []
    assert sc.fmt_metric("otp", card["agencies"]["wmata"]["metrics"]["otp"]["mean"]) in md


@pytest.mark.smoke
def test_cli_rejects_reversed_period(tmp_path):
    """--end before --start exits non-zero without touching a database."""
    assert cli.main(["--start", "2026-10-04", "--end", "2026-10-01", "--out", str(tmp_path)]) == 2


@pytest.mark.smoke
def test_fmt_metric_units():
    """OTP is already a percent; ratios render as percents; waits as minutes and seconds."""
    assert sc.fmt_metric("otp", 67.84) == "67.8%"
    assert sc.fmt_metric("service_delivered", 0.9591) == "95.9%"
    assert sc.fmt_metric("bunching", 0.0384) == "3.8%"
    assert sc.fmt_metric("ewt", 173.0) == "2.88 min (173 s)"
    assert sc.fmt_metric("ewt", None) == "n/a"


# ---------------------------------------------------------------------------
# Review round 1: service patterns, dominance guard, clamp, filters, floors
# ---------------------------------------------------------------------------

_VALID_FROM = datetime(2026, 8, 1)
_WEEKDAY_FIELDS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def _cal(service_id: str, days: str, snapshot_id: int = 9) -> Calendar:
    """Calendar row active on the weekdays in ``days`` (letters from "MTWRFSU")."""
    flags = {f: int(c in days) for f, c in zip(_WEEKDAY_FIELDS, "MTWRFSU", strict=True)}
    return Calendar(
        service_id=service_id,
        start_date="20260801",
        end_date="20261231",
        snapshot_id=snapshot_id,
        valid_from=_VALID_FROM,
        is_current=True,
        **flags,
    )


def _cal_date(service_id: str, d: date, exception_type: int, snapshot_id: int = 9) -> CalendarDate:
    """calendar_dates row for ``d``."""
    return CalendarDate(
        service_id=service_id,
        date=d.strftime("%Y%m%d"),
        exception_type=exception_type,
        snapshot_id=snapshot_id,
        valid_from=_VALID_FROM,
        is_current=True,
    )


def _trips(session, service_id: str, n: int, snapshot_id: int = 9) -> None:
    """Add ``n`` trips for ``service_id``."""
    session.add_all(
        Trip(
            trip_id=f"{service_id}-{i}",
            route_id="R",
            service_id=service_id,
            snapshot_id=snapshot_id,
            valid_from=_VALID_FROM,
            is_current=True,
        )
        for i in range(n)
    )


def _october_snapshots(snapshot_id: int = 9) -> dict:
    """Map every October 2026 ISO date to ``snapshot_id``."""
    return {
        d.isoformat(): snapshot_id for d in sc.period_dates(date(2026, 10, 1), date(2026, 10, 31))
    }


@pytest.mark.smoke
def test_service_pattern_detects_additions_only_feed(db_session):
    """SFMTA-style feed: weekdays exist only as type-1 additions; 10/12 adds Saturday service."""
    db_session.add(GTFSSnapshot(snapshot_id=9, snapshot_date=_VALID_FROM, feed_version="AO"))
    db_session.flush()
    db_session.add_all([_cal("SAT", "S"), _cal("SUN", "U")])
    d = date(2026, 8, 1)
    while d <= date(2026, 12, 31):
        if d.weekday() < 5:
            service = "SAT" if d == date(2026, 10, 12) else "WK"
            db_session.add(_cal_date(service, d, 1))
        d += timedelta(days=1)
    _trips(db_session, "WK", 10)
    _trips(db_session, "SAT", 7)
    _trips(db_session, "SUN", 7)
    db_session.commit()

    out = sc.service_pattern_dates(db_session, _october_snapshots())
    assert len(out) == 1
    p = out[0]
    assert (p["date"], p["kind"], p["runs_as"], p["holiday_type"]) == (
        "2026-10-12",
        "other_day_type",
        "saturday",
        True,
    )
    assert p["usual_services"] == [["WK"]] and p["active_services"] == ["SAT"]


@pytest.mark.smoke
def test_service_pattern_separates_reduced_from_substitution(db_session):
    """WMATA-style feed: a fewer-trips holiday is 'reduced'; a one-for-one swap is 'substitution'."""
    db_session.add(GTFSSnapshot(snapshot_id=9, snapshot_date=_VALID_FROM, feed_version="W"))
    db_session.flush()
    db_session.add_all(
        [
            _cal("WD", "MTWRF"),
            _cal("SAT", "S"),
            _cal("SUN", "U"),
            _cal_date("WD", date(2026, 10, 12), 2),
            _cal_date("HOL", date(2026, 10, 12), 1),
            _cal_date("WD", date(2026, 10, 13), 2),
            _cal_date("SWAP", date(2026, 10, 13), 1),
            # The same alternate timetable three weeks later: scattered, not persistent.
            _cal_date("WD", date(2026, 11, 3), 2),
            _cal_date("SWAP", date(2026, 11, 3), 1),
        ]
    )
    _trips(db_session, "WD", 100)
    _trips(db_session, "HOL", 78)
    _trips(db_session, "SWAP", 99)
    _trips(db_session, "SAT", 70)
    _trips(db_session, "SUN", 60)
    db_session.commit()

    out = {p["date"]: p for p in sc.service_pattern_dates(db_session, _october_snapshots())}
    assert set(out) == {"2026-10-12", "2026-10-13"}
    assert out["2026-10-12"]["kind"] == "reduced" and out["2026-10-12"]["holiday_type"]
    assert out["2026-10-12"]["trip_ratio"] == pytest.approx(0.78)
    assert out["2026-10-13"]["kind"] == "substitution"
    assert out["2026-10-13"]["holiday_type"] is False
    assert "timetable substitution" in sc._describe_pattern(out["2026-10-13"])
    assert "reduced service, 78%" in sc._describe_pattern(out["2026-10-12"])


@pytest.mark.smoke
def test_day_type_split_with_excluded_and_holiday_days():
    """An excluded weekday drops out of n and the mean; a holiday-type date is footnoted."""
    rows = {
        "2026-10-01": {"data_quality": "complete", "otp": 60.0},
        "2026-10-02": {"data_quality": "partial", "otp": 10.0},
        "2026-10-05": {"data_quality": "complete", "otp": 70.0},
        "2026-10-06": {"data_quality": "complete", "otp": 80.0},
    }
    dates = [date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5), date(2026, 10, 6)]
    split = sc.day_type_split(dates, rows, frozenset({"2026-10-05"}))
    wk = split["weekday"]
    assert wk["otp"] == {"mean": pytest.approx(70.0), "n_days_included": 3, "n_days": 4}
    assert wk["holiday_type_dates"] == ["2026-10-05"]
    assert wk["excluding_holiday_type"]["otp"] == {"mean": 70.0, "n_days_included": 2}
    assert split["saturday"]["excluding_holiday_type"] is None


@pytest.mark.smoke
def test_dominance_guard_withholds_route_from_ranking():
    """A route whose pooled EWT is ~11x its median day is listed, not ranked worst."""
    big_h = sc.ROUTE_EWT_MIN_OBSERVED_HEADWAYS
    big_otp = sc.ROUTE_OTP_MIN_OBSERVATIONS
    big_trips = sc.ROUTE_OTP_MIN_DELIVERED_TRIPS
    metrics = {
        "D50": _fields(ewt=1869.0, ewt_n=big_h, cov=0.9),
        "A40": _fields(ewt=75.0, ewt_n=big_h, cov=0.9),
        "C53": _fields(ewt=272.0, ewt_n=big_h, cov=0.9),
        "N1": _fields(otp=70.0, otp_n=big_otp, deliv=big_trips),
        "N2": _fields(otp=40.0, otp_n=big_otp, deliv=big_trips),
    }
    metrics["D50"]["ewt_median_day"] = 170.0
    metrics["C53"]["ewt_median_day"] = 170.0  # 1.6x: still ranked
    metrics["N2"]["otp_median_day"] = 65.0  # 25 pp off: withheld
    r = sc.rank_routes(metrics, frozenset({"D50", "A40", "C53"}), {}, route_issues={"D50": "#269"})
    freq = r["frequent"]
    ranked = [e["route_id"] for e in freq["best"] + freq["worst"]]
    assert "D50" not in ranked and "C53" in ranked
    assert [e["route_id"] for e in freq["dominated"]] == ["D50"]
    assert freq["dominated"][0]["issue"] == "#269"
    assert r["non_frequent"]["dominated"][0]["issue"] is None  # not tracked
    assert "(170 s) (hand-entered annotation: #269)" in "".join(sc._render_dominated(freq, "ewt"))
    assert "(not yet tracked)" in "".join(sc._render_dominated(r["non_frequent"], "otp"))
    assert [e["route_id"] for e in r["non_frequent"]["dominated"]] == ["N2"]

    assert sc.is_dominated("ewt", 200.0, 50.0) is True  # +150 s > max(50, 60)
    assert sc.is_dominated("ewt", 100.0, 50.0) is False  # +50 s < 60 s floor
    assert sc.is_dominated("otp", 70.0, 61.0) is False
    assert sc.is_dominated("ewt", None, 50.0) is False


@pytest.mark.smoke
def test_route_ewt_is_clamped_at_zero():
    """Observed waits shorter than scheduled give a negative raw EWT; the ranked value is 0."""
    bundles = {
        "2026-10-01": {"R": _bundle(0, 0, 0, obs=(1200.0, 360000.0, 4), sch=(1200.0, 720000.0, 2))}
    }
    pooled = sc.pool_route_metrics(bundles)["R"]
    assert pooled["ewt_seconds_unclamped"] < 0
    assert pooled["ewt_seconds"] == 0.0
    assert pooled["ewt_median_day"] == 0.0


@pytest.mark.smoke
def test_floors_apply_independently():
    """Delivered trips alone, and observed headways alone, each keep a route out of the ranking."""
    metrics = {
        "TRIPS_THIN": _fields(otp=99.0, otp_n=10**6, deliv=sc.ROUTE_OTP_MIN_DELIVERED_TRIPS - 1),
        "OK": _fields(otp=50.0, otp_n=10**6, deliv=10**6),
        "HW_THIN": _fields(ewt=1.0, ewt_n=sc.ROUTE_EWT_MIN_OBSERVED_HEADWAYS - 1, cov=1.0),
        "FOK": _fields(ewt=90.0, ewt_n=10**6, cov=1.0),
    }
    r = sc.rank_routes(metrics, frozenset({"HW_THIN", "FOK"}), {})
    assert r["non_frequent"]["below_floor"] == ["TRIPS_THIN"]
    assert r["frequent"]["below_floor"] == ["HW_THIN"]


@pytest.mark.smoke
@pytest.mark.parametrize(
    ("system_quality", "overlay_quality", "pooled"),
    [
        ("complete", "complete", True),
        ("partial", "complete", False),  # system-row eligibility filter alone
        ("complete", "partial", False),  # overlay data_quality filter alone
    ],
)
def test_route_pooling_filters_are_independent(db_session, system_quality, overlay_quality, pooled):
    """Either a non-complete system row or a non-complete overlay row keeps a date out."""
    db_session.add(_sys_row("2026-10-01", otp=60.0, sd=1.0, quality=system_quality))
    db_session.add(
        _overlay(
            "R1", "2026-10-01", on_time=60, late=40, sched=10, deliv=10, quality=overlay_quality
        )
    )
    db_session.commit()
    dates = [date(2026, 10, 1)]
    rows = sc.read_system_rows(db_session, dates[0], dates[0])
    bundles = sc.read_overlay_bundles(db_session, sc.eligible_dates(dates, rows))
    assert ("R1" in sc.pool_route_metrics(bundles)) is pooled


def _rob(mean, median, wo_flagged=None, wo_routes=None):
    """Minimal robustness block for comparison tests."""
    return {
        "mean": mean,
        "median_day": median,
        "mean_excluding_flagged": wo_flagged,
        "mean_excluding_routes": wo_routes,
    }


@pytest.mark.smoke
def test_compare_direction_flags_non_robust_difference():
    """Mean says the peer is worse, median day says the subject is: not a finding."""
    sections = {
        "wmata": {"robustness": {m: _rob(173, 153, 149, 145) for m in sc.HEADLINE_METRICS}},
        "sfmta": {"robustness": {m: _rob(221, 151, 157) for m in sc.HEADLINE_METRICS}},
    }
    sections["wmata"]["robustness"]["otp"] = _rob(67.8, 67.8, 67.9)
    sections["sfmta"]["robustness"]["otp"] = _rob(65.8, 66.7, 66.6)
    comp = sc.compare_direction(sections, "wmata")["sfmta"]
    assert comp["ewt"]["direction_robust"] is False
    assert comp["otp"]["direction_robust"] is True


@pytest.mark.smoke
def test_headline_robustness_marks_moved_figure_and_issue():
    """Dropping one extreme flagged day moves the mean >5%; the day's issue is carried."""
    dates = sc.period_dates(date(2026, 10, 1), date(2026, 10, 10))
    rows = {
        d.isoformat(): {
            "data_quality": "complete",
            **dict.fromkeys(sc.SYSTEM_METRICS, 150.0 + d.day),
        }
        for d in dates
    }
    rows["2026-10-07"]["ewt"] = 1900.0
    metrics = {m: sc.summarize_daily_metric(dates, rows, m) for m in sc.SYSTEM_METRICS}
    rob = sc.headline_robustness(dates, rows, metrics, {"2026-10-07": "#270"}, None, [])
    assert rob["ewt"]["moved"] is True
    assert rob["ewt"]["issues"] == ["#270"]
    assert rob["otp"]["moved"] is False


@pytest.mark.smoke
def test_provenance_records_hashes_and_dirty_state(tmp_path):
    """The CLI stamp carries source hashes and a dirty-path list; rendering is honest."""
    prov = cli.build_provenance(tmp_path)
    assert set(prov) == {"git_commit", "git_dirty_paths", "source_sha256", "source_digest"}
    # Every loaded repo module that computes a number is hashed, not just the scorecard.
    for rel in ("src/scorecard.py", "api/aggregations.py", "src/gtfs_calendar.py", "src/ewt.py"):
        assert len(prov["source_sha256"][rel]) == 64
    assert len(prov["source_digest"]) == 64
    line = sc._render_provenance(
        {"git_commit": "f" * 40, "git_dirty_paths": ["src/scorecard.py"], "source_sha256": {}}
    )[0]
    assert "uncommitted changes in `src/scorecard.py`" in line
    assert "may not be reachable after a squash merge" in line


# ---------------------------------------------------------------------------
# Review round 2: uncovered feeds, undetermined dates, ties, schedule changes
# ---------------------------------------------------------------------------


def _cal_range(service_id: str, days: str, start: str, end: str, snapshot_id: int = 9) -> Calendar:
    """Calendar row for ``days`` (letters from "MTWRFSU") between start and end (YYYYMMDD)."""
    row = _cal(service_id, days, snapshot_id)
    row.start_date, row.end_date = start, end
    return row


def _seed_snapshot(session, rows: list) -> None:
    """Snapshot 9 plus the given calendar / calendar_dates rows."""
    session.add(GTFSSnapshot(snapshot_id=9, snapshot_date=_VALID_FROM, feed_version="T"))
    session.flush()
    session.add_all(rows)
    session.commit()


@pytest.mark.smoke
def test_feed_not_covering_is_unknown_not_holiday(db_session):
    """A date past the feed's calendar is 'pattern unknown', unless nothing says service ran."""
    _seed_snapshot(db_session, [_cal_range("WD", "MTWRF", "20260801", "20260912")])
    sep16 = {"2026-09-16": 9}

    by_reason = sc.service_pattern_dates(
        db_session, sep16, reason_by_date={"2026-09-16": "fallback_loaded_by_date"}
    )
    by_observed = sc.service_pattern_dates(
        db_session, sep16, observed_service_dates=frozenset({"2026-09-16"})
    )
    for out in (by_reason, by_observed):
        assert [(p["kind"], p["holiday_type"]) for p in out] == [("feed_not_covering", False)]
    assert "did not cover this date" in sc._describe_pattern(by_reason[0])

    # With no fallback and no observed service, an empty set against a usual WD
    # set is a genuine no-service day (holiday-type).
    genuine = sc.service_pattern_dates(db_session, {"2026-09-09": 9})
    assert genuine == []  # 9/09 is covered and runs WD
    db_session.add(_cal_date("WD", date(2026, 9, 9), 2))
    db_session.commit()
    genuine = sc.service_pattern_dates(db_session, {"2026-09-09": 9})
    assert [(p["kind"], p["holiday_type"]) for p in genuine] == [("no_service", True)]


@pytest.mark.smoke
def test_date_without_neighbour_baseline_is_listed_undetermined(db_session):
    """No same-weekday neighbour has service: listed as undetermined, never skipped."""
    _seed_snapshot(db_session, [_cal_range("WK", "MTWRF", "20261012", "20261016")])
    out = sc.service_pattern_dates(db_session, {"2026-10-14": 9})
    assert [(p["kind"], p["holiday_type"]) for p in out] == [("undetermined", False)]
    assert "no neighbouring" in out[0]["detail"]


@pytest.mark.smoke
@pytest.mark.parametrize(
    ("added", "expected"),
    [
        ("C", [("undetermined", False)]),  # matches neither tied pattern
        ("B", [("service_change", False)]),  # first day of the new pattern
        ("A", []),  # same as last week: regular, not listed
    ],
)
def test_tied_baseline_guard(db_session, added, expected):
    """Neighbours split 4/4 between A and B (a schedule change at the date)."""
    _seed_snapshot(
        db_session,
        [
            _cal_range("A", "MTWRF", "20260801", "20261013"),
            _cal_range("B", "MTWRF", "20261015", "20261231"),
            _cal_date(added, date(2026, 10, 14), 1),
        ],
    )
    out = sc.service_pattern_dates(db_session, {"2026-10-14": 9})
    assert [(p["kind"], p["holiday_type"]) for p in out] == expected
    if out:
        assert out[0]["usual_services"] == [["A"], ["B"]]


@pytest.mark.smoke
def test_schedule_change_run_is_a_service_change_not_exceptions(db_session):
    """A new weekday pattern for two weeks is a schedule change, rendered as one run."""
    _seed_snapshot(
        db_session,
        [
            _cal_range("OLD", "MTWRF", "20260801", "20261016"),
            _cal_range("NEW", "MTWRF", "20261019", "20261030"),
        ],
    )
    snaps = {d.isoformat(): 9 for d in sc.period_dates(date(2026, 10, 19), date(2026, 10, 23))}
    out = sc.service_pattern_dates(db_session, snaps)
    assert {p["kind"] for p in out} == {"service_change"}
    assert not any(p["holiday_type"] for p in out)
    runs = sc.group_pattern_runs(out)
    assert len(runs) == 1 and runs[0]["n_dates"] == len(out)
    assert runs[0]["start"] == "2026-10-19"


@pytest.mark.smoke
def test_comparison_route_variant_cleans_both_agencies():
    """The excluding-routes variant uses each agency's own dominated-route mean."""
    sections = {
        "wmata": {"robustness": {m: _rob(173, 160, None, 145) for m in sc.HEADLINE_METRICS}},
        "sfmta": {"robustness": {m: _rob(221, 170, None, 150) for m in sc.HEADLINE_METRICS}},
    }
    comp = sc.compare_direction(sections, "wmata")["sfmta"]["ewt"]
    assert comp["differences"]["excluding_routes"] == pytest.approx(145 - 150)
    assert comp["direction_robust"] is True


@pytest.mark.smoke
def test_dominated_routes_applies_both_guards_to_any_route():
    """Peers have no frequent list, so the variant rule ignores designation entirely."""
    big_h = sc.ROUTE_EWT_MIN_OBSERVED_HEADWAYS
    metrics = {
        "R1": _fields(ewt=900.0, ewt_n=big_h, cov=0.9),
        "R2": _fields(ewt=100.0, ewt_n=big_h, cov=0.9),
    }
    metrics["R1"]["ewt_median_day"] = 100.0
    metrics["R2"]["ewt_median_day"] = 90.0
    assert sc.dominated_routes(metrics) == [{"route_id": "R1", "metrics": ["ewt"]}]


@pytest.mark.smoke
def test_annotation_with_unknown_agency_fails(tmp_path):
    """A typo'd agency is an error, not a silently dropped annotation."""
    bad = tmp_path / "ann.yaml"
    bad.write_text('dates:\n  - agency: wmta\n    date: "2026-10-21"\n    issue: "#1"\n')
    with pytest.raises(ValueError, match="unknown agency 'wmta'"):
        sc.load_annotations(bad)
    good = tmp_path / "ok.yaml"
    good.write_text('routes:\n  - agency: wmata\n    route_id: D50\n    issue: "#269"\n')
    assert sc.load_annotations(good)["routes"][0]["route_id"] == "D50"
