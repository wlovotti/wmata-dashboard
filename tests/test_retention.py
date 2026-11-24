"""Tests for the local-DB retention tiers (src/retention.py, pipelines/apply_retention.py).

SQLite ``db_session`` throughout (the deletes are dialect-portable
``id IN (SELECT ... LIMIT n)`` batches), so these run under ``-m smoke``.
"""

import json
from datetime import date, datetime, timedelta

import pytest

from src.agency_config import RetentionPolicy, parse_retention
from src.models import (
    RouteHeadwayMetrics,
    RouteMetricsDailyOverlay,
    Run,
    StopEvent,
    SystemMetricsDaily,
    TripUpdateState,
    VehiclePosition,
    VpArchiveLoadedFile,
)
from src.retention import (
    aggregate_status,
    apply_retention,
    has_stop_level_rows,
    is_rederivable,
    latest_derived_date,
    plan_retention,
    read_progress_statuses,
    stop_level_floor,
    vp_archive_start,
)
from src.timezones import local_today


@pytest.fixture
def db_session(tmp_path):
    """A real file-backed SQLite session (shadows conftest's rollback-wrapped one).

    Retention commits per batch and rolls back read transactions; under the
    shared fixture's outer transaction those would end the test's own
    transaction, so each test gets a throwaway database instead.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from src.models import Base

    engine = create_engine(f"sqlite:///{tmp_path / 'retention.db'}")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()
    engine.dispose()


TZ = "America/New_York"
POLICY = RetentionPolicy(stop_level_days=15, raw_days=3)
TODAY = local_today(TZ)
ANCHOR = TODAY - timedelta(days=2)
EARLY_EPOCH = 1_700_000_000  # 2023-11-14 — archive covers every test date


def _add_day(session, d, *, quality="complete", otp=80.0, sd=0.9, overlay=True, headway=True):
    """Seed one service date: stop_events + runs + (optionally) its aggregates."""
    iso = d.isoformat()
    for seq in (1, 2):
        for source in ("proximity", "trip_update"):
            session.add(
                StopEvent(
                    service_date=iso,
                    trip_id="t1",
                    route_id="R1",
                    direction_id=0,
                    stop_id=f"s{seq}",
                    stop_sequence=seq,
                    source=source,
                )
            )
    session.add(
        Run(service_date=iso, trip_id="t1", route_id="R1", direction_id=0, source="proximity")
    )
    if quality is not None:
        session.add(
            SystemMetricsDaily(
                service_date=iso,
                otp_percentage=otp,
                service_delivered_ratio=sd,
                data_quality=quality,
            )
        )
    if overlay:
        session.add(
            RouteMetricsDailyOverlay(
                route_id="R1", service_date=iso, day_type="weekday", otp_all_on_time=5
            )
        )
    if headway:
        session.add(
            RouteHeadwayMetrics(
                route_id="R1",
                date=iso,
                time_period="am_peak",
                day_type="weekday",
                total_headways=3,
            )
        )
    session.commit()


def _manifest_everything(session, epoch=EARLY_EPOCH):
    """One manifest row whose open epoch defines the S3 VP archive start."""
    session.add(
        VpArchiveLoadedFile(
            filename=f"2023-11-14.1.{epoch}.jsonl.zst", row_count=0, dropped_count=0
        )
    )
    session.commit()


def _counts(session, d):
    """(stop_events, runs) rows for one date."""
    iso = d.isoformat()
    return (
        session.query(StopEvent).filter(StopEvent.service_date == iso).count(),
        session.query(Run).filter(Run.service_date == iso).count(),
    )


@pytest.mark.smoke
def test_parse_retention_defaults_and_floors():
    """Missing block = shallow tier; unknown keys and sub-floor windows are rejected."""
    assert parse_retention(None) == RetentionPolicy(35, 7, False)
    assert parse_retention({"stop_level_days": 100}).stop_level_days == 100
    with pytest.raises(ValueError, match="Unknown retention"):
        parse_retention({"stop_level_dayz": 100})
    with pytest.raises(ValueError, match="below 15"):
        parse_retention({"stop_level_days": 14})
    with pytest.raises(ValueError, match="below 3"):
        parse_retention({"raw_days": 2})


@pytest.mark.smoke
def test_agency_yaml_tiers():
    """WMATA is the deep tier, SFMTA shallow; both default to not applying after refresh."""
    from src.agency_config import load_agency_config

    w, s = load_agency_config("wmata").retention, load_agency_config("sfmta").retention
    assert (w.stop_level_days, w.raw_days, w.apply_after_refresh) == (100, 7, False)
    assert (s.stop_level_days, s.raw_days, s.apply_after_refresh) == (35, 7, False)


@pytest.mark.smoke
def test_latest_derived_date_skips_partial_null_and_runless(db_session):
    """The anchor is the newest complete, non-null date that still has runs."""
    _add_day(db_session, ANCHOR - timedelta(days=3))
    _add_day(db_session, ANCHOR - timedelta(days=2), quality="partial")
    _add_day(db_session, ANCHOR - timedelta(days=1), otp=None)
    # Newest row has aggregates but no runs (pruned): never an anchor.
    db_session.add(
        SystemMetricsDaily(
            service_date=ANCHOR.isoformat(), otp_percentage=1.0, data_quality="complete"
        )
    )
    db_session.commit()
    assert latest_derived_date(db_session, TZ) == ANCHOR - timedelta(days=3)


@pytest.mark.smoke
def test_no_anchor_prunes_nothing(db_session):
    """With no fully derived date the plan is empty."""
    _add_day(db_session, ANCHOR - timedelta(days=40), quality="partial")
    plan = plan_retention(db_session, "wmata", TZ, POLICY)
    assert plan.anchor is None and plan.dates == [] and plan.vp_days == []


@pytest.mark.smoke
def test_aggregate_status_reasons(db_session):
    """Each missing/partial/null aggregate is named."""
    d = ANCHOR - timedelta(days=30)
    _add_day(db_session, d)
    assert aggregate_status(db_session, d) == "ok"
    _add_day(db_session, d - timedelta(days=1), quality=None)
    assert aggregate_status(db_session, d - timedelta(days=1)) == "no system_metrics_daily row"
    _add_day(db_session, d - timedelta(days=2), overlay=False)
    assert "overlay" in aggregate_status(db_session, d - timedelta(days=2))
    _add_day(db_session, d - timedelta(days=3), headway=False)
    assert "route_headway_metrics" in aggregate_status(db_session, d - timedelta(days=3))
    _add_day(db_session, d - timedelta(days=4), sd=None)
    assert "null" in aggregate_status(db_session, d - timedelta(days=4))


@pytest.mark.smoke
def test_plan_and_apply_stop_level_with_every_guard(db_session):
    """Only final, finished, re-derivable dates before the window lose their rows.

    Aggregates are never touched; runs go first so the floor moves honestly.
    """
    _manifest_everything(db_session)
    first_kept = ANCHOR - timedelta(days=POLICY.stop_level_days - 1)
    inside = first_kept
    ok_old = first_kept - timedelta(days=1)
    partial = first_kept - timedelta(days=2)
    missing = first_kept - timedelta(days=3)
    failed = first_kept - timedelta(days=4)
    allowed_partial = first_kept - timedelta(days=5)
    for d in (ANCHOR, inside, ok_old, failed):
        _add_day(db_session, d)
    _add_day(db_session, partial, quality="partial")
    _add_day(db_session, allowed_partial, quality="partial")
    _add_day(db_session, missing, quality=None)
    progress = {failed: "failed"}

    plan = plan_retention(
        db_session, "wmata", TZ, POLICY, progress=progress, allow_partial_dates={allowed_partial}
    )
    assert plan.anchor == ANCHOR and plan.stop_level_first_kept == first_kept
    by_date = {p.service_date: p for p in plan.dates}
    assert inside not in by_date and ANCHOR not in by_date
    assert by_date[ok_old].deletable and by_date[allowed_partial].deletable
    assert "partial" in by_date[partial].reason
    assert "no system_metrics_daily" in by_date[missing].reason
    assert "failed" in by_date[failed].reason
    assert by_date[ok_old].stop_events == 4 and by_date[ok_old].runs == 1
    # Dry run: nothing changed.
    assert _counts(db_session, ok_old) == (4, 1)

    result = apply_retention(
        db_session, plan, TZ, progress=progress, allow_partial_dates={allowed_partial}, batch_size=3
    )
    assert (result.stop_events, result.runs) == (8, 2)
    assert _counts(db_session, ok_old) == (0, 0)
    assert _counts(db_session, allowed_partial) == (0, 0)
    for kept in (partial, missing, failed, inside, ANCHOR):
        assert _counts(db_session, kept) == (4, 1)
    # Aggregates of the pruned date survive.
    assert db_session.get(SystemMetricsDaily, ok_old.isoformat()) is not None
    assert (
        db_session.query(RouteMetricsDailyOverlay)
        .filter(RouteMetricsDailyOverlay.service_date == ok_old.isoformat())
        .count()
        == 1
    )
    # The oldest surviving runs are the kept dates'.
    assert stop_level_floor(db_session) == min(partial, missing, failed)
    assert not has_stop_level_rows(db_session, ok_old)
    assert has_stop_level_rows(db_session, failed)


@pytest.mark.smoke
def test_unrederivable_dates_are_kept_unless_allowed(db_session):
    """A date whose VP window predates the S3 VP archive keeps its rows by default."""
    old = ANCHOR - timedelta(days=40)
    _add_day(db_session, ANCHOR)
    _add_day(db_session, old)
    archive_epoch = int(datetime.combine(old + timedelta(days=5), datetime.min.time()).timestamp())
    _manifest_everything(db_session, epoch=archive_epoch)
    plan = plan_retention(db_session, "wmata", TZ, POLICY)
    (p,) = plan.dates
    assert "not re-derivable" in p.reason
    plan = plan_retention(db_session, "wmata", TZ, POLICY, allow_unrederivable=True)
    assert plan.dates[0].deletable


@pytest.mark.smoke
def test_is_rederivable_uses_window_start_minus_guard():
    """The VP window for D starts at local midnight D; the guard widens it by 30 min."""
    d = date(2026, 8, 23)
    start = datetime(2026, 8, 23, 4, 0)  # EDT midnight in UTC
    assert is_rederivable(d, TZ, start - timedelta(minutes=30))
    assert not is_rederivable(d, TZ, start - timedelta(minutes=29))
    assert not is_rederivable(d, TZ, None)


@pytest.mark.smoke
def test_vp_archive_start_parses_epochs(db_session):
    """Earliest manifested file open time (epoch names) or UTC midnight (legacy names)."""
    db_session.add(
        VpArchiveLoadedFile(filename="2026-08-22.7414.1787432050.jsonl.zst", row_count=1)
    )
    db_session.add(VpArchiveLoadedFile(filename="2026-08-23.jsonl.zst", row_count=1))
    db_session.commit()
    assert vp_archive_start(db_session) == datetime(2026, 8, 22, 20, 54, 10)


def _vp(session, ts, n):
    """Insert ``n`` VP rows collected at ``ts``."""
    for i in range(n):
        session.add(
            VehiclePosition(
                vehicle_id=f"v{i}",
                latitude=38.9,
                longitude=-77.0,
                timestamp=ts,
                collected_at=ts,
            )
        )
    session.commit()


@pytest.mark.smoke
def test_vp_days_prune_only_manifested_finished_days_and_keep_manifest(db_session):
    """Manifested days go (manifest kept → reconcile reloads); unknown-provenance and
    days a not-yet-derived date reads stay."""
    from pipelines.load_vp_archive import reconcile_utc_day

    _add_day(db_session, ANCHOR)
    old_ok = ANCHOR - timedelta(days=20)
    unknown = ANCHOR - timedelta(days=22)
    needed_day = ANCHOR - timedelta(days=25)
    for d in (old_ok, unknown, needed_day):
        _vp(db_session, datetime.combine(d, datetime.min.time()) + timedelta(hours=12), 3)
    for d in (old_ok, needed_day):
        db_session.add(
            VpArchiveLoadedFile(filename=f"{d.isoformat()}.1.{EARLY_EPOCH}.jsonl.zst", row_count=3)
        )
    db_session.add(  # unknown: 3 rows but manifest only accounts for 1
        VpArchiveLoadedFile(
            filename=f"{unknown.isoformat()}.1.{EARLY_EPOCH}.jsonl.zst", row_count=1
        )
    )
    # Every service date in range has an aggregate row except needed_day - 1.
    d = needed_day - timedelta(days=3)
    while d < ANCHOR:
        if d != needed_day - timedelta(days=1):
            db_session.add(SystemMetricsDaily(service_date=d.isoformat(), otp_percentage=1.0))
        d += timedelta(days=1)
    db_session.commit()

    plan = plan_retention(db_session, "wmata", TZ, POLICY)
    days = {v.utc_day: v for v in plan.vp_days}
    assert days[old_ok].deletable
    assert "manifested" in days[unknown].reason
    assert "not-yet-derived" in days[needed_day].reason

    result = apply_retention(db_session, plan, TZ)
    assert result.vehicle_positions == 3
    assert (
        db_session.query(VpArchiveLoadedFile)
        .filter(VpArchiveLoadedFile.filename.startswith(old_ok.isoformat()))
        .count()
        == 1
    )
    db_session.commit()
    assert reconcile_utc_day(db_session, old_ok) == "reload"

    plan = plan_retention(db_session, "wmata", TZ, POLICY, include_unmanifested_vp=True)
    assert {v.utc_day for v in plan.deletable_vp_days()} == {unknown}


@pytest.mark.smoke
def test_trip_update_state_uses_existing_cleanup(db_session):
    """trip_update_state older than raw_days (today-anchored) is counted and deleted."""
    _add_day(db_session, ANCHOR)
    for days_ago in (1, 10):
        db_session.add(
            TripUpdateState(
                trip_id="t",
                stop_sequence=1,
                service_date=TODAY - timedelta(days=days_ago),
                stop_id="s",
                final_snapshot_ts=datetime(2026, 1, 1),
            )
        )
    db_session.commit()
    plan = plan_retention(db_session, "wmata", TZ, POLICY)
    assert plan.tus_rows == 1
    assert apply_retention(db_session, plan, TZ).trip_update_state == 1
    assert db_session.query(TripUpdateState).count() == 1


@pytest.mark.smoke
def test_read_progress_statuses(tmp_path):
    """Missing file = empty; garbage raises (retention must not guess)."""
    assert read_progress_statuses(tmp_path / "nope.json") == {}
    p = tmp_path / "p.json"
    p.write_text(json.dumps({"dates": {"2026-10-01": {"status": "failed"}}}))
    assert read_progress_statuses(p) == {date(2026, 10, 1): "failed"}
    p.write_text("{not json")
    with pytest.raises(ValueError, match="refusing to guess"):
        read_progress_statuses(p)


@pytest.mark.smoke
def test_cli_dry_run_deletes_nothing_and_apply_deletes(db_session, tmp_path, capsys, monkeypatch):
    """The CLI is read-only without --apply; --apply deletes what the plan printed."""
    import pipelines.apply_retention as mod
    from src import agency_config

    monkeypatch.setattr(
        mod,
        "load_agency_config",
        lambda name: _cfg_with(agency_config.load_agency_config(name), POLICY),
    )
    _manifest_everything(db_session)
    old = ANCHOR - timedelta(days=30)
    _add_day(db_session, ANCHOR)
    _add_day(db_session, old)
    db_session.commit()

    args = mod.build_parser().parse_args(["--agency", "wmata", "--progress-dir", str(tmp_path)])
    assert mod.run(args, session_factory=lambda: db_session) == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out and "stop_events: 4 rows over 1 date(s)" in out
    assert _counts(db_session, old) == (4, 1)

    args = mod.build_parser().parse_args(
        ["--agency", "wmata", "--apply", "--progress-dir", str(tmp_path)]
    )
    assert mod.run(args, session_factory=lambda: db_session) == 0
    assert _counts(db_session, old) == (0, 0)


def _cfg_with(cfg, policy):
    """Copy of an AgencyConfig with a different retention policy."""
    from dataclasses import replace

    return replace(cfg, retention=policy)


@pytest.mark.smoke
def test_cli_apply_respects_the_run_lock(tmp_path, monkeypatch):
    """--apply exits 75 while another process holds the refresh lock."""
    import pipelines.apply_retention as mod
    from src import run_lock

    lock_dir = tmp_path / ".lock.d"
    monkeypatch.setattr(run_lock, "LOCK_DIR", lock_dir)
    monkeypatch.delenv(run_lock.LOCK_ENV, raising=False)
    run_lock.acquire(1, "someone else", lock_dir)  # pid 1 is always alive
    try:
        assert mod.main(["--agency", "wmata", "--apply"]) == run_lock.EXIT_LOCKED
    finally:
        import shutil

        shutil.rmtree(lock_dir, ignore_errors=True)
