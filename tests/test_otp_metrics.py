"""
Unit and integration tests for src/otp_metrics.py.

Covers the `_eastern_hour` local-hour helper's `tz_name` parameter
(NOTES-103) and a regression proving `compute_otp_split`'s `period_key`
filter buckets by the agency's own local hour, not hardcoded Eastern, when
`tz_name` is passed through.
"""

from __future__ import annotations

from datetime import date, datetime

from src.models import Run, StopEvent
from src.otp_metrics import _eastern_hour, compute_otp_split

ROUTE = "TEST1"
SERVICE_DATE = date(2026, 4, 14)  # Tuesday, EDT/PDT both in effect
SERVICE_DATE_STR = SERVICE_DATE.isoformat()


class TestEasternHourTzName:
    """`_eastern_hour` accepts `tz_name`, defaulting to Eastern; returns None
    for a None input regardless of zone."""

    def test_defaults_to_eastern(self):
        # 2026-04-14 11:00 UTC = 7:00 AM EDT (UTC-4 in April).
        assert _eastern_hour(datetime(2026, 4, 14, 11, 0, 0)) == 7

    def test_pacific_late_afternoon_buckets_correctly(self):
        # 2026-04-15 00:00 UTC = 17:00 (5pm) PDT the prior day (UTC-7 in
        # April) — diverges from the Eastern reading (20:00 / 8pm) for the
        # same instant, proving this isn't hardcoded to Eastern anymore.
        assert _eastern_hour(datetime(2026, 4, 15, 0, 0, 0), tz_name="America/Los_Angeles") == 17
        assert _eastern_hour(datetime(2026, 4, 15, 0, 0, 0)) == 20

    def test_none_passthrough_regardless_of_tz_name(self):
        assert _eastern_hour(None) is None
        assert _eastern_hour(None, tz_name="America/Los_Angeles") is None


class TestComputeOtpSplitPacificPeriodFilter:
    """NOTES-103 regression: `period_key` restricts by the agency's own
    local hour when `tz_name` is passed — not always Eastern.

    A 7:00 AM Pacific observation (14:00 UTC in April) sits squarely in
    `am_peak` (6-10) Pacific-local, but 14:00 UTC read as Eastern is 10:00
    — the exclusive upper bound of `am_peak` — so the *default* (Eastern)
    reading drops it from the am_peak filter entirely. This is the bug:
    a real 7am Pacific rider's data silently vanishes from the am_peak
    slice unless `tz_name="America/Los_Angeles"` is passed.
    """

    def _seed_pacific_7am_data(self, db_session) -> None:
        pacific_7am_utc = datetime(2026, 4, 14, 14, 0, 0)  # 7:00 AM PDT
        common = {
            "service_date": SERVICE_DATE_STR,
            "route_id": ROUTE,
            "direction_id": 0,
            "stops_observed": 10,
        }
        db_session.add_all(
            [
                Run(
                    **common,
                    trip_id="T1",
                    source="proximity",
                    origin_dev_sec=-30,
                    first_obs_ts=pacific_7am_utc,
                ),
                Run(
                    **common,
                    trip_id="T1",
                    source="trip_update",
                    destination_dev_sec=60,
                    last_obs_ts=pacific_7am_utc,
                ),
            ]
        )
        db_session.add(
            StopEvent(
                service_date=SERVICE_DATE_STR,
                route_id=ROUTE,
                direction_id=0,
                trip_id="T1",
                stop_id="S1",
                stop_sequence=1,
                source="proximity",
                schedule_relationship="SCHEDULED",
                observed_arrival_ts=pacific_7am_utc,
                deviation_sec=0,
            )
        )
        db_session.commit()

    def test_pacific_tz_name_includes_am_peak_observation(self, db_session):
        self._seed_pacific_7am_data(db_session)
        out = compute_otp_split(
            db_session,
            ROUTE,
            SERVICE_DATE,
            period_key="am_peak",
            tz_name="America/Los_Angeles",
        )
        assert out["origin"]["n"] == 1
        assert out["destination"]["n"] == 1
        assert out["all_timepoints"]["n"] == 1

    def test_eastern_default_on_same_fixture_drops_it(self, db_session):
        """Same fixture, default `tz_name` — reproduces the NOTES-103 bug:
        14:00 UTC reads as 10:00 Eastern, one hour past `am_peak`'s
        exclusive upper bound, so every sub-block silently reports n=0.
        """
        self._seed_pacific_7am_data(db_session)
        out = compute_otp_split(db_session, ROUTE, SERVICE_DATE, period_key="am_peak")
        assert out["origin"]["n"] == 0
        assert out["destination"]["n"] == 0
        assert out["all_timepoints"]["n"] == 0

    def test_unfiltered_path_unaffected_by_tz_name(self, db_session):
        """The default `period_key='all'` path never calls the hour helper
        at all (per the module docstring) — `tz_name` is a no-op there,
        confirming `otp_percentage`/`service_delivered_ratio` (which only
        ever use the unfiltered path) are unaffected by this fix either way.
        """
        self._seed_pacific_7am_data(db_session)
        out_default = compute_otp_split(db_session, ROUTE, SERVICE_DATE)
        out_pacific = compute_otp_split(
            db_session, ROUTE, SERVICE_DATE, tz_name="America/Los_Angeles"
        )
        assert out_default["origin"]["n"] == out_pacific["origin"]["n"] == 1
        assert out_default["destination"]["n"] == out_pacific["destination"]["n"] == 1
        assert out_default["all_timepoints"]["n"] == out_pacific["all_timepoints"]["n"] == 1
