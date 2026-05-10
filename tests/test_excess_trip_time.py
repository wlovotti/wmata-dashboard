"""
Unit tests for src/excess_trip_time.py.

Covers per-trip dedup with the endpoint-asymmetry rule (proximity for origin,
trip_update for destination), single-source fallback, filtering of trips that
lack literal-endpoint coverage, and the aggregate stats themselves.
"""

from datetime import date, datetime, timedelta

from src.excess_trip_time import (
    _trip_actual_duration_sec,
    compute_excess_trip_time,
    compute_excess_trip_time_for_routes,
)
from src.models import Run

ROUTE = "TEST1"
SERVICE_DATE = date(2026, 4, 15)
SERVICE_DATE_STR = SERVICE_DATE.isoformat()
SCHED_FIRST = datetime(2026, 4, 15, 12, 0, 0)
SCHED_LAST = datetime(2026, 4, 15, 13, 0, 0)  # scheduled trip = 3600s


def _make_run(
    trip_id: str,
    source: str,
    *,
    first_obs_ts: datetime | None,
    last_obs_ts: datetime | None,
    origin_dev_sec: int | None,
    destination_dev_sec: int | None,
    sched_first_arrival_ts: datetime | None = SCHED_FIRST,
    sched_last_arrival_ts: datetime | None = SCHED_LAST,
    route_id: str = ROUTE,
    service_date: str = SERVICE_DATE_STR,
    direction_id: int = 0,
) -> Run:
    """Build one Run row with sensible defaults for the fields the metric ignores."""
    return Run(
        service_date=service_date,
        trip_id=trip_id,
        route_id=route_id,
        direction_id=direction_id,
        source=source,
        stops_observed=10,
        stops_skipped=0,
        sched_first_arrival_ts=sched_first_arrival_ts,
        sched_last_arrival_ts=sched_last_arrival_ts,
        first_obs_ts=first_obs_ts,
        last_obs_ts=last_obs_ts,
        origin_dev_sec=origin_dev_sec,
        destination_dev_sec=destination_dev_sec,
    )


class TestTripActualDuration:
    """Unit tests for the per-trip source-picking helper."""

    def test_joined_uses_proximity_origin_and_tu_destination(self):
        prox = _make_run(
            "T1",
            "proximity",
            first_obs_ts=SCHED_FIRST + timedelta(seconds=30),
            last_obs_ts=SCHED_FIRST + timedelta(minutes=45),  # ignored
            origin_dev_sec=30,
            destination_dev_sec=None,
        )
        tu = _make_run(
            "T1",
            "trip_update",
            first_obs_ts=SCHED_FIRST + timedelta(minutes=2),  # ignored
            last_obs_ts=SCHED_LAST + timedelta(seconds=120),
            origin_dev_sec=None,
            destination_dev_sec=120,
        )
        # TU last - prox first = (13:02:00 - 12:00:30) = 3690s
        assert _trip_actual_duration_sec(prox, tu) == 3690

    def test_joined_skipped_when_proximity_origin_missing(self):
        prox = _make_run(
            "T1",
            "proximity",
            first_obs_ts=SCHED_FIRST + timedelta(minutes=5),
            last_obs_ts=SCHED_FIRST + timedelta(minutes=45),
            origin_dev_sec=None,  # literal origin not observed
            destination_dev_sec=None,
        )
        tu = _make_run(
            "T1",
            "trip_update",
            first_obs_ts=SCHED_FIRST + timedelta(minutes=2),
            last_obs_ts=SCHED_LAST,
            origin_dev_sec=None,
            destination_dev_sec=0,
        )
        assert _trip_actual_duration_sec(prox, tu) is None

    def test_joined_skipped_when_tu_destination_missing(self):
        prox = _make_run(
            "T1",
            "proximity",
            first_obs_ts=SCHED_FIRST,
            last_obs_ts=SCHED_FIRST + timedelta(minutes=45),
            origin_dev_sec=0,
            destination_dev_sec=None,
        )
        tu = _make_run(
            "T1",
            "trip_update",
            first_obs_ts=SCHED_FIRST + timedelta(minutes=2),
            last_obs_ts=SCHED_FIRST + timedelta(minutes=50),
            origin_dev_sec=None,
            destination_dev_sec=None,  # literal destination not observed
        )
        assert _trip_actual_duration_sec(prox, tu) is None

    def test_proximity_only_with_both_endpoints(self):
        prox = _make_run(
            "T1",
            "proximity",
            first_obs_ts=SCHED_FIRST,
            last_obs_ts=SCHED_LAST + timedelta(seconds=60),
            origin_dev_sec=0,
            destination_dev_sec=60,  # rare but possible
        )
        # Single-source fallback: prox last - prox first = 3660s
        assert _trip_actual_duration_sec(prox, None) == 3660

    def test_proximity_only_skipped_without_destination(self):
        prox = _make_run(
            "T1",
            "proximity",
            first_obs_ts=SCHED_FIRST,
            last_obs_ts=SCHED_FIRST + timedelta(minutes=45),
            origin_dev_sec=0,
            destination_dev_sec=None,  # typical proximity blind spot
        )
        assert _trip_actual_duration_sec(prox, None) is None

    def test_tu_only_with_both_endpoints(self):
        tu = _make_run(
            "T1",
            "trip_update",
            first_obs_ts=SCHED_FIRST + timedelta(seconds=15),
            last_obs_ts=SCHED_LAST + timedelta(seconds=15),
            origin_dev_sec=15,  # rare but possible
            destination_dev_sec=15,
        )
        assert _trip_actual_duration_sec(None, tu) == 3600

    def test_tu_only_skipped_without_origin(self):
        tu = _make_run(
            "T1",
            "trip_update",
            first_obs_ts=SCHED_FIRST + timedelta(minutes=2),
            last_obs_ts=SCHED_LAST,
            origin_dev_sec=None,  # typical TU blind spot
            destination_dev_sec=0,
        )
        assert _trip_actual_duration_sec(None, tu) is None

    def test_zero_or_negative_duration_rejected(self):
        prox = _make_run(
            "T1",
            "proximity",
            first_obs_ts=SCHED_LAST,  # later than TU's last_obs_ts
            last_obs_ts=SCHED_LAST,
            origin_dev_sec=3600,
            destination_dev_sec=None,
        )
        tu = _make_run(
            "T1",
            "trip_update",
            first_obs_ts=SCHED_FIRST,
            last_obs_ts=SCHED_FIRST,  # earlier than prox's first_obs_ts
            origin_dev_sec=None,
            destination_dev_sec=-3600,
        )
        assert _trip_actual_duration_sec(prox, tu) is None


class TestComputeExcessTripTime:
    """Integration tests against an in-memory DB with synthetic Run rows."""

    def _add_joined_trip(self, db_session, trip_id: str, *, actual_minus_sched_sec: int) -> None:
        """Add a typical (proximity + TU) pair so the joined duration is
        scheduled + actual_minus_sched_sec."""
        db_session.add_all(
            [
                _make_run(
                    trip_id,
                    "proximity",
                    first_obs_ts=SCHED_FIRST,
                    last_obs_ts=SCHED_FIRST + timedelta(minutes=30),  # ignored
                    origin_dev_sec=0,
                    destination_dev_sec=None,
                ),
                _make_run(
                    trip_id,
                    "trip_update",
                    first_obs_ts=SCHED_FIRST + timedelta(seconds=60),  # ignored
                    last_obs_ts=SCHED_LAST + timedelta(seconds=actual_minus_sched_sec),
                    origin_dev_sec=None,
                    destination_dev_sec=actual_minus_sched_sec,
                ),
            ]
        )
        db_session.commit()

    def test_no_runs_returns_zero(self, db_session):
        result = compute_excess_trip_time(db_session, ROUTE, SERVICE_DATE)
        assert result == {
            "route_id": ROUTE,
            "service_date": SERVICE_DATE_STR,
            "n_trips": 0,
            "median_actual_sec": None,
            "p95_actual_sec": None,
            "median_scheduled_sec": None,
            "pct_over_110": None,
        }

    def test_single_on_time_trip(self, db_session):
        self._add_joined_trip(db_session, "T1", actual_minus_sched_sec=0)
        result = compute_excess_trip_time(db_session, ROUTE, SERVICE_DATE)
        assert result["n_trips"] == 1
        assert result["median_actual_sec"] == 3600
        assert result["median_scheduled_sec"] == 3600
        assert result["pct_over_110"] == 0.0

    def test_pct_over_110_threshold(self, db_session):
        # Scheduled = 3600s; 110% threshold = 3960s.
        # Three trips at +200s (under), one at +400s (just over).
        self._add_joined_trip(db_session, "T1", actual_minus_sched_sec=200)
        self._add_joined_trip(db_session, "T2", actual_minus_sched_sec=200)
        self._add_joined_trip(db_session, "T3", actual_minus_sched_sec=200)
        self._add_joined_trip(db_session, "T4", actual_minus_sched_sec=400)
        result = compute_excess_trip_time(db_session, ROUTE, SERVICE_DATE)
        assert result["n_trips"] == 4
        assert result["pct_over_110"] == 25.0

    def test_trip_lacking_literal_endpoints_excluded(self, db_session):
        # T1 = clean joined trip → counts.
        self._add_joined_trip(db_session, "T1", actual_minus_sched_sec=0)
        # T2 = TU has no destination_dev_sec → drops out, even though both rows exist.
        db_session.add_all(
            [
                _make_run(
                    "T2",
                    "proximity",
                    first_obs_ts=SCHED_FIRST,
                    last_obs_ts=SCHED_LAST,
                    origin_dev_sec=0,
                    destination_dev_sec=None,
                ),
                _make_run(
                    "T2",
                    "trip_update",
                    first_obs_ts=SCHED_FIRST + timedelta(minutes=1),
                    last_obs_ts=SCHED_LAST,
                    origin_dev_sec=None,
                    destination_dev_sec=None,
                ),
            ]
        )
        db_session.commit()
        result = compute_excess_trip_time(db_session, ROUTE, SERVICE_DATE)
        assert result["n_trips"] == 1

    def test_other_routes_and_dates_ignored(self, db_session):
        self._add_joined_trip(db_session, "T1", actual_minus_sched_sec=0)
        # Different route, same date.
        db_session.add_all(
            [
                _make_run(
                    "OTHER1",
                    "proximity",
                    first_obs_ts=SCHED_FIRST,
                    last_obs_ts=SCHED_LAST,
                    origin_dev_sec=0,
                    destination_dev_sec=None,
                    route_id="OTHER",
                ),
                _make_run(
                    "OTHER1",
                    "trip_update",
                    first_obs_ts=SCHED_FIRST + timedelta(minutes=1),
                    last_obs_ts=SCHED_LAST + timedelta(minutes=10),
                    origin_dev_sec=None,
                    destination_dev_sec=600,
                    route_id="OTHER",
                ),
            ]
        )
        # Same route, different date.
        other_date_str = "2026-04-14"
        db_session.add_all(
            [
                _make_run(
                    "T_PREVDAY",
                    "proximity",
                    first_obs_ts=SCHED_FIRST,
                    last_obs_ts=SCHED_LAST,
                    origin_dev_sec=0,
                    destination_dev_sec=None,
                    service_date=other_date_str,
                ),
                _make_run(
                    "T_PREVDAY",
                    "trip_update",
                    first_obs_ts=SCHED_FIRST + timedelta(minutes=1),
                    last_obs_ts=SCHED_LAST + timedelta(minutes=20),
                    origin_dev_sec=None,
                    destination_dev_sec=1200,
                    service_date=other_date_str,
                ),
            ]
        )
        db_session.commit()
        result = compute_excess_trip_time(db_session, ROUTE, SERVICE_DATE)
        assert result["n_trips"] == 1


class TestComputeExcessTripTimeForRoutes:
    """Batch helper covers all routes with runs on the date."""

    def test_default_scans_all_routes_with_runs(self, db_session):
        db_session.add_all(
            [
                _make_run(
                    "T1",
                    "proximity",
                    first_obs_ts=SCHED_FIRST,
                    last_obs_ts=SCHED_LAST,
                    origin_dev_sec=0,
                    destination_dev_sec=None,
                    route_id="R_A",
                ),
                _make_run(
                    "T1",
                    "trip_update",
                    first_obs_ts=SCHED_FIRST + timedelta(minutes=1),
                    last_obs_ts=SCHED_LAST,
                    origin_dev_sec=None,
                    destination_dev_sec=0,
                    route_id="R_A",
                ),
                _make_run(
                    "T2",
                    "proximity",
                    first_obs_ts=SCHED_FIRST,
                    last_obs_ts=SCHED_LAST,
                    origin_dev_sec=0,
                    destination_dev_sec=None,
                    route_id="R_B",
                ),
                _make_run(
                    "T2",
                    "trip_update",
                    first_obs_ts=SCHED_FIRST + timedelta(minutes=1),
                    last_obs_ts=SCHED_LAST,
                    origin_dev_sec=None,
                    destination_dev_sec=0,
                    route_id="R_B",
                ),
            ]
        )
        db_session.commit()
        results = compute_excess_trip_time_for_routes(db_session, SERVICE_DATE)
        assert [r["route_id"] for r in results] == ["R_A", "R_B"]
        assert all(r["n_trips"] == 1 for r in results)

    def test_explicit_route_ids_restricts(self, db_session):
        db_session.add_all(
            [
                _make_run(
                    "T1",
                    "proximity",
                    first_obs_ts=SCHED_FIRST,
                    last_obs_ts=SCHED_LAST,
                    origin_dev_sec=0,
                    destination_dev_sec=None,
                    route_id="R_A",
                ),
            ]
        )
        db_session.commit()
        results = compute_excess_trip_time_for_routes(
            db_session, SERVICE_DATE, route_ids=["R_NONEXISTENT"]
        )
        assert len(results) == 1
        assert results[0]["route_id"] == "R_NONEXISTENT"
        assert results[0]["n_trips"] == 0
