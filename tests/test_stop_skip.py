"""
Unit tests for src/stop_skip.py.

Covers route-day skip rate over the runs aggregation, the source restriction
to TU-only, the RUN_EXISTED denominator filter, and the per-stop breakdown
with `(direction_id, stop_id)` grouping per the CLAUDE.md stop_id/direction
gotcha.
"""

from datetime import date, datetime

from src.models import Run, StopEvent
from src.stop_skip import (
    RUN_EXISTED_MIN_STOPS,
    compute_per_stop_skip_rate,
    compute_stop_skip_rate,
    compute_stop_skip_rate_for_routes,
)

ROUTE = "TEST1"
SERVICE_DATE = date(2026, 4, 15)
SERVICE_DATE_STR = SERVICE_DATE.isoformat()


def _make_run(
    trip_id: str,
    source: str,
    *,
    stops_observed: int,
    stops_skipped: int,
    stops_scheduled: int,
    stops_observable: int | None = None,
    route_id: str = ROUTE,
    service_date: str = SERVICE_DATE_STR,
    direction_id: int = 0,
) -> Run:
    """Build one Run row with sensible defaults for the fields the metric ignores.

    `stops_observable` defaults to the per-source structural ceiling
    `aggregate_runs.py` writes in production: `stops_scheduled - 1` for TU
    rows (origin is unobservable), `stops_scheduled` for proximity rows.
    Callers can override to model edge cases.
    """
    if stops_observable is None:
        stops_observable = (
            max(0, stops_scheduled - 1) if source == "trip_update" else stops_scheduled
        )
    return Run(
        service_date=service_date,
        trip_id=trip_id,
        route_id=route_id,
        direction_id=direction_id,
        source=source,
        stops_observed=stops_observed,
        stops_skipped=stops_skipped,
        stops_scheduled=stops_scheduled,
        stops_observable=stops_observable,
    )


def _make_stop_event(
    trip_id: str,
    stop_id: str,
    stop_sequence: int,
    *,
    source: str = "trip_update",
    schedule_relationship: str = "SCHEDULED",
    route_id: str = ROUTE,
    service_date: str = SERVICE_DATE_STR,
    direction_id: int = 0,
    observed_arrival_ts: datetime | None = None,
) -> StopEvent:
    """Build one StopEvent row. SCHEDULED gets a default observed_arrival_ts;
    SKIPPED leaves it null per the StopEvent docstring."""
    if observed_arrival_ts is None and schedule_relationship == "SCHEDULED":
        observed_arrival_ts = datetime(2026, 4, 15, 12, 0, stop_sequence)
    return StopEvent(
        service_date=service_date,
        trip_id=trip_id,
        route_id=route_id,
        direction_id=direction_id,
        stop_id=stop_id,
        stop_sequence=stop_sequence,
        source=source,
        schedule_relationship=schedule_relationship,
        observed_arrival_ts=observed_arrival_ts,
    )


class TestComputeStopSkipRate:
    """Route-day rollup over the runs table."""

    def test_no_runs_returns_zero_with_null_rate(self, db_session):
        result = compute_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert result == {
            "route_id": ROUTE,
            "service_date": SERVICE_DATE_STR,
            "n_runs": 0,
            "stops_skipped": 0,
            "stops_observable": 0,
            "skip_rate": None,
        }

    def test_single_qualifying_run(self, db_session):
        # TU run: stops_scheduled=60, helper sets stops_observable=59
        # (origin unobservable in TU feed). Denominator sums stops_observable.
        db_session.add(
            _make_run("T1", "trip_update", stops_observed=50, stops_skipped=8, stops_scheduled=60)
        )
        db_session.commit()
        result = compute_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert result["n_runs"] == 1
        assert result["stops_skipped"] == 8
        assert result["stops_observable"] == 59
        assert result["skip_rate"] == round(8 / 59, 4)

    def test_proximity_runs_excluded(self, db_session):
        # Proximity row with bogus skip count — must not contribute.
        db_session.add_all(
            [
                _make_run(
                    "T1",
                    "proximity",
                    stops_observed=50,
                    stops_skipped=99,
                    stops_scheduled=60,
                ),
                _make_run(
                    "T1",
                    "trip_update",
                    stops_observed=50,
                    stops_skipped=8,
                    stops_scheduled=60,
                ),
            ]
        )
        db_session.commit()
        result = compute_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert result["n_runs"] == 1
        assert result["stops_skipped"] == 8
        assert result["stops_observable"] == 59

    def test_run_existed_filter_excludes_thin_runs(self, db_session):
        # A barely-observed TU run — drops out of the denominator so its
        # 60 scheduled stops don't dilute the rate.
        db_session.add_all(
            [
                _make_run(
                    "T_THIN",
                    "trip_update",
                    stops_observed=RUN_EXISTED_MIN_STOPS - 1,
                    stops_skipped=0,
                    stops_scheduled=60,
                ),
                _make_run(
                    "T_FULL",
                    "trip_update",
                    stops_observed=50,
                    stops_skipped=8,
                    stops_scheduled=60,
                ),
            ]
        )
        db_session.commit()
        result = compute_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert result["n_runs"] == 1
        assert result["stops_observable"] == 59
        assert result["skip_rate"] == round(8 / 59, 4)

    def test_multiple_qualifying_runs_sum(self, db_session):
        db_session.add_all(
            [
                _make_run(
                    "T1", "trip_update", stops_observed=50, stops_skipped=4, stops_scheduled=60
                ),
                _make_run(
                    "T2", "trip_update", stops_observed=50, stops_skipped=6, stops_scheduled=60
                ),
            ]
        )
        db_session.commit()
        result = compute_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert result["n_runs"] == 2
        assert result["stops_skipped"] == 10
        # Two TU runs, each with stops_observable=59 (60 - 1 for unobservable origin).
        assert result["stops_observable"] == 118
        assert result["skip_rate"] == round(10 / 118, 4)

    def test_denominator_uses_stops_observable_not_stops_scheduled(self, db_session):
        # Direct assertion of the NOTES-32 fix: the denominator must sum
        # `stops_observable` (the per-source structural ceiling), not
        # `stops_scheduled`. A TU run with stops_scheduled=10 has
        # stops_observable=9 because the origin row is structurally absent
        # from the TripUpdates feed (NOTES-31). Including it inflates the
        # denominator by 1 per qualifying TU run and biases skip rate down.
        db_session.add(
            _make_run(
                "T_TU",
                "trip_update",
                stops_observed=9,
                stops_skipped=1,
                stops_scheduled=10,
                stops_observable=9,
            )
        )
        db_session.commit()
        result = compute_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        # 1/9 (observable), not 1/10 (scheduled).
        assert result["stops_observable"] == 9
        assert result["skip_rate"] == round(1 / 9, 4)

    def test_per_source_asymmetry_only_tu_observable_in_denominator(self, db_session):
        # Per-source asymmetry: TU has stops_observable=9 (origin
        # unobservable), proximity has stops_observable=10 (origin
        # observable). Even though both rows are physically present, the
        # source restriction filters proximity out, so the denominator is
        # 9, not 19, and definitely not 20 (the old `stops_scheduled` sum).
        db_session.add_all(
            [
                _make_run(
                    "T1",
                    "trip_update",
                    stops_observed=9,
                    stops_skipped=1,
                    stops_scheduled=10,
                    stops_observable=9,
                ),
                _make_run(
                    "T1",
                    "proximity",
                    stops_observed=10,
                    stops_skipped=0,
                    stops_scheduled=10,
                    stops_observable=10,
                ),
            ]
        )
        db_session.commit()
        result = compute_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert result["n_runs"] == 1  # only the TU row counts
        assert result["stops_observable"] == 9
        assert result["skip_rate"] == round(1 / 9, 4)

    def test_other_routes_and_dates_ignored(self, db_session):
        db_session.add_all(
            [
                _make_run(
                    "T1", "trip_update", stops_observed=50, stops_skipped=8, stops_scheduled=60
                ),
                _make_run(
                    "T_OTHER",
                    "trip_update",
                    stops_observed=50,
                    stops_skipped=99,
                    stops_scheduled=60,
                    route_id="OTHER",
                ),
                _make_run(
                    "T_PREVDAY",
                    "trip_update",
                    stops_observed=50,
                    stops_skipped=99,
                    stops_scheduled=60,
                    service_date="2026-04-14",
                ),
            ]
        )
        db_session.commit()
        result = compute_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert result["n_runs"] == 1
        assert result["stops_skipped"] == 8


class TestComputeStopSkipRateForRoutes:
    """Batch helper covers all routes with TU runs on the date."""

    def test_default_scans_all_routes_with_tu_runs(self, db_session):
        db_session.add_all(
            [
                _make_run(
                    "T1",
                    "trip_update",
                    stops_observed=50,
                    stops_skipped=4,
                    stops_scheduled=60,
                    route_id="R_A",
                ),
                _make_run(
                    "T2",
                    "trip_update",
                    stops_observed=50,
                    stops_skipped=10,
                    stops_scheduled=50,
                    route_id="R_B",
                ),
            ]
        )
        db_session.commit()
        results = compute_stop_skip_rate_for_routes(db_session, SERVICE_DATE)
        assert [r["route_id"] for r in results] == ["R_A", "R_B"]
        # TU `stops_observable = stops_scheduled - 1` per the helper default,
        # mirroring `aggregate_runs.py` production behavior (origin unobservable).
        assert results[0]["skip_rate"] == round(4 / 59, 4)
        assert results[1]["skip_rate"] == round(10 / 49, 4)

    def test_default_skips_proximity_only_routes(self, db_session):
        # R_PROX_ONLY has a proximity run but no TU run — shouldn't surface
        # because skip rate is undefined without TU evidence.
        db_session.add_all(
            [
                _make_run(
                    "T_TU",
                    "trip_update",
                    stops_observed=50,
                    stops_skipped=4,
                    stops_scheduled=60,
                    route_id="R_TU",
                ),
                _make_run(
                    "T_PROX",
                    "proximity",
                    stops_observed=50,
                    stops_skipped=0,
                    stops_scheduled=60,
                    route_id="R_PROX_ONLY",
                ),
            ]
        )
        db_session.commit()
        results = compute_stop_skip_rate_for_routes(db_session, SERVICE_DATE)
        assert [r["route_id"] for r in results] == ["R_TU"]

    def test_explicit_route_ids_includes_empty(self, db_session):
        results = compute_stop_skip_rate_for_routes(
            db_session, SERVICE_DATE, route_ids=["R_NONEXISTENT"]
        )
        assert len(results) == 1
        assert results[0]["route_id"] == "R_NONEXISTENT"
        assert results[0]["n_runs"] == 0
        assert results[0]["skip_rate"] is None


class TestComputePerStopSkipRate:
    """Per-stop breakdown reads stop_events directly with direction grouping."""

    def _add_run_and_events(
        self,
        db_session,
        trip_id: str,
        stops_observed: int,
        events: list[StopEvent],
        direction_id: int = 0,
    ) -> None:
        db_session.add(
            _make_run(
                trip_id,
                "trip_update",
                stops_observed=stops_observed,
                stops_skipped=sum(1 for e in events if e.schedule_relationship == "SKIPPED"),
                stops_scheduled=len(events),
                direction_id=direction_id,
            )
        )
        db_session.add_all(events)

    def test_orders_by_skip_rate_desc(self, db_session):
        # 12 TU runs → STOP_BAD has 4 skips / 12 (33%), STOP_OK has 1 / 12 (8%).
        for i in range(12):
            trip_id = f"T{i}"
            events = [
                _make_stop_event(
                    trip_id,
                    "STOP_BAD",
                    1,
                    schedule_relationship="SKIPPED" if i < 4 else "SCHEDULED",
                ),
                _make_stop_event(
                    trip_id,
                    "STOP_OK",
                    2,
                    schedule_relationship="SKIPPED" if i == 0 else "SCHEDULED",
                ),
            ]
            self._add_run_and_events(db_session, trip_id, stops_observed=10, events=events)
        db_session.commit()

        results = compute_per_stop_skip_rate(db_session, ROUTE, SERVICE_DATE, min_observations=10)
        assert [r["stop_id"] for r in results] == ["STOP_BAD", "STOP_OK"]
        assert results[0]["stops_skipped"] == 4
        assert results[0]["stops_total"] == 12
        assert results[1]["stops_skipped"] == 1

    def test_direction_grouping_splits_shared_stop_id(self, db_session):
        # Same stop_id served in both directions — termini scenario from the
        # CLAUDE.md gotcha. Must produce two output rows, not one.
        for i in range(10):
            for direction in (0, 1):
                trip_id = f"T{i}_D{direction}"
                events = [
                    _make_stop_event(
                        trip_id,
                        "TERMINUS",
                        1,
                        direction_id=direction,
                        schedule_relationship="SKIPPED"
                        if direction == 0 and i < 3
                        else "SCHEDULED",
                    ),
                ]
                self._add_run_and_events(
                    db_session,
                    trip_id,
                    stops_observed=10,
                    events=events,
                    direction_id=direction,
                )
        db_session.commit()

        results = compute_per_stop_skip_rate(db_session, ROUTE, SERVICE_DATE, min_observations=5)
        by_dir = {r["direction_id"]: r for r in results}
        assert set(by_dir) == {0, 1}
        assert by_dir[0]["stops_skipped"] == 3
        assert by_dir[1]["stops_skipped"] == 0

    def test_min_observations_filters_thin_stops(self, db_session):
        # STOP_BUSY has 10 events, STOP_THIN has 2 — only STOP_BUSY survives
        # the default min_observations=10.
        for i in range(10):
            trip_id = f"T{i}"
            events = [_make_stop_event(trip_id, "STOP_BUSY", 1)]
            if i < 2:
                events.append(
                    _make_stop_event(trip_id, "STOP_THIN", 2, schedule_relationship="SKIPPED")
                )
            self._add_run_and_events(db_session, trip_id, stops_observed=10, events=events)
        db_session.commit()

        results = compute_per_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert [r["stop_id"] for r in results] == ["STOP_BUSY"]

    def test_excludes_proximity_source_events(self, db_session):
        # Proximity stop_events for the same trip should not contribute.
        for i in range(10):
            trip_id = f"T{i}"
            events = [
                _make_stop_event(
                    trip_id, "STOP_X", 1, schedule_relationship="SKIPPED" if i < 2 else "SCHEDULED"
                ),
                # Phantom proximity event — wrong source, must be ignored.
                _make_stop_event(trip_id, "STOP_X", 1, source="proximity"),
            ]
            self._add_run_and_events(db_session, trip_id, stops_observed=10, events=events)
        db_session.commit()

        results = compute_per_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert len(results) == 1
        assert results[0]["stops_total"] == 10  # not 20
        assert results[0]["stops_skipped"] == 2

    def test_excludes_events_on_non_qualifying_runs(self, db_session):
        # T_THIN's TU run has stops_observed below RUN_EXISTED — its stop_events
        # must not contribute to either numerator or denominator.
        for i in range(10):
            trip_id = f"T{i}"
            self._add_run_and_events(
                db_session,
                trip_id,
                stops_observed=10,
                events=[_make_stop_event(trip_id, "STOP_X", 1)],
            )
        # Cancelled run with 5 SKIPPED events — should drop out wholesale.
        thin_events = [
            _make_stop_event("T_THIN", "STOP_X", seq, schedule_relationship="SKIPPED")
            for seq in range(1, 6)
        ]
        self._add_run_and_events(
            db_session,
            "T_THIN",
            stops_observed=RUN_EXISTED_MIN_STOPS - 1,
            events=thin_events,
        )
        db_session.commit()

        results = compute_per_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert len(results) == 1
        assert results[0]["stops_total"] == 10  # not 15
        assert results[0]["stops_skipped"] == 0

    def test_other_routes_ignored(self, db_session):
        for i in range(10):
            self._add_run_and_events(
                db_session,
                f"T{i}",
                stops_observed=10,
                events=[
                    _make_stop_event(f"T{i}", "STOP_X", 1, schedule_relationship="SKIPPED"),
                ],
            )
        # Same stop_id on a different route — must not pollute.
        for i in range(10):
            trip_id = f"OTHER_T{i}"
            db_session.add(
                _make_run(
                    trip_id,
                    "trip_update",
                    stops_observed=10,
                    stops_skipped=0,
                    stops_scheduled=1,
                    route_id="OTHER",
                )
            )
            db_session.add(_make_stop_event(trip_id, "STOP_X", 1, route_id="OTHER"))
        db_session.commit()

        results = compute_per_stop_skip_rate(db_session, ROUTE, SERVICE_DATE)
        assert len(results) == 1
        assert results[0]["stops_total"] == 10
        assert results[0]["stops_skipped"] == 10
        assert results[0]["skip_rate"] == 1.0
