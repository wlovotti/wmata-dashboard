"""Tests for the EWT long-headway cap (issue #269).

Observed headways longer than `MAX_OBSERVED_HEADWAY_SEC` (120 min) are
dropped from EWT whatever their cause, and scheduled headways get the same
cap so AWT and SWT pool comparable headways. The observed pairing lives in
one helper (`src.ewt.pair_observed_headways`) that all four EWT call sites
share; the agreement test below pins that they produce the same numbers.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import api.aggregations as agg
import src.bunching as bunching
from src.ewt import (
    MAX_OBSERVED_HEADWAY_SEC,
    _capped_scheduled,
    _is_countable_headway,
    _observed_headways_by_cell_hour,
    compute_awt,
    compute_ewt_for_route_date,
    compute_ewt_headline_for_route,
    compute_ewt_headline_for_routes,
    compute_ewt_headline_for_routes_multi_date,
    pair_observed_headways,
)
from src.models import Calendar, Route, StopEvent, StopTime, Trip

ROUTE = "CAP1"
SERVICE_DATE = date(2026, 10, 6)  # Tuesday, EDT (UTC-4)
SERVICE_DATE_STR = SERVICE_DATE.isoformat()
EASTERN_OFFSET = timedelta(hours=4)


def _utc(hh: int, mm: int, ss: int = 0) -> datetime:
    """Naive-UTC timestamp for an Eastern clock time on SERVICE_DATE (EDT)."""
    return datetime(2026, 10, 6, hh, mm, ss) + EASTERN_OFFSET


class TestSharedConstant:
    """One cap, shared by EWT and bunching."""

    def test_bunching_uses_the_ewt_constant(self):
        """Bunching re-exports the EWT constant rather than keeping its own copy."""
        assert bunching.MAX_OBSERVED_HEADWAY_SEC is MAX_OBSERVED_HEADWAY_SEC
        assert MAX_OBSERVED_HEADWAY_SEC == 120 * 60


class TestIsCountableHeadway:
    """Boundary behaviour of the per-headway predicate."""

    @pytest.mark.parametrize(
        "delta,expected",
        [
            (0.0, False),
            (-5.0, False),
            (60.0, True),
            (119 * 60.0, True),
            (120 * 60.0, True),  # exactly 120 min is kept, as in bunching
            (120 * 60.0 + 1, False),
            (16 * 3600.0, False),
        ],
    )
    def test_boundaries(self, delta, expected):
        """Positive and at most 120 min counts; anything else does not."""
        assert _is_countable_headway(delta) is expected


class TestPairObservedHeadways:
    """The shared pairing helper, without a database."""

    def test_pair_over_120_min_dropped_119_min_kept(self):
        """A 121-min pair is excluded; a 119-min pair at another stop is kept."""
        rows = [
            ("R", 0, "S1", _utc(7, 0)),
            ("R", 0, "S1", _utc(9, 1)),  # 121 min -> dropped
            ("R", 0, "S2", _utc(7, 0)),
            ("R", 0, "S2", _utc(8, 59)),  # 119 min -> kept
        ]
        out = pair_observed_headways(rows, ZoneInfo("America/New_York"))
        assert out["R"].get((0, "S1", 7), []) == []
        assert out["R"][(0, "S2", 7)] == [119 * 60.0]

    def test_pairing_resets_on_group_direction_and_stop(self):
        """Pairs never cross a group, direction or stop boundary."""
        rows = [
            ("A", 0, "S1", _utc(7, 0)),
            ("B", 0, "S1", _utc(7, 5)),  # new group: no pair
            ("B", 1, "S1", _utc(7, 6)),  # new direction: no pair
            ("B", 1, "S2", _utc(7, 7)),  # new stop: no pair
            ("B", 1, "S2", _utc(7, 17)),  # 600 s pair
        ]
        out = pair_observed_headways(rows, ZoneInfo("America/New_York"))
        assert dict(out["B"]) == {(1, "S2", 7): [600.0]}
        assert "A" not in out or not any(out["A"].values())

    def test_d50_like_route_day_yields_sane_awt(self):
        """One 16-hour pair among normal 15-min headways no longer dominates AWT.

        Mirrors D50 on 2026-10-02: the 05:03 trip is missing, so 04:48
        pairs with 21:36 (~60,400 s). Without the cap that single pair
        drives AWT to hours; with it AWT is the 15-min service's ~450 s.
        """
        times = [_utc(1, 3) + timedelta(minutes=15 * i) for i in range(16)]  # 01:03..04:48
        times += [_utc(21, 36) + timedelta(minutes=15 * i) for i in range(12)]  # 21:36..00:21
        rows = [("D50", 1, "9478", ts) for ts in times]
        out = pair_observed_headways(rows, ZoneInfo("America/New_York"))
        pooled = [h for cell in out["D50"].values() for h in cell]
        assert max(pooled) == 900.0
        assert compute_awt(pooled) == pytest.approx(450.0)
        # The old (uncapped) pooling would have included the 60,480 s pair.
        uncapped = pooled + [60_480.0]
        assert compute_awt(uncapped) > 40 * compute_awt(pooled)


class TestCappedScheduled:
    """The scheduled-side cap."""

    def test_drops_only_over_cap(self):
        """Scheduled headways over 120 min drop out; 120 min itself stays."""
        assert _capped_scheduled([600.0, 7200.0, 7201.0, 60_000.0]) == [600.0, 7200.0]


def _seed_schedule(db_session) -> None:
    """Route CAP1, stop S1 dir 0: a frequent hour-7 cell with an embedded > 2 h scheduled gap.

    Scheduled arrivals 07:00..07:08 every minute (8 headways of 60 s), then
    a last trip at 09:09 (a 7,260 s headway bucketed in hour 7).
    Hour-7 cell mean = (8*60 + 7260) / 9 = 860 s <= 900 s, so the cell is
    frequent; the 7,260 s scheduled headway is what the scheduled cap drops.
    """
    db_session.add(Route(route_id=ROUTE, route_short_name=ROUTE, route_type=3, is_current=True))
    db_session.add(
        Calendar(
            service_id="WK",
            monday=1,
            tuesday=1,
            wednesday=1,
            thursday=1,
            friday=1,
            saturday=0,
            sunday=0,
            start_date="20260101",
            end_date="20261231",
            is_current=True,
        )
    )
    sched = [f"07:0{m}:00" for m in range(9)] + ["09:09:00"]
    for i, t in enumerate(sched):
        trip_id = f"T{i}"
        db_session.add(
            Trip(
                trip_id=trip_id,
                route_id=ROUTE,
                service_id="WK",
                direction_id=0,
                trip_headsign="x",
                is_current=True,
            )
        )
        db_session.add(
            StopTime(
                trip_id=trip_id,
                stop_id="S1",
                arrival_time=t,
                departure_time=t,
                stop_sequence=1,
                is_current=True,
            )
        )
    db_session.commit()


def _seed_observed(db_session, eastern_times: list[tuple[int, int]]) -> None:
    """Insert trip_update stop_events at S1 dir 0 for the given Eastern (hh, mm) times."""
    for i, (hh, mm) in enumerate(eastern_times):
        db_session.add(
            StopEvent(
                service_date=SERVICE_DATE_STR,
                trip_id=f"O{i}",
                route_id=ROUTE,
                direction_id=0,
                stop_id="S1",
                stop_sequence=1,
                observed_arrival_ts=_utc(hh, mm),
                source="trip_update",
                schedule_relationship="SCHEDULED",
            )
        )
    db_session.commit()


# Observed: 07:00..07:07 every minute (7 x 60 s), then 07:07 -> 21:36 — a
# 14.5 h pair bucketed in frequent hour 7 (the D50 shape), which the cap
# drops. The 21:36 -> 21:46 pair lands in hour 21, which has no scheduled
# service, so it never enters the pool.
OBSERVED = [(7, m) for m in range(8)] + [(21, 36), (21, 46)]


class TestFourCallSitesAgree:
    """All four EWT paths drop the same pairs and report the same numbers."""

    def test_observed_cell_excludes_long_pair(self, db_session):
        """`_observed_headways_by_cell_hour` keeps the 60 s pairs, drops the 14.5 h one."""
        _seed_schedule(db_session)
        _seed_observed(db_session, OBSERVED)
        cells = _observed_headways_by_cell_hour(db_session, ROUTE, SERVICE_DATE_STR)
        assert cells[(0, "S1", 7)] == [60.0] * 7
        assert cells[(0, "S1", 21)] == [600.0]

    def test_route_and_system_paths_agree(self, db_session):
        """Single-route, vectorized, multi-date and system rollups give identical EWT/SWT."""
        _seed_schedule(db_session)
        _seed_observed(db_session, OBSERVED)

        single = compute_ewt_headline_for_route(db_session, ROUTE, SERVICE_DATE)
        vectorized = compute_ewt_headline_for_routes(db_session, SERVICE_DATE, route_ids=[ROUTE])[
            ROUTE
        ]
        multi = compute_ewt_headline_for_routes_multi_date(
            db_session, [SERVICE_DATE], route_ids=[ROUTE]
        )[SERVICE_DATE_STR][ROUTE]
        sys_ewt, sys_swt, _bunching = agg._system_ewt_and_bunching_for_date(
            db_session, SERVICE_DATE, {}
        )

        # Observed pool: seven 60 s pairs (the 14.5 h pair is dropped).
        # Scheduled pool: eight 60 s headways (the 7,260 s one is dropped).
        expected_awt = 30.0
        expected_swt = 30.0
        for res in (single, vectorized, multi):
            assert res["n_observed_headways"] == 7
            assert res["n_scheduled_headways"] == 8
            assert res["awt_seconds"] == pytest.approx(expected_awt)
            assert res["swt_seconds"] == pytest.approx(expected_swt)
            assert res["ewt_seconds"] == pytest.approx(0.0)
            assert res["frequent_cell_hours"] == 1
        assert sys_swt == pytest.approx(expected_swt)
        assert sys_ewt == pytest.approx(0.0)
        assert single == vectorized == multi

    def test_long_pair_at_119_minutes_is_kept_everywhere(self, db_session):
        """A 119-min pair from a frequent cell enters every path's observed pool."""
        _seed_schedule(db_session)
        # 07:00..07:07 every minute, then 07:07 -> 09:06 (119 min).
        _seed_observed(db_session, [(7, m) for m in range(8)] + [(9, 6)])

        pool = [60.0] * 7 + [119 * 60.0]
        awt = compute_awt(pool)
        single = compute_ewt_headline_for_route(db_session, ROUTE, SERVICE_DATE)
        vectorized = compute_ewt_headline_for_routes(db_session, SERVICE_DATE, route_ids=[ROUTE])[
            ROUTE
        ]
        multi = compute_ewt_headline_for_routes_multi_date(
            db_session, [SERVICE_DATE], route_ids=[ROUTE]
        )[SERVICE_DATE_STR][ROUTE]
        sys_ewt, sys_swt, _ = agg._system_ewt_and_bunching_for_date(db_session, SERVICE_DATE, {})
        for res in (single, vectorized, multi):
            assert res["n_observed_headways"] == 8
            assert res["awt_seconds"] == pytest.approx(awt, abs=0.01)
        assert sys_ewt == pytest.approx(awt - sys_swt, abs=0.01)


class TestPerPeriodPathCapsScheduled:
    """`compute_ewt_for_route_date` (per-period rows) applies the scheduled cap too."""

    def test_am_peak_row_drops_long_scheduled_headway(self, db_session):
        """The 7,260 s scheduled headway in hour 7 is left out of the AM Peak SWT pool."""
        _seed_schedule(db_session)
        _seed_observed(db_session, OBSERVED)
        rows = {
            r["time_period"]: r for r in compute_ewt_for_route_date(db_session, ROUTE, SERVICE_DATE)
        }
        am = rows["AM Peak (6-9)"]
        assert am["frequent_cell_hours"] == 1
        assert am["n_scheduled_headways"] == 8
        assert am["swt_seconds"] == pytest.approx(30.0)
        assert am["n_observed_headways"] == 7
        assert am["awt_seconds"] == pytest.approx(30.0)
