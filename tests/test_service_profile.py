"""Unit tests for `src.service_profile` classification helpers."""

from src.models import CalendarDate, Route, Stop, StopTime, Trip
from src.service_profile import (
    FREQUENCY_CLASS_LIMITED,
    FREQUENCY_CLASS_LIMITED_STOP,
    classify_route_frequency,
    compute_route_service_profile,
)

ROUTE = "TESTSF"


def _seed_route(db_session, route_id: str = ROUTE) -> None:
    """Insert a current Route row."""
    db_session.add(
        Route(
            route_id=route_id,
            route_short_name=route_id,
            route_long_name=f"Test Route {route_id}",
            route_type=3,
            is_current=True,
        )
    )
    db_session.commit()


def _seed_calendar_date(db_session, service_id: str, date_str: str, exception_type: int) -> None:
    """Insert a `calendar_dates` exception row (1=added, 2=removed)."""
    db_session.add(
        CalendarDate(
            service_id=service_id,
            date=date_str,
            exception_type=exception_type,
            is_current=True,
        )
    )
    db_session.commit()


def _seed_trip_with_service(
    db_session, trip_id: str, route_id: str, service_id: str, direction_id: int = 0
) -> None:
    """Insert a current Trip row tied to an arbitrary `service_id`."""
    db_session.add(
        Trip(
            trip_id=trip_id,
            route_id=route_id,
            service_id=service_id,
            direction_id=direction_id,
            trip_headsign="Downtown",
            is_current=True,
        )
    )
    db_session.commit()


def _seed_stop(db_session, stop_id: str) -> None:
    """Minimal Stop row."""
    db_session.add(Stop(stop_id=stop_id, stop_name=stop_id, stop_lat=0.0, stop_lon=0.0))
    db_session.commit()


def _seed_stop_time(
    db_session, trip_id: str, stop_id: str, arrival_time: str, stop_sequence: int = 1
) -> None:
    """Insert one StopTime row in current-version state."""
    db_session.add(
        StopTime(
            trip_id=trip_id,
            stop_id=stop_id,
            arrival_time=arrival_time,
            departure_time=arrival_time,
            stop_sequence=stop_sequence,
            is_current=True,
        )
    )
    db_session.commit()


class TestClassifyRouteFrequency:
    """Boundary and edge-case checks on the P90 frequency classifier."""

    def test_x_suffix_overrides_frequency(self):
        """Routes ending in 'X' classify as limited-stop regardless of headways."""
        assert classify_route_frequency([5.0, 6.0, 7.0], "D4X") == FREQUENCY_CLASS_LIMITED_STOP
        assert classify_route_frequency([], "A1X") == FREQUENCY_CLASS_LIMITED_STOP

    def test_no_data_returns_none(self):
        """Empty headway list (and non-X route) yields None."""
        assert classify_route_frequency([], "C21") is None

    def test_high_frequency(self):
        """All hours ≤12 min → high."""
        assert classify_route_frequency([8.0, 9.0, 10.0, 11.0, 12.0], "D20") == "high"

    def test_medium_frequency(self):
        """P90 between 12 and 20 → medium."""
        assert classify_route_frequency([10.0, 12.0, 15.0, 18.0, 20.0], "C21") == "medium"

    def test_low_frequency(self):
        """P90 between 20 and 30 → low."""
        assert classify_route_frequency([15.0, 20.0, 25.0, 28.0, 30.0], "C27") == "low"

    def test_limited_frequency(self):
        """P90 above 30 → limited."""
        assert (
            classify_route_frequency([20.0, 30.0, 35.0, 40.0, 45.0], "C43")
            == FREQUENCY_CLASS_LIMITED
        )

    def test_p90_absorbs_single_hour_artifact(self):
        """A lone 700-min hour shouldn't drag a frequent route to limited.

        Reproduces the D50 hr=1=710-min late-night artifact: most hours under
        20 min, one extreme outlier. With max-rule this would classify as
        limited; P90 keeps it at medium.
        """
        # 23 hours under 20 min, 1 hour at 710 → P90 should land in the high-teens.
        headways = [10.0] * 11 + [15.0] * 11 + [20.0, 710.0]
        assert classify_route_frequency(headways, "D50") == "medium"

    def test_threshold_boundaries_inclusive(self):
        """Exactly 12 / 20 / 30 fall into high / medium / low respectively."""
        # All values ≤12 → high.
        assert classify_route_frequency([12.0] * 5, "R1") == "high"
        # All ≤20 with at least one 20 → medium.
        assert classify_route_frequency([10.0, 15.0, 20.0, 20.0, 20.0], "R2") == "medium"
        # All ≤30 with at least one 30 → low.
        assert classify_route_frequency([20.0, 25.0, 30.0, 30.0, 30.0], "R3") == "low"


class TestComputeRouteServiceProfileCalendarDates:
    """NOTES-107: `compute_route_service_profile` must honor `calendar_dates`
    when `calendar.txt` itself defines no day-of-week service for a
    day_type — the Muni/SFMTA shape, where weekday service exists purely
    as a `calendar_dates` exception_type=1 (added) row. Before the fix,
    `_service_ids_for_day_type` filtered `Calendar.<field> == 1` directly
    and never consulted `calendar_dates`, so `route_service_profile` got
    zero weekday rows for any such agency (confirmed in prod data: 1,010
    saturday/sunday rows, 0 weekday rows). The fix reuses
    `src.ewt._resolve_service_ids_for_day_type` — the same modal resolver
    NOTES-106 introduced for EWT/bunching.
    """

    def test_weekday_only_via_calendar_dates_addition_is_resolved(self, db_session):
        """Muni shape: no `calendar` rows at all — weekday service exists
        purely as a single `calendar_dates` exception_type=1 addition on a
        Tuesday. Before the fix, `compute_route_service_profile` produced
        zero weekday rows for this route."""
        _seed_route(db_session)
        _seed_calendar_date(db_session, "WKADD", "20260414", exception_type=1)  # Tuesday
        _seed_stop(db_session, "S1")
        for i, t in enumerate(["07:00:00", "07:10:00", "07:20:00"]):
            trip_id = f"WT{i + 1}"
            _seed_trip_with_service(db_session, trip_id, ROUTE, "WKADD")
            _seed_stop_time(db_session, trip_id, "S1", t)

        rows = compute_route_service_profile(db_session)
        weekday_rows = [r for r in rows if r["day_type"] == "weekday" and r["route_id"] == ROUTE]

        assert weekday_rows, "weekday rows must not be empty on a calendar_dates-only feed"
        by_hour = {r["hour"]: r for r in weekday_rows}
        assert by_hour[7]["scheduled_trips"] == 3
        assert by_hour[7]["mean_headway_min"] == 10.0
