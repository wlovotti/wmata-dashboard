"""Tests for pipelines.derive_stop_events_from_state."""

from datetime import date, datetime

import pytest
from sqlalchemy import select

from src.models import GTFSSnapshot, StopEvent, StopTime, Trip, TripUpdateState, VehiclePosition


@pytest.mark.parametrize(
    "items, size, expected",
    [
        ([1, 2, 3, 4, 5], 2, [[1, 2], [3, 4], [5]]),
        ([1, 2, 3, 4], 2, [[1, 2], [3, 4]]),
        ([1, 2], 5, [[1, 2]]),
        ([], 3, []),
    ],
)
def test_iter_chunks_partitions_input(items, size, expected):
    """_iter_chunks splits a sequence into successive lists of length ``size``,
    with a possibly-short final chunk."""
    from pipelines.derive_stop_events_from_state import _iter_chunks

    assert [list(c) for c in _iter_chunks(items, size)] == expected


def test_iter_chunks_rejects_non_positive_size():
    """_iter_chunks raises ValueError for size <= 0 (a silent infinite loop
    is worse than a clear error)."""
    from pipelines.derive_stop_events_from_state import _iter_chunks

    with pytest.raises(ValueError):
        list(_iter_chunks([1, 2, 3], 0))


def _seed_minimal_route(pg_session, *, route_id="R1", trip_id="T1"):
    """Seed the minimum DB state for a single-stop derivation test."""
    pg_session.add_all(
        [
            Trip(trip_id=trip_id, route_id=route_id, direction_id=0, is_current=True),
            StopTime(
                trip_id=trip_id,
                stop_sequence=1,
                stop_id="S1",
                arrival_time="14:05:00",
                departure_time="14:05:30",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id=trip_id,
                route_id=route_id,
                trip_start_date="20260517",
                latitude=0,
                longitude=0,
                timestamp=datetime(2026, 5, 17, 14, 0, 0),
            ),
        ]
    )


@pytest.mark.integration
def test_derive_produces_stop_event_with_correct_observed_arrival(pg_session):
    """A trip_update_state row produces a stop_event with last_predicted_arrival_ts as observed."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    _seed_minimal_route(pg_session)
    pred = datetime(2026, 5, 17, 14, 6, 30)  # 90s late vs 14:05 schedule
    pg_session.add(
        TripUpdateState(
            trip_id="T1",
            stop_sequence=1,
            service_date=date(2026, 5, 17),
            stop_id="S1",
            vehicle_id="V1",
            final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
            final_schedule_relationship="SCHEDULED",
            last_pred_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
            last_predicted_arrival_ts=pred,
        )
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 5, 17),
        target_table_name="stop_events",
    )
    pg_session.commit()

    event = pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T1")).scalar_one()
    assert event.trip_id == "T1"
    assert event.observed_arrival_ts == pred
    assert event.schedule_relationship == "SCHEDULED"
    assert event.source == "trip_update"


@pytest.mark.integration
def test_derive_emits_skipped_stops(pg_session):
    """A SKIPPED final_schedule_relationship produces a stop_event with observed_arrival_ts=None."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    _seed_minimal_route(pg_session)
    pg_session.add(
        TripUpdateState(
            trip_id="T1",
            stop_sequence=1,
            service_date=date(2026, 5, 17),
            stop_id="S1",
            vehicle_id="V1",
            final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
            final_schedule_relationship="SKIPPED",
            last_pred_snapshot_ts=None,
            last_predicted_arrival_ts=None,
        )
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 5, 17),
        target_table_name="stop_events",
    )
    pg_session.commit()

    event = pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T1")).scalar_one()
    assert event.schedule_relationship == "SKIPPED"
    assert event.observed_arrival_ts is None


@pytest.mark.integration
def test_derive_ignores_phantom_timestamp_positions_for_attribution(pg_session):
    """A trip whose only vehicle_position has a timestamp months outside the
    service day is NOT attributed to that service date, even when
    trip_start_date matches.

    Regression for the timestamp-window fix: service-date attribution reads
    vehicle_positions by (route_id, trip_start_date) and must bound the scan
    by timestamp both for index use and to reject NOTES-81 phantom rows.
    """
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    pg_session.add_all(
        [
            Trip(trip_id="T1", route_id="R1", direction_id=0, is_current=True),
            StopTime(
                trip_id="T1",
                stop_sequence=1,
                stop_id="S1",
                arrival_time="14:05:00",
                departure_time="14:05:30",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T1",
                route_id="R1",
                trip_start_date="20260517",
                latitude=0,
                longitude=0,
                timestamp=datetime(2025, 10, 15, 12, 0, 0),  # phantom, not 2026-05-17
            ),
            TripUpdateState(
                trip_id="T1",
                stop_sequence=1,
                service_date=date(2026, 5, 17),
                stop_id="S1",
                vehicle_id="V1",
                final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
                final_schedule_relationship="SCHEDULED",
                last_pred_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
                last_predicted_arrival_ts=datetime(2026, 5, 17, 14, 6, 30),
            ),
        ]
    )
    pg_session.commit()

    result = derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 5, 17),
        target_table_name="stop_events",
    )
    pg_session.commit()

    assert result["note"] == "No vehicle_positions for any current trip on this service_date"
    events = pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T1")).scalars().all()
    assert events == []


@pytest.mark.integration
def test_derive_with_gtfs_snapshot_id_uses_historical_schedule(pg_session):
    """gtfs_snapshot_id lets from_state derive against a superseded GTFS
    snapshot — the backfill path for dates older than the current reload."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    pg_session.add_all(
        [
            GTFSSnapshot(snapshot_id=1, snapshot_date=datetime(2026, 5, 3, 18, 30, 0)),
            Trip(
                trip_id="T_HIST",
                route_id="R1",
                direction_id=0,
                snapshot_id=1,
                is_current=False,
            ),
            StopTime(
                trip_id="T_HIST",
                stop_sequence=1,
                stop_id="S1",
                arrival_time="14:05:00",
                departure_time="14:05:30",
                snapshot_id=1,
                is_current=False,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T_HIST",
                route_id="R1",
                trip_start_date="20260611",
                latitude=0,
                longitude=0,
                timestamp=datetime(2026, 6, 11, 14, 0, 0),
            ),
            TripUpdateState(
                trip_id="T_HIST",
                stop_sequence=1,
                service_date=date(2026, 6, 11),
                stop_id="S1",
                vehicle_id="V1",
                final_snapshot_ts=datetime(2026, 6, 11, 14, 6, 0),
                final_schedule_relationship="SCHEDULED",
                last_pred_snapshot_ts=datetime(2026, 6, 11, 14, 6, 0),
                last_predicted_arrival_ts=datetime(2026, 6, 11, 14, 6, 30),
            ),
        ]
    )
    pg_session.commit()

    # Without the flag: the trip only exists in the old snapshot → no events.
    result = derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 6, 11),
        target_table_name="stop_events",
    )
    assert result["note"] == "No current trips for route"

    # With the flag: derived against the historical schedule.
    derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 6, 11),
        target_table_name="stop_events",
        gtfs_snapshot_id=1,
    )
    pg_session.commit()
    event = pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T_HIST")).scalar_one()
    assert event.observed_arrival_ts == datetime(2026, 6, 11, 14, 6, 30)


@pytest.mark.integration
def test_derive_sets_derived_at_on_state_rows(pg_session):
    """After derivation, the source state rows have derived_at set — but ONLY
    for the derived service_date.

    GTFS trip_ids recur every service day, so the derived_at UPDATE must bind
    the full (trip_id, stop_sequence, service_date) natural key. Without the
    service_date predicate it phantom-stamps other dates' rows for the same
    trip and takes row locks on the collector's live rows (the 2026-07
    backfill deadlocked against the collector three times this way).
    """
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    _seed_minimal_route(pg_session)
    pg_session.add_all(
        [
            TripUpdateState(
                trip_id="T1",
                stop_sequence=1,
                service_date=date(2026, 5, 17),
                stop_id="S1",
                vehicle_id="V1",
                final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
                final_schedule_relationship="SCHEDULED",
                last_pred_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
                last_predicted_arrival_ts=datetime(2026, 5, 17, 14, 6, 30),
            ),
            # Same (trip_id, stop_sequence) on the NEXT service day — the
            # recurring-trip_id case. Deriving 5/17 must not touch it.
            TripUpdateState(
                trip_id="T1",
                stop_sequence=1,
                service_date=date(2026, 5, 18),
                stop_id="S1",
                vehicle_id="V2",
                final_snapshot_ts=datetime(2026, 5, 18, 14, 6, 0),
                final_schedule_relationship="SCHEDULED",
                last_pred_snapshot_ts=datetime(2026, 5, 18, 14, 6, 0),
                last_predicted_arrival_ts=datetime(2026, 5, 18, 14, 6, 30),
            ),
        ]
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 5, 17),
        target_table_name="stop_events",
    )
    pg_session.commit()

    derived_row = pg_session.execute(
        select(TripUpdateState).where(
            TripUpdateState.trip_id == "T1",
            TripUpdateState.service_date == date(2026, 5, 17),
        )
    ).scalar_one()
    assert derived_row.derived_at is not None

    other_date_row = pg_session.execute(
        select(TripUpdateState).where(
            TripUpdateState.trip_id == "T1",
            TripUpdateState.service_date == date(2026, 5, 18),
        )
    ).scalar_one()
    assert other_date_row.derived_at is None


@pytest.mark.integration
def test_derive_corrects_owl_route_service_date_off_by_one(pg_session):
    """NOTES-105: a trip_update_state row bucketed under a misattributed
    service_date (one day later than the trip's true GTFS service day —
    the same defect confirmed for vehicle_positions.trip_start_date on
    SFMTA's 24h owl routes) must not produce a ~24h deviation.

    trip_update_state.service_date comes from
    src.wmata_collector._service_date_for_row, which shares the
    trip_start_date-or-snapshot_ts-calendar-day logic that misattributes
    owl trips. Here the state row's service_date (2026-07-23) is the
    misattributed date; the schedule's arrival_time (">=24:00", i.e. after
    the true service day's midnight) combined with the real prediction
    2026-07-23 07:07:22 must resolve to a normal few-minute deviation, not
    ~24h.

    Based on the real production row (trip 12097226_M11, stop_sequence 2),
    whose GTFS static arrival_time is "24:09:40" Pacific -- this test uses
    tz_name="UTC" and the UTC-equivalent "31:09:40" instead purely to avoid
    DST arithmetic; the two are the same instant.
    """
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    pg_session.add_all(
        [
            Trip(trip_id="T_OWL", route_id="R14", direction_id=0, is_current=True),
            StopTime(
                trip_id="T_OWL",
                stop_sequence=2,
                stop_id="S1",
                arrival_time="31:09:40",  # UTC-equivalent of 24:09:40 Pacific
                departure_time="31:09:40",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T_OWL",
                route_id="R14",
                trip_start_date="20260723",
                latitude=0,
                longitude=0,
                timestamp=datetime(2026, 7, 23, 7, 7, 22),
            ),
            TripUpdateState(
                trip_id="T_OWL",
                stop_sequence=2,
                service_date=date(2026, 7, 23),  # misattributed +1 day
                stop_id="S1",
                vehicle_id="V1",
                final_snapshot_ts=datetime(2026, 7, 23, 7, 7, 22),
                final_schedule_relationship="SCHEDULED",
                last_pred_snapshot_ts=datetime(2026, 7, 23, 7, 7, 22),
                last_predicted_arrival_ts=datetime(2026, 7, 23, 7, 7, 22),
            ),
        ]
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R14",
        service_date=date(2026, 7, 23),
        target_table_name="stop_events",
        tz_name="UTC",
    )
    pg_session.commit()

    event = pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T_OWL")).scalar_one()
    assert event.scheduled_arrival_ts == datetime(2026, 7, 23, 7, 9, 40)
    assert abs(event.deviation_sec) < 300
    # NOTES-110: the corrected anchor day must be persisted as
    # stop_events.service_date, not the misattributed date the
    # pipeline was invoked with -- otherwise day_type-filtered
    # queries and calendar_dates joins pick the wrong day.
    assert event.service_date == "2026-07-22"


@pytest.mark.integration
def test_derive_does_not_shift_service_date_for_ordinary_trips(pg_session):
    """NOTES-110 companion: a normal, non-owl trip_update_state row (no
    anchor shift needed) must keep stop_events.service_date equal to the
    outer-loop service_date argument -- the correction only applies when
    the resolver actually picks the day-before anchor."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    _seed_minimal_route(pg_session)
    pred = datetime(2026, 5, 17, 14, 6, 30)  # 90s late vs 14:05 schedule
    pg_session.add(
        TripUpdateState(
            trip_id="T1",
            stop_sequence=1,
            service_date=date(2026, 5, 17),
            stop_id="S1",
            vehicle_id="V1",
            final_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
            final_schedule_relationship="SCHEDULED",
            last_pred_snapshot_ts=datetime(2026, 5, 17, 14, 6, 0),
            last_predicted_arrival_ts=pred,
        )
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 5, 17),
        target_table_name="stop_events",
    )
    pg_session.commit()

    event = pg_session.execute(select(StopEvent).where(StopEvent.trip_id == "T1")).scalar_one()
    assert event.service_date == "2026-05-17"


@pytest.mark.integration
def test_derive_skipped_branch_corrects_service_date_via_final_snapshot_ts(pg_session):
    """NOTES-110 latent-gap fix: a SKIPPED row has no arrival prediction to
    disambiguate the anchor against, but final_snapshot_ts (always
    populated) is a serviceable proxy observation. A SKIPPED owl-route row
    filed under the misattributed service_date must still resolve to the
    corrected (day-before) service_date, the same way SCHEDULED rows do.
    """
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    pg_session.add_all(
        [
            Trip(trip_id="T_OWL_SKIP", route_id="R14", direction_id=0, is_current=True),
            StopTime(
                trip_id="T_OWL_SKIP",
                stop_sequence=2,
                stop_id="S1",
                arrival_time="31:09:40",  # UTC-equivalent of 24:09:40 Pacific
                departure_time="31:09:40",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T_OWL_SKIP",
                route_id="R14",
                trip_start_date="20260723",
                latitude=0,
                longitude=0,
                timestamp=datetime(2026, 7, 23, 7, 7, 22),
            ),
            TripUpdateState(
                trip_id="T_OWL_SKIP",
                stop_sequence=2,
                service_date=date(2026, 7, 23),  # misattributed +1 day
                stop_id="S1",
                vehicle_id="V1",
                final_snapshot_ts=datetime(2026, 7, 23, 7, 7, 22),
                final_schedule_relationship="SKIPPED",
                last_pred_snapshot_ts=None,
                last_predicted_arrival_ts=None,
            ),
        ]
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R14",
        service_date=date(2026, 7, 23),
        target_table_name="stop_events",
        tz_name="UTC",
    )
    pg_session.commit()

    event = pg_session.execute(
        select(StopEvent).where(StopEvent.trip_id == "T_OWL_SKIP")
    ).scalar_one()
    assert event.schedule_relationship == "SKIPPED"
    assert event.observed_arrival_ts is None
    assert event.scheduled_arrival_ts == datetime(2026, 7, 23, 7, 9, 40)
    assert event.service_date == "2026-07-22"


@pytest.mark.integration
def test_derive_supersedes_misattributed_row_on_reresolve(pg_session):
    """PR #193 review: a stop_events row previously written under the
    misattributed service_date (pre-NOTES-110 code, or a prior run before
    this fix) must be deleted when re-deriving lands the corrected row on
    the adjacent date -- otherwise both copies survive (service_date is
    part of uq_stop_events_run_stop_source, so the corrected row is a
    distinct upsert target, not a conflict against the stale one) and the
    same real-world event double-counts across two service dates.
    """
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    pg_session.add_all(
        [
            Trip(trip_id="T_OWL", route_id="R14", direction_id=0, is_current=True),
            StopTime(
                trip_id="T_OWL",
                stop_sequence=2,
                stop_id="S1",
                arrival_time="31:09:40",  # UTC-equivalent of 24:09:40 Pacific
                departure_time="31:09:40",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T_OWL",
                route_id="R14",
                trip_start_date="20260723",
                latitude=0,
                longitude=0,
                timestamp=datetime(2026, 7, 23, 7, 7, 22),
            ),
            TripUpdateState(
                trip_id="T_OWL",
                stop_sequence=2,
                service_date=date(2026, 7, 23),  # misattributed +1 day
                stop_id="S1",
                vehicle_id="V1",
                final_snapshot_ts=datetime(2026, 7, 23, 7, 7, 22),
                final_schedule_relationship="SCHEDULED",
                last_pred_snapshot_ts=datetime(2026, 7, 23, 7, 7, 22),
                last_predicted_arrival_ts=datetime(2026, 7, 23, 7, 7, 22),
            ),
            # Simulates a stop_event a prior derivation run (pre-NOTES-110,
            # or a subsequent PR #193 review round without the dedup fix) wrote under the
            # misattributed date -- same (trip_id, stop_sequence, source)
            # identity the corrected row will resolve to, but the wrong
            # service_date.
            StopEvent(
                service_date="2026-07-23",
                trip_id="T_OWL",
                route_id="R14",
                direction_id=0,
                vehicle_id="V1",
                stop_id="S1",
                stop_sequence=2,
                scheduled_arrival_ts=datetime(2026, 7, 24, 7, 9, 40),
                scheduled_departure_ts=datetime(2026, 7, 24, 7, 9, 40),
                observed_arrival_ts=datetime(2026, 7, 23, 7, 7, 22),
                deviation_sec=-86538,
                source="trip_update",
                schedule_relationship="SCHEDULED",
                derived_at=datetime(2026, 7, 23, 8, 0, 0),
            ),
        ]
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R14",
        service_date=date(2026, 7, 23),
        target_table_name="stop_events",
        tz_name="UTC",
    )
    pg_session.commit()

    events = (
        pg_session.execute(
            select(StopEvent).where(
                StopEvent.trip_id == "T_OWL",
                StopEvent.stop_sequence == 2,
                StopEvent.source == "trip_update",
            )
        )
        .scalars()
        .all()
    )
    assert len(events) == 1
    assert events[0].service_date == "2026-07-22"
    assert abs(events[0].deviation_sec) < 300


@pytest.mark.integration
def test_derive_is_idempotent_across_reruns(pg_session):
    """PR #193 review (companion to the above): re-deriving the SAME service_date twice (the
    normal bin/pull-and-derive.sh re-derive pattern) must not leave a
    duplicate pair across adjacent dates for an owl-route row."""
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    pg_session.add_all(
        [
            Trip(trip_id="T_OWL", route_id="R14", direction_id=0, is_current=True),
            StopTime(
                trip_id="T_OWL",
                stop_sequence=2,
                stop_id="S1",
                arrival_time="31:09:40",
                departure_time="31:09:40",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T_OWL",
                route_id="R14",
                trip_start_date="20260723",
                latitude=0,
                longitude=0,
                timestamp=datetime(2026, 7, 23, 7, 7, 22),
            ),
            TripUpdateState(
                trip_id="T_OWL",
                stop_sequence=2,
                service_date=date(2026, 7, 23),
                stop_id="S1",
                vehicle_id="V1",
                final_snapshot_ts=datetime(2026, 7, 23, 7, 7, 22),
                final_schedule_relationship="SCHEDULED",
                last_pred_snapshot_ts=datetime(2026, 7, 23, 7, 7, 22),
                last_predicted_arrival_ts=datetime(2026, 7, 23, 7, 7, 22),
            ),
        ]
    )
    pg_session.commit()

    for _ in range(2):
        derive_for_route_date(
            pg_session,
            route_id="R14",
            service_date=date(2026, 7, 23),
            target_table_name="stop_events",
            tz_name="UTC",
        )
        pg_session.commit()

    events = (
        pg_session.execute(
            select(StopEvent).where(
                StopEvent.trip_id == "T_OWL",
                StopEvent.stop_sequence == 2,
                StopEvent.source == "trip_update",
            )
        )
        .scalars()
        .all()
    )
    assert len(events) == 1
    assert events[0].service_date == "2026-07-22"


@pytest.mark.integration
def test_derive_skipped_branch_proxy_guard_keeps_plain_anchor_for_moderate_gap(pg_session):
    """PR #193 review: a SKIPPED row whose final_snapshot_ts is merely several
    hours from its (ordinary, non-owl) scheduled arrival -- e.g. the
    vehicle went quiet earlier in its run -- must keep the plain
    service_date anchor, not shift to service_date - 1. The resolver's
    "never >12h early" invariant was verified only for genuine arrival
    observations; final_snapshot_ts is a wall-clock proxy that could
    plausibly be far from schedule for reasons unrelated to an owl-route
    misattribution, so only a same-day gap that clears the 12h bar is
    trusted as evidence of a real anchor correction.
    """
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    pg_session.add_all(
        [
            Trip(trip_id="T_GAP_SKIP", route_id="R1", direction_id=0, is_current=True),
            StopTime(
                trip_id="T_GAP_SKIP",
                stop_sequence=1,
                stop_id="S1",
                arrival_time="14:05:00",
                departure_time="14:05:30",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T_GAP_SKIP",
                route_id="R1",
                trip_start_date="20260517",
                latitude=0,
                longitude=0,
                timestamp=datetime(2026, 5, 17, 8, 0, 0),
            ),
            TripUpdateState(
                trip_id="T_GAP_SKIP",
                stop_sequence=1,
                service_date=date(2026, 5, 17),
                stop_id="S1",
                vehicle_id="V1",
                # Went dark ~6h before its own scheduled arrival -- an
                # unremarkable early feed dropout, not an owl-route
                # misattribution.
                final_snapshot_ts=datetime(2026, 5, 17, 8, 0, 0),
                final_schedule_relationship="SKIPPED",
                last_pred_snapshot_ts=None,
                last_predicted_arrival_ts=None,
            ),
        ]
    )
    pg_session.commit()

    derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 5, 17),
        target_table_name="stop_events",
        tz_name="UTC",
    )
    pg_session.commit()

    event = pg_session.execute(
        select(StopEvent).where(StopEvent.trip_id == "T_GAP_SKIP")
    ).scalar_one()
    assert event.schedule_relationship == "SKIPPED"
    assert event.scheduled_arrival_ts == datetime(2026, 5, 17, 14, 5, 0)
    assert event.service_date == "2026-05-17"


@pytest.mark.integration
def test_derive_tz_name_widens_window_for_late_pacific_positions(pg_session):
    """NOTES-100 sibling of the equivalent test in test_derive_stop_events.py.

    Active-trip attribution here comes from a vehicle_positions scan
    windowed the same way (see local_service_date_position_window_utc).
    A position at 2026-07-04 05:00 UTC is inside the Pacific 48h window
    for service_date=2026-07-02 but past the end of the Eastern window
    for the same service_date -- without tz_name threaded through, the
    trip is invisible ("no vehicle_positions for any current trip"), so
    even a fully-populated trip_update_state row for it is silently
    skipped.
    """
    from pipelines.derive_stop_events_from_state import derive_for_route_date

    pg_session.add_all(
        [
            Trip(trip_id="T1", route_id="R1", direction_id=0, is_current=True),
            StopTime(
                trip_id="T1",
                stop_sequence=1,
                stop_id="S1",
                arrival_time="14:05:00",
                departure_time="14:05:30",
                is_current=True,
            ),
            VehiclePosition(
                vehicle_id="V1",
                trip_id="T1",
                route_id="R1",
                trip_start_date="20260702",
                latitude=0,
                longitude=0,
                timestamp=datetime(2026, 7, 4, 5, 0, 0),
            ),
            TripUpdateState(
                trip_id="T1",
                stop_sequence=1,
                service_date=date(2026, 7, 2),
                stop_id="S1",
                vehicle_id="V1",
                final_snapshot_ts=datetime(2026, 7, 2, 14, 6, 0),
                final_schedule_relationship="SCHEDULED",
                last_pred_snapshot_ts=datetime(2026, 7, 2, 14, 6, 0),
                last_predicted_arrival_ts=datetime(2026, 7, 2, 14, 6, 30),
            ),
        ]
    )
    pg_session.commit()

    result_eastern = derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 7, 2),
        target_table_name="stop_events",
    )
    assert (
        result_eastern["note"] == "No vehicle_positions for any current trip on this service_date"
    )

    result_pacific = derive_for_route_date(
        pg_session,
        route_id="R1",
        service_date=date(2026, 7, 2),
        target_table_name="stop_events",
        tz_name="America/Los_Angeles",
    )
    pg_session.commit()
    assert result_pacific["rows_written"] == 1
