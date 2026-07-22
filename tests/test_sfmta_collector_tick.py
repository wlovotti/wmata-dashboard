"""Tick-schedule test: TU every 2nd tick, VP every 3rd tick (spec cadence)."""

from unittest.mock import MagicMock, patch

from scripts.sfmta_collector import run_one_tick


def _tick(tick_idx, collector):
    """Run one tick with the DB session factory patched out."""
    with patch("scripts.sfmta_collector.get_session", return_value=MagicMock()):
        run_one_tick(tick_idx, collector, db_url="sqlite:///:memory:")


def test_tick_schedule_matches_cadence_budget():
    """Over 6 ticks (one full cycle): 3 TU fetches, 2 VP fetches."""
    collector = MagicMock()
    collector.get_realtime_trip_updates.return_value = (None, [])
    collector.get_realtime_vehicle_positions.return_value = []
    for i in range(6):
        _tick(i, collector)
    assert collector.get_realtime_trip_updates.call_count == 3  # ticks 0,2,4
    assert collector.get_realtime_vehicle_positions.call_count == 2  # ticks 0,3
