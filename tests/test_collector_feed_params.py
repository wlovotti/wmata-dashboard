"""Feed-parameterization tests: the collector class must serve any GTFS-RT agency."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from google.transit import gtfs_realtime_pb2

from src.wmata_collector import WMATADataCollector


def _tu_feed_bytes(trip_id="muni_trip_1", stop_id="S1", ts=1784500000):
    """Build a minimal serialized TripUpdates FeedMessage for mocking."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = ts
    ent = feed.entity.add()
    ent.id = "1"
    ent.trip_update.trip.trip_id = trip_id
    stu = ent.trip_update.stop_time_update.add()
    stu.stop_id = stop_id
    stu.stop_sequence = 3
    stu.arrival.time = ts + 120
    return feed.SerializeToString()


def _make_collector(tmp_path, **kwargs):
    """Collector with archive redirected into tmp_path (required in tests)."""
    return WMATADataCollector("KEY", archive_root=tmp_path, **kwargs)


def test_custom_tu_url_and_query_params_used(tmp_path):
    """A 511-style collector fetches the configured URL with query params."""
    collector = _make_collector(
        tmp_path,
        tu_feed_url="https://api.511.org/transit/tripupdates",
        request_params={"api_key": "KEY", "agency": "SF"},
    )
    resp = MagicMock(status_code=200, content=_tu_feed_bytes())
    with patch("src.wmata_collector.requests.get", return_value=resp) as mock_get:
        snapshot_ts, rows = collector.get_realtime_trip_updates()
    assert mock_get.call_args.args[0] == "https://api.511.org/transit/tripupdates"
    assert mock_get.call_args.kwargs["params"] == {"api_key": "KEY", "agency": "SF"}
    assert len(rows) == 1 and rows[0]["trip_id"] == "muni_trip_1"


def test_default_urls_unchanged_for_wmata(tmp_path):
    """No params -> exact current WMATA URL and no query params (regression guard)."""
    collector = _make_collector(tmp_path)
    resp = MagicMock(status_code=200, content=_tu_feed_bytes())
    with patch("src.wmata_collector.requests.get", return_value=resp) as mock_get:
        collector.get_realtime_trip_updates()
    assert mock_get.call_args.args[0] == "https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb"
    assert "params" not in mock_get.call_args.kwargs or mock_get.call_args.kwargs["params"] is None


@pytest.mark.integration
def test_save_trip_updates_uses_tz_and_heartbeat_name(tmp_path, pg_session):
    """Pacific service-date fallback + custom heartbeat collector_name land in the DB.

    ``upsert_trip_update_state`` (src/upsert_helpers.py) is Postgres-only
    (uses ``pg_insert`` with an ON CONFLICT DO UPDATE clause) — SQLite
    cannot represent it. Existing ``_save_trip_updates`` tests
    (tests/test_collector_dual_write.py) all use the ``pg_session``
    fixture for this reason, so this test follows the same pattern
    instead of the SQLite-backed ``db_session`` fixture.
    """
    from src.models import CollectorHeartbeat, TripUpdateState

    collector = _make_collector(
        tmp_path,
        db_session=pg_session,
        service_date_tz="America/Los_Angeles",
        heartbeat_name="sfmta-combined",
    )
    # 05:30 UTC on Jul 22 = 22:30 PDT Jul 21 -> Pacific service_date must be Jul 21.
    snapshot = datetime(2026, 7, 22, 5, 30, 0)
    rows = [
        {
            "snapshot_ts": snapshot,
            "trip_id": "muni_trip_1",
            "route_id": "38R",
            "vehicle_id": "v1",
            "stop_id": "S1",
            "stop_sequence": 3,
            "predicted_arrival_ts": None,
            "predicted_departure_ts": None,
            "schedule_relationship": "UNSET",
            "trip_start_date": None,
        }
    ]
    try:
        collector._save_trip_updates(rows)
        # Filtered lookups: pg_session runs against the real dev DB (writes
        # roll back via SAVEPOINT, but pre-existing rows are still visible),
        # so an unfiltered .one() can hit MultipleResultsFound.
        state = pg_session.query(TripUpdateState).filter_by(trip_id="muni_trip_1").one()
        assert state.service_date.isoformat() == "2026-07-21"
        hb = pg_session.query(CollectorHeartbeat).filter_by(ts=snapshot).one()
        assert hb.collector_name == "sfmta-combined"
    finally:
        collector.close()
