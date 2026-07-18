"""Tests for pipelines/load_vp_from_parquet.py (pg_session — uses ON CONFLICT)."""

from datetime import datetime

import pyarrow as pa

from pipelines.archive_vehicle_positions import ARCHIVE_SCHEMA
from pipelines.load_vp_from_parquet import load_parquet_into_vp
from src.models import VehiclePosition


def _table(rows: list[dict]) -> pa.Table:
    """Build a pyarrow Table in the archive schema from row dicts."""
    cols = {name: [r.get(name) for r in rows] for name in ARCHIVE_SCHEMA.names}
    return pa.table(cols, schema=ARCHIVE_SCHEMA)


def _row(row_id: int) -> dict:
    """One minimal-but-valid archived VP row."""
    return {
        "id": row_id,
        "vehicle_id": f"bus-{row_id}",
        "route_id": "D72",
        "trip_id": "t1",
        "latitude": 38.9,
        "longitude": -77.0,
        "speed": 5.0,
        "current_stop_sequence": 3,
        "stop_id": "1001",
        "current_status": 2,
        "direction_id": 0,
        "trip_start_date": "20260611",
        "timestamp": datetime(2026, 6, 11, 12, 0, 0),
        "collected_at": datetime(2026, 6, 11, 12, 0, 1),
    }


def test_load_inserts_rows(pg_session):
    """All rows land; the count returned matches."""
    inserted = load_parquet_into_vp(pg_session, _table([_row(9000001), _row(9000002)]))
    assert inserted == 2
    assert (
        pg_session.query(VehiclePosition).filter(VehiclePosition.id.in_([9000001, 9000002])).count()
        == 2
    )


def test_load_is_idempotent_on_id_conflict(pg_session):
    """Re-loading the same file is a no-op, not an error (ON CONFLICT DO NOTHING)."""
    t = _table([_row(9000003)])
    assert load_parquet_into_vp(pg_session, t) == 1
    assert load_parquet_into_vp(pg_session, t) == 0
