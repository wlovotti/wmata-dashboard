"""Tests for the ``vp_archive_loaded_files`` manifest model (NOTES-95).

Started here (Task 6: model + migration); extended in Task 7 with the
loader function that consumes this manifest for idempotency.
"""

from src.models import VpArchiveLoadedFile


def test_manifest_model_round_trip(db_session):
    """VpArchiveLoadedFile persists filename/counts/loaded_at."""
    db_session.add(
        VpArchiveLoadedFile(filename="2026-08-22.1.100.jsonl.zst", row_count=10, dropped_count=1)
    )
    db_session.commit()
    row = db_session.query(VpArchiveLoadedFile).one()
    assert row.row_count == 10 and row.loaded_at is not None
