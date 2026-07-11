"""Selecting a GTFS version in queries against the versioned schedule tables.

`scripts/reload_gtfs_complete.py` never deletes schedule rows: a reload marks
the previous snapshot's rows `is_current=False` and inserts the new set, so
every historical snapshot's full row set survives in place. Pipelines
normally pin to `is_current=True` (the project-wide convention), but
backfilling a service date whose schedule has since been superseded needs
the snapshot that was in force on that date — e.g. re-deriving June 2026
dates against snapshot 12 after the July reload made snapshot 13 current.
"""


def gtfs_version_filter(model, gtfs_snapshot_id=None):
    """Return a SQLAlchemy criterion selecting one GTFS version of ``model``.

    With ``gtfs_snapshot_id=None`` (the default), selects the live snapshot
    via ``is_current`` — identical to the conventional filter. With an int,
    selects that historical snapshot's rows regardless of currency.

    ``model`` must be one of the versioned GTFS models (Route, Stop, Trip,
    StopTime, Calendar, CalendarDate) — anything with both ``is_current``
    and ``snapshot_id`` columns.
    """
    if gtfs_snapshot_id is None:
        return model.is_current
    return model.snapshot_id == gtfs_snapshot_id
