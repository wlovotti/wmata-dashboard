"""Timezone conventions for the WMATA dashboard.

Storage convention: every datetime column in the database holds a NAIVE
UTC value. There are no `timestamptz` columns; the tz info is implicit
in this rule.

Service-date convention: WMATA buses run on Washington DC local time.
Anything user-facing — "today's metrics", "last 7 days", a service_date
on `runs` or `stop_events` — is an Eastern question even though the
storage is UTC. Use the helpers here to bridge the two; never call
``datetime.now()`` (naive local) for date math.

This module exists because vehicle_positions.timestamp was historically
written via ``datetime.fromtimestamp`` (naive local) while every other
timestamp column used UTC, which silently mis-aligned the two by 4
hours on every join.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func

EASTERN = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def eastern_today():
    """Return the current Eastern date (the WMATA service date)."""
    return datetime.now(EASTERN).date()


def utcnow_naive():
    """Return current UTC time as a naive datetime (matches DB storage convention).

    Replaces the deprecated ``datetime.utcnow()`` (Python 3.12+) without
    changing storage semantics — every DateTime column in the database
    holds naive UTC, so we strip the tzinfo after constructing the
    tz-aware now-in-UTC. Pass the bare callable (``utcnow_naive``) — not
    the call expression — to SQLAlchemy ``Column(default=...)`` so the
    default is evaluated per-row, not once at class-definition time.
    """
    return datetime.now(UTC).replace(tzinfo=None)


def from_epoch_naive_utc(ts):
    """Convert a POSIX epoch timestamp to a naive UTC datetime (matches DB storage convention).

    Replaces the deprecated ``datetime.utcfromtimestamp(ts)`` (Python 3.12+).
    Same approach as ``utcnow_naive()``: build a tz-aware datetime in UTC,
    then strip tzinfo to match the naive-UTC storage convention used by
    every ``DateTime`` column in the database. Used by the GTFS-RT
    collector (``src/wmata_collector.py``) when parsing epoch-second
    timestamps from feed headers and stop_time_update arrival/departure
    fields.
    """
    return datetime.fromtimestamp(ts, UTC).replace(tzinfo=None)


def eastern_date_from_naive_utc(naive_utc_dt):
    """Return the Eastern calendar date for a naive-UTC datetime.

    Used by the collector to compute a row's ``service_date`` at UPSERT
    time when ``tripDescriptor.start_date`` is not populated in the
    GTFS-RT feed (WMATA omits it for ~24% of vehicle rows). Correct for
    99%+ of WMATA bus trips since service-day-crossing overnight bus
    operations are rare.
    """
    return naive_utc_dt.replace(tzinfo=UTC).astimezone(EASTERN).date()


def eastern_midnight_as_utc(date):
    """Convert midnight on the given Eastern-zone date to a naive UTC datetime.

    Use when filtering naive-UTC timestamp columns by an Eastern service
    date. ``zoneinfo`` handles DST transitions correctly, so this is safe
    across the spring-forward and fall-back boundaries.
    """
    aware = datetime.combine(date, datetime.min.time(), tzinfo=EASTERN)
    return aware.astimezone(UTC).replace(tzinfo=None)


def eastern_day_bounds_utc(date):
    """Return (start, end) naive-UTC datetimes spanning one Eastern service day.

    Equivalent to (eastern_midnight_as_utc(date), eastern_midnight_as_utc(date+1))
    but expressed in one call. Note end - start may be 23 or 25 hours on DST
    transition days — that's correct service-day semantics.
    """
    start = eastern_midnight_as_utc(date)
    end = eastern_midnight_as_utc(date + timedelta(days=1))
    return start, end


def to_eastern_sql(naive_utc_col):
    """SQLAlchemy expression: convert a naive-UTC timestamp column to naive Eastern.

    Use when extracting service-date components in SQL (e.g., to match
    ``CalendarDate.date`` in YYYYMMDD form). The double ``timezone()`` is
    Postgres's idiom for "this naive value is UTC; reinterpret it as Eastern".
    """
    return func.timezone("America/New_York", func.timezone("UTC", naive_utc_col))
