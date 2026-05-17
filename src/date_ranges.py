"""Service-date range helpers for Eastern-zone iteration.

Centralizes the "iterate over service dates" pattern that several
pipelines (`compute_bunching.py`, `run_daily_batch.py`, and others)
were each reconstructing by hand. Service-date semantics are an
Eastern question (per `src/timezones.py`), so the helpers here speak
in ``date`` objects already in Eastern semantics — no tz conversion
happens here.

Both helpers yield dates in **ascending (chronological) order**. Most
pipelines either don't care about order or process catch-up dates
oldest-first to match human re-run intuition. Callers that need
descending order can wrap with ``reversed(list(...))``.
"""

from collections.abc import Iterator
from datetime import date as date_type
from datetime import timedelta

from src.timezones import eastern_today


def iter_eastern_dates(start: date_type, end: date_type) -> Iterator[date_type]:
    """Yield each Eastern-zone service date from ``start`` to ``end`` inclusive.

    Both endpoints are already-Eastern ``date`` objects — no timezone
    conversion happens here. Yields in ascending (chronological) order.

    Args:
        start: First service date to yield (inclusive).
        end: Last service date to yield (inclusive). Must be >= ``start``.

    Yields:
        Each ``date`` in ``[start, end]`` in ascending order.

    Raises:
        ValueError: If ``end < start``. Callers that explicitly want an
            empty range should pass equal endpoints (yields the single
            date) or handle the empty case before calling.
    """
    if end < start:
        raise ValueError(f"end ({end.isoformat()}) must be >= start ({start.isoformat()})")
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def iter_recent_eastern_dates(lookback_days: int) -> Iterator[date_type]:
    """Yield the most recent ``lookback_days`` service dates ending today (Eastern).

    Equivalent to ``iter_eastern_dates(eastern_today() - timedelta(days=lookback_days - 1),
    eastern_today())``. Yields in ascending (chronological) order: oldest
    first, today last.

    Args:
        lookback_days: Number of service dates to yield, including today.
            Must be >= 1. ``lookback_days=1`` yields only today.

    Yields:
        Each ``date`` in the window in ascending order.

    Raises:
        ValueError: If ``lookback_days < 1``.
    """
    if lookback_days < 1:
        raise ValueError(f"lookback_days must be >= 1 (got {lookback_days})")
    today = eastern_today()
    start = today - timedelta(days=lookback_days - 1)
    yield from iter_eastern_dates(start, today)
