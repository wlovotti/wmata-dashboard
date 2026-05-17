"""
Shared helper for per-route × per-date pipeline iterations.

Every per-date pipeline in `pipelines/` follows the same pattern:
for each (route_id, service_date) in the Cartesian product, call a
processing function and collect the result dict. This module extracts
that loop into one place so convergence is trivial and a later
``pool_workers > 1`` path requires only a one-line change per pipeline.

Usage example::

    from src.batch_iterator import run_route_date_grid
    from pipelines.compute_bunching import materialize_bunching_for_route_date

    results = run_route_date_grid(
        process_func=materialize_bunching_for_route_date,
        db=db,
        route_ids=route_ids,
        service_dates=service_dates,
    )

``process_func`` must accept ``(db, route_id, service_date)`` as its first
three positional arguments and return a ``dict``.  Any additional keyword
arguments are forwarded via ``**kwargs``.

The iteration order matches the historical convention: outer loop over dates,
inner loop over routes.  ``pool_workers`` is accepted but unused in this
release — it exists only to mark the seam for a future parallel path.
"""

from collections.abc import Callable
from datetime import date as date_type

from sqlalchemy.orm import Session


def run_route_date_grid(
    process_func: Callable[..., dict],
    db: Session,
    route_ids: list[str],
    service_dates: list[date_type],
    *,
    pool_workers: int = 1,
    **kwargs,
) -> list[dict]:
    """Iterate ``process_func`` over a (route_ids × service_dates) grid.

    Args:
        process_func: Callable with signature
            ``(db, route_id, service_date, **kwargs) -> dict``.
            The function is responsible for its own logging and error
            handling — this iterator preserves whatever behaviour each
            caller already had.
        db: SQLAlchemy session forwarded to every call.
        route_ids: Ordered list of route identifiers to process.
        service_dates: Ordered list of service dates to process.
            Outer loop, matching the historical dates-first convention.
        pool_workers: Reserved for a future parallel path.  The only
            supported value today is ``1`` (single-threaded).  Passing a
            higher value raises ``NotImplementedError`` so callers don't
            silently get serial behaviour when they intended parallel.
        **kwargs: Extra keyword arguments forwarded verbatim to
            ``process_func`` on every call (e.g. ``verbose=True``).

    Returns:
        A flat list of result dicts, one per (service_date, route_id)
        cell, in dates-outer / routes-inner order.

    Raises:
        NotImplementedError: If ``pool_workers`` > 1 (parallel path not
            yet implemented).
    """
    if pool_workers != 1:
        raise NotImplementedError(
            "pool_workers > 1 is not yet implemented; parallel support is the next step."
        )

    out: list[dict] = []
    for service_date in service_dates:
        for route_id in route_ids:
            out.append(process_func(db, route_id, service_date, **kwargs))
    return out
