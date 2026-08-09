"""
Upsert per-(route, service_date) overlay rows for the given service date.

Materializes the sufficient statistics the windowed-scorecard endpoint
reads, replacing the previous live-compute path (which paid ~35s on cold
cache pulling 3M+ stop_events rows). Designed to be called per service
date by `pipelines/run_daily_batch.py` after the per-date derivation
pipelines have committed their stop_events / runs rows.

Multi-agency (NOTES-100): pass ``--agency`` (matching a
``config/agencies/<agency>.yaml``) to upsert into a non-WMATA database;
only the completeness guard's coverage window becomes agency-local (see
``upsert_route_metrics_for_date``'s ``tz_name`` docstring) — the metric
computation itself is still Eastern-hardcoded (NOTES-103).

Usage:
  uv run python -m pipelines.upsert_route_metrics_overlay --date 2026-05-08
  uv run python -m pipelines.upsert_route_metrics_overlay --date 2026-07-23 --agency sfmta
"""

from __future__ import annotations

import argparse
import sys
from datetime import date as date_type
from datetime import datetime

from src.agency_config import load_agency_config, resolve_agency_db_url
from src.database import get_session
from src.route_metrics_overlay import upsert_route_metrics_for_date


def _parse_date(value: str) -> date_type:
    """Parse YYYY-MM-DD into a date; argparse hands the raw string in."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    """Upsert per-route overlay rows for one date; return 0 on success, 1 on failure."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        type=_parse_date,
        required=True,
        help="Eastern service date (YYYY-MM-DD)",
    )
    # Accepted but ignored — the overlay materializes every route active
    # on the date. The flag exists so `pipelines/run_daily_batch.py` can
    # dispatch every per-date pipeline with the same args.
    parser.add_argument("--all-routes", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--gtfs-snapshot-id",
        type=int,
        default=None,
        help=(
            "Pin the scheduled side (service-delivered denominators, EWT SWT "
            "pools) to a historical GTFS snapshot when backfilling a date "
            "whose schedule has been superseded; default reads is_current"
        ),
    )
    parser.add_argument(
        "--agency",
        default="wmata",
        help=(
            "Agency name matching config/agencies/<agency>.yaml (default: "
            "'wmata'). Selects the completeness guard's timezone and the "
            "target database — see pipelines/run_daily_batch.py."
        ),
    )
    args = parser.parse_args()

    cfg = load_agency_config(args.agency)
    db = get_session(db_url=resolve_agency_db_url(cfg))
    try:
        result = upsert_route_metrics_for_date(
            db, args.date, args.gtfs_snapshot_id, tz_name=cfg.timezone
        )
        return 0 if result is not None else 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
