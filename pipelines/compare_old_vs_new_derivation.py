"""Phase D validation: diff stop_events vs stop_events_v2.

For each (route_id, service_date) in the requested window, compute:
  - Row counts in both tables
  - For matching (trip_id, stop_sequence), agreement on
    (observed_arrival_ts, schedule_relationship, deviation_sec)
  - Per-route disagreement %; flag any > 1%.

Phase E cutover requires agreement_pct >= 99.5 for at least 7 consecutive
days including one full weekend (see design doc).

Usage:
    uv run python pipelines/compare_old_vs_new_derivation.py --date 2026-05-17
    uv run python pipelines/compare_old_vs_new_derivation.py --days-back 7
"""

import argparse
import sys
from datetime import date as date_type
from datetime import datetime, timedelta

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database import get_session
from src.timezones import utcnow_naive

COMPARE_SQL = """
WITH joined AS (
    SELECT
        old.route_id,
        old.trip_id,
        old.stop_sequence,
        old.observed_arrival_ts  AS old_ts,
        v2.observed_arrival_ts   AS new_ts,
        old.schedule_relationship AS old_sr,
        v2.schedule_relationship  AS new_sr,
        old.deviation_sec         AS old_dev,
        v2.deviation_sec          AS new_dev
    FROM stop_events old
    LEFT JOIN stop_events_v2 v2
        ON v2.trip_id = old.trip_id
       AND v2.stop_sequence = old.stop_sequence
       AND v2.service_date = old.service_date
    WHERE old.service_date = :service_date
      AND old.source = 'trip_update'
)
SELECT
    route_id,
    COUNT(*) AS total,
    SUM(CASE WHEN new_ts IS NULL THEN 1 ELSE 0 END) AS missing_in_v2,
    SUM(CASE WHEN old_ts = new_ts AND old_sr = new_sr AND old_dev IS NOT DISTINCT FROM new_dev THEN 1 ELSE 0 END) AS matched
FROM joined
GROUP BY route_id
"""


def compare_one_day(db: Session, target_date: date_type) -> dict:
    """Compute agreement metrics for one service date.

    Joins stop_events (old derivation) against stop_events_v2 (new derivation)
    on (trip_id, stop_sequence, service_date) for trip_update-source rows, then
    checks field-level agreement on observed_arrival_ts, schedule_relationship,
    and deviation_sec.

    Args:
        db: Active SQLAlchemy session pointing at a Postgres database.
        target_date: The service_date (Eastern operational day) to evaluate.

    Returns:
        A dict with keys:
            service_date: str  ISO date string
            total_rows: int    rows in stop_events for the date
            matched_rows: int  rows with identical fields in v2
            agreement_pct: float  (0-100)
            diverging_routes: list[dict]  routes with > 1% disagreement
            per_route: dict[route_id -> {total, matched, missing_in_v2, agreement_pct}]
    """
    rows = db.execute(
        text(COMPARE_SQL),
        {"service_date": target_date.isoformat()},
    ).all()

    per_route = {}
    total_all = 0
    matched_all = 0
    diverging = []
    for r in rows:
        route_id, total, missing, matched = r
        total_all += total or 0
        matched_all += matched or 0
        agreement = (matched / total * 100) if total else 100.0
        per_route[route_id] = {
            "total": total or 0,
            "matched": matched or 0,
            "missing_in_v2": missing or 0,
            "agreement_pct": round(agreement, 2),
        }
        if total and (total - matched) / total > 0.01:
            diverging.append({"route_id": route_id, **per_route[route_id]})

    overall = (matched_all / total_all * 100) if total_all else 100.0
    return {
        "service_date": target_date.isoformat(),
        "total_rows": total_all,
        "matched_rows": matched_all,
        "agreement_pct": round(overall, 2),
        "diverging_routes": diverging,
        "per_route": per_route,
    }


def main() -> int:
    """CLI entry point for the Phase D comparison pipeline.

    Parses --date or --days-back, runs compare_one_day for each requested
    date, and prints a one-line summary per date plus per-route detail for
    any diverging routes.

    Returns:
        0 on success.
    """
    parser = argparse.ArgumentParser(description="Compare stop_events vs stop_events_v2.")
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument(
        "--days-back",
        type=int,
        default=1,
        help="Compare the last N days (default: 1)",
    )
    args = parser.parse_args()

    load_dotenv()
    db = get_session()
    try:
        if args.date:
            dates = [datetime.strptime(args.date, "%Y-%m-%d").date()]
        else:
            today = utcnow_naive().date()
            dates = [today - timedelta(days=i) for i in range(1, args.days_back + 1)]
        for d in dates:
            result = compare_one_day(db, d)
            print(
                f"{result['service_date']}: "
                f"{result['agreement_pct']}% agreement "
                f"({result['matched_rows']:,}/{result['total_rows']:,}), "
                f"{len(result['diverging_routes'])} routes with >1% disagreement"
            )
            for d_route in result["diverging_routes"]:
                print(f"  ! {d_route}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
