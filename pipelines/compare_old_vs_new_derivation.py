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
        v2.deviation_sec          AS new_dev,
        -- v2.trip_id is the join's PK presence signal: NULL ONLY when the
        -- LEFT JOIN found no matching v2 row. Bare NULL comparisons on
        -- new_ts/new_sr/new_dev can't distinguish "no v2 row" from
        -- "matched v2 row that legitimately has NULL value" (e.g.,
        -- SKIPPED stops have NULL observed_arrival_ts).
        v2.trip_id                AS v2_trip_id
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
    SUM(CASE WHEN v2_trip_id IS NULL THEN 1 ELSE 0 END) AS missing_in_v2,
    -- IS NOT DISTINCT FROM treats NULL = NULL as TRUE, which is required
    -- for SKIPPED stops (observed_arrival_ts and deviation_sec are both
    -- NULL on SKIPPED). Bare `=` would always return NULL and count
    -- every SKIPPED row as mismatched.
    SUM(
        CASE WHEN
            old_ts IS NOT DISTINCT FROM new_ts
            AND old_sr IS NOT DISTINCT FROM new_sr
            AND old_dev IS NOT DISTINCT FROM new_dev
        THEN 1 ELSE 0 END
    ) AS matched
FROM joined
GROUP BY route_id
"""


def compare_one_day(db: Session, target_date: date_type) -> dict:
    """Compute agreement metrics for one service date.

    Joins stop_events (old derivation) against stop_events_v2 (new derivation)
    on (trip_id, stop_sequence, service_date) for trip_update-source rows, then
    checks field-level agreement on observed_arrival_ts, schedule_relationship,
    and deviation_sec.

    NULL handling: SKIPPED stops have NULL observed_arrival_ts and deviation_sec
    in both tables. IS NOT DISTINCT FROM is used for all three compared columns
    so that NULL = NULL evaluates to TRUE (matched) rather than NULL (mismatched).
    The missing_in_v2 count uses v2.trip_id IS NULL as the join-presence sentinel
    to avoid false positives from legitimately NULL fields in matched SKIPPED rows.

    Args:
        db: Active SQLAlchemy session pointing at a Postgres database.
        target_date: The service_date (Eastern operational day) to evaluate.

    Returns:
        A dict with keys:
            service_date: str  ISO date string
            total_rows: int    rows in stop_events for the date
            matched_rows: int  rows with identical fields in v2
            agreement_pct: float  (0-100)
            v2_only_rows: int  rows in v2 with no counterpart in old (soft regression signal)
            diverging_routes: list[dict]  routes with > 1% disagreement, sorted by route_id
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

    # Detect v2-only rows: rows the new derivation produced that have no
    # counterpart in the old. The primary LEFT JOIN above is FROM old, so
    # it can't see them. Phase D treats v2-only rows as a soft signal —
    # the new pipeline may have legitimate new coverage, but truly
    # invented rows (no old counterpart for a date when the old pipeline
    # also ran) deserve scrutiny.
    v2_only_count = (
        db.execute(
            text(
                """
            SELECT COUNT(*)
            FROM stop_events_v2 v2
            LEFT JOIN stop_events old
                ON old.trip_id = v2.trip_id
               AND old.stop_sequence = v2.stop_sequence
               AND old.service_date = v2.service_date
               AND old.source = 'trip_update'
            WHERE v2.service_date = :service_date
              AND old.trip_id IS NULL
            """
            ),
            {"service_date": target_date.isoformat()},
        ).scalar()
        or 0
    )

    overall = (matched_all / total_all * 100) if total_all else 100.0
    return {
        "service_date": target_date.isoformat(),
        "total_rows": total_all,
        "matched_rows": matched_all,
        "agreement_pct": round(overall, 2),
        "v2_only_rows": v2_only_count,
        "diverging_routes": sorted(diverging, key=lambda r: r["route_id"]),
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
                f"{result['v2_only_rows']:,} v2-only rows, "
                f"{len(result['diverging_routes'])} routes with >1% disagreement"
            )
            for d_route in result["diverging_routes"]:
                print(f"  ! {d_route}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
