"""
Compare TripUpdate-source vs Proximity-source stop_events for a service_date —
the agreement study that calibrates which derivation path to trust as primary
downstream (PR #44, completing the stop_events foundation).

The two derivation pipelines produce stop_events from independent signals:
proximity from VehiclePosition geometry (50 m radius, first detection per
stop), trip_update from the prediction trail in TripUpdate snapshots. Each
is recorded as its own row distinguished by `source`, so a real-world
arrival typically becomes two rows. Phase A2 + B1 already populate the
table; this module only reads it.

The comparison computes:
  * coverage — how many events appear in both sources vs. in only one
  * temporal agreement — |Δ| distribution on the BOTH set (median, p95,
    p99) and signed bias (TU − proximity)
  * per-route breakdown — same numbers grouped by route_id

It does NOT cross-check SKIPPED stops: proximity can't observe a skip, so
a TU SKIPPED row has `observed_arrival_ts IS NULL` and is filtered out by
the same predicate that protects the |Δ| math. Skip-rate measurement is
NOTES.md NOTES-14, a separate scope.

Run on 2026-05-03 baseline (all routes): TU 207k, proximity 82k, BOTH
77k (93% of proximity); |Δ| median 10s, p95 56s, p99 265s; signed bias
TU − prox median −6s (TU tends to predict slightly earlier than proximity
detects, consistent with proximity matching at 50m radius rather than at
the stop).

Usage:
  uv run python pipelines/compare_stop_event_sources.py --date 2026-05-03
  uv run python pipelines/compare_stop_event_sources.py --date 2026-05-03 --route C51
"""

import argparse
from collections import defaultdict
from datetime import date as date_type
from datetime import datetime

import numpy as np
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from src.database import get_session
from src.models import StopEvent
from src.timezones import eastern_today


def compare_stop_event_sources(
    db: Session,
    service_date: date_type,
    route_id: str | None = None,
) -> dict:
    """Compute TU vs Proximity agreement stats over stop_events for one service_date.

    Pulls both sources' rows in a single query, pivots in Python by
    (trip_id, stop_sequence), and counts pairings as TU-only / proximity-only /
    both. Filters `observed_arrival_ts IS NOT NULL` so SKIPPED rows don't
    pollute the |Δ| math; that filter is also why a TU-only row here means
    "TU saw a real arrival proximity didn't" — not "TU said skipped".

    Returns a dict with overall counts, |Δ| percentiles, signed bias, and a
    `per_route` list (omitted when `route_id` restricts to one route).
    """
    q = db.query(
        StopEvent.trip_id,
        StopEvent.stop_sequence,
        StopEvent.route_id,
        StopEvent.source,
        StopEvent.observed_arrival_ts,
    ).filter(
        StopEvent.service_date == service_date.isoformat(),
        StopEvent.source.in_(("trip_update", "proximity")),
        StopEvent.observed_arrival_ts.isnot(None),
    )
    if route_id is not None:
        q = q.filter(StopEvent.route_id == route_id)
    rows = q.all()

    # Pivot to one entry per (trip_id, stop_sequence) carrying both timestamps.
    # The unique constraint on stop_events guarantees at most one TU and one
    # proximity row per key, so a plain dict suffices.
    by_key: dict[tuple[str, int], dict] = {}
    for trip_id, stop_seq, r_id, source, ts in rows:
        entry = by_key.setdefault((trip_id, stop_seq), {"route_id": r_id})
        entry[source] = ts

    overall = _Bucket()
    per_route: dict[str, _Bucket] = defaultdict(_Bucket)

    for entry in by_key.values():
        tu_ts = entry.get("trip_update")
        prox_ts = entry.get("proximity")
        bucket = per_route[entry["route_id"]]
        if tu_ts is not None and prox_ts is not None:
            delta = (tu_ts - prox_ts).total_seconds()
            overall.add_both(delta)
            bucket.add_both(delta)
        elif tu_ts is not None:
            overall.tu_only += 1
            bucket.tu_only += 1
        elif prox_ts is not None:
            overall.prox_only += 1
            bucket.prox_only += 1

    report = overall.to_dict()
    report["service_date"] = service_date.isoformat()
    report["route_id"] = route_id

    if route_id is None:
        per_route_out = []
        for rid, b in per_route.items():
            row = b.to_dict()
            row["route_id"] = rid
            per_route_out.append(row)
        # Sort by combined event count, descending — the noisiest routes first.
        per_route_out.sort(key=lambda r: -(r["tu_total"] + r["proximity_total"]))
        report["per_route"] = per_route_out

    return report


class _Bucket:
    """Mutable accumulator for one comparison slice (overall or per-route)."""

    def __init__(self) -> None:
        self.tu_only = 0
        self.prox_only = 0
        self.deltas_abs: list[float] = []
        self.deltas_signed: list[float] = []

    def add_both(self, delta_sec: float) -> None:
        """Record a (trip, stop) pair observed by both sources."""
        self.deltas_abs.append(abs(delta_sec))
        self.deltas_signed.append(delta_sec)

    @property
    def both_count(self) -> int:
        """Number of pairs observed by both sources."""
        return len(self.deltas_abs)

    def to_dict(self) -> dict:
        """Render the bucket as a plain dict with derived ratios and Δ percentiles."""
        both = self.both_count
        tu_total = both + self.tu_only
        prox_total = both + self.prox_only
        return {
            "tu_total": tu_total,
            "proximity_total": prox_total,
            "both_count": both,
            "tu_only_count": self.tu_only,
            "proximity_only_count": self.prox_only,
            "coverage_of_proximity": (both / prox_total) if prox_total else None,
            "coverage_of_tu": (both / tu_total) if tu_total else None,
            "delta_stats": _delta_stats(self.deltas_abs, self.deltas_signed),
        }


def _delta_stats(deltas_abs: list[float], deltas_signed: list[float]) -> dict | None:
    """Summarize a |Δ| / signed-Δ list as median/p95/p99/mean. None if empty."""
    if not deltas_abs:
        return None
    a = np.asarray(deltas_abs)
    s = np.asarray(deltas_signed)
    return {
        "n": len(a),
        "abs_median_sec": float(np.median(a)),
        "abs_p95_sec": float(np.percentile(a, 95)),
        "abs_p99_sec": float(np.percentile(a, 99)),
        "signed_median_sec": float(np.median(s)),
        "signed_mean_sec": float(np.mean(s)),
    }


def format_summary(report: dict, top_n_routes: int = 10) -> str:
    """Render the report dict as a human-readable text block for stdout."""
    lines = [f"Comparison: stop_events for service_date {report['service_date']}"]
    if report.get("route_id"):
        lines.append(f"Restricted to route_id={report['route_id']}")
    lines.append("=" * 60)
    lines.append("Overall:")
    lines.append(f"  TU total:           {report['tu_total']:>9,}")
    lines.append(f"  Proximity total:    {report['proximity_total']:>9,}")
    cov_p = report.get("coverage_of_proximity")
    cov_t = report.get("coverage_of_tu")
    cov_p_str = f"{cov_p * 100:.1f}%" if cov_p is not None else "n/a"
    cov_t_str = f"{cov_t * 100:.1f}%" if cov_t is not None else "n/a"
    lines.append(
        f"  Both (matched):     {report['both_count']:>9,} "
        f"({cov_p_str} of proximity, {cov_t_str} of TU)"
    )
    lines.append(f"  TU only:            {report['tu_only_count']:>9,}")
    lines.append(f"  Proximity only:     {report['proximity_only_count']:>9,}")

    d = report.get("delta_stats")
    if d:
        lines.append(
            f"  |delta| seconds (n={d['n']:,}): "
            f"median={d['abs_median_sec']:.0f}  "
            f"p95={d['abs_p95_sec']:.0f}  "
            f"p99={d['abs_p99_sec']:.0f}"
        )
        lines.append(
            f"  Signed delta (TU - prox): "
            f"median={d['signed_median_sec']:+.0f}  "
            f"mean={d['signed_mean_sec']:+.1f}"
        )

    per_route = report.get("per_route") or []
    if per_route:
        shown = per_route[:top_n_routes]
        lines.append("")
        lines.append(f"Per route (top {len(shown)} of {len(per_route)} by combined event count):")
        lines.append(
            f"  {'route':<10} {'tu':>8} {'prox':>8} {'both':>8} "
            f"{'cov_p':>7} {'med|d|':>7} {'p95|d|':>7}"
        )
        for r in shown:
            cov = r.get("coverage_of_proximity")
            cov_str = f"{cov * 100:.1f}%" if cov is not None else "n/a"
            d_r = r.get("delta_stats")
            med = f"{d_r['abs_median_sec']:.0f}" if d_r else "-"
            p95 = f"{d_r['abs_p95_sec']:.0f}" if d_r else "-"
            lines.append(
                f"  {str(r['route_id']):<10} {r['tu_total']:>8,} {r['proximity_total']:>8,} "
                f"{r['both_count']:>8,} {cov_str:>7} {med:>7} {p95:>7}"
            )
    return "\n".join(lines)


def main():
    """CLI entry point — parse args, run the comparison, print a summary."""
    parser = argparse.ArgumentParser(
        description=(
            "Compare TU vs proximity stop_events for one service_date. "
            "Reads from the existing stop_events table — no derivation work."
        )
    )
    parser.add_argument(
        "--date",
        help="Service date in YYYY-MM-DD form (Eastern). Defaults to today (Eastern).",
    )
    parser.add_argument(
        "--route",
        help="Optional single route_id to restrict the comparison to.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of per-route rows to print (default: 10). Ignored with --route.",
    )
    args = parser.parse_args()

    load_dotenv()
    if args.date:
        service_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        service_date = eastern_today()

    db = get_session()
    try:
        report = compare_stop_event_sources(db, service_date, route_id=args.route)
        print(format_summary(report, top_n_routes=args.top_n))
    finally:
        db.close()


if __name__ == "__main__":
    main()
