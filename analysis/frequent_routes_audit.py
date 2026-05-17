"""
Cross-validation: WMATA-published frequent routes vs data-driven check.

One-shot analysis comparing the tiered route list in
`config/frequent_routes.yaml` (NOTES-56) against the per-cell-hour
frequent classification computed live from GTFS schedule.

Per-route gating: each designated route is checked under its own
tier's gate (`src/frequent_routes.py:get_cell_hour_gate_sec` —
15 min for high-freq, 20 min for medium-freq). Undesignated routes
are checked against the default 15-min gate, since the question for
them is "would this clear the high-freq bar?" not "what tier should
we put it in?".

Two cases are interesting either way:

  (a) Routes WMATA publishes as frequent but sparse under their
      own tier gate -> WMATA list may be stale, the route's tier
      assignment may be wrong, or our schedule snapshot lags
      WMATA's published map.

  (b) Routes WMATA does NOT publish as frequent but with a high
      fraction of frequent cell-hours -> WMATA omission, or the
      route has a frequent trunk + sparse branches whose route-level
      mean still meets the 15-min bar.

Run with:
    uv run python -m analysis.frequent_routes_audit

Output is a markdown-formatted summary printed to stdout. The script
queries the live DB; it does NOT write anything back. Not wired into
CI / the nightly pipeline — re-run after WMATA publishes a new map
or when you want a snapshot of the drift.
"""

from __future__ import annotations

import sys

from src.database import get_session
from src.ewt import (
    DAY_TYPE_REPRESENTATIVE_FIELD,
    FREQUENT_HEADWAY_MAX_SEC,
    fetch_scheduled_cell_hours_for_routes,
)
from src.frequent_routes import get_cell_hour_gate_sec, load_frequent_route_ids
from src.models import Route

# A route is flagged as "data-driven frequent" when at least this
# fraction of its weekday daytime cell-hours have a mean scheduled
# headway at or below FREQUENT_HEADWAY_MAX_SEC. The threshold is the
# question "is the route running frequently *most of the time*" — a
# single dense AM-peak hour shouldn't trigger frequent-status; we
# want sustained daytime coverage. WMATA's own published criterion
# is "every 12 (or 20) minutes or better, 7am-9pm all-week"; this is
# the closest approximation we can compute from GTFS alone.
DATA_DRIVEN_FREQUENT_HOUR_RATIO = 0.50

# Restrict the cell-hour denominator to WMATA's published 7am-9pm
# all-day window so the comparison is apples-to-apples.
DAYTIME_HOURS = range(7, 21)


def _daytime_frequent_share(
    cell_hour_to_headways: dict[tuple[int, str, int], list[float]],
    gate_sec: int = FREQUENT_HEADWAY_MAX_SEC,
) -> tuple[int, int]:
    """Return (frequent_cell_hours, total_cell_hours) restricted to daytime.

    Only hours in `DAYTIME_HOURS` count. A cell-hour is "frequent" iff its
    mean scheduled headway is at most `gate_sec`. The caller is expected
    to pass the route's per-tier gate.
    """
    frequent = 0
    total = 0
    for (_dir, _stop, hour), headways in cell_hour_to_headways.items():
        if hour not in DAYTIME_HOURS:
            continue
        if not headways:
            continue
        total += 1
        mean_h = sum(headways) / len(headways)
        if mean_h <= gate_sec:
            frequent += 1
    return frequent, total


def main() -> int:
    """Print the markdown-formatted audit summary to stdout.

    Returns the process exit code (always 0 on a successful run; non-zero
    only on infrastructure failures like a missing DB).
    """
    db = get_session()
    try:
        # Universe of routes is whatever GTFS currently knows about.
        # Compare against the published list directly — extra routes on
        # the published list that don't appear in GTFS get flagged in a
        # separate "unknown to current GTFS" block.
        designated = load_frequent_route_ids()
        gtfs_route_ids = {r for (r,) in db.query(Route.route_id).filter(Route.is_current).all()}

        # Use weekday as the day_type — WMATA's frequent-service standard
        # is "7am-9pm all-week", which weekday is the strictest test of
        # (every weekday must qualify). Saturday/Sunday tend to be more
        # constrained anyway; a route that's frequent on weekday but not
        # weekend wouldn't be expected to flip designation here.
        assert "weekday" in DAY_TYPE_REPRESENTATIVE_FIELD
        sched = fetch_scheduled_cell_hours_for_routes(db, day_type="weekday")

        # Per-route share computation. Each route is gated under its own
        # tier's threshold — designated routes use get_cell_hour_gate_sec;
        # undesignated routes fall back to the default 15-min gate inside
        # that helper. Routes with zero cell-hours in daytime (likely
        # school-day-only routes that don't run weekday off-peak) get
        # share = None and drop into a separate "no weekday daytime
        # service" bucket.
        share_by_route: dict[str, float | None] = {}
        details: dict[str, tuple[int, int]] = {}
        for route_id in gtfs_route_ids:
            gate_sec = get_cell_hour_gate_sec(route_id)
            frequent, total = _daytime_frequent_share(sched.get(route_id, {}), gate_sec)
            details[route_id] = (frequent, total)
            share_by_route[route_id] = (frequent / total) if total > 0 else None

        # Case (a): designated frequent but data-driven sparse.
        sparse_designated: list[tuple[str, int, int, float | None]] = []
        # Case (b): not designated but data-driven dense.
        dense_undesignated: list[tuple[str, int, int, float]] = []

        for route_id in sorted(gtfs_route_ids):
            share = share_by_route[route_id]
            frequent, total = details[route_id]
            in_list = route_id in designated
            if in_list and (share is None or share < DATA_DRIVEN_FREQUENT_HOUR_RATIO):
                sparse_designated.append((route_id, frequent, total, share))
            elif not in_list and share is not None and share >= DATA_DRIVEN_FREQUENT_HOUR_RATIO:
                dense_undesignated.append((route_id, frequent, total, share))

        # Routes on the published list that aren't in current GTFS — most
        # likely a renamed/dropped route from the Better Bus redesign;
        # surfaces immediately so the YAML can be cleaned up.
        designated_missing_from_gtfs = sorted(designated - gtfs_route_ids)

        # ---- Output ---------------------------------------------------
        out = []
        out.append("# Frequent-routes audit\n")
        out.append(
            f"Designated routes: **{len(designated)}** (from `config/frequent_routes.yaml`)\n"
        )
        out.append(f"GTFS current routes: **{len(gtfs_route_ids)}**\n")
        out.append(
            f"Threshold: a route passes the data-driven check when "
            f"≥ **{int(DATA_DRIVEN_FREQUENT_HOUR_RATIO * 100)}%** of its "
            f"weekday daytime (7-21) cell-hours have a mean scheduled "
            f"headway ≤ its tier gate "
            f"(15 min for high-freq / undesignated, 20 min for "
            f"medium-freq — see `src/frequent_routes.py`).\n"
        )
        out.append("")

        out.append("## (a) Designated frequent but data-driven sparse\n")
        out.append(
            "_Candidates for WMATA list staleness, GTFS lag, or a route whose "
            "frequent trunk doesn't propagate to enough cell-hours._\n"
        )
        if sparse_designated:
            out.append("| route_id | frequent cell-hours | total daytime cell-hours | share |")
            out.append("| --- | ---: | ---: | ---: |")
            for route_id, frequent, total, share in sparse_designated:
                share_str = "—" if share is None else f"{share * 100:.1f}%"
                out.append(f"| {route_id} | {frequent} | {total} | {share_str} |")
        else:
            out.append("_None — every designated route clears the threshold._")
        out.append("")

        out.append("## (b) Not designated but data-driven dense\n")
        out.append(
            "_Candidates for WMATA omission, or a route whose schedule looks "
            "frequent in GTFS but doesn't meet the all-day-all-week service-span "
            "test the published map requires._\n"
        )
        if dense_undesignated:
            # Sort dense_undesignated by share desc so the most-likely
            # WMATA omissions float to the top.
            dense_undesignated.sort(key=lambda x: -(x[3] or 0))
            out.append("| route_id | frequent cell-hours | total daytime cell-hours | share |")
            out.append("| --- | ---: | ---: | ---: |")
            for route_id, frequent, total, share in dense_undesignated:
                out.append(f"| {route_id} | {frequent} | {total} | {share * 100:.1f}% |")
        else:
            out.append("_None — every data-driven-dense route is on the published list._")
        out.append("")

        if designated_missing_from_gtfs:
            out.append("## Designated routes not present in current GTFS\n")
            out.append(
                "_These route_ids appear in `config/frequent_routes.yaml` but "
                "no `routes.is_current=True` row matches. Likely a renamed or "
                "retired route — remove from the YAML or re-pull WMATA's map._\n"
            )
            for route_id in designated_missing_from_gtfs:
                out.append(f"- {route_id}")
            out.append("")

        print("\n".join(out))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
