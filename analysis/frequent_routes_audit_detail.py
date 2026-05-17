"""
Per-route deep-dive for designated frequent routes failing their tier gate.

The companion to `analysis/frequent_routes_audit.py`. That script
flags designated routes that fail the data-driven check; this one
shows the share-under-each-candidate-gate per route so an operator
can decide the right corrective action when a route fails:

  - Wrong tier — a high-freq route would qualify under the
    medium-freq tier's 20-min gate (or vice-versa). Move it.
  - Corridor-not-route — fails even a generous 30-min gate. Likely
    a branch/limited variant sharing a trunk with peer routes;
    WMATA's published map shows the corridor as frequent, not the
    individual route. Remove from yaml.
  - No-weekday-service — no cell-hour denominator to evaluate
    (school-tripper, weekend-only, or thin GTFS coverage).

Each route's pass/fail judgement uses its own tier gate (resolved
via `src/frequent_routes.py:get_cell_hour_gate_sec`). The share-
under-X columns surface the data the operator needs to pick the
fix. Post-migration this report should be empty — anything that
appears is drift between the yaml and the live schedule.

Run with:
    uv run python -m analysis.frequent_routes_audit_detail

Output is markdown to stdout. Read-only — does not modify the DB
or the yaml.
"""

from __future__ import annotations

import statistics
import sys
from collections import defaultdict

from src.database import get_session
from src.ewt import fetch_scheduled_cell_hours_for_routes
from src.frequent_routes import (
    HIGH_FREQ_GATE_SEC,
    MEDIUM_FREQ_GATE_SEC,
    get_cell_hour_gate_sec,
    load_high_freq_route_ids,
    load_medium_freq_route_ids,
)
from src.models import Route

# Match the audit script's daytime window (WMATA's 7am-9pm spec).
DAYTIME_HOURS = range(7, 21)

# Gates surfaced in the share-under-X columns. The first two match the
# production tier gates; 30 min is the "even loose gates don't help"
# sanity floor — fails it and the route is unambiguously not frequent.
GATES_MIN = (HIGH_FREQ_GATE_SEC // 60, MEDIUM_FREQ_GATE_SEC // 60, 30)

# A route counts as passing its tier gate when this fraction of its
# daytime cell-hours qualify. Matches the headline audit threshold.
QUALIFY_SHARE = 0.50


def _per_cell_mean_seconds(
    cell_hour_to_headways: dict[tuple[int, str, int], list[float]],
) -> list[tuple[int, float]]:
    """Return one (direction_id, mean_headway_sec) tuple per daytime cell-hour.

    Drops cell-hours outside `DAYTIME_HOURS` and cell-hours with no
    scheduled headways. Each surviving cell-hour contributes one mean
    headway — the same per-cell-hour mean `_is_cell_hour_frequent` gates
    on. Direction is preserved so per-direction breakdowns can split.
    """
    out: list[tuple[int, float]] = []
    for (direction_id, _stop, hour), headways in cell_hour_to_headways.items():
        if hour not in DAYTIME_HOURS:
            continue
        if not headways:
            continue
        out.append((direction_id, sum(headways) / len(headways)))
    return out


def _share_under_gate(mean_secs: list[float], gate_min: int) -> float | None:
    """Fraction of cell-hours whose mean headway is ≤ `gate_min` minutes.

    Returns `None` when the list is empty so callers can distinguish
    "no daytime cell-hours" from "0 of N qualifying."
    """
    if not mean_secs:
        return None
    gate_sec = gate_min * 60
    return sum(1 for m in mean_secs if m <= gate_sec) / len(mean_secs)


def _per_direction_share(
    per_cell_means: list[tuple[int, float]], gate_min: int
) -> dict[int, tuple[int, int, float]]:
    """Per-direction (n_qualifying, n_total, share) under one gate.

    Splits the cell-hours by `direction_id` and returns the share-under-
    gate per direction. Surfaces trunk-frequent / branch-sparse routes:
    a route whose direction 0 share is 80% and direction 1 share is 5%
    is read very differently from one with 40% in both.
    """
    by_dir: dict[int, list[float]] = defaultdict(list)
    for direction_id, mean_sec in per_cell_means:
        by_dir[direction_id].append(mean_sec)
    gate_sec = gate_min * 60
    out: dict[int, tuple[int, int, float]] = {}
    for direction_id, means in sorted(by_dir.items()):
        n_total = len(means)
        n_qual = sum(1 for m in means if m <= gate_sec)
        out[direction_id] = (n_qual, n_total, n_qual / n_total if n_total else 0.0)
    return out


def _format_row(row: dict) -> str:
    """Render one audit-row dict as a markdown table row."""

    def fmt_share(s: float | None) -> str:
        """Render a share-under-gate as percent, or em-dash for None."""
        return "—" if s is None else f"{s * 100:.0f}%"

    def fmt_min(x: float | None) -> str:
        """Render a minutes value with one decimal, or em-dash for None."""
        return "—" if x is None else f"{x:.1f}"

    per_dir_str = ", ".join(
        f"dir {d}: {n}/{tot} ({share * 100:.0f}%)"
        for d, (n, tot, share) in row["per_direction_tier"].items()
    )
    if not per_dir_str:
        per_dir_str = "—"
    shares = row["share_by_gate"]
    return (
        f"| {row['route_id']} "
        f"| {row['tier']} "
        f"| {row['n_cells']} "
        f"| {fmt_min(row['mean_h_min'])} "
        f"| {fmt_min(row['median_h_min'])} "
        f"| {fmt_min(row['min_h_min'])} "
        f"| {fmt_share(shares[GATES_MIN[0]])} "
        f"| {fmt_share(shares[GATES_MIN[1]])} "
        f"| {fmt_share(shares[GATES_MIN[2]])} "
        f"| {per_dir_str} |"
    )


def main() -> int:
    """Print the markdown-formatted per-route detail to stdout.

    Reads the live DB and reports any designated route that fails
    its own tier gate. Exits 0 on success; non-zero only on
    infrastructure failures.
    """
    db = get_session()
    try:
        high = load_high_freq_route_ids()
        medium = load_medium_freq_route_ids()
        designated = high | medium
        gtfs_route_ids = {r for (r,) in db.query(Route.route_id).filter(Route.is_current).all()}

        sched = fetch_scheduled_cell_hours_for_routes(db, day_type="weekday")

        # Build per-route detail for every designated route present in
        # current GTFS. Only routes failing their own tier gate are
        # surfaced — passing routes don't need a deep dive.
        present = sorted(designated & gtfs_route_ids)
        failing: list[dict] = []
        for route_id in present:
            tier = "high" if route_id in high else "medium"
            tier_gate_min = (HIGH_FREQ_GATE_SEC if tier == "high" else MEDIUM_FREQ_GATE_SEC) // 60

            per_cell = _per_cell_mean_seconds(sched.get(route_id, {}))
            mean_secs = [m for _d, m in per_cell]
            per_gate_share = {g: _share_under_gate(mean_secs, g) for g in GATES_MIN}

            tier_share = per_gate_share[tier_gate_min]
            if tier_share is not None and tier_share >= QUALIFY_SHARE:
                continue

            failing.append(
                {
                    "route_id": route_id,
                    "tier": tier,
                    "n_cells": len(mean_secs),
                    "mean_h_min": (statistics.mean(mean_secs) / 60.0) if mean_secs else None,
                    "median_h_min": (statistics.median(mean_secs) / 60.0) if mean_secs else None,
                    "min_h_min": (min(mean_secs) / 60.0) if mean_secs else None,
                    "share_by_gate": per_gate_share,
                    "per_direction_tier": _per_direction_share(per_cell, tier_gate_min),
                }
            )

        out: list[str] = []
        out.append("# Frequent-routes deep-dive\n")
        out.append(
            f"Routes designated in `config/frequent_routes.yaml`: "
            f"**{len(designated)}** ({len(high)} high-freq + {len(medium)} medium-freq)\n"
        )
        out.append(
            f"Routes failing their own tier's cell-hour gate (< "
            f"{int(QUALIFY_SHARE * 100)}% of daytime cell-hours qualify): "
            f"**{len(failing)}**\n"
        )
        out.append("")

        # When everything passes, say so clearly — that's the post-migration
        # expected state and the most-useful single sentence for an operator.
        if not failing:
            out.append("_All designated routes clear their tier gate. No drift detected._")
            print("\n".join(out))
            return 0

        # Tip table for interpreting the share columns.
        out.append(
            "Reading the share columns: a high-freq route failing at "
            f"{GATES_MIN[0]} min but passing at {GATES_MIN[1]} min should "
            "likely move to medium-freq. Any route failing at "
            f"{GATES_MIN[2]} min is almost certainly corridor-frequency "
            "(branch/limited variant sharing a trunk with peer routes) and "
            "should come off the yaml entirely. Routes with no cell-hours "
            "(`—` in every share column) have no weekday daytime service to "
            "evaluate."
        )
        out.append("")
        out.append(
            "| route_id | tier | n cells | mean h (min) | median h | min h "
            f"| share ≤{GATES_MIN[0]}m | share ≤{GATES_MIN[1]}m "
            f"| share ≤{GATES_MIN[2]}m | per-dir share at tier gate |"
        )
        out.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")

        # Sort by share at the most-permissive gate (30 min) so the
        # genuinely-not-frequent routes (case "remove") float to the bottom
        # and the borderline ones (case "move tier") sit at the top.
        def sort_key(r: dict) -> float:
            s = r["share_by_gate"][GATES_MIN[2]]
            return -(s if s is not None else -1.0)

        for row in sorted(failing, key=sort_key):
            out.append(_format_row(row))

        # Footer: surface what the per-route gate currently resolves to,
        # so the operator doesn't have to cross-reference the loader.
        out.append("")
        out.append(
            "Gate policy (from `src/frequent_routes.py:get_cell_hour_gate_sec`): "
            f"high-freq + undesignated = {HIGH_FREQ_GATE_SEC // 60} min; "
            f"medium-freq = {MEDIUM_FREQ_GATE_SEC // 60} min."
        )

        # Sanity: the gate values reported above match what the helper
        # actually returns; if this assertion ever fires the script's
        # output has silently drifted from runtime behavior.
        sample_high = next(iter(high), None)
        sample_medium = next(iter(medium), None)
        if sample_high is not None:
            assert get_cell_hour_gate_sec(sample_high) == HIGH_FREQ_GATE_SEC
        if sample_medium is not None:
            assert get_cell_hour_gate_sec(sample_medium) == MEDIUM_FREQ_GATE_SEC

        print("\n".join(out))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
