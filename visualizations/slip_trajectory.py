"""
Per-segment slip + cumulative slip trajectory along a route.

Slip = observed segment travel time - scheduled segment travel time, averaged
across all observed trips. Cumulative slip = running sum from origin, which
equals the typical schedule deviation at each stop for a bus that started on
time. Reveals where the schedule under-budgets running time (positive slope)
vs where it has built-in recovery padding (negative slope, usually at
timepoints).

Usage:
    uv run python visualizations/slip_trajectory.py D80
    uv run python visualizations/slip_trajectory.py D80 --period pm_peak
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy import text

from src.database import get_session

sns.set_theme(style="whitegrid")


PERIOD_HOURS = {
    "all": None,
    "am_peak": (6, 10),
    "midday": (10, 15),
    "pm_peak": (15, 19),
    "evening": (19, 22),
    "late": None,  # 22-6, handled specially
}


def fetch_slip(db, route_id: str, period: str) -> pd.DataFrame:
    """Pull average per-segment slip and timepoint flag for one route.

    Returns one row per (direction_id, stop_sequence) for segments where the
    bus arrives next (the slip is attributed to the *arriving* stop). Joins
    against `timepoints` by stop_id to flag designated schedule checkpoints.
    """
    hour_filter = ""
    if period != "all":
        if period == "late":
            hour_filter = "AND (et_hr >= 22 OR et_hr < 6)"
        else:
            lo, hi = PERIOD_HOURS[period]
            hour_filter = f"AND et_hr >= {lo} AND et_hr < {hi}"

    sql = text(
        f"""
        WITH ordered AS (
          SELECT
            se.trip_id, se.service_date, se.direction_id, se.stop_sequence,
            se.stop_id, se.observed_arrival_ts, se.scheduled_arrival_ts,
            EXTRACT(HOUR FROM (se.observed_arrival_ts AT TIME ZONE 'UTC')
              AT TIME ZONE 'America/New_York')::INT AS et_hr,
            LEAD(se.stop_sequence) OVER w AS next_seq,
            LEAD(se.stop_id) OVER w AS next_stop_id,
            LEAD(se.observed_arrival_ts) OVER w AS next_obs,
            LEAD(se.scheduled_arrival_ts) OVER w AS next_sched
          FROM stop_events se
          WHERE route_id = :route_id
            AND source = 'proximity'
            AND observed_arrival_ts IS NOT NULL
            AND scheduled_arrival_ts IS NOT NULL
            AND schedule_relationship = 'SCHEDULED'
          WINDOW w AS (PARTITION BY service_date, trip_id ORDER BY stop_sequence)
        ),
        seg AS (
          SELECT direction_id, next_seq AS arrive_seq, next_stop_id AS arrive_stop_id,
            EXTRACT(EPOCH FROM (next_obs - observed_arrival_ts)) AS obs_gap,
            EXTRACT(EPOCH FROM (next_sched - scheduled_arrival_ts)) AS sched_gap
          FROM ordered
          WHERE next_obs IS NOT NULL
            {hour_filter}
            AND EXTRACT(EPOCH FROM (next_obs - observed_arrival_ts)) BETWEEN 0 AND 1800
        )
        SELECT seg.direction_id,
               seg.arrive_seq,
               seg.arrive_stop_id,
               s.stop_name,
               COUNT(*) AS n,
               AVG(seg.obs_gap - seg.sched_gap)::FLOAT AS mean_slip_sec,
               CASE WHEN tp.stop_id IS NOT NULL THEN 1 ELSE 0 END AS is_timepoint
        FROM seg
        JOIN stops s ON s.stop_id = seg.arrive_stop_id AND s.is_current
        LEFT JOIN timepoints tp ON tp.stop_id = seg.arrive_stop_id
        GROUP BY seg.direction_id, seg.arrive_seq, seg.arrive_stop_id, s.stop_name, is_timepoint
        HAVING COUNT(*) >= 50
        ORDER BY seg.direction_id, seg.arrive_seq;
        """
    )

    rows = db.execute(sql, {"route_id": route_id}).fetchall()
    df = pd.DataFrame(
        rows,
        columns=[
            "direction_id",
            "arrive_seq",
            "stop_id",
            "stop_name",
            "n",
            "mean_slip_sec",
            "is_timepoint",
        ],
    )
    # Drop the origin-departure "slip" — it's dominated by the bus parking at
    # the layover before scheduled pull-out, not actual on-route slippage.
    # `arrive_seq` is the segment's *arriving* stop, so the origin-departure
    # segment lands at the minimum arrive_seq per direction.
    min_arrive = df.groupby("direction_id")["arrive_seq"].transform("min")
    df = df[df["arrive_seq"] != min_arrive].reset_index(drop=True)
    df["cum_slip_sec"] = df.groupby("direction_id")["mean_slip_sec"].cumsum()
    return df


def fetch_timepoints_by_latlon(db, route_id: str) -> set[str]:
    """Identify route stops that are within 50m of a stop in the timepoints table.

    The timepoints table uses WMATA's GTFS-Plus internal stop_ids, which don't
    match the public GTFS stop_ids in the `stops` table — so we have to match
    on location. 50m is loose enough to absorb GTFS centerline drift but tight
    enough to disambiguate adjacent NB/SB stops.
    """
    sql = text(
        """
        WITH route_stops AS (
          SELECT DISTINCT se.stop_id, s.stop_lat, s.stop_lon
          FROM stop_events se
          JOIN stops s ON s.stop_id = se.stop_id AND s.is_current
          WHERE se.route_id = :route_id AND se.source = 'proximity'
        )
        SELECT DISTINCT rs.stop_id
        FROM route_stops rs
        JOIN timepoints tp ON
          6371000 * 2 * ASIN(SQRT(
            POWER(SIN(RADIANS(rs.stop_lat - tp.stop_lat) / 2), 2) +
            COS(RADIANS(rs.stop_lat)) * COS(RADIANS(tp.stop_lat)) *
            POWER(SIN(RADIANS(rs.stop_lon - tp.stop_lon) / 2), 2)
          )) < 50;
        """
    )
    return {row[0] for row in db.execute(sql, {"route_id": route_id}).fetchall()}


def plot(df: pd.DataFrame, tp_stop_ids: set[str], route_id: str, period: str, out_path: Path):
    """Two-panel slip trajectory chart, one panel per direction.

    Each panel: bar chart of per-segment slip (top) + line chart of cumulative
    slip (bottom). Timepoint arrivals get marker emphasis on the cumulative
    line and a vertical line through both panels.
    """
    df = df.copy()
    df["is_tp"] = df["stop_id"].isin(tp_stop_ids).astype(int)

    dirs = sorted(df["direction_id"].unique())
    n_dirs = len(dirs)
    fig, axes = plt.subplots(2, n_dirs, figsize=(7 * n_dirs, 9), sharex=False)
    if n_dirs == 1:
        axes = axes.reshape(2, 1)

    for col, direction in enumerate(dirs):
        sub = df[df["direction_id"] == direction].reset_index(drop=True)
        x = sub["arrive_seq"].to_numpy()
        slip_min = sub["mean_slip_sec"].to_numpy() / 60
        cum_min = sub["cum_slip_sec"].to_numpy() / 60
        tp_mask = sub["is_tp"].astype(bool).to_numpy()

        ax_top = axes[0, col]
        colors = ["#c0392b" if s > 0 else "#27ae60" for s in slip_min]
        ax_top.bar(x, slip_min, color=colors, width=0.85, alpha=0.85)
        ax_top.axhline(0, color="black", linewidth=0.6)
        ax_top.set_ylabel("per-segment slip (min)")
        ax_top.set_title(f"D80 dir {direction} — per-segment slip ({period})")
        for xi in x[tp_mask]:
            ax_top.axvline(xi, color="#3498db", linestyle="--", alpha=0.35, linewidth=1)

        ax_bot = axes[1, col]
        ax_bot.plot(x, cum_min, color="#2c3e50", linewidth=2, marker="o", markersize=3)
        ax_bot.fill_between(x, 0, cum_min, color="#2c3e50", alpha=0.08)
        cum_max = max(cum_min.max(), 1)
        cum_min_val = min(cum_min.min(), 0)
        ax_bot.set_ylim(min(cum_min_val - 1, -1), cum_max + 1.5)
        ax_bot.scatter(
            x[tp_mask],
            cum_min[tp_mask],
            s=110,
            facecolor="#3498db",
            edgecolor="white",
            zorder=5,
            label="timepoint",
        )
        ax_bot.axhline(0, color="black", linewidth=0.6)
        ax_bot.set_xlabel("stop sequence (origin → terminus)")
        ax_bot.set_ylabel("cumulative slip (min)")
        ax_bot.set_title(f"D80 dir {direction} — cumulative slip from origin")
        ax_bot.legend(loc="upper left", fontsize=9)

        for xi, name in zip(x[tp_mask], sub.loc[tp_mask, "stop_name"]):
            short = name.replace("Wisconsin Av NW+", "Wisc+").replace("Av NW", "Av")
            short = short.replace("Pennsylvania", "Penn").replace("Massachusetts", "Mass")
            ax_bot.annotate(
                short,
                xy=(xi, cum_min[list(x).index(xi)]),
                xytext=(4, 6),
                textcoords="offset points",
                fontsize=7.5,
                color="#2c3e50",
                rotation=0,
            )

    fig.suptitle(
        f"{route_id} schedule slip trajectory ({period}) — origin-departure segment excluded\n"
        "bars: per-segment slip (red = late, green = recovery); line: cumulative deviation from origin departure",
        fontsize=11,
        y=0.995,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"saved: {out_path}")


def main():
    """CLI entrypoint."""
    p = argparse.ArgumentParser()
    p.add_argument("route_id")
    p.add_argument("--period", choices=list(PERIOD_HOURS.keys()), default="all")
    p.add_argument("--output-dir", default="visualizations/output")
    args = p.parse_args()

    db = get_session()
    try:
        df = fetch_slip(db, args.route_id, args.period)
        if df.empty:
            print(f"no data for route {args.route_id}", file=sys.stderr)
            sys.exit(1)
        tp_ids = fetch_timepoints_by_latlon(db, args.route_id)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = Path(args.output_dir) / f"{args.route_id}_slip_{args.period}_{ts}.png"
        out.parent.mkdir(parents=True, exist_ok=True)
        plot(df, tp_ids, args.route_id, args.period, out)
    finally:
        db.close()


if __name__ == "__main__":
    main()
