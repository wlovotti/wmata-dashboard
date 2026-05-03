"""
Exploratory: build a per-run quality dataframe and report distributions.

Throwaway. Goal is to inform the schema for a future run-level
materialized table by seeing which columns actually have signal on the
existing 21k runs (Oct 12 - Oct 21, 2025).

A "run" is keyed by (trip_id, vehicle_id, service_date), where service_date
is DATE(min(timestamp)) for the run. trip_start_date in the raw data is
NULL (WMATA wasn't emitting it during this window), so we synthesize.

Output:
- prints distributions to stdout
- writes analysis/run_quality.csv for hand inspection
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.database import get_session

# Pings inside a run shouldn't normally have gaps over 5 min; flag if exceeded.
RUN_GAP_FLAG_SEC = 300

# A stop is considered "observed" the first time the bus's reported stop_id
# equals that stop, regardless of current_status. STOPPED_AT (=1) is preferred
# but IN_TRANSIT_TO (=2) still localizes the bus to that stop within ~30s.
STATUS_STOPPED_AT = 1
STATUS_IN_TRANSIT_TO = 2


def parse_gtfs_time(time_str: str, service_date: date) -> datetime | None:
    """
    Parse a GTFS time string (HH:MM:SS, possibly hours >= 24) anchored to a service_date.

    Returns None for unparseable inputs.
    """
    try:
        h, m, s = (int(x) for x in time_str.split(":"))
    except (ValueError, AttributeError):
        return None
    extra_days, h = divmod(h, 24)
    return datetime.combine(service_date, datetime.min.time()).replace(
        hour=h, minute=m, second=s
    ) + timedelta(days=extra_days)


def load_positions(db) -> pd.DataFrame:
    """Pull all vehicle positions for runs that have a trip_id."""
    print("Loading vehicle_positions...", flush=True)
    df = pd.read_sql(
        text(
            """
            SELECT vehicle_id, trip_id, route_id, timestamp,
                   stop_id, current_stop_sequence, current_status,
                   latitude, longitude, speed
            FROM vehicle_positions
            WHERE trip_id IS NOT NULL AND trip_id != ''
            ORDER BY trip_id, vehicle_id, timestamp
            """
        ),
        db.bind,
    )
    df["service_date"] = df["timestamp"].dt.date
    df["run_id"] = (
        df["trip_id"].astype(str)
        + "|"
        + df["vehicle_id"].astype(str)
        + "|"
        + df["service_date"].astype(str)
    )
    print(f"  {len(df):,} positions across {df['run_id'].nunique():,} runs", flush=True)
    return df


def load_schedules(db, trip_ids: set[str]) -> pd.DataFrame:
    """
    Pull stop_times rows for the trip_ids we observed.

    Returns columns: trip_id, stop_id, stop_sequence, arrival_time (text).
    """
    print(f"Loading stop_times for {len(trip_ids):,} observed trips...", flush=True)
    df = pd.read_sql(
        text(
            """
            SELECT trip_id, stop_id, stop_sequence, arrival_time, departure_time
            FROM stop_times
            WHERE is_current = true AND trip_id = ANY(:tids)
            """
        ),
        db.bind,
        params={"tids": list(trip_ids)},
    )
    print(f"  {len(df):,} scheduled stop_times rows", flush=True)
    return df


def compute_schedule_anchor(
    positions: pd.DataFrame, schedules: pd.DataFrame
) -> pd.Series:
    """
    Per run, pick the GTFS service date that aligns the trip's first scheduled
    stop to the first observed ping.

    GTFS encodes "service day continues past midnight" by letting arrival_time
    exceed 24:00 (e.g. "24:01:14" = 01:14 AM the day after the service date).
    A run keyed on DATE(timestamp) of post-midnight pings would anchor those
    schedules to the wrong day and produce ~-24h deviations. We try both
    DATE(min_ts) and DATE(min_ts) - 1d and keep whichever places the first
    scheduled stop closest to the first observed ping.
    """
    first_ts = positions.groupby("run_id").agg(
        trip_id=("trip_id", "first"),
        first_ts=("timestamp", "min"),
    )
    first_sched = (
        schedules.sort_values(["trip_id", "stop_sequence"])
        .groupby("trip_id")["arrival_time"]
        .first()
        .rename("first_arrival_str")
    )
    df = first_ts.join(first_sched, on="trip_id")

    def pick(row):
        if not isinstance(row["first_arrival_str"], str):
            return row["first_ts"].date()
        cand1 = row["first_ts"].date()
        cand2 = cand1 - timedelta(days=1)
        s1 = parse_gtfs_time(row["first_arrival_str"], cand1)
        s2 = parse_gtfs_time(row["first_arrival_str"], cand2)
        if s1 is None or s2 is None:
            return cand1
        d1 = abs((row["first_ts"] - s1).total_seconds())
        d2 = abs((row["first_ts"] - s2).total_seconds())
        return cand2 if d2 < d1 else cand1

    return df.apply(pick, axis=1).rename("anchor_date")


def build_observed_arrivals(positions: pd.DataFrame) -> pd.DataFrame:
    """
    First ping at each stop within a run = observed arrival at that stop.

    Filters to pings where stop_id is set; preference order:
    STOPPED_AT > IN_TRANSIT_TO > anything else. Within a run+stop we keep
    the earliest qualifying ping.
    """
    df = positions.dropna(subset=["stop_id"]).copy()
    df = df[df["stop_id"] != ""]
    rank_map = {STATUS_STOPPED_AT: 0, STATUS_IN_TRANSIT_TO: 1}
    df["status_rank"] = df["current_status"].map(rank_map).fillna(2).astype(int)
    df = df.sort_values(["run_id", "stop_id", "status_rank", "timestamp"])
    arrivals = df.drop_duplicates(subset=["run_id", "stop_id"], keep="first")
    return arrivals[
        [
            "run_id",
            "trip_id",
            "vehicle_id",
            "service_date",
            "route_id",
            "stop_id",
            "current_stop_sequence",
            "current_status",
            "timestamp",
        ]
    ].rename(columns={"timestamp": "actual_arrival"})


def build_run_quality(
    positions: pd.DataFrame,
    arrivals: pd.DataFrame,
    schedules: pd.DataFrame,
    anchor: pd.Series,
) -> pd.DataFrame:
    """
    Build one row per run.

    `anchor` is run_id -> service_date as picked by compute_schedule_anchor;
    we use it instead of DATE(timestamp) when parsing scheduled times so
    runs that cross midnight don't produce ~-24h deviations.
    """
    print("Computing per-run aggregates...", flush=True)

    # Schedule counts per trip.
    scheduled_counts = (
        schedules.groupby("trip_id").size().rename("stops_scheduled")
    )

    # Build a run -> [stops_observed, deviation distribution] table by joining
    # observed arrivals with their scheduled time.
    sched = schedules[["trip_id", "stop_id", "stop_sequence", "arrival_time"]]
    obs = arrivals.merge(sched, on=["trip_id", "stop_id"], how="left")

    # Use the GTFS-aware anchor date (not DATE(timestamp)) to parse schedule.
    obs = obs.join(anchor, on="run_id")
    obs["scheduled_arrival"] = [
        parse_gtfs_time(t, d) if isinstance(t, str) else None
        for t, d in zip(obs["arrival_time"], obs["anchor_date"])
    ]
    obs["deviation_sec"] = (
        obs["actual_arrival"] - obs["scheduled_arrival"]
    ).dt.total_seconds()

    # Position-level intra-run stats: max gap, ping count, span.
    pos = positions.sort_values(["run_id", "timestamp"]).copy()
    pos["prev_ts"] = pos.groupby("run_id")["timestamp"].shift()
    pos["gap_sec"] = (pos["timestamp"] - pos["prev_ts"]).dt.total_seconds()
    pos_agg = pos.groupby("run_id").agg(
        n_pings=("timestamp", "count"),
        run_start=("timestamp", "min"),
        run_end=("timestamp", "max"),
        max_gap_sec=("gap_sec", "max"),
        avg_speed_mps=("speed", "mean"),
    )
    pos_agg["actual_duration_sec"] = (
        pos_agg["run_end"] - pos_agg["run_start"]
    ).dt.total_seconds()

    # Per-run identifiers. service_date is the anchor (GTFS service date),
    # not DATE(min(timestamp)), so post-midnight runs report under the
    # service day they actually belong to.
    ids = (
        positions.groupby("run_id")
        .agg(
            trip_id=("trip_id", "first"),
            vehicle_id=("vehicle_id", "first"),
            route_id=("route_id", "first"),
        )
        .join(anchor.rename("service_date"))
    )

    # Observed-stop aggregates per run: count, deviation distribution, etc.
    obs_with_dev = obs.dropna(subset=["deviation_sec"])
    obs_agg = obs_with_dev.groupby("run_id").agg(
        stops_observed=("stop_id", "nunique"),
        first_obs_seq=("stop_sequence", "min"),
        last_obs_seq=("stop_sequence", "max"),
        first_obs_dev_sec=("deviation_sec", "first"),
        last_obs_dev_sec=("deviation_sec", "last"),
        dev_p50_sec=("deviation_sec", lambda s: float(np.percentile(s, 50))),
        dev_p95_sec=("deviation_sec", lambda s: float(np.percentile(s, 95))),
        early_stops=("deviation_sec", lambda s: int((s < -60).sum())),
        on_time_stops=(
            "deviation_sec",
            lambda s: int(((s >= -60) & (s <= 300)).sum()),
        ),
        late_stops=("deviation_sec", lambda s: int((s > 300).sum())),
    )

    # Scheduled duration per trip from stop_times.
    sched_bounds = schedules.groupby("trip_id")["arrival_time"].agg(["min", "max"])
    sched_bounds.columns = ["sched_first_str", "sched_last_str"]

    runs = (
        ids.join(pos_agg, how="left")
        .join(obs_agg, how="left")
        .join(scheduled_counts, on="trip_id")
        .join(sched_bounds, on="trip_id")
    )
    runs["coverage_pct"] = (
        runs["stops_observed"] / runs["stops_scheduled"]
    ).clip(upper=1.0) * 100

    # Scheduled duration in seconds (handle 24+ hours).
    def _sched_duration(row):
        a = parse_gtfs_time(row["sched_first_str"], row["service_date"])
        b = parse_gtfs_time(row["sched_last_str"], row["service_date"])
        if a is None or b is None:
            return None
        return (b - a).total_seconds()

    runs["scheduled_duration_sec"] = runs.apply(_sched_duration, axis=1)

    # Completeness flag: enough stops observed, both endpoints covered, no big gap.
    runs["is_complete"] = (
        (runs["coverage_pct"] >= 70)
        & (runs["first_obs_seq"] <= 3)
        & (runs["last_obs_seq"] >= (runs["stops_scheduled"] - 3))
        & (runs["max_gap_sec"].fillna(0) < RUN_GAP_FLAG_SEC)
    )

    return runs.reset_index()


def report(runs: pd.DataFrame) -> None:
    """Print distributions to stdout."""
    n = len(runs)
    print(f"\n=== run_quality: {n:,} runs ===")
    print(
        f"  service dates: {runs['service_date'].min()} → {runs['service_date'].max()}"
    )
    print(f"  routes: {runs['route_id'].nunique()}")
    print(f"  is_complete: {runs['is_complete'].sum():,} ({runs['is_complete'].mean() * 100:.1f}%)")
    print()

    def pct(s, p):
        return float(np.nanpercentile(s.dropna(), p))

    def dist(label, s, fmt="{:.1f}"):
        s = s.dropna()
        if s.empty:
            print(f"  {label:<28} (no data)")
            return
        print(
            f"  {label:<28} "
            f"p05={fmt.format(pct(s, 5))}  p50={fmt.format(pct(s, 50))}  "
            f"p95={fmt.format(pct(s, 95))}  mean={fmt.format(s.mean())}"
        )

    print("All runs:")
    dist("n_pings", runs["n_pings"], "{:.0f}")
    dist("actual_duration (min)", runs["actual_duration_sec"] / 60)
    dist("scheduled_duration (min)", runs["scheduled_duration_sec"] / 60)
    dist("max_gap_sec", runs["max_gap_sec"], "{:.0f}")
    dist("stops_observed", runs["stops_observed"], "{:.0f}")
    dist("stops_scheduled", runs["stops_scheduled"], "{:.0f}")
    dist("coverage_pct", runs["coverage_pct"])
    dist("dev_p50_sec", runs["dev_p50_sec"], "{:.0f}")
    dist("dev_p95_sec", runs["dev_p95_sec"], "{:.0f}")
    dist("first_obs_dev_sec", runs["first_obs_dev_sec"], "{:.0f}")
    dist("last_obs_dev_sec", runs["last_obs_dev_sec"], "{:.0f}")
    dist("avg_speed_mph", runs["avg_speed_mps"] * 2.23694)
    print()

    complete = runs[runs["is_complete"]]
    print(f"Complete runs only ({len(complete):,}):")
    dist("dev_p50_sec", complete["dev_p50_sec"], "{:.0f}")
    dist("dev_p95_sec", complete["dev_p95_sec"], "{:.0f}")
    dist("actual_duration (min)", complete["actual_duration_sec"] / 60)
    dist("scheduled_duration (min)", complete["scheduled_duration_sec"] / 60)
    dist("coverage_pct", complete["coverage_pct"])
    print()

    # OTP rolled up across complete runs (stop-level).
    if not complete.empty:
        early = complete["early_stops"].sum()
        ot = complete["on_time_stops"].sum()
        late = complete["late_stops"].sum()
        total = early + ot + late
        if total:
            print("Stop-level OTP across complete runs (window: -60..+300s):")
            print(f"  early {early / total * 100:5.1f}%  "
                  f"on-time {ot / total * 100:5.1f}%  "
                  f"late {late / total * 100:5.1f}%  "
                  f"(N={total:,} stop arrivals)")
            print()

    print("Top 5 routes by complete-run count:")
    top = (
        complete.groupby("route_id")
        .size()
        .sort_values(ascending=False)
        .head(5)
    )
    for r, c in top.items():
        sub = complete[complete["route_id"] == r]
        print(
            f"  {r:<8} {c:>4} runs   "
            f"dev_p50={sub['dev_p50_sec'].median():+6.0f}s  "
            f"dev_p95={sub['dev_p95_sec'].median():+7.0f}s  "
            f"coverage={sub['coverage_pct'].median():.0f}%"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        default="analysis/run_quality.csv",
        help="Output CSV path (default: analysis/run_quality.csv)",
    )
    args = parser.parse_args()

    db = get_session()
    try:
        positions = load_positions(db)
        observed_trips = set(positions["trip_id"].dropna().unique())
        schedules = load_schedules(db, observed_trips)
        arrivals = build_observed_arrivals(positions)
        anchor = compute_schedule_anchor(positions, schedules)
        runs = build_run_quality(positions, arrivals, schedules, anchor)
    finally:
        db.close()

    report(runs)
    runs.to_csv(args.csv, index=False)
    print(f"\nWrote {len(runs):,} rows -> {args.csv}")


if __name__ == "__main__":
    main()
