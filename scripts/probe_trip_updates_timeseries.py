"""Capture multiple TripUpdates snapshots and analyze how predictions evolve.

The core question: when a bus passes a stop, does the corresponding StopTimeUpdate
disappear from subsequent feed snapshots? If yes, the last predicted arrival time
before the stop drops off ≈ WMATA's best guess at the actual arrival.

This script polls the feed N times at INTERVAL seconds apart, then analyzes per
(trip_id, stop_id):
  - how the predicted arrival_time changed across snapshots (convergence)
  - whether the stop dropped off (i.e., the bus passed it)
  - how the last-seen prediction compares to scheduled time

Read-only. No DB writes.
"""

import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

load_dotenv()

API_KEY = os.environ["WMATA_API_KEY"]
URL = "https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb"

N_SNAPSHOTS = 4
INTERVAL_SEC = 45


@dataclass
class StuObservation:
    """A single (trip_id, stop_id) prediction observed in one snapshot."""

    snapshot_idx: int
    feed_ts: int
    predicted_arrival_ts: int | None
    schedule_relationship: str
    vehicle_id: str


def fetch() -> gtfs_realtime_pb2.FeedMessage:
    """Fetch and parse one TripUpdates protobuf snapshot."""
    resp = requests.get(URL, headers={"api_key": API_KEY}, timeout=20)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def collect_snapshots(n: int, interval: int) -> list[gtfs_realtime_pb2.FeedMessage]:
    """Pull n snapshots interval seconds apart."""
    out = []
    for i in range(n):
        if i > 0:
            time.sleep(interval)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] snapshot {i + 1}/{n}...", flush=True)
        out.append(fetch())
    return out


def index_observations(
    snapshots: list[gtfs_realtime_pb2.FeedMessage],
) -> dict[tuple[str, str], list[StuObservation]]:
    """Build (trip_id, stop_id) → list of observations across snapshots."""
    by_key: dict[tuple[str, str], list[StuObservation]] = defaultdict(list)
    for idx, feed in enumerate(snapshots):
        feed_ts = feed.header.timestamp
        for ent in feed.entity:
            if not ent.HasField("trip_update"):
                continue
            tu = ent.trip_update
            trip_id = tu.trip.trip_id
            vehicle_id = tu.vehicle.id if tu.HasField("vehicle") else ""
            for stu in tu.stop_time_update:
                if not stu.stop_id:
                    continue
                pred_ts = None
                if stu.HasField("arrival") and stu.arrival.HasField("time"):
                    pred_ts = stu.arrival.time
                sr = (
                    stu.ScheduleRelationship.Name(stu.schedule_relationship)
                    if stu.HasField("schedule_relationship")
                    else "UNSET"
                )
                by_key[(trip_id, stu.stop_id)].append(
                    StuObservation(
                        snapshot_idx=idx,
                        feed_ts=feed_ts,
                        predicted_arrival_ts=pred_ts,
                        schedule_relationship=sr,
                        vehicle_id=vehicle_id,
                    )
                )
    return by_key


def analyze(by_key: dict[tuple[str, str], list[StuObservation]], n_snapshots: int) -> None:
    """Print summary stats and a few illustrative examples."""
    n_keys = len(by_key)
    snapshots_seen_in = [len(obs) for obs in by_key.values()]
    distribution = {i: snapshots_seen_in.count(i) for i in range(1, n_snapshots + 1)}

    print()
    print("=" * 72)
    print(f"OBSERVATIONS ACROSS {n_snapshots} SNAPSHOTS")
    print(f"  unique (trip_id, stop_id) pairs:  {n_keys}")
    print("  snapshots-seen-in distribution:")
    for k in sorted(distribution.keys()):
        v = distribution[k]
        pct = v / n_keys
        print(f"    seen in {k} snapshot(s): {v}  ({pct:.1%})")

    dropped_off = sum(
        1 for obs in by_key.values() if len(obs) < n_snapshots and obs[0].snapshot_idx == 0
    )
    print(f"  pairs that dropped off (in early snapshot, gone later): {dropped_off}")

    converged = []
    for obs in by_key.values():
        preds = [o.predicted_arrival_ts for o in obs if o.predicted_arrival_ts is not None]
        if len(preds) >= 2:
            converged.append(max(preds) - min(preds))
    if converged:
        s = sorted(converged)
        n = len(s)
        print()
        print("PREDICTION VOLATILITY (max - min predicted_arrival_time, seconds)")
        print(f"  n pairs with >=2 predictions: {n}")
        print(f"  median: {s[n // 2]}s")
        print(f"  p95:    {s[int(n * 0.95)]}s")
        print(f"  max:    {s[-1]}s")

    print()
    print("=" * 72)
    print("DROPOFF EXAMPLES (pairs seen in snapshot 0 but missing later)")
    examples_shown = 0
    for (trip_id, stop_id), obs in by_key.items():
        if examples_shown >= 6:
            break
        if obs[0].snapshot_idx != 0:
            continue
        last_seen_idx = obs[-1].snapshot_idx
        if last_seen_idx == n_snapshots - 1:
            continue
        examples_shown += 1
        last = obs[-1]
        last_pred = (
            datetime.fromtimestamp(last.predicted_arrival_ts, tz=UTC).strftime("%H:%M:%S")
            if last.predicted_arrival_ts
            else "?"
        )
        first_pred = (
            datetime.fromtimestamp(obs[0].predicted_arrival_ts, tz=UTC).strftime("%H:%M:%S")
            if obs[0].predicted_arrival_ts
            else "?"
        )
        last_feed_ts = datetime.fromtimestamp(last.feed_ts, tz=UTC).strftime("%H:%M:%S")
        print(
            f"  trip={trip_id} stop={stop_id} vehicle={last.vehicle_id} "
            f"first_pred={first_pred} last_pred={last_pred} last_feed={last_feed_ts} "
            f"snapshots={[o.snapshot_idx for o in obs]} "
            f"last_pred_minus_last_feed={last.predicted_arrival_ts - last.feed_ts if last.predicted_arrival_ts else '?'}s"
        )

    print()
    print("CONVERGENCE EXAMPLES (predictions that updated across snapshots)")
    examples_shown = 0
    for (trip_id, stop_id), obs in by_key.items():
        if examples_shown >= 4:
            break
        preds = [o.predicted_arrival_ts for o in obs if o.predicted_arrival_ts is not None]
        if len(preds) < n_snapshots:
            continue
        if max(preds) == min(preds):
            continue
        examples_shown += 1
        print(f"  trip={trip_id} stop={stop_id} vehicle={obs[0].vehicle_id}")
        for o in obs:
            ts = (
                datetime.fromtimestamp(o.predicted_arrival_ts, tz=UTC).strftime("%H:%M:%S")
                if o.predicted_arrival_ts
                else "?"
            )
            feed_ts = datetime.fromtimestamp(o.feed_ts, tz=UTC).strftime("%H:%M:%S")
            print(f"    snapshot {o.snapshot_idx}  feed={feed_ts}  pred_arrival={ts}")


def main() -> int:
    """Collect snapshots, index by (trip, stop), and analyze."""
    print(f"Capturing {N_SNAPSHOTS} snapshots {INTERVAL_SEC}s apart")
    snapshots = collect_snapshots(N_SNAPSHOTS, INTERVAL_SEC)
    by_key = index_observations(snapshots)
    analyze(by_key, N_SNAPSHOTS)
    return 0


if __name__ == "__main__":
    sys.exit(main())
