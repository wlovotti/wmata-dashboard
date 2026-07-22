"""Day-1 SFMTA feed validation: do RT trip_ids match static GTFS trip_ids?

The trip-matching fast path (src/trip_matching.py, ~90% of WMATA matches)
assumes GTFS-RT tripDescriptor.trip_id values exist in the static GTFS
trips.txt. This spike measures that overlap for Muni BEFORE any pipeline
work builds on it (spec 2026-07-21 §1: go/no-go gate, threshold 80%).

Manual one-shot (uses 3 of the 60/hr request budget):

    uv run python scripts/sfmta_feed_spike.py

Exit 0: match rate >= 80%. Exit 1: below threshold or fetch failure —
scope fallback matching before proceeding.
"""

import csv
import io
import os
import sys
import zipfile

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

from src.agency_config import load_agency_config, request_kwargs

load_dotenv()

MATCH_THRESHOLD = 0.80
STATIC_GTFS_URL = "https://api.511.org/transit/datafeeds"


def trip_id_match_rate(rt_trip_ids: set, static_trip_ids: set) -> float:
    """Fraction of RT trip_ids present in the static GTFS trip_id set.

    Returns 0.0 when there are no RT trips (empty feed reads as failure,
    not vacuous success).
    """
    if not rt_trip_ids:
        return 0.0
    return len(rt_trip_ids & static_trip_ids) / len(rt_trip_ids)


def fetch_rt_trip_ids(cfg, api_key) -> set:
    """Fetch one TripUpdates snapshot and return its distinct trip_ids."""
    kwargs = request_kwargs(cfg, api_key)
    resp = requests.get(cfg.trip_updates_url, timeout=30, **kwargs)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return {
        e.trip_update.trip.trip_id
        for e in feed.entity
        if e.HasField("trip_update") and e.trip_update.trip.trip_id
    }


def fetch_static_trip_ids(cfg, api_key) -> set:
    """Download the SF static GTFS zip from 511 and return trips.txt trip_ids."""
    kwargs = request_kwargs(cfg, api_key)
    kwargs.setdefault("params", {})
    kwargs["params"]["operator_id"] = cfg.extra_params.get("agency", "SF")
    resp = requests.get(STATIC_GTFS_URL, timeout=120, **kwargs)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open("trips.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            return {row["trip_id"] for row in reader if row.get("trip_id")}


def main() -> int:
    """Run the spike; print a verdict; return a process exit code."""
    cfg = load_agency_config("sfmta")
    api_key = os.getenv(cfg.api_key_env)
    if not api_key:
        print(f"ERROR: {cfg.api_key_env} not set")
        return 1

    rt_ids = fetch_rt_trip_ids(cfg, api_key)
    static_ids = fetch_static_trip_ids(cfg, api_key)
    rate = trip_id_match_rate(rt_ids, static_ids)

    print(f"RT trip_ids:      {len(rt_ids)}")
    print(f"Static trip_ids:  {len(static_ids)}")
    print(f"Match rate:       {rate:.1%}  (threshold {MATCH_THRESHOLD:.0%})")
    unmatched = sorted(rt_ids - static_ids)[:10]
    if unmatched:
        print(f"Sample unmatched RT trip_ids: {unmatched}")

    if rate >= MATCH_THRESHOLD:
        print("VERDICT: PASS — trip-matching fast path viable for Muni")
        return 0
    print("VERDICT: FAIL — scope fallback matching before pipeline work")
    return 1


if __name__ == "__main__":
    sys.exit(main())
