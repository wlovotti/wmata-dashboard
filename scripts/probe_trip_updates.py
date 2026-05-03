"""Probe WMATA's GTFS-RT TripUpdates feed.

Pulls a single snapshot, prints feed-level stats and a few sample entities so we
can decide whether the data is rich enough to use as a foundational stop-event
source. Read-only and idempotent — safe to run repeatedly.
"""

import os
import sys
from collections import Counter
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

load_dotenv()

API_KEY = os.environ["WMATA_API_KEY"]
URL = "https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb"


def fetch() -> gtfs_realtime_pb2.FeedMessage:
    """Fetch and parse the TripUpdates protobuf from WMATA."""
    resp = requests.get(URL, headers={"api_key": API_KEY}, timeout=20)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def summarize(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    """Print a structured summary of the feed for human inspection."""
    header = feed.header
    feed_ts = (
        datetime.fromtimestamp(header.timestamp, tz=timezone.utc) if header.timestamp else None
    )
    print("=" * 72)
    print("FEED HEADER")
    print(f"  gtfs_realtime_version: {header.gtfs_realtime_version}")
    print(f"  incrementality:        {header.Incrementality.Name(header.incrementality)}")
    print(f"  timestamp:             {feed_ts}  ({header.timestamp})")
    print(f"  total entities:        {len(feed.entity)}")

    n_with_trip_update = 0
    n_with_vehicle = 0
    n_with_alert = 0
    schedule_rel = Counter()
    stop_time_updates_per_trip = []
    arrival_field_present = 0
    departure_field_present = 0
    arrival_time_set = 0
    arrival_delay_set = 0
    departure_time_set = 0
    schedule_relationship_stu = Counter()
    routes_seen = set()
    trips_with_vehicle_id = 0
    sample_entities = []

    for ent in feed.entity:
        if ent.HasField("trip_update"):
            n_with_trip_update += 1
            tu = ent.trip_update
            if tu.trip.route_id:
                routes_seen.add(tu.trip.route_id)
            if tu.trip.HasField("schedule_relationship"):
                schedule_rel[tu.trip.ScheduleRelationship.Name(tu.trip.schedule_relationship)] += 1
            else:
                schedule_rel["UNSET"] += 1
            if tu.HasField("vehicle") and tu.vehicle.id:
                trips_with_vehicle_id += 1
            stus = list(tu.stop_time_update)
            stop_time_updates_per_trip.append(len(stus))
            for stu in stus:
                if stu.HasField("arrival"):
                    arrival_field_present += 1
                    if stu.arrival.HasField("time"):
                        arrival_time_set += 1
                    if stu.arrival.HasField("delay"):
                        arrival_delay_set += 1
                if stu.HasField("departure"):
                    departure_field_present += 1
                    if stu.departure.HasField("time"):
                        departure_time_set += 1
                if stu.HasField("schedule_relationship"):
                    schedule_relationship_stu[
                        stu.ScheduleRelationship.Name(stu.schedule_relationship)
                    ] += 1
                else:
                    schedule_relationship_stu["UNSET"] += 1
            if len(sample_entities) < 3 and len(stus) >= 2:
                sample_entities.append(ent)
        if ent.HasField("vehicle"):
            n_with_vehicle += 1
        if ent.HasField("alert"):
            n_with_alert += 1

    print()
    print("ENTITY KIND BREAKDOWN")
    print(f"  trip_update: {n_with_trip_update}")
    print(f"  vehicle:     {n_with_vehicle}")
    print(f"  alert:       {n_with_alert}")
    print()
    print("TRIP-LEVEL FIELDS")
    print(f"  distinct routes seen:        {len(routes_seen)}")
    print(f"  trip_updates with vehicle.id: {trips_with_vehicle_id}/{n_with_trip_update}")
    print("  schedule_relationship:")
    for k, v in schedule_rel.most_common():
        print(f"    {k}: {v}")

    if stop_time_updates_per_trip:
        s = sorted(stop_time_updates_per_trip)
        n = len(s)
        print()
        print("STOP_TIME_UPDATE COUNTS PER TRIP")
        print(f"  min/median/p95/max:  {s[0]} / {s[n // 2]} / {s[int(n * 0.95)]} / {s[-1]}")
        print(f"  total STUs across feed: {sum(s)}")

    total_stus = sum(stop_time_updates_per_trip)
    if total_stus:
        print()
        print(f"STOP_TIME_UPDATE FIELD PRESENCE (out of {total_stus})")
        print(
            f"  arrival present:        {arrival_field_present}  ({arrival_field_present / total_stus:.1%})"
        )
        print(
            f"  arrival.time set:       {arrival_time_set}  ({arrival_time_set / total_stus:.1%})"
        )
        print(
            f"  arrival.delay set:      {arrival_delay_set}  ({arrival_delay_set / total_stus:.1%})"
        )
        print(
            f"  departure present:      {departure_field_present}  ({departure_field_present / total_stus:.1%})"
        )
        print(
            f"  departure.time set:     {departure_time_set}  ({departure_time_set / total_stus:.1%})"
        )
        print("  schedule_relationship:")
        for k, v in schedule_relationship_stu.most_common():
            print(f"    {k}: {v}  ({v / total_stus:.1%})")

    print()
    print("=" * 72)
    print("SAMPLE ENTITIES (first 3 with >=2 stop_time_updates)")
    for i, ent in enumerate(sample_entities, 1):
        print()
        print(f"--- sample {i}: entity.id = {ent.id!r} ---")
        tu = ent.trip_update
        print("  trip:")
        print(f"    trip_id:        {tu.trip.trip_id!r}")
        print(f"    route_id:       {tu.trip.route_id!r}")
        print(f"    start_date:     {tu.trip.start_date!r}")
        print(f"    start_time:     {tu.trip.start_time!r}")
        print(
            f"    direction_id:   {tu.trip.direction_id if tu.trip.HasField('direction_id') else '<unset>'}"
        )
        sr = (
            tu.trip.ScheduleRelationship.Name(tu.trip.schedule_relationship)
            if tu.trip.HasField("schedule_relationship")
            else "<unset>"
        )
        print(f"    schedule_rel:   {sr}")
        if tu.HasField("vehicle"):
            print(f"  vehicle: id={tu.vehicle.id!r}  label={tu.vehicle.label!r}")
        if tu.HasField("timestamp"):
            ts = datetime.fromtimestamp(tu.timestamp, tz=timezone.utc)
            print(f"  trip_update.timestamp: {ts}  ({tu.timestamp})")
        print(f"  stop_time_updates ({len(tu.stop_time_update)}):")
        for j, stu in enumerate(tu.stop_time_update[:6]):
            arr = ""
            if stu.HasField("arrival"):
                bits = []
                if stu.arrival.HasField("time"):
                    arr_ts = datetime.fromtimestamp(stu.arrival.time, tz=timezone.utc)
                    bits.append(f"time={arr_ts}")
                if stu.arrival.HasField("delay"):
                    bits.append(f"delay={stu.arrival.delay}s")
                if stu.arrival.HasField("uncertainty"):
                    bits.append(f"uncertainty={stu.arrival.uncertainty}")
                arr = "arrival(" + ", ".join(bits) + ")"
            dep = ""
            if stu.HasField("departure"):
                bits = []
                if stu.departure.HasField("time"):
                    dep_ts = datetime.fromtimestamp(stu.departure.time, tz=timezone.utc)
                    bits.append(f"time={dep_ts}")
                if stu.departure.HasField("delay"):
                    bits.append(f"delay={stu.departure.delay}s")
                dep = " departure(" + ", ".join(bits) + ")"
            stu_sr = (
                stu.ScheduleRelationship.Name(stu.schedule_relationship)
                if stu.HasField("schedule_relationship")
                else ""
            )
            stu_sr_str = f" sr={stu_sr}" if stu_sr else ""
            seq = stu.stop_sequence if stu.HasField("stop_sequence") else "?"
            sid = stu.stop_id or "?"
            print(f"    [{j}] seq={seq} stop_id={sid}{stu_sr_str} {arr}{dep}")
        if len(tu.stop_time_update) > 6:
            print(f"    ... ({len(tu.stop_time_update) - 6} more)")


def main() -> int:
    """Fetch the feed and print a summary."""
    print(f"Fetching: {URL}")
    print(f"Local time: {datetime.now()}")
    print()
    feed = fetch()
    summarize(feed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
