"""
Test WMATA BusPositions JSON API endpoint

This endpoint provides real-time bus positions and may include additional
fields beyond what's available in GTFS-RT VehiclePositions feed.
"""
import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("WMATA_API_KEY")

print(f"API Key loaded: {API_KEY[:8]}... (length: {len(API_KEY)})")

url = "https://api.wmata.com/Bus.svc/json/jBusPositions"
headers = {"api_key": API_KEY}
params = {"RouteID": "C51"}

print(f"\nTesting: {url}")
print(f"Route: {params['RouteID']}")
print("=" * 70)

response = requests.get(url, headers=headers, params=params)

print(f"\nStatus Code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print(f"\nResponse keys: {list(data.keys())}")

    if 'BusPositions' in data:
        positions = data['BusPositions']
        print(f"Number of buses: {len(positions)}")

        if positions:
            print("\n" + "=" * 70)
            print("First bus position (all fields):")
            print("=" * 70)
            print(json.dumps(positions[0], indent=2, default=str))

            print("\n" + "=" * 70)
            print("Fields available in BusPositions API:")
            print("=" * 70)
            for key in positions[0].keys():
                print(f"  {key}")
        else:
            print("\nNo buses found on this route")
    else:
        print("\nUnexpected response format")
        print(json.dumps(data, indent=2))
else:
    print(f"Error: {response.text}")
