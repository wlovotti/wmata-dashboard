"""
Check if WMATA GTFS includes shapes.txt
"""
import requests
import zipfile
import io
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("WMATA_API_KEY")
if not api_key:
    print("No API key found in .env")
    exit(1)

url = "https://api.wmata.com/gtfs/bus-gtfs-static.zip"
headers = {"api_key": api_key}

print("Downloading GTFS to check contents...")
response = requests.get(url, headers=headers, timeout=30)

if response.status_code == 200:
    zip_data = zipfile.ZipFile(io.BytesIO(response.content))
    files = zip_data.namelist()
    print(f"\nFiles in GTFS ZIP ({len(files)} files):")
    for f in sorted(files):
        info = zip_data.getinfo(f)
        print(f"  - {f:30s} ({info.file_size:,} bytes)")

    if 'shapes.txt' in files:
        print("\n" + "=" * 60)
        print("✓ shapes.txt IS available!")
        print("=" * 60)
        info = zip_data.getinfo('shapes.txt')
        print(f"Size: {info.file_size:,} bytes ({info.file_size / 1024 / 1024:.1f} MB)")

        # Sample first few lines
        content = zip_data.read('shapes.txt').decode('utf-8-sig')
        lines = content.split('\n')[:10]
        print(f"\nFirst few lines:")
        for line in lines:
            print(f"  {line}")
    else:
        print("\n" + "=" * 60)
        print("✗ shapes.txt is NOT available in WMATA GTFS")
        print("=" * 60)
else:
    print(f"Failed to download: HTTP {response.status_code}")
