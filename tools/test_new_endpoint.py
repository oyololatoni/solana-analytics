
import os
import requests
import json
import time

API_KEY = os.environ.get("BIRDEYE_API_KEY")
if not API_KEY:
    # Try local env
    try:
        with open(".env.local") as f:
            for line in f:
                if "BIRDEYE_API_KEY=" in line:
                    API_KEY = line.split("=")[1].strip().strip('"')
                    break
    except: pass

print(f"Using Key: {API_KEY[:5]}...")

# 1. Try public-api.birdeye.so/defi/new_listing
url = "https://public-api.birdeye.so/defi/new_listing"
headers = {"X-API-KEY": API_KEY, "accept": "application/json"}
params = {"limit": 10}

print(f"\nTesting {url}...")
try:
    r = requests.get(url, headers=headers, params=params, timeout=10)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        items = data.get("data", {}).get("items", [])
        print(f"Found {len(items)} items.")
        if items:
            print("Sample Item Keys:", items[0].keys())
            print("Sample Item:", json.dumps(items[0], indent=2))
    else:
        print(r.text)
except Exception as e:
    print(f"Error: {e}")

# 2. Try v2/tokens/new_listing
url2 = "https://public-api.birdeye.so/defi/v2/tokens/new_listing"
print(f"\nTesting {url2}...")
try:
    r = requests.get(url2, headers=headers, params=params, timeout=10)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        items = data.get("data", {}).get("items", [])
        print(f"Found {len(items)} items.")
        if items:
             print("Sample Item:", json.dumps(items[0], indent=2))
    else:
        print(r.text)
except Exception as e:
    print(f"Error: {e}")
