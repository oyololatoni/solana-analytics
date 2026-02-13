
import os
import requests
import json

API_KEY = os.environ.get("BIRDEYE_API_KEY")
if not API_KEY:
    try:
        with open(".env.local") as f:
            for line in f:
                if "BIRDEYE_API_KEY=" in line:
                    API_KEY = line.split("=")[1].strip().strip('"')
                    break
    except: pass

url = "https://public-api.birdeye.so/defi/v2/tokens/new_listing"
headers = {"X-API-KEY": API_KEY, "accept": "application/json"}

scenarios = [
    {"name": "No Params", "params": {"limit": 10}},
    {"name": "With Offset 0", "params": {"limit": 10, "offset": 0}},
    {"name": "With Meme Platform True", "params": {"limit": 10, "meme_platform_enabled": "true"}},
    {"name": "With Meme Platform False", "params": {"limit": 10, "meme_platform_enabled": "false"}},
    {"name": "With Offset & Meme", "params": {"limit": 10, "offset": 0, "meme_platform_enabled": "true"}},
]

for s in scenarios:
    print(f"\nTesting: {s['name']}")
    try:
        r = requests.get(url, headers=headers, params=s['params'], timeout=10)
        if r.status_code == 200:
            items = r.json().get("data", {}).get("items", [])
            print(f"Success. Items: {len(items)}")
        else:
            print(f"Failed: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"Error: {e}")
