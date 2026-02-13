import requests
import json
import os

BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
HEADERS = {
    "X-API-KEY": BIRDEYE_API_KEY,
    "accept": "application/json"
}

URL = "https://public-api.birdeye.so/defi/new_currency"

def test_new():
    try:
        r = requests.get(URL, headers=HEADERS, params={"limit": 10})
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            if data.get("success"):
                items = data.get("data", {}).get("items", [])
                print(f"Items: {len(items)}")
                if items:
                    print(f"Keys: {list(items[0].keys())}")
                    print(f"Sample: {json.dumps(items[0], indent=2)}")
            else:
                print("Failed success check")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    if BIRDEYE_API_KEY:
        test_new()
    else:
        print("No KEY")
