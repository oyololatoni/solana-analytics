import requests
import json
import os

BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
HEADERS = {"X-API-KEY": BIRDEYE_API_KEY, "accept": "application/json"}

# Variations
URLS = [
    "https://public-api.birdeye.so/defi/new_currency",
    "https://public-api.birdeye.so/defi/v2/tokens/new_list",
    "https://public-api.birdeye.so/public/new_currency",
    "https://public-api.birdeye.so/defi/tokenlist?sort_by=rank&sort_type=asc"
]

def test_urls():
    for url in URLS:
        print(f"\nTesting {url}")
        try:
            r = requests.get(url, headers=HEADERS, params={"limit": 5})
            print(f"Status: {r.status_code}")
            if r.status_code == 200:
                print(f"Success! Body Sample: {r.text[:300]}")
        except Exception as e:
            print(e)

if __name__ == "__main__":
    if BIRDEYE_API_KEY:
        test_urls()
