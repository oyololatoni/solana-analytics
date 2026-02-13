import requests
import json
import os

BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
HEADERS = {"X-API-KEY": BIRDEYE_API_KEY, "accept": "application/json"}

# Moo Deng
MINT = "ED5nyyWEzpPPiWimP8vYm7sD7TD3LAt3Q3gRTWHzPJBY"

def test_creation():
    url = f"https://public-api.birdeye.so/defi/token_creation_info"
    print(f"Testing {url} for {MINT}")
    try:
        r = requests.get(url, headers=HEADERS, params={"address": MINT})
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
             print(f"Body: {r.text}")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    if BIRDEYE_API_KEY:
        test_creation()
