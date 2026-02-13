import os
import requests
import time

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
BASE_URL = "https://api.helius.xyz/v0/addresses"
MINT = "So11111111111111111111111111111111111111112" # Wrapped SOL

def test_limit(limit):
    url = f"{BASE_URL}/{MINT}/transactions"
    params = {
        "api-key": HELIUS_API_KEY,
        "limit": limit
    }
    print(f"Testing limit={limit}...")
    r = requests.get(url, params=params)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"Returned: {len(data)}")
    else:
        print(f"Error: {r.text}")

if __name__ == "__main__":
    test_limit(500)
    time.sleep(1)
    test_limit(1000)
