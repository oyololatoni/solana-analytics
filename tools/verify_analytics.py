import requests
import json
import time
import os
import random

# Use .env.local via python-dotenv or assume environment is set
BASE_URL = "http://localhost:8000"
TOKEN_MINT = "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump"
HEADERS = {"Authorization": "x-helius-signature:helius_wh_9b4c8e2f1a7d4c6e91f3a0b2c5d8e7f4"} # From .env.local

def create_payload(direction="in", amount=100.0):
    tx_sig = f"TEST_ANALYTICS_{direction}_{int(time.time())}_{random.randint(1000,9999)}"
    
    # Base structure
    tx = {
        "signature": tx_sig,
        "slot": 123456789,
        "timestamp": int(time.time()),
        "events": {
            "swap": {
                "nativeInput": None,
                "nativeOutput": None,
                "tokenInputs": [],
                "tokenOutputs": [],
                "tokenFees": [],
                "nativeFees": [],
                "innerSwaps": []
            }
        }
    }
    
    amount_raw = int(amount * 1e6)
    
    leg = {
        "userAccount": "TEST_WALLET_ABC",
        "mint": TOKEN_MINT,
        "rawTokenAmount": {"tokenAmount": str(amount), "decimals": 6}
    }
    
    other_leg = {
        "userAccount": "TEST_WALLET_ABC",
        "mint": "So11111111111111111111111111111111111111112", # SOL
        "rawTokenAmount": {"tokenAmount": "1.0", "decimals": 9}
    }

    if direction == "in":
        # User RECV tracked token (Output)
        tx["events"]["swap"]["tokenOutputs"] = [leg]
        tx["events"]["swap"]["tokenInputs"] = [other_leg]
    else:
        # User SENT tracked token (Input)
        tx["events"]["swap"]["tokenInputs"] = [leg]
        tx["events"]["swap"]["tokenOutputs"] = [other_leg]
        
    return [tx]

def run_test():
    print("1. Injecting BUY (100.0)...")
    load_buy = create_payload("in", 100.0)
    r = requests.post(f"{BASE_URL}/webhooks/helius", json=load_buy, headers=HEADERS)
    r.raise_for_status()
    print("   Sent Buy.")

    print("2. Injecting SELL (50.5)...")
    load_sell = create_payload("out", 50.5)
    r = requests.post(f"{BASE_URL}/webhooks/helius", json=load_sell, headers=HEADERS)
    r.raise_for_status()
    print("   Sent Sell.")

    print("3. Waiting for Worker (5s)...")
    time.sleep(5)

    print("4. Querying Analytics API...")
    r = requests.get(f"{BASE_URL}/api/analytics/token/{TOKEN_MINT}?window=1h")
    # Wait, api/main.py includes router prefix "/analytics"?
    # router = APIRouter(prefix="/analytics")
    # app.include_router(analytics_router)
    # So URL is /analytics/token/...
    # But verifying script uses /api/analytics? No, root router.
    
    # Try fetching directly
    url = f"{BASE_URL}/analytics/token/{TOKEN_MINT}?window=1h"
    r = requests.get(url)
    if r.status_code == 404:
        # Maybe prefix issue?
        print(f"   404 at {url}. Trying /api/analytics...")
        url = f"{BASE_URL}/api/analytics/token/{TOKEN_MINT}?window=1h"
        r = requests.get(url)
    
    r.raise_for_status()
    data = r.json()
    
    print("   Response:", json.dumps(data, indent=2))
    
    # Assertions (Soft, as DB acts as accumulator)
    vol_buy = data["volume_buy"]
    vol_sell = data["volume_sell"]
    
    if vol_buy >= 100.0:
        print(f"   [PASS] Buy Volume {vol_buy} >= 100.0")
    else:
        print(f"   [FAIL] Buy Volume {vol_buy} < 100.0")
        
    if vol_sell >= 50.5:
        print(f"   [PASS] Sell Volume {vol_sell} >= 50.5")
    else:
        print(f"   [FAIL] Sell Volume {vol_sell} < 50.5")

    print("5. Health Check...")
    r = requests.get(f"{BASE_URL}/analytics/health")
    r.raise_for_status()
    print("   Health:", r.json())

if __name__ == "__main__":
    run_test()
