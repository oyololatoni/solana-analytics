
import os
import sys
import json
import logging
import requests
import time
import random
from datetime import datetime, timezone

# Add project root
sys.path.insert(0, os.getcwd())

# Load .env
if not os.environ.get("BIRDEYE_API_KEY"):
    env_path = ".env.local"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")

# Config
BIRDEYE_KEYS = []
if os.getenv("BIRDEYE_API_KEY"): BIRDEYE_KEYS.append(os.getenv("BIRDEYE_API_KEY"))
if os.getenv("BIRDEYE_API_KEY_2"): BIRDEYE_KEYS.append(os.getenv("BIRDEYE_API_KEY_2"))

BASE_URL = "https://public-api.birdeye.so/defi/tokenlist"

# Standard Limits
STD_MIN_LIQ = 50000
STD_MIN_VOL = 10000
STD_MAX_AGE = 14

def get_birdeye_header():
    if not BIRDEYE_KEYS: return {}
    return {"X-API-KEY": random.choice(BIRDEYE_KEYS), "accept": "application/json"}

def check_age(mint):
    url = f"https://public-api.birdeye.so/defi/token_creation_info?address={mint}"
    try:
        time.sleep(0.15)
        r = requests.get(url, headers=get_birdeye_header(), timeout=5)
        if r.status_code == 200:
            d = r.json()
            if d.get("success"):
                info = d.get("data")
                if info and info.get("blockUnixTime"):
                    ts = info.get("blockUnixTime")
                    created = datetime.fromtimestamp(ts, tz=timezone.utc)
                    age_days = (datetime.now(timezone.utc) - created).days
                    return age_days
    except:
        pass
    return 9999 # Unknown/Old

def run_test():
    print("Fetching batch of 50 tokens (Random Offset)...")
    offset = random.randint(0, 40) * 50
    params = {"sort_by": "v24hUSD", "sort_type": "desc", "offset": offset, "limit": 50}
    
    r = None
    for _ in range(3):
        try:
            r = requests.get(BASE_URL, headers=get_birdeye_header(), params=params, timeout=10)
            if r.status_code == 200: break
            elif r.status_code == 429:
                print("429 on list fetch... sleeping")
                time.sleep(2)
        except:
             pass
             
    if not r or r.status_code != 200:
        print(f"API Error: {r.status_code if r else 'None'}")
        return

    items = r.json().get("data", {}).get("tokens", [])
    print(f"Fetched {len(items)} tokens. Running Scenarios...\n")

    # Scenarios
    scenarios = {
        "BASELINE (All Filters)": {"liq": STD_MIN_LIQ, "vol": STD_MIN_VOL, "age": STD_MAX_AGE},
        "NO LIQUIDITY FILTER":    {"liq": 0,           "vol": STD_MIN_VOL, "age": STD_MAX_AGE},
        "NO VOLUME FILTER":       {"liq": STD_MIN_LIQ, "vol": 0,           "age": STD_MAX_AGE},
        "NO AGE FILTER":          {"liq": STD_MIN_LIQ, "vol": STD_MIN_VOL, "age": 99999},
    }

    # Pre-fetch ages for candidates that might pass *some* filter to save API calls
    # We only check age if it passes at least the most lenient size filters (liq>0, vol>0)
    print("Checking ages for tokens...")
    token_ages = {}
    for t in items:
        mint = t.get("address")
        token_ages[mint] = check_age(mint)
        print(f".", end="", flush=True)
    print("\n")

    for name, limits in scenarios.items():
        passed = 0
        reasons = {"liq": 0, "vol": 0, "age": 0}
        
        for t in items:
            mint = t.get("address")
            liq = t.get("liquidity", 0) or 0
            vol = t.get("v24hUSD") or 0
            age = token_ages.get(mint, 9999)

            fail = False
            if liq < limits["liq"]: 
                reasons["liq"] += 1
                fail = True
            if vol < limits["vol"]: 
                reasons["vol"] += 1
                fail = True
            if age > limits["age"]: 
                reasons["age"] += 1
                fail = True
            
            if not fail:
                passed += 1

        print(f"Scenario: {name}")
        print(f"  Passed: {passed} / {len(items)}")
        if passed == 0:
            print(f"  Failures: Liq={reasons['liq']}, Vol={reasons['vol']}, Age={reasons['age']}")
        print("-" * 30)

if __name__ == "__main__":
    run_test()
