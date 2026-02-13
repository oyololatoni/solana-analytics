
import os
import requests
import json
import logging

# Load env
if not os.environ.get("HELIUS_API_KEY"):
    env_path = ".env.local"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")

HELIUS_KEY = os.getenv("HELIUS_API_KEY") or os.getenv("HELIUS_API_KEY_2")

def check_asset(mint):
    print(f"--- Checking {mint} ---")
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_KEY}"
    
    # 1. getAsset
    payload = {
        "jsonrpc": "2.0", 
        "id": "test", 
        "method": "getAsset", 
        "params": {
            "id": mint
        }
    }
    try:
        r = requests.post(url, json=payload, timeout=5)
        if r.status_code == 200:
            res = r.json().get("result")
            print("getAsset Keys:", res.keys() if res else "None")
            if res:
                print("Content:", json.dumps(res.get("content"), indent=2))
                print("Token Info:", json.dumps(res.get("token_info"), indent=2))
        else:
            print(f"getAsset Failed: {r.status_code}")
    except Exception as e:
        print(f"getAsset Error: {e}")

    # 2. getSignaturesForAddress (Oldest?)
    # Trying to get the very last signature (which would be the first in history) requires walking back.
    # But for testing, let's see the first page.
    params = [mint, {"limit": 5}]
    payload = {
        "jsonrpc": "2.0", 
        "id": "test-sig", 
        "method": "getSignaturesForAddress", 
        "params": params
    }
    try:
        r = requests.post(url, json=payload, timeout=5)
        if r.status_code == 200:
            sigs = r.json().get("result", [])
            print(f"getSignatures (Latest 5): {len(sigs)}")
            if sigs:
                print(f"Latest Sig Time: {sigs[0].get('blockTime')}")
        else:
            print(f"getSignatures Failed: {r.status_code}")
    except Exception as e:
        print(f"getSignatures Error: {e}")

if __name__ == "__main__":
    # Test with Wrapped SOL (Old)
    check_asset("So11111111111111111111111111111111111111112")
    # Test with a token from the logs (likely new-ish or at least active)
    # Using '9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump' which was tracked earlier
    check_asset("9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump")
