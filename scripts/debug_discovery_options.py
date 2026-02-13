import requests
import json
import os
import time

BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")

B_HEADERS = {"X-API-KEY": BIRDEYE_API_KEY, "accept": "application/json"}

def test_birdeye(name, subpath):
    url = f"https://public-api.birdeye.so/defi/{subpath}"
    print(f"\n--- Testing Birdeye {name} ---")
    try:
        r = requests.get(url, headers=B_HEADERS, params={"limit": 5, "offset":0})
        print(f"URL: {url} | Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            if data.get("success"):
                # Keys inside 'data'
                d = data.get("data")
                if isinstance(d, dict):
                    print(f"Data Keys: {list(d.keys())}")
                    if "tokens" in d:
                        print(f"Tokens in list: {len(d['tokens'])}")
                        if d['tokens']:
                             print(f"Sample: {json.dumps(d['tokens'][0], indent=2)}")
                    elif "items" in d:
                        print(f"Items in list: {len(d['items'])}")
                        if d['items']:
                             print(f"Sample: {json.dumps(d['items'][0], indent=2)}")
                elif isinstance(d, list):
                     print(f"Data is list len {len(d)}")
                     if d: print(f"Sample: {json.dumps(d[0], indent=2)}")
    except Exception as e:
        print(f"Err: {e}")

def test_helius_asset(mint):
    print(f"\n--- Testing Helius getAsset ({mint}) ---")
    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    payload = {
        "jsonrpc": "2.0",
        "id": "my-id",
        "method": "getAsset",
        "params": {
            "id": mint
        }
    }
    
    try:
        r = requests.post(url, json=payload)
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            res = data.get("result")
            if res:
                print(f"Keys: {list(res.keys())}")
                # Check for creation info?
                # Sometimes in 'compression' or 'content' or 'authorities'
                # Or simply doesn't exist for standard SPL tokens in DAS?
                
                # Try fetch signatures oldest?
                pass
            else:
                 print(f"No result. {data}")
    except Exception as e:
        print(f"Err: {e}")

if __name__ == "__main__":
    if BIRDEYE_API_KEY:
        # standard tokenlist (we know this works but no age)
        # test_birdeye("TokenList", "tokenlist") 
        
        # New options
        test_birdeye("Trending", "token_trending")
        test_birdeye("New Currency", "new_currency")
        test_birdeye("New Currency V2", "v2/tokens/new_list") # guess
        
    if HELIUS_API_KEY:
        # Test USDC
        test_helius_asset("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
