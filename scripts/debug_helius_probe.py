import os
import requests
import json
import time

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
BASE_URL = "https://api.helius.xyz/v0/addresses"

PUMP_PROGRAM_ID = "6EF8rrecthR5DkzkRgZNpmjDoE7YQDdyCjTiMQuYzfoP"
RAYDIUM_PROGRAM_ID = "RVKd61ztZW9qTMWvHdQWfhKGXT5VErC7E3q6MZBqYg"

def test_endpoint(name, address):
    print(f"\n--- TESTING {name} ({address}) ---")
    url = f"{BASE_URL}/{address}/transactions"
    params = {
        "api-key": HELIUS_API_KEY,
        "limit": 1 # Minimal cost
    }
    
    print(f"URL: {url}")
    # print(f"Params: {params}") # Don't print API key in logs if possible, but safe here locally
    
    try:
        response = requests.get(url, params=params)
        print(f"Status: {response.status_code}")
        try:
            data = response.json()
            # Truncate for display
            print(f"Response: {str(data)[:500]}...") 
        except:
            print(f"Response Text: {response.text}")
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    if not HELIUS_API_KEY:
        print("HELIUS_API_KEY not set!")
    else:
        test_endpoint("Pump.fun", PUMP_PROGRAM_ID)
        test_endpoint("Raydium", RAYDIUM_PROGRAM_ID)
