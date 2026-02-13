import requests
import time

GECKO_NEW_POOLS_URL = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools"
DEX_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEX_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"

def analyze():
    print("--- Source Analysis ---")
    
    # Gecko
    try:
        r = requests.get(GECKO_NEW_POOLS_URL, timeout=10)
        if r.status_code == 200:
            data = r.json().get("data", [])
            print(f"Gecko New Pools: {len(data)} items")
        else:
            print(f"Gecko Error: {r.status_code}")
    except Exception as e: print(f"Gecko Exception: {e}")

    # Dex Profiles
    try:
        r = requests.get(DEX_PROFILES_URL, timeout=10)
        if r.status_code == 200:
            data = r.json()
            sol_items = [p for p in data if p.get("chainId") == "solana"]
            print(f"Dex Profiles (Solana): {len(sol_items)} / {len(data)} total")
        else:
            print(f"Dex Profiles Error: {r.status_code}")
    except Exception as e: print(f"Dex Profiles Exception: {e}")

    # Dex Search
    search_queries = ["Raydium", "SOL", "USDC"]
    for q in search_queries:
        try:
            r = requests.get(f"{DEX_SEARCH_URL}?q={q}", timeout=10)
            if r.status_code == 200:
                data = r.json().get("pairs", [])
                sol_items = [p for p in data if p.get("chainId") == "solana"]
                print(f"Dex Search '{q}' (Solana): {len(sol_items)} / {len(data)} total")
            else:
                print(f"Dex Search '{q}' Error: {r.status_code}")
        except Exception as e: print(f"Dex Search '{q}' Exception: {e}")

if __name__ == "__main__":
    analyze()
