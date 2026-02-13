
import requests
import sys

try:
    print("Testing DexScreener Connectivity via requests...")
    r = requests.get("https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112", timeout=5)
    print(f"Status: {r.status_code}")
    print(f"Headers: {r.headers}")
    if r.status_code == 200:
        print("Success!")
    else:
        print("Failed with status code.")
except Exception as e:
    print(f"FAILED: {e}")
    sys.exit(1)
