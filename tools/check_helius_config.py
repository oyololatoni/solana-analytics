import os
import requests
from app.core.config import HELIUS_API_KEY

def check_helius():
    if not HELIUS_API_KEY:
        print("No HELIUS_API_KEY found.")
        return

    url = f"https://api.helius.xyz/v0/webhooks?api-key={HELIUS_API_KEY}"
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            hooks = resp.json()
            print(f"Found {len(hooks)} webhooks.")
            for h in hooks:
                print(f"ID: {h.get('webhookID')}")
                print(f"Type: {h.get('webhookType')}")
                print(f"Account Addresses: {len(h.get('accountAddresses', []))}")
                print(f"Sample Addresses: {h.get('accountAddresses', [])[:5]}")
        else:
            print(f"Failed to fetch webhooks: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_helius()
