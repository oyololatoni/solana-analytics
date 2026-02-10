import os
import json
import requests
from pathlib import Path

# Paths
ROOT = Path(__file__).parent.parent
SAMPLE_PAYLOAD = ROOT / "tools" / "sample_webhook_payload.json"

# Config - manually set if .env.local isn't working for you
HELIUS_WEBHOOK_SECRET = "helius_wh_9b4c8e2f1a7d4c6e91f3a0b2c5d8e7f4"
WEBHOOK_URL = "http://127.0.0.1:8000/webhooks/helius"

def replay():
    if not SAMPLE_PAYLOAD.exists():
        print(f"Sample payload not found at {SAMPLE_PAYLOAD}")
        return

    payload = json.loads(SAMPLE_PAYLOAD.read_text())
    
    # Update timestamp to now to pass the time window guard
    import time
    now_ts = int(time.time())
    for tx in payload:
        tx["timestamp"] = now_ts

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"x-helius-signature: {HELIUS_WEBHOOK_SECRET}"
    }

    print(f"Sending replay to {WEBHOOK_URL}...")
    r = requests.post(WEBHOOK_URL, json=payload, headers=headers)
    
    print(f"Status: {r.status_code}")
    print(json.dumps(r.json(), indent=2))

if __name__ == "__main__":
    replay()
