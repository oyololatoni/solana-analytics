
import os
import sys
import json
import asyncio
import logging
import requests
import random
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.getcwd())
try:
    from app.core.db import get_db_connection, init_db
except ImportError:
    sys.path.append(os.path.join(os.getcwd(), 'app'))
    from app.core.db import get_db_connection, init_db

# Config
API_KEYS = []
if os.getenv("HELIUS_API_KEY"): API_KEYS.append(os.getenv("HELIUS_API_KEY"))
if os.getenv("HELIUS_API_KEY_2"): API_KEYS.append(os.getenv("HELIUS_API_KEY_2"))

# Fallback load if not expecting standard env load in script
if not API_KEYS:
    env_path = ".env.local"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line:
                    k, v = line.split("=", 1)
                    if k.strip() == "HELIUS_API_KEY": API_KEYS.append(v.strip().strip('"'))
                    if k.strip() == "HELIUS_API_KEY_2": API_KEYS.append(v.strip().strip('"'))

BASE_URL = "https://api.helius.xyz/v0/addresses"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stage2_3_precheck")

def get_random_key():
    return random.choice(API_KEYS) if API_KEYS else None

async def run_precheck():
    if not os.path.exists("candidate_tokens.json"): return
    with open("candidate_tokens.json", "r") as f:
        candidates = json.load(f)
        
    await init_db()
    async with get_db_connection() as conn:
        passed = []
        
        for c in candidates:
            mint = c.get("mint")
            created = c.get("created_at")
            start_dt = datetime.fromisoformat(created)
            
            # Helius Request
            url = f"{BASE_URL}/{mint}/transactions"
            key = get_random_key()
            if not key:
                logger.error("No API Keys available")
                return

            params = {
                "api-key": key,
                "startTime": int(start_dt.timestamp()),
                "endTime": int((start_dt + timedelta(minutes=10)).timestamp()),
                "limit": 100
            }
            
            try:
                r = requests.get(url, params=params, timeout=10)
                if r.status_code != 200: 
                    logger.warning(f"API Error {r.status_code} for {mint}")
                    continue
                txs = r.json()
                
                tx_10m = len(txs)
                tx_5m = len([t for t in txs if t.get("timestamp") <= (start_dt + timedelta(minutes=5)).timestamp()])
                
                # Classify 
                d_class = "LOW_ACTIVITY"
                if tx_5m > 1000: d_class = "HYPE"
                elif tx_5m > 100: d_class = "STANDARD"
                
                async with conn.cursor() as cur:
                    await cur.execute("""
                        UPDATE tokens 
                        SET discovery_class = CASE 
                            WHEN discovery_class LIKE 'NEW_LISTING%' THEN discovery_class 
                            ELSE %s 
                        END 
                        WHERE address = %s
                    """, (d_class, mint))
                
                # 7. Precheck (10m < 20 reject)
                if tx_10m < 20: 
                    continue 
                
                passed.append(c)
                
            except Exception as e:
                logger.error(f"Error checking {mint}: {e}")
                continue
            
        await conn.commit()
        
    with open("backfill_queue.json", "w") as f:
        json.dump(passed, f, indent=2)

if __name__ == "__main__":
    asyncio.run(run_precheck())
