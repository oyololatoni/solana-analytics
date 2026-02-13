import os
import requests
import json
import time
from datetime import datetime, timedelta, timezone
import random
import concurrent.futures
import logging
import sys

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("backfill_token_level.log")
    ]
)
logger = logging.getLogger("backfill")

# Configuration
# Load .env.local explicitly
if not os.environ.get("HELIUS_API_KEY"):
    env_path = ".env.local"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "HELIUS_API_KEY" in line and not line.strip().startswith("#"):
                    os.environ["HELIUS_API_KEY"] = line.split("=", 1)[1].strip().strip('"').strip("'")

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
BASE_URL = "https://api.helius.xyz/v0/addresses"

# Safety Limits
LIMIT_PER_PAGE = 100
# OPTIMIZATION: Hard Cap at 1000 (Strict Budget Enforced)
HARD_TX_LIMIT = 1000 

CACHE_DIR = "backfill_cache"
os.makedirs(f"{CACHE_DIR}/tokens", exist_ok=True)

def fetch_bounded_token_transactions(mint, created_at_iso, session):
    """
    Fetch transactions with strict bounds and optimizations.
    """
    now = datetime.now(timezone.utc)
    
    # 1. Determine Time Window
    if created_at_iso:
        try:
            creation_time = datetime.fromisoformat(created_at_iso)
            end_time = creation_time + timedelta(hours=72)
            if end_time > now: end_time = now
            start_time = creation_time
        except:
            end_time = now
            start_time = now - timedelta(hours=72)
    else:
        end_time = now
        start_time = now - timedelta(hours=72)
    
    before = None
    all_tx = []
    
    # Safety: Max Retries
    retries = 0
    MAX_RETRIES = 10

    while True:
        params = {
            "api-key": HELIUS_API_KEY,
            "limit": LIMIT_PER_PAGE,
            "endTime": int(end_time.timestamp())
        }
        
        if created_at_iso:
             params["startTime"] = int(start_time.timestamp())

        if before:
            params["before"] = before
            
        url = f"{BASE_URL}/{mint}/transactions"
        
        try:
             r = session.get(url, params=params, timeout=10)
        except Exception as e:
             logger.error(f"Err {mint}: {e}")
             time.sleep(2)
             retries += 1
             if retries > MAX_RETRIES:
                 break
             continue
             
        if r.status_code == 429:
             retries += 1
             sleep_time = 5 * retries
             logger.warning(f"Rate Limited (429) {mint}. Retrying {retries}/{MAX_RETRIES} in {sleep_time}s...")
             time.sleep(sleep_time)
             if retries > MAX_RETRIES:
                 break
             continue

        retries = 0

        if r.status_code != 200:
             logger.error(f"Status {r.status_code}: {r.text}")
             break
             
        data = r.json()
        if not data: 
            break
        
        all_tx.extend(data)
        
        # Optimization: Early Exit on Limit
        if len(all_tx) >= HARD_TX_LIMIT:
            break
            
        last_sig = data[-1].get("signature")
        if not last_sig: break
        before = last_sig

        if len(data) < LIMIT_PER_PAGE:
            break
            
    return all_tx

def process_token(token_data, session):
    mint = token_data.get("mint")
    created_at = token_data.get("created_at")
    
    txs = fetch_bounded_token_transactions(mint, created_at, session)
    
    out_file = os.path.join(CACHE_DIR, "tokens", f"{mint}.json")
    temp_file = f"{out_file}.tmp"
    
    output = {
        "mint": mint,
        "created_at": created_at,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "transactions": txs
    }
    
    try:
        with open(temp_file, "w") as f:
            json.dump(output, f, indent=2)
            f.flush()
            os.fsync(f.fileno()) 
            
        os.rename(temp_file, out_file)
        logger.info(f"Saved {len(txs)} txs for {mint}")
        
    except Exception as e:
        logger.error(f"Failed to write {mint}: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)

def run_backfill():
    # Modified to read from backfill_queue.json (Stage 2/3 Output)
    if not os.path.exists("backfill_queue.json"):
        print("backfill_queue.json not found. Run Stage 2/3 Precheck first.")
        # Fallback to candidate if queue missing? No, user strict.
        return
        
    with open("backfill_queue.json", "r") as f:
        candidates = json.load(f)
        
    print(f"Loaded {len(candidates)} candidates from Queue.")
    
    existing_files = set(os.listdir(os.path.join(CACHE_DIR, "tokens")))
    to_process = []
    
    for c in candidates:
        mint = c.get("mint")
        if f"{mint}.json" in existing_files:
            continue
        to_process.append(c)

    # 6 Workers
    MAX_WORKERS = 6
    logger.info(f"Processing {len(to_process)} tokens in parallel (Max {MAX_WORKERS} workers).")

    def process_token_wrapper(token_data):
        try:
            time.sleep(random.uniform(0.1, 0.5))
            with requests.Session() as s:
                process_token(token_data, s)
        except Exception as e:
            logger.error(f"Failed {token_data.get('mint')}: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_token_wrapper, t): t['mint'] for t in to_process}
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Worker Error: {e}")

    logger.info("Backfill Complete.")

if __name__ == "__main__":
    if not HELIUS_API_KEY:
        print("HELIUS_API_KEY missing")
    else:
        run_backfill()
