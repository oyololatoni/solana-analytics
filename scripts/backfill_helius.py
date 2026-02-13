import os
import time
import json
import requests
from datetime import datetime, timedelta, timezone

# ============================================
# CONFIGURATION
# ============================================

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
BASE_URL = "https://api.helius.xyz/v0/addresses"

PUMP_PROGRAM_ID = "6EF8rrecthR5DkzkRgZNpmjDoE7YQDdyCjTiMQuYzfoP"
RAYDIUM_PROGRAM_ID = "RVKd61ztZW9qTMWvHdQWfhKGXT5VErC7E3q6MZBqYg"

MAX_ELIGIBLE_TOKENS = 500
# CREDIT_SAFETY_THRESHOLD = 400_000  # manually monitored
LIMIT_PER_PAGE = 100 # Reduced to 100 to fix 400 Bad Request

CACHE_DIR = "backfill_cache"
os.makedirs(f"{CACHE_DIR}/pump", exist_ok=True)
os.makedirs(f"{CACHE_DIR}/raydium", exist_ok=True)

# ============================================
# HELPER: API CALL WITH PAGINATION
# ============================================

def fetch_transactions(program_id, start_time, end_time, day_str):
    """
    Paginate via 'before' signature.
    Store raw JSON to disk.
    """
    before = None
    page = 0
    all_transactions = []

    while True:
        params = {
            "api-key": HELIUS_API_KEY,
            "limit": LIMIT_PER_PAGE,
            "startTime": int(start_time.timestamp()),
            "endTime": int(end_time.timestamp())
        }

        if before:
            params["before"] = before

        url = f"{BASE_URL}/{program_id}/transactions"

        # Rate limit safety
        time.sleep(0.2) 

        print(f"DEBUG REF: {url} | Params: limit={params.get('limit')} startTime={params.get('startTime')} endTime={params.get('endTime')}")
        
        try:
            response = requests.get(url, params=params)
        except Exception as e:
            print(f"Request failed: {e}")
            time.sleep(5)
            continue

        if response.status_code == 429:
            print("Rate limited. Sleeping 10 seconds...")
            time.sleep(10)
            continue

        if response.status_code != 200:
            print(f"Error: {response.status_code} {response.text}")
            break

        try:
            data = response.json()
        except json.JSONDecodeError:
            print("Failed to decode JSON")
            break

        if not data:
            print(f"No more data for {day_str} page {page}")
            break

        # Save page locally
        filename = f"{CACHE_DIR}/{day_str}_page_{page}.json"
        with open(filename, "w") as f:
            json.dump(data, f)

        all_transactions.extend(data)

        # Pagination logic
        last_sig = data[-1].get("signature")
        if not last_sig:
            break
            
        before = last_sig
        page += 1

        print(f"Fetched page {page} for {day_str} (Count: {len(data)})")

        if len(data) < LIMIT_PER_PAGE:
            break

    return all_transactions

# ============================================
# PUMP TOKEN CREATION PARSER
# ============================================

def extract_pump_tokens(transactions):
    tokens = []

    for tx in transactions:
        instructions = tx.get("parsedInstructions", [])
        if not instructions:
            continue

        for ix in instructions:
            # Detect mint/init instruction (simplified check)
            # Pump.fun creates token in a specific way. 
            # We look for the 'initializeMint' or specific program interaction
            # User skeleton suggested: if "initializeMint" in str(ix):
            # But checking 'program' field is safer if parsed. 
            # For now adhering to user skeleton logic but making it robust.
            
            ix_str = str(ix)
            if "initializeMint" in ix_str:
                # Find mint
                token_transfers = tx.get("tokenTransfers", [])
                mint = None
                if token_transfers:
                    mint = token_transfers[0].get("mint")
                
                # If parsed params available
                if not mint and "info" in ix and "mint" in ix["info"]:
                    mint = ix["info"]["mint"]

                if mint:
                    tokens.append({
                        "mint": mint,
                        "block_time": tx.get("timestamp"),
                        "slot": tx.get("slot"),
                        "signature": tx.get("signature"),
                        "creator": tx.get("feePayer")
                    })
                    # Break loop to avoid dupe per tx
                    break

    return tokens

# ============================================
# RAYDIUM SWAP & LIQUIDITY PARSER
# ============================================

def extract_raydium_events(transactions):
    swaps = []
    liquidity_events = []

    for tx in transactions:
        instructions = tx.get("parsedInstructions", [])
        if not instructions:
            continue

        for ix in instructions:
            ix_str = str(ix)
            ix_type = ix.get("type", "")

            # Swap Detection
            if "swap" in ix_str.lower() or ix_type == "swap":
                swaps.append({
                    "signature": tx.get("signature"),
                    "slot": tx.get("slot"),
                    "block_time": tx.get("timestamp"),
                    "accounts": ix.get("accounts", []), # extract accounts if available
                    "info": ix.get("info", {}),
                    "raw": ix
                })

            # Liquidity Detection
            # Raydium often uses 'transfer' or specific instructions for add/remove
            # If parsed by Helius as "unknown", we rely on program log or instruction parsing
            if "addLiquidity" in ix_str or "removeLiquidity" in ix_str:
                liquidity_events.append({
                    "signature": tx.get("signature"),
                    "slot": tx.get("slot"),
                    "block_time": tx.get("timestamp"),
                    "raw": ix
                })

    return swaps, liquidity_events

# ============================================
# MAIN BACKFILL LOOP
# ============================================

def run_backfill():
    if not HELIUS_API_KEY:
        print("Error: HELIUS_API_KEY not set")
        return

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=14)

    current_day = start_date

    all_tokens = []
    all_swaps = []
    all_liquidity = []

    print(f"Starting Backfill from {start_date} to {end_date}")

    while current_day < end_date:
        next_day = current_day + timedelta(days=1)
        day_str = current_day.strftime("%Y-%m-%d")

        print(f"\nProcessing day: {day_str}")

        # ----------------------------------------
        # Pump token creations
        # ----------------------------------------
        print(f"--- Fetching Pump.fun for {day_str} ---")
        pump_tx = fetch_transactions(
            PUMP_PROGRAM_ID,
            current_day,
            next_day,
            f"pump/{day_str}"
        )

        tokens = extract_pump_tokens(pump_tx)
        all_tokens.extend(tokens)
        print(f"Extracted {len(tokens)} new tokens.")

        # ----------------------------------------
        # Raydium swaps
        # ----------------------------------------
        # print(f"--- Fetching Raydium for {day_str} ---")
        # NOTE: Temporarily disabled Raydium sweep due to API endpoint limitation with Program ID
        # We will focus on Pump tokens first, then resolve Strategy for Raydium.
        # ray_tx = fetch_transactions(
        #     RAYDIUM_PROGRAM_ID,
        #     current_day,
        #     next_day,
        #     f"raydium/{day_str}"
        # )

        # swaps, liquidity = extract_raydium_events(ray_tx)
        # all_swaps.extend(swaps)
        # all_liquidity.extend(liquidity)
        # print(f"Extracted {len(swaps)} swaps, {len(liquidity)} liquidity events.")

        # ----------------------------------------
        # Stop Condition: Eligible Token Count
        # ----------------------------------------
        if len(all_tokens) > MAX_ELIGIBLE_TOKENS * 10: 
             print(f"Reached {len(all_tokens)} raw tokens (approx limit). Stopping fetch.")
             break

        current_day = next_day

    # --------------------------------------------
    # Save consolidated files
    # --------------------------------------------
    print("\nSaving consolidated data...")
    with open("historical_tokens.json", "w") as f:
        json.dump(all_tokens, f, default=str)

    with open("historical_swaps.json", "w") as f:
        json.dump(all_swaps, f, default=str)

    with open("historical_liquidity.json", "w") as f:
        json.dump(all_liquidity, f, default=str)

    print("Backfill fetch complete.")
    print(f"Total Tokens: {len(all_tokens)}")
    print(f"Total Swaps: {len(all_swaps)}")
    print(f"Total Liq Events: {len(all_liquidity)}")

if __name__ == "__main__":
    run_backfill()
