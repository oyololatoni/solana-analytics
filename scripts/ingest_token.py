
import os
import sys
import asyncio
import logging
import requests
import json
import time
import gc
import random
from datetime import datetime, timedelta, timezone

# Add project root
sys.path.insert(0, os.getcwd())

# Load .env explicitly
env_path = ".env.local"
if os.path.exists(env_path):
    with open(env_path, "r") as f:
        for line in f:
            if not line.strip().startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip().strip('"').strip("'")

try:
    from app.core.db import get_db_connection, init_db
except ImportError:
    sys.path.insert(0, os.path.join(os.getcwd(), 'app'))
    from app.core.db import get_db_connection, init_db

# Configuration
# Support Multiple Keys
API_KEYS = []
if os.getenv("HELIUS_API_KEY"): API_KEYS.append(os.getenv("HELIUS_API_KEY"))
if os.getenv("HELIUS_API_KEY_2"): API_KEYS.append(os.getenv("HELIUS_API_KEY_2"))

if not API_KEYS:
    print("❌ No HELIUS_API_KEY found.")
    sys.exit(1)

BASE_URL = "https://api.helius.xyz/v0/addresses"
LIMIT_PER_PAGE = 100
MAX_TRADES_PER_TOKEN = 100_000

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ingest_token")

def get_random_key():
    return random.choice(API_KEYS)

async def get_solana_chain_id(conn):
    async with conn.cursor() as cur:
        await cur.execute("SELECT id FROM chains WHERE name = 'solana'")
        row = await cur.fetchone()
        if row: return row[0]
        await cur.execute("INSERT INTO chains (name) VALUES ('solana') RETURNING id")
        row = await cur.fetchone()
        return row[0]

async def get_token_price_and_liquidity(mint):
    """Fetch current price and liquidity from DexScreener to estimate historical USD values."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            data = r.json()
            pairs = data.get("pairs", [])
            if pairs:
                # Find the best raydium pair
                raydium_pairs = [p for p in pairs if p.get("dexId") == "raydium"]
                best_pair = max(raydium_pairs if raydium_pairs else pairs, key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0))
                return {
                    "price_usd": float(best_pair.get("priceUsd", 0) or 0),
                    "liquidity_usd": float(best_pair.get("liquidity", {}).get("usd", 0) or 0)
                }
    except Exception as e:
        logger.warning(f"Failed to fetch price for {mint}: {e}")
    return {"price_usd": 0, "liquidity_usd": 0}

async def ingest_single_token(mint, created_at_iso, pair_address):
    if not mint or not created_at_iso:
        sys.exit(1)

    if not pair_address or pair_address == "None":
        logger.error(f"❌ REJECTING {mint}: Missing Primary Pair Address")
        sys.exit(1)

    try:
        creation_dt = datetime.fromisoformat(created_at_iso)
        if creation_dt.tzinfo is None:
             creation_dt = creation_dt.replace(tzinfo=timezone.utc)
    except:
        sys.exit(1)
        
    await init_db()
    
    async with get_db_connection() as conn:
        chain_id = await get_solana_chain_id(conn)
        
        async with conn.cursor() as cur:
            await cur.execute("SELECT id, primary_pair_address, eligibility_status FROM tokens WHERE chain_id = %s AND address = %s", (chain_id, mint))
            row = await cur.fetchone()
            if row:
                token_id, db_pair, status = row
                if status in ('ELIGIBLE', 'INACTIVE'):
                    logger.info(f"⏭️ SKIP {mint}")
                    return 
                if not db_pair:
                    await cur.execute("UPDATE tokens SET primary_pair_address = %s WHERE id = %s", (pair_address, token_id))
            else:
                 await cur.execute("""
                    INSERT INTO tokens (chain_id, address, detected_at, created_at_chain, is_active, eligibility_status, primary_pair_address)
                    VALUES (%s, %s, %s, %s, TRUE, 'BACKFILLING', %s)
                    RETURNING id
                """, (chain_id, mint, creation_dt, creation_dt, pair_address))
                 token_id = (await cur.fetchone())[0]

        cutoff_time = creation_dt + timedelta(hours=72)
        now = datetime.now(timezone.utc)
        search_end = min(now, cutoff_time)
        
        before = None
        total_trades = 0
        termination_reason = "COMPLETE" 
        
        logger.info(f"Ingesting {mint} [Window: {creation_dt} -> {search_end}] Ends: {len(API_KEYS)} Keys")
        
        current_page_max_ts = None
        
        while True:
            if total_trades >= MAX_TRADES_PER_TOKEN:
                logger.warning(f"Hit limit {MAX_TRADES_PER_TOKEN}.")
                termination_reason = "MAX_TRADE_CAP"
                break
                
            params = {
                "api-key": get_random_key(),
                "limit": LIMIT_PER_PAGE,
                "endTime": int(search_end.timestamp()),
                "startTime": int(creation_dt.timestamp())
            }
            if before: params["before"] = before
            
            try:
                resp = requests.get(f"{BASE_URL}/{mint}/transactions", params=params, timeout=15)
                
                if resp.status_code == 429:
                    logger.warning("429 Rate Limit. Rotating key & Sleeping...")
                    time.sleep(2)
                    continue
                if resp.status_code != 200:
                    termination_reason = f"API_ERROR_{resp.status_code}"
                    logger.error(termination_reason)
                    break
                    
                data = resp.json()
                if not data: break
                
                trades_to_insert = []
                seen_sigs_page = set()
                
                for tx in data:
                    ts = tx.get("timestamp")
                    if not ts: continue
                    dt_ts = datetime.fromtimestamp(ts, tz=timezone.utc)
                    
                    if current_page_max_ts is None or dt_ts > current_page_max_ts:
                        current_page_max_ts = dt_ts
                    
                    if dt_ts > cutoff_time: continue 
                    if dt_ts < creation_dt: continue
                        
                    sig = tx.get("signature")
                    if sig in seen_sigs_page: continue
                    seen_sigs_page.add(sig)

                    if sig == before: break

                    slot = tx.get("slot")
                    token_transfers = tx.get("tokenTransfers", [])
                    if not token_transfers: continue

                    relevant = None
                    for t in token_transfers:
                         if t.get("mint") == mint:
                             relevant = t
                             break
                    if not relevant: continue
                    
                    amount = float(relevant.get("tokenAmount", 0))
                    if amount == 0: continue
                    
                    fee_payer = tx.get("feePayer")
                    from_user = relevant.get("fromUserAccount")
                    to_user = relevant.get("toUserAccount")
                    side = "buy"
                    wallet = to_user
                    if from_user == fee_payer:
                        side = "sell"
                        wallet = from_user
                    elif to_user == fee_payer:
                        side = "buy"
                        wallet = to_user

                    # Fetch price and liquidity for estimation
                    market_data = await get_token_price_and_liquidity(mint)
                    current_price_usd = market_data["price_usd"]
                    current_liq_usd = market_data["liquidity_usd"]
                    est_amount_usd = amount * current_price_usd

                    trades_to_insert.append((
                        chain_id, token_id, sig, wallet, side, 
                        amount, est_amount_usd, current_price_usd, 
                        slot, dt_ts, 
                        0, current_liq_usd, pair_address 
                    ))
                
                if trades_to_insert:
                     async with conn.cursor() as cur:
                        await cur.executemany("""
                            INSERT INTO trades (
                                chain_id, token_id, tx_signature, wallet_address, side, 
                                amount_token, amount_usd, price_usd, 
                                slot, "timestamp", 
                                amount_sol, liquidity_usd, pair_address
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (chain_id, tx_signature, "timestamp") DO NOTHING
                        """, trades_to_insert)
                     await conn.commit()
                     total_trades += len(trades_to_insert)

                last_sig = data[-1].get("signature")
                if not last_sig: break
                if last_sig == before: break
                
                before = last_sig
                
                del data
                del trades_to_insert
                gc.collect() 
                
            except Exception as e:
                logger.error(f"Page Error: {e}")
                termination_reason = "EXCEPTION"
                break

        is_truncated = (termination_reason == "MAX_TRADE_CAP")
        
        async with conn.cursor() as cur:
             await cur.execute("""
                UPDATE tokens 
                SET eligibility_status = 'PRE_ELIGIBLE', ingestion_truncated = %s 
                WHERE id = %s
             """, (is_truncated, token_id))
        await conn.commit()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(1)
    
    asyncio.run(ingest_single_token(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None))
