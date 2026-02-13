
import os
import sys
import asyncio
import logging
import json
import requests
import time
from datetime import datetime, timedelta, timezone

# Add project root
sys.path.insert(0, os.getcwd())

# Load .env explicitly
if not os.environ.get("HELIUS_API_KEY"):
    env_path = ".env.local"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "HELIUS_API_KEY" in line and not line.strip().startswith("#"):
                    os.environ["HELIUS_API_KEY"] = line.split("=", 1)[1].strip().strip('"').strip("'")

from app.core.db import get_db_connection, init_db

# Configuration
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
BASE_URL = "https://api.helius.xyz/v0/addresses"
LIMIT_PER_PAGE = 100
MAX_TRADES_PER_TOKEN = 100_000
MAX_CONCURRENT_WORKERS = 3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("stage4_backfill")

async def get_solana_chain_id(conn):
    # Optimistic check
    async with conn.cursor() as cur:
        await cur.execute("SELECT id FROM chains WHERE name = 'solana'")
        row = await cur.fetchone()
        if row: return row[0]
        await cur.execute("INSERT INTO chains (name) VALUES ('solana') RETURNING id")
        row = await cur.fetchone()
        return row[0]

async def process_token_stream(conn, chain_id, token_data):
    mint = token_data.get("mint")
    created_at_iso = token_data.get("created_at")
    
    if not mint or not created_at_iso:
        logger.warning(f"Skipping Invalid Token: {token_data}")
        return

    try:
        creation_dt = datetime.fromisoformat(created_at_iso)
    except:
        logger.error(f"Invalid Date for {mint}: {created_at_iso}")
        return

    # 4.1 Pagination Logic
    cutoff_time = creation_dt + timedelta(hours=72)
    start_time = creation_dt 
    
    now = datetime.now(timezone.utc)
    search_end = min(now, cutoff_time)
    
    # Get Token ID & Primary Pair
    token_id = None
    primary_pair = None
    
    async with conn.cursor() as cur:
        # Ensure Token
        await cur.execute("SELECT id, primary_pair_address FROM tokens WHERE chain_id = %s AND address = %s", (chain_id, mint))
        row = await cur.fetchone()
        if row:
            token_id, primary_pair = row
        else:
            await cur.execute("""
                INSERT INTO tokens (chain_id, address, detected_at, created_at_chain, is_active, eligibility_status)
                VALUES (%s, %s, %s, %s, TRUE, 'BACKFILLING')
                RETURNING id
            """, (chain_id, mint, creation_dt, creation_dt))
            token_id = (await cur.fetchone())[0]

    # Stream
    before = None
    total_trades = 0
    
    logger.info(f"Stream {mint}: Window [{creation_dt} -> {search_end}]")

    while True:
        # Check Limits
        if total_trades > MAX_TRADES_PER_TOKEN:
            logger.warning(f"  Hit Max Trades ({MAX_TRADES_PER_TOKEN}) for {mint}")
            break
            
        params = {
            "api-key": HELIUS_API_KEY,
            "limit": LIMIT_PER_PAGE,
            "endTime": int(search_end.timestamp()),
            "startTime": int(creation_dt.timestamp())
        }
        if before: params["before"] = before
        
        try:
            # Using asyncio.to_thread to prevent blocking other workers during HTTP req
            loop = asyncio.get_running_loop()
            resp = await loop.run_in_executor(None, lambda: requests.get(f"{BASE_URL}/{mint}/transactions", params=params, timeout=15))
            
            if resp.status_code == 429:
                await asyncio.sleep(2)
                continue
                
            if resp.status_code != 200:
                logger.error(f"  API Err {resp.status_code}")
                break
                
            data = resp.json()
            if not data: 
                break 
                
            trades_to_insert = []
            
            for tx in data:
                ts = tx.get("timestamp")
                if not ts: continue
                dt_ts = datetime.fromtimestamp(ts, tz=timezone.utc)
                
                sig = tx.get("signature")
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
                    
                trades_to_insert.append((
                    chain_id, token_id, sig, wallet, side, 
                    amount, 0, 0, 
                    slot, dt_ts, 
                    0, 0, primary_pair 
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
            before = last_sig
            
            if len(data) < LIMIT_PER_PAGE:
                break
                
        except Exception as e:
            logger.error(f"  Page Error: {e}")
            break
            
    logger.info(f"✅ Finished {mint}: {total_trades} trades.")
    
    # Update Status to PENDING_ELIGIBILITY
    async with conn.cursor() as cur:
        await cur.execute("UPDATE tokens SET eligibility_status = 'PENDING_ELIGIBILITY' WHERE id = %s", (token_id,))
    await conn.commit()

async def worker(queue, chain_id):
    await init_db()
    # Each worker gets its own DB connection to ensure thread safety / isolation
    async with get_db_connection() as conn:
        while True:
            try:
                token = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            
            try:
                await process_token_stream(conn, chain_id, token)
            except Exception as e:
                logger.error(f"Worker Exception on {token.get('mint')}: {e}")
            finally:
                queue.task_done()

async def run_stage4():
    if not HELIUS_API_KEY:
        logger.error("HELIUS_API_KEY missing")
        return

    if not os.path.exists("backfill_queue.json"):
        logger.error("backfill_queue.json missing")
        return
        
    with open("backfill_queue.json", "r") as f:
        queue_data = json.load(f)
        
    logger.info(f"Starting Stage 4 with {MAX_CONCURRENT_WORKERS} Workers for {len(queue_data)} tokens...")
    
    await init_db()
    
    # Get Chain ID once
    async with get_db_connection() as conn:
        chain_id = await get_solana_chain_id(conn)
    
    # Setup Queue
    queue = asyncio.Queue()
    for t in queue_data:
        queue.put_nowait(t)
        
    # Launch Workers
    workers = []
    for _ in range(MAX_CONCURRENT_WORKERS):
        workers.append(asyncio.create_task(worker(queue, chain_id)))
        
    await asyncio.gather(*workers)

    logger.info("Stage 4 Complete.")

if __name__ == "__main__":
    try:
        asyncio.run(run_stage4())
    except KeyboardInterrupt:
        pass
