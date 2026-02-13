import asyncio
import os
import sys
import json
import logging
import glob
import time
from datetime import datetime, timedelta, timezone

# Add project root to path
sys.path.insert(0, os.getcwd())

# Load .env
if not os.environ.get("DATABASE_URL"):
    env_file = ".env" if os.path.exists(".env") else ".env.local"
    try:
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    os.environ[key] = value.strip('"').strip("'")
    except FileNotFoundError:
        pass

from app.core.db import get_db_connection, init_db, close_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ingest_loop")

CACHE_DIR = "backfill_cache/tokens"

async def get_solana_chain_id(conn):
    async with conn.cursor() as cur:
        await cur.execute("SELECT id FROM chains WHERE name = 'solana'")
        row = await cur.fetchone()
        if row: return row[0]
        # Create if missing?
        await cur.execute("INSERT INTO chains (name) VALUES ('solana') RETURNING id")
        row = await cur.fetchone()
        return row[0]

async def ingest_file(conn, filepath, chain_id):
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {filepath}: {e}")
        return

    mint = data.get("mint")
    created_at_iso = data.get("created_at")
    transactions = data.get("transactions", [])
    
    if not mint: return

    creation_dt = None
    if created_at_iso:
        try:
            creation_dt = datetime.fromisoformat(created_at_iso)
        except:
            pass

    # 1. Ensure Token Exists
    token_detected = creation_dt if creation_dt else datetime.now(timezone.utc)
    
    token_id = None
    async with conn.cursor() as cur:
        await cur.execute("SELECT id FROM tokens WHERE chain_id = %s AND address = %s", (chain_id, mint))
        row = await cur.fetchone()
        if row:
            token_id = row[0]
        else:
            await cur.execute("""
                INSERT INTO tokens (chain_id, address, detected_at, created_at_chain, is_active, eligibility_status)
                VALUES (%s, %s, %s, %s, TRUE, 'PRE_ELIGIBLE')
                RETURNING id
            """, (chain_id, mint, token_detected, creation_dt))
            token_id = (await cur.fetchone())[0]

    # V2 Fix: Fetch primary_pair_address for linkage
    primary_pair = None
    async with conn.cursor() as cur:
        await cur.execute("SELECT primary_pair_address FROM tokens WHERE id = %s", (token_id,))
        row = await cur.fetchone()
        if row: primary_pair = row[0]

    # 2. Process Transactions
    trades_to_insert = []
    
    cutoff_time = None
    if creation_dt:
        cutoff_time = creation_dt + timedelta(hours=72)

    for tx in transactions:
        sig = tx.get("signature")
        slot = tx.get("slot")
        ts = tx.get("timestamp")
        if not ts: continue
        dt_ts = datetime.fromtimestamp(ts, tz=timezone.utc)
        
        if cutoff_time and dt_ts > cutoff_time: continue

        token_transfers = tx.get("tokenTransfers", [])
        if not token_transfers: continue

        relevant = None
        for t in token_transfers:
            if t.get("mint") == mint:
                relevant = t
                break
        
        if not relevant: continue

        try:
            amount_token = float(relevant.get("tokenAmount", 0))
        except:
            amount_token = 0

        if amount_token == 0: continue

        fee_payer = tx.get("feePayer")
        from_user = relevant.get("fromUserAccount")
        to_user = relevant.get("toUserAccount")
        
        side = "buy" # Default
        wallet = to_user
        
        if from_user == fee_payer:
            side = "sell"
            wallet = from_user
        elif to_user == fee_payer:
            side = "buy"
            wallet = to_user

        trades_to_insert.append((
            chain_id, token_id, sig, wallet, side, 
            amount_token, 0, 0, 
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
    
    return len(trades_to_insert)

async def run_loop():
    await init_db()
    
    processed_files = set()
    logger.info("Starting Ingestion Loop...")
    
    while True:
        try:
            async with get_db_connection() as conn:
                chain_id = await get_solana_chain_id(conn)
                
                files = glob.glob(os.path.join(CACHE_DIR, "*.json"))
                new_files = [f for f in files if f not in processed_files]
                
                if new_files:
                    logger.info(f"Found {len(new_files)} new files.")
                    for f in new_files:
                        count = await ingest_file(conn, f, chain_id)
                        processed_files.add(f)
                        if count:
                            logger.info(f"Ingested {count} trades from {os.path.basename(f)}")
                else:
                    # logger.info("No new files. Sleeping...")
                    pass
                
                await conn.commit() # Commit batch
                
        except Exception as e:
            logger.error(f"Loop Error: {e}")
            
        await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        pass
