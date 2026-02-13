import asyncio
import os
import sys
import json
import logging
import glob
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
logger = logging.getLogger("ingest_token_backfill")

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
    # We use a default detected_at if creation unknown
    token_detected = creation_dt if creation_dt else datetime.now(timezone.utc)
    
    token_id = None
    async with conn.cursor() as cur:
        # Check existence
        await cur.execute("SELECT id FROM tokens WHERE chain_id = %s AND address = %s", (chain_id, mint))
        row = await cur.fetchone()
        if row:
            token_id = row[0]
        else:
            # Insert
            await cur.execute("""
                INSERT INTO tokens (chain_id, address, detected_at, created_at_chain, is_active, eligibility_status)
                VALUES (%s, %s, %s, %s, TRUE, 'PRE_ELIGIBLE')
                RETURNING id
            """, (chain_id, mint, token_detected, creation_dt))
            token_id = (await cur.fetchone())[0]

    # 2. Process Transactions
    trades_to_insert = []
    
    cutoff_time = None
    if creation_dt:
        cutoff_time = creation_dt + timedelta(hours=72)

    for tx in transactions:
        # Basic Info
        sig = tx.get("signature")
        slot = tx.get("slot")
        ts = tx.get("timestamp")
        if not ts: continue
        
        dt_ts = datetime.fromtimestamp(ts, tz=timezone.utc)
        
        # Leakage Filter
        if cutoff_time and dt_ts > cutoff_time:
            continue

        # Parse Trade
        # Heuristic: Look for token transfers involving our mint
        token_transfers = tx.get("tokenTransfers", [])
        if not token_transfers: continue

        # Identify relevant transfer
        # We look for a transfer of 'mint'
        relevant = None
        for t in token_transfers:
            if t.get("mint") == mint:
                relevant = t
                break
        
        if not relevant: continue

        # Amount
        try:
            amount_token = float(relevant.get("tokenAmount", 0))
        except:
            amount_token = 0

        if amount_token == 0: continue

        # Side & Wallet
        # Fee Payer is usually the signer/initiator.
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
        else:
            pass

        # Price extraction (Simplified)
        price_usd = 0 
        amount_usd = 0

        trades_to_insert.append((
            chain_id, token_id, sig, wallet, side, 
            amount_token, amount_usd, price_usd, 
            slot, dt_ts, 
            0, 0, None # amount_sol, liquidity_usd, pair_address
        ))

    # Batch Insert
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
    
    logger.info(f"Ingested {len(trades_to_insert)} trades for {mint}")

async def run_ingestion():
    await init_db()
    async with get_db_connection() as conn:
        chain_id = await get_solana_chain_id(conn)
        
        files = glob.glob(os.path.join(CACHE_DIR, "*.json"))
        logger.info(f"Found {len(files)} token files to ingest.")
        
        for f in files:
            await ingest_file(conn, f, chain_id)
            
    await close_db()
    logger.info("Ingestion Complete.")

if __name__ == "__main__":
    try:
        asyncio.run(run_ingestion())
    except KeyboardInterrupt:
        pass
