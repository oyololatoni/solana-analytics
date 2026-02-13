
import os
import sys
import asyncio
import logging
from datetime import timedelta, timezone

sys.path.insert(0, os.getcwd())

# Load .env explicitly
if not os.environ.get("DATABASE_URL"):
    env_path = ".env.local"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")

try:
    from app.core.db import get_db_connection, init_db
    from app.engines.v2.label_worker import run_label_worker_v2
except ImportError:
    from app.core.db import get_db_connection, init_db
    # Mock
    async def run_label_worker_v2(): return {}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stage7_labels")

async def run_stage7():
    print("Running Stage 7: Label Resolution...")
    
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 2. LIFECYCLE LABEL LOGIC (FIXED)
            # Only mark TRUNCATED if:
            # 1. ingestion_truncated = TRUE
            # 2. AND Last Trade Timestamp < Detected + 72h
            
            await cur.execute("""
                SELECT t.id, t.detected_at, t.primary_pair_address 
                FROM tokens t
                WHERE t.ingestion_truncated = TRUE 
                  AND t.id NOT IN (SELECT token_id FROM lifecycle_labels)
            """)
            truncated_candidates = await cur.fetchall()
            
            for (tid, detected_at, pair) in truncated_candidates:
                if detected_at.tzinfo is None:
                     detected_at = detected_at.replace(tzinfo=timezone.utc)
                     
                await cur.execute("""
                    SELECT MAX(timestamp) FROM trades WHERE token_id = %s
                """, (tid,))
                row = await cur.fetchone()
                last_ts = row[0]
                
                if last_ts:
                    if last_ts.tzinfo is None:
                         last_ts = last_ts.replace(tzinfo=timezone.utc)
                         
                    cutoff = detected_at + timedelta(hours=72)
                    
                    # If we have data BEYOND or AT cutoff (within reasonably small margin), 
                    # it means we captured the full window despite the cap?
                    # Actually, if we hit the cap, we stopped ingestion. 
                    # So last_ts tells us where we stopped.
                    # IF last_ts < cutoff, then we definitively missed data -> TRUNCATED.
                    # IF last_ts >= cutoff (unlikely if capped, unless huge volume instantly at 72h?),
                    # then we have the full window.
                    
                    if last_ts < cutoff - timedelta(minutes=10): # 10m buffer for close calls
                        logger.info(f"Labeling {tid} as TRUNCATED (Gap: {cutoff - last_ts})")
                        await cur.execute("""
                            INSERT INTO lifecycle_labels (token_id, outcome, failure_reason, labeled_at)
                            VALUES (%s, 'TRUNCATED', 'Ingestion Limit Reached', NOW())
                            ON CONFLICT (token_id) DO NOTHING
                        """, (tid,))
                    else:
                        logger.info(f"Token {tid} hit cap but covers 72h (Last: {last_ts}). Proceeding to normal label.")
                else:
                    logger.warning(f"Token {tid} truncated but no trades? Labeling TRUNCATED.")
                    await cur.execute("""
                        INSERT INTO lifecycle_labels (token_id, outcome, failure_reason, labeled_at)
                        VALUES (%s, 'TRUNCATED', 'No Trades', NOW())
                        ON CONFLICT (token_id) DO NOTHING
                    """, (tid,))
                
            await conn.commit()
            
    # Run standard worker for remaining
    stats = await run_label_worker_v2()
    print(f"Labels Resolved: {stats}")

if __name__ == "__main__":
    asyncio.run(run_stage7())
