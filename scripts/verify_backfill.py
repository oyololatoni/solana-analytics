import asyncio
import os
import sys
import logging
from datetime import timedelta

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
logger = logging.getLogger("verify_backfill")

async def verify():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            logger.info("--- BACKFILL VERIFICATION ---")
            
            # 1. Count Tokens
            await cur.execute("SELECT COUNT(*) FROM tokens")
            token_count = (await cur.fetchone())[0]
            logger.info(f"Total Tokens: {token_count}")
            
            # 2. Count Trades
            await cur.execute("SELECT COUNT(*) FROM trades")
            trade_count = (await cur.fetchone())[0]
            logger.info(f"Total Trades: {trade_count}")

            # 3. Count Liquidity Events
            await cur.execute("SELECT COUNT(*) FROM liquidity_events")
            liq_count = (await cur.fetchone())[0]
            logger.info(f"Total Liquidity Events: {liq_count}")
            
            if token_count == 0:
                logger.warning("No tokens found. Backfill empty.")
                return

            # 4. Leakage Check (Pre-birth trades)
            logger.info("Checking for pre-birth trades (Leakage Type A)...")
            await cur.execute("""
                SELECT COUNT(*) 
                FROM trades tr
                JOIN tokens t ON tr.token_id = t.id
                WHERE tr.timestamp < t.detected_at
            """)
            pre_birth = (await cur.fetchone())[0]
            if pre_birth > 0:
                logger.error(f"FAIL: Found {pre_birth} trades BEFORE token creation!")
            else:
                logger.info("PASS: No pre-birth trades found.")
                
            # 5. Leakage Check (Post-72h trades)
            logger.info("Checking for >72h trades (Leakage Type B)...")
            await cur.execute("""
                SELECT COUNT(*) 
                FROM trades tr
                JOIN tokens t ON tr.token_id = t.id
                WHERE tr.timestamp > t.detected_at + INTERVAL '72 hours'
            """)
            post_72h = (await cur.fetchone())[0]
            if post_72h > 0:
                logger.error(f"FAIL: Found {post_72h} trades AFTER 72h window!")
            else:
                logger.info("PASS: No post-72h trades found.")

            # 6. Eligibility Distribution
            await cur.execute("SELECT eligibility_status, COUNT(*) FROM tokens GROUP BY eligibility_status")
            rows = await cur.fetchall()
            logger.info("Eligibility Distribution:")
            for r in rows:
                logger.info(f"  {r[0]}: {r[1]}")

    await close_db()

if __name__ == "__main__":
    asyncio.run(verify())
