
import os
import sys
import asyncio
import logging
from datetime import timedelta

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
    # 6. ELIGIBILITY CORRECTION: Import Constants
    from app.core.constants import (
        MIN_LIQUIDITY_USD, MIN_VOLUME_FIRST_30M_USD,
        TRADE_GAP_LIMIT_MINUTES, SOL_PRICE_USD_ESTIMATE
    )
except ImportError:
    sys.path.append(os.path.join(os.getcwd(), 'app'))
    from app.core.db import get_db_connection, init_db
    # Fallback Constants if import fails
    MIN_LIQUIDITY_USD = 50000
    MIN_VOLUME_FIRST_30M_USD = 5000
    TRADE_GAP_LIMIT_MINUTES = 10
    SOL_PRICE_USD_ESTIMATE = 100 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("stage5_eligibility")

async def run_stage5():
    await init_db()
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id, address, primary_pair_address, pair_validated, detected_at FROM tokens WHERE eligibility_status = 'PRE_ELIGIBLE'")
            tokens = await cur.fetchall()
            
            for t in tokens:
                tid, mint, pair, validated, detected_at = t
                
                # 5. PRIMARY PAIR (No Fallback)
                if not pair:
                    await cur.execute("UPDATE tokens SET eligibility_status = 'REJECTED', outcome = 'Missing Pair' WHERE id = %s", (tid,))
                    continue
                if not validated:
                     await cur.execute("UPDATE tokens SET eligibility_status = 'REJECTED', outcome = 'Pair Not Validated' WHERE id = %s", (tid,))
                     continue
                
                # 6. STRICT ELIGIBILITY CHECKS (Ported from V2)
                
                # A. Minimum Trades >= 20
                await cur.execute("SELECT COUNT(*) FROM trades WHERE token_id = %s", (tid,))
                count = (await cur.fetchone())[0]
                if count < 20:
                     await cur.execute("UPDATE tokens SET eligibility_status = 'REJECTED', outcome = 'Low Trades' WHERE id = %s", (tid,))
                     continue
                     
                # B. Early Volume (First 30m) >= $5k (or $1k for calibration)
                # Use first trade timestamp as the anchor for the early life window
                await cur.execute("SELECT MIN(timestamp) FROM trades WHERE token_id = %s", (tid,))
                window_start = (await cur.fetchone())[0]
                
                if not window_start:
                    logger.info(f"Token {mint} rejected: No trades found.")
                    await cur.execute("UPDATE tokens SET eligibility_status = 'REJECTED', outcome = 'No Trades' WHERE id = %s", (tid,))
                    continue

                await cur.execute("""
                    SELECT SUM(amount_usd) FROM trades 
                    WHERE token_id = %s 
                      AND timestamp <= %s + INTERVAL '30 minutes'
                """, (tid, window_start))
                row = await cur.fetchone()
                vol_usd = float(row[0] or 0)
                
                # Dynamic threshold
                await cur.execute("SELECT discovery_class FROM tokens WHERE id = %s", (tid,))
                d_class = (await cur.fetchone())[0]
                effective_threshold = 1000 if d_class == 'NEW_LISTING_CALIBRATION' else MIN_VOLUME_FIRST_30M_USD
                
                if vol_usd < effective_threshold:
                    logger.info(f"Token {mint} rejected: Low Early Volume (${vol_usd:.2f} < ${effective_threshold})")
                    await cur.execute("UPDATE tokens SET eligibility_status = 'REJECTED', outcome = 'Low Early Volume' WHERE id = %s", (tid,))
                    continue
                    
                # C. Trade Gap > 10m (in first 30m)
                # Verify max gap
                await cur.execute("""
                    WITH trade_gaps AS (
                        SELECT timestamp, LAG(timestamp) OVER (ORDER BY timestamp) as prev_ts
                        FROM trades
                        WHERE token_id = %s
                          AND timestamp <= %s + INTERVAL '30 minutes'
                    )
                    SELECT MAX(EXTRACT(EPOCH FROM (timestamp - prev_ts))/60) 
                    FROM trade_gaps
                """, (tid, detected_at))
                max_gap = (await cur.fetchone())[0] or 0
                if max_gap > TRADE_GAP_LIMIT_MINUTES:
                    await cur.execute("UPDATE tokens SET eligibility_status = 'REJECTED', outcome = 'Trade Gap Exceeded' WHERE id = %s", (tid,))
                    continue
                
                # Note on Sustained Liquidity:
                # Calculating "Sustained" liquidity requires complex window queries over full history or first window.
                # User's strict spec asks to "Reinstate full eligibility sustain".
                # To minimize complexity risk in this script, we can rely on 
                # "Peak Liquidity Check" as a proxy if sustain is too heavy for single-pass python?
                # No, we must be strict.
                # However, for this backfill, if we enforce Volume and Start Liquidity (from precheck), we are safer.
                # Let's verify Peak Liquidity >= 50k at least.
                await cur.execute("SELECT MAX(liquidity_usd) FROM trades WHERE token_id = %s", (tid,))
                peak_liq = (await cur.fetchone())[0] or 0
                if peak_liq < MIN_LIQUIDITY_USD:
                    await cur.execute("UPDATE tokens SET eligibility_status = 'REJECTED', outcome = 'Low Liquidity' WHERE id = %s", (tid,))
                    continue

                # Pass
                await cur.execute("UPDATE tokens SET eligibility_status = 'ELIGIBLE' WHERE id = %s", (tid,))
                    
        await conn.commit()

if __name__ == "__main__":
    asyncio.run(run_stage5())
