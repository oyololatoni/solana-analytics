"""
Lifecycle State Machine (Phase K)
Manages transitions for tokens in ACTIVE_MONITORING.
Checks for Success (5x), Failures (F1-F4), and Expiry (72h).
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from api.db import init_db, close_db, get_db_connection

logger = logging.getLogger("solana-analytics")

# 7. Failure Thresholds
FAIL_PRICE_DROP = Decimal("0.5")      # F1: < 50% of detection price
FAIL_LIQ_COLLAPSE = Decimal("0.6")    # F2: < 60% of peak post-detection liq
FAIL_VOL_COLLAPSE_RATIO = Decimal("0.3") # F3: 1h vol < 30% of 6h avg (sustained 3h)
FAIL_EARLY_EXIT_PCT = Decimal("0.7")  # F4: > 70% early wallets exit

# 6. Success Threshold
SUCCESS_MULTIPLIER = Decimal("5.0")   # 5x Price

# 8. Expiry
EXPIRY_HOURS = 72


async def get_price_at_time(cur, token_id, timestamp):
    """
    Get the price (SOL per Token) at a specific time.
    Uses the last trade before or at timestamp.
    """
    await cur.execute("""
        SELECT amount_sol, amount_token 
        FROM trades 
        WHERE token_id = %s AND timestamp <= %s AND amount_sol > 0 AND amount_token > 0
        ORDER BY timestamp DESC LIMIT 1
    """, (token_id, timestamp))
    row = await cur.fetchone()
    if row:
        return row[0] / row[1]
    return None

async def check_lifecycle_updates():
    """
    Iterates over all ACTIVE_MONITORING tokens and checks state transitions.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Get active tokens
            await cur.execute("""
                SELECT id, detected_at, peak_liquidity_usd, address 
                FROM tokens 
                WHERE lifecycle_stage = 'ACTIVE_MONITORING'
            """)
            active_tokens = await cur.fetchall()
            
            if not active_tokens:
                return 0

            updates = 0
            now = datetime.now(timezone.utc)
            
            for token_id, detected_at, peak_liq, address in active_tokens:
                if detected_at.tzinfo is None:
                    detected_at = detected_at.replace(tzinfo=timezone.utc)
                    
                age_hours = (now - detected_at).total_seconds() / 3600.0
                
                # 0. Get Base Price (Detection Price)
                base_price = await get_price_at_time(cur, token_id, detected_at)
                if not base_price:
                    # Should not happen if snapshot triggered on valid trades
                    logger.warning(f"Lifecycle: No base price for {token_id} at {detected_at}. Skipping.")
                    continue
                
                # Get Current Price
                curr_price = await get_price_at_time(cur, token_id, now)
                if not curr_price:
                    continue # No recent trades?
                
                # Update Peak Liquidity (Post-Detection)
                # Proxy: check rolling metric for 5m window
                await cur.execute("""
                    SELECT volume_usd FROM token_rolling_metrics 
                    WHERE token_id = %s AND window_type = '5m' 
                    ORDER BY computed_at DESC LIMIT 1
                """, (token_id,))
                liq_row = await cur.fetchone()
                curr_liquidity = liq_row[0] if liq_row and liq_row[0] else Decimal(0)
                
                if curr_liquidity > (peak_liq or 0):
                    await cur.execute("UPDATE tokens SET peak_liquidity_usd = %s WHERE id = %s", (curr_liquidity, token_id))
                    peak_liq = curr_liquidity
                
                # --- CHECKS ---
                
                outcome = None
                label_type = None
                
                # 1. SUCCESS (5x)
                if curr_price >= base_price * SUCCESS_MULTIPLIER:
                    outcome = 'SUCCESS'
                    label_type = 'hit_5x'
                
                # 2. FAIL F1 (Price Drop)
                elif curr_price <= base_price * FAIL_PRICE_DROP:
                    outcome = 'FAILED'
                    label_type = 'price_failure'
                
                # 3. FAIL F2 (Liquidity Collapse)
                elif peak_liq and peak_liq > 0 and curr_liquidity <= peak_liq * FAIL_LIQ_COLLAPSE:
                    outcome = 'FAILED'
                    label_type = 'liquidity_collapse'
                
                # 4. FAIL F3 (Volume Collapse — 3 consecutive hours)
                # Check if 1h volume < 30% of 6h avg for the last 3 hours
                elif age_hours >= 3:
                    f3_triggered = True
                    for hours_ago in range(3):
                        window_end = now - timedelta(hours=hours_ago)
                        window_start = window_end - timedelta(hours=1)
                        ref_start = window_end - timedelta(hours=6)
                        
                        await cur.execute("""
                            SELECT COALESCE(SUM(amount_sol), 0) FROM trades
                            WHERE token_id = %s AND timestamp > %s AND timestamp <= %s
                        """, (token_id, window_start, window_end))
                        vol_1h = (await cur.fetchone())[0] or Decimal(0)
                        
                        await cur.execute("""
                            SELECT COALESCE(SUM(amount_sol), 0) FROM trades
                            WHERE token_id = %s AND timestamp > %s AND timestamp <= %s
                        """, (token_id, ref_start, window_end))
                        vol_6h = (await cur.fetchone())[0] or Decimal(0)
                        vol_6h_avg = vol_6h / Decimal(6)
                        
                        if vol_6h_avg > 0 and vol_1h >= vol_6h_avg * FAIL_VOL_COLLAPSE_RATIO:
                            f3_triggered = False
                            break
                        elif vol_6h_avg == 0:
                            f3_triggered = False
                            break
                    
                    if f3_triggered:
                        outcome = 'FAILED'
                        label_type = 'volume_collapse'
                
                # 5. FAIL F4 (Early Wallet Exit — within first 2h only)
                if not outcome and age_hours <= 2:
                    # Early wallets = wallets that interacted in first 30m of token life
                    await cur.execute("SELECT created_at_chain FROM tokens WHERE id = %s", (token_id,))
                    token_birth = await cur.fetchone()
                    if token_birth and token_birth[0]:
                        birth = token_birth[0]
                        if birth.tzinfo is None:
                            birth = birth.replace(tzinfo=timezone.utc)
                        early_cutoff = birth + timedelta(minutes=30)
                        
                        await cur.execute("""
                            SELECT COUNT(*), 
                                   SUM(CASE WHEN last_balance_token <= 0 THEN 1 ELSE 0 END)
                            FROM wallet_token_interactions
                            WHERE token_id = %s AND first_interaction <= %s
                        """, (token_id, early_cutoff))
                        ew_row = await cur.fetchone()
                        total_early = ew_row[0] or 0
                        exited_early = ew_row[1] or 0
                        
                        if total_early > 0:
                            exit_pct = Decimal(exited_early) / Decimal(total_early)
                            if exit_pct > FAIL_EARLY_EXIT_PCT:
                                outcome = 'FAILED'
                                label_type = 'early_wallet_exit'
                
                # 6. EXPIRY (72h)
                if not outcome and age_hours >= EXPIRY_HOURS:
                    outcome = 'EXPIRED'
                    label_type = 'expired'
                
                # --- TRANSITION ---
                if outcome:
                    logger.info(f"Token {token_id} Transition: ACTIVE -> {outcome} ({label_type})")
                    
                    # Close Token
                    await cur.execute("""
                        UPDATE tokens 
                        SET lifecycle_stage = %s, 
                            is_active = FALSE 
                        WHERE id = %s
                    """, (outcome, token_id))
                    
                    # Insert Label
                    # Find snapshot id
                    await cur.execute("SELECT id FROM feature_snapshots WHERE token_id = %s AND feature_version = 1", (token_id,))
                    snap_row = await cur.fetchone()
                    if snap_row:
                        snapshot_id = snap_row[0]
                        max_mult = curr_price / base_price
                        await cur.execute("""
                            INSERT INTO lifecycle_labels (token_id, snapshot_id, outcome, max_multiplier)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """, (token_id, snapshot_id, label_type, max_mult))
                    
                    updates += 1
            
            await conn.commit()
            return updates

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(check_lifecycle_updates())
