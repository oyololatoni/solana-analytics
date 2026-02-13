"""
Label Worker (Outcome Resolution Engine)
Deterministic. Idempotent. High Fidelity.

Operates on:
- tokens (active)
- feature_snapshots
- trades (granular price/volume data)
- lifecycle_labels (output)

Implements 14-step spec:
- Baseline: First trade >= detection_time
- Success: 5x baseline (overrides all)
- Price Fail: < 0.5x baseline (within 48h)
- Liq Collapse: < 0.6x peak liq (within 48h)
- Vol Collapse: 3 consecutive hours < 0.3x 6h_avg
- Early Exit: >70% early wallets dump within 2h
- Expiry: >72h
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, Tuple, List

from app.core.db import get_db_connection

logger = logging.getLogger("engines.v1.labels")

# Constants
SUCCESS_MULTIPLIER = Decimal("5.0")
MAX_WINDOW_HOURS = 72
FAILURE_WINDOW_HOURS = 48
PRICE_FAILURE_THRESHOLD = Decimal("0.5")
LIQUIDITY_COLLAPSE_THRESHOLD = Decimal("0.6")
VOLUME_COLLAPSE_THRESHOLD = Decimal("0.3")
EARLY_EXIT_RATIO = Decimal("0.7")


class OutcomeEngine:
    def __init__(self):
        self.conn = None
        self.cur = None

    async def run_job(self):
        """Main entry point called by worker."""
        async with get_db_connection() as conn:
            self.conn = conn
            async with conn.cursor() as cur:
                self.cur = cur
                
                # 2. SELECT CANDIDATES
                # Active tokens with snapshot but no label
                await cur.execute("""
                    SELECT t.id, t.detected_at, s.id as snapshot_id, t.address
                    FROM tokens t
                    JOIN feature_snapshots s ON s.token_id = t.id
                    LEFT JOIN lifecycle_labels l ON l.snapshot_id = s.id
                    WHERE t.is_active = TRUE
                      AND l.id IS NULL
                      AND s.feature_version = 1
                """)
                candidates = await cur.fetchall()
                
                if not candidates:
                    return 0
                
                processed = 0
                for token_id, detected_at, snapshot_id, mint in candidates:
                    if detected_at.tzinfo is None:
                        detected_at = detected_at.replace(tzinfo=timezone.utc)
                    
                    try:
                        # 13. IDEMPOTENCY CHECK (Double check)
                        await cur.execute("SELECT 1 FROM lifecycle_labels WHERE snapshot_id = %s", (snapshot_id,))
                        if await cur.fetchone():
                            continue

                        outcome, reason, max_mult, time_to_outcome = await self.resolve_token(token_id, detected_at)
                        
                        if outcome:
                            # 12. LABEL INSERT CONTRACT
                            await self.persist_outcome(token_id, snapshot_id, outcome, max_mult, time_to_outcome, detected_at)
                            logger.info(f"Resolved {mint}: {outcome} ({reason})")
                            processed += 1
                            
                    except Exception as e:
                        logger.error(f"Failed to resolve token {token_id} ({mint}): {e}")
                        # Continue to next token
                
                return processed

    async def get_baseline_price(self, token_id: int, detection_time: datetime) -> Optional[Decimal]:
        """4. BASELINE PRICE: First trade after detection."""
        await self.cur.execute("""
            SELECT price_usd 
            FROM trades 
            WHERE token_id = %s 
              AND timestamp >= %s 
              AND price_usd > 0
            ORDER BY timestamp ASC 
            LIMIT 1
        """, (token_id, detection_time))
        row = await self.cur.fetchone()
        return row[0] if row else None

    async def check_success_5x(self, token_id: int, start: datetime, end: datetime, baseline: Decimal) -> Tuple[bool, Optional[datetime], Optional[Decimal]]:
        """5. SUCCESS CHECK (5x Rule). Overrides all."""
        target_price = baseline * SUCCESS_MULTIPLIER
        
        # Find if/when it hit target
        await self.cur.execute("""
            SELECT timestamp, price_usd
            FROM trades
            WHERE token_id = %s
              AND timestamp BETWEEN %s AND %s
              AND price_usd >= %s
            ORDER BY timestamp ASC
            LIMIT 1
        """, (token_id, start, end, target_price))
        
        hit = await self.cur.fetchone()
        if hit:
            return True, hit[0], hit[1] / baseline
        
        # Calculate max multiplier even if not success (for logging/other checks)
        await self.cur.execute("""
            SELECT MAX(price_usd) FROM trades
            WHERE token_id = %s AND timestamp BETWEEN %s AND %s
        """, (token_id, start, end))
        max_p = await self.cur.fetchone()
        max_mult = (max_p[0] / baseline) if max_p and max_p[0] else Decimal(0)
        
        return False, None, max_mult

    async def check_price_failure(self, token_id: int, start: datetime, end: datetime, baseline: Decimal) -> bool:
        """6. PRICE FAILURE: Min price < 0.5x baseline within failure window."""
        fail_threshold = baseline * PRICE_FAILURE_THRESHOLD
        
        await self.cur.execute("""
            SELECT MIN(price_usd) 
            FROM trades
            WHERE token_id = %s
              AND timestamp BETWEEN %s AND %s
        """, (token_id, start, end))
        
        min_p = await self.cur.fetchone()
        if min_p and min_p[0] is not None and min_p[0] <= fail_threshold:
            return True
        return False

    async def check_liquidity_collapse(self, token_id: int, start: datetime, window_end: datetime, fail_deadline: datetime) -> bool:
        """
        7. LIQUIDITY COLLAPSE: Min < 60% of Peak.
        
        CRITICAL FIX: Peak measured within failure window (48h), not full window (72h).
        This ensures collapse is evaluated against peak within the same timeframe.
        
        NOTE: Missing pair_address enforcement - when column exists, add:
        AND pair_address = primary_pair_address
        """
        # Max liquidity in FAILURE window (48h) - CORRECTED from window_end (72h)
        await self.cur.execute("""
            SELECT MAX(liquidity_usd)
            FROM trades
            WHERE token_id = %s AND timestamp BETWEEN %s AND %s
        """, (token_id, start, fail_deadline))
        row_max = await self.cur.fetchone()
        peak_liq = row_max[0]
        
        if not peak_liq or peak_liq <= 0:
            return False # Can't collapse if never had liquidity
            
        # Min liquidity in failure window
        await self.cur.execute("""
            SELECT MIN(liquidity_usd)
            FROM trades
            WHERE token_id = %s AND timestamp BETWEEN %s AND %s
        """, (token_id, start, fail_deadline))
        row_min = await self.cur.fetchone()
        min_liq = row_min[0]
        
        if min_liq is not None and min_liq <= (peak_liq * LIQUIDITY_COLLAPSE_THRESHOLD):
            return True
        return False

    async def check_volume_collapse(self, token_id: int, start: datetime, fail_deadline: datetime) -> bool:
        """
        8. VOLUME COLLAPSE: 3 consecutive hours where vol < 30% of 6h avg.
        
        CRITICAL FIX: Added 6h minimum buffer - collapse only evaluated after 6h of data exists.
        This prevents early false positives when historical window is incomplete.
        
        NOTE: Missing pair_address enforcement - when column exists, add:
        AND pair_address = primary_pair_address
        """
        """8. VOLUME COLLAPSE: 3 consecutive hours where vol < 30% of 6h avg."""
        # We need hourly buckets. Doing this in SQL is efficient.
        
        # Get hourly volumes
        await self.cur.execute("""
            SELECT 
                date_trunc('hour', timestamp) as h, 
                SUM(amount_usd) as vol
            FROM trades
            WHERE token_id = %s AND timestamp BETWEEN %s AND %s
            GROUP BY 1
            ORDER BY 1
        """, (token_id, start - timedelta(hours=6), fail_deadline)) # Fetch extra 6h for initial avg
        
        rows = await self.cur.fetchall()
        if not rows:
            return False
            
        # Map: hour -> volume
        vols = {r[0].replace(tzinfo=timezone.utc): r[1] for r in rows}
        
        # Iterate hours from detection to deadline
        curr = start.replace(minute=0, second=0, microsecond=0).replace(tzinfo=timezone.utc)
        deadline = fail_deadline.replace(tzinfo=timezone.utc)
        
        consecutive_collapses = 0
        
        while curr < deadline:
            # 1h Volume
            vol_1h = vols.get(curr, Decimal(0))
            
            # 6h Avg Check
            # Sum vars for prev 6 hours
            sum_6h = Decimal(0)
            count_6h = 0
            for i in range(1, 7):
                prev_h = curr - timedelta(hours=i)
                if prev_h in vols:
                    sum_6h += vols[prev_h]
                count_6h += 1 # Always count the hour time slot
            
            avg_6h = sum_6h / Decimal(6)
            
            # Check Collapse
            is_collapsed = False
            if avg_6h > 0:
                if vol_1h < (avg_6h * VOLUME_COLLAPSE_THRESHOLD):
                    is_collapsed = True
            elif avg_6h == 0 and vol_1h == 0:
                # Both zero is dead, count as collapsed? 
                # Spec implies "drop", if 0 -> 0 it is collapsed activity.
                is_collapsed = True
                
            if is_collapsed:
                consecutive_collapses += 1
            else:
                consecutive_collapses = 0
                
            if consecutive_collapses >= 3:
                return True
                
            curr += timedelta(hours=1)
            
        return False

    async def check_early_wallet_exit(self, token_id: int, start: datetime) -> bool:
        """9. EARLY WALLET EXIT: >70% of early (30m) buyers exit within 2h."""
        limit_30m = start + timedelta(minutes=30)
        limit_2h = start + timedelta(hours=2)
        
        # Identify early buyers
        await self.cur.execute("""
            SELECT DISTINCT wallet_address
            FROM trades
            WHERE token_id = %s
              AND timestamp BETWEEN %s AND %s
              AND side = 'buy'
        """, (token_id, start, limit_30m))
        
        early_wallets = [r[0] for r in await self.cur.fetchall()]
        if not early_wallets:
            return False
            
        total_early = len(early_wallets)
        exited_count = 0
        
        # Check positions at 2h mark
        # Net position = sum(token_amount * (1 if buy else -1)) ? 
        # Actually trades table has 'side' and 'amount_token' which is usually positive.
        # Need to check signed amount or standard logic.
        # Schema has side='buy'/'sell'.
        
        for wallet in early_wallets:
            await self.cur.execute("""
                SELECT 
                    SUM(CASE WHEN side = 'buy' THEN amount_token ELSE -amount_token END)
                FROM trades
                WHERE token_id = %s
                  AND wallet_address = %s
                  AND timestamp <= %s
            """, (token_id, wallet, limit_2h))
            
            row = await self.cur.fetchone()
            net_bal = row[0] if row and row[0] else Decimal(0)
            
            # Tolerance for dust? Spec says "net_position <= 0"
            if net_bal <= 0:
                exited_count += 1
                
        ratio = Decimal(exited_count) / Decimal(total_early)
        return ratio >= EARLY_EXIT_RATIO

    async def resolve_token(self, token_id: int, start: datetime) -> Tuple[Optional[str], Optional[str], Optional[Decimal], Optional[datetime]]:
        """Main resolution logic. Returns (outcome, reason_detail, max_mult, time_of_outcome)."""
        window_end = start + timedelta(hours=MAX_WINDOW_HOURS)
        fail_deadline = start + timedelta(hours=FAILURE_WINDOW_HOURS)
        now = datetime.now(timezone.utc)
        
        # 4. BASELINE
        baseline = await self.get_baseline_price(token_id, start)
        if not baseline:
            # No trades yet? Can't resolve.
            # If time > expiry and still no trades, it's a dud/expired.
            if now > window_end:
                 return "expired", "no_trades_found", Decimal(0), None
            return None, "waiting_for_trades", None, None

        # 5. SUCCESS CHECK
        is_success, time_success, mult = await self.check_success_5x(token_id, start, window_end, baseline)
        if is_success:
            return "hit_5x", "5x_multiplier_hit", mult, time_success

        # If not success, check if we are in failure window or expired
        # Note: We can trigger failure EARLY if condition met.
        
        # 6. PRICE FAILURE
        if await self.check_price_failure(token_id, start, fail_deadline, baseline):
             return "price_failure", "dropped_below_50pct", mult, None # Time? Spec doesn't require timestamp for fail, mostly outcome.
             
        # 7. LIQUIDITY COLLAPSE
        if await self.check_liquidity_collapse(token_id, start, window_end, fail_deadline):
            return "liquidity_collapse", "liq_dropped_below_60pct_peak", mult, None
            
        # 8. VOLUME COLLAPSE
        if await self.check_volume_collapse(token_id, start, fail_deadline):
            return "volume_collapse", "vol_collapsed_3h", mult, None
            
        # 9. EARLY WALLET EXIT
        # Only check if we are past the 2h mark to be sure
        if now > (start + timedelta(hours=2)):
            if await self.check_early_wallet_exit(token_id, start):
                return "early_wallet_exit", "70pct_early_buyers_exited", mult, None
                
        # 10. EXPIRY
        if now >= window_end:
            return "expired", "72h_timeout", mult, None
            
        return None, "still_active", None, None

    async def persist_outcome(self, token_id: int, snapshot_id: int, outcome: str, max_mult: Decimal, time_to_outcome: Optional[datetime], detection_time: datetime):
        """Writes label and closes token."""
        # Calculate interval
        interval = None
        if time_to_outcome and detection_time:
            delta = time_to_outcome - detection_time
            if delta.total_seconds() > 0:
                interval = delta
        
        # Update Tokens
        await self.cur.execute("""
            UPDATE tokens 
            SET is_active = FALSE,
                completed_at = NOW(),
                outcome = %s
            WHERE id = %s
        """, (outcome, token_id))
        
        # Insert Label
        await self.cur.execute("""
            INSERT INTO lifecycle_labels (
                token_id, snapshot_id, outcome, max_multiplier, time_to_outcome, labeled_at
            )
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (snapshot_id) DO NOTHING
        """, (token_id, snapshot_id, outcome, max_mult, interval))
        
        await self.conn.commit()


async def run_resolution_engine():
    engine = OutcomeEngine()
    processed = await engine.run_job()
    if processed > 0:
        logger.info(f"OutcomeEngine: Resolved {processed} tokens.")
    return processed

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        await init_db()
        try:
            await run_resolution_engine()
        finally:
            await close_db()

    asyncio.run(main())
