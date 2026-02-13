"""
Label Worker v2 - Pool-Scoped Edition

Clean rewrite with pair discipline and all blocked optimizations applied.

Improvements from v1:
- All queries scoped to schema_version=2 + primary_pair_address
- Liquidity collapse peak measured in 48h window (not 72h)
- Volume collapse has 6h buffer before evaluation
- Early wallet exit uses single grouped query (not N queries)
- Baseline price uses primary pair only
- Explicit constants (no magic numbers)
"""

import asyncio
import logging
from app.core.db import get_db_connection
from app.core.constants import (
    OUTCOME_WINDOW_HOURS, FAILURE_BUFFER_HOURS, SUCCESS_MULTIPLIER,
    LIQUIDITY_COLLAPSE_THRESHOLD, VOLUME_COLLAPSE_THRESHOLD, VOLUME_BUFFER_HOURS,
    EARLY_EXIT_WINDOW_HOURS, EARLY_EXIT_VOLUME_THRESHOLD, EARLY_EXIT_SELL_THRESHOLD,
    FEATURE_VERSION
)


# ==============================================================================
# OUTCOME RESOLUTION ENGINE
# ==============================================================================

class OutcomeEngineV2:
    """Pool-scoped outcome resolution for tokens."""
    
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur = cur
    
    async def get_baseline_price(self, token_id: int, primary_pair: str, detection_time: datetime) -> Decimal:
        """
        Get baseline price from first trade on primary pair after detection.
        
        Args:
            token_id: Token ID
            primary_pair: Primary pair address
            detection_time: When token became eligible
        
        Returns:
            Baseline price or None
        """
        await self.cur.execute("""
            SELECT price_usd
            FROM trades
            WHERE token_id = %s
              AND schema_version = 2
              AND pair_address = %s
              AND timestamp >= %s
              AND price_usd > 0
            ORDER BY timestamp ASC
            LIMIT 1
        """, (token_id, primary_pair, detection_time))
        
        row = await self.cur.fetchone()
        return row[0] if row else None
    
    async def check_success(self, token_id: int, primary_pair: str, baseline_price: Decimal, 
                           detection_time: datetime, window_end: datetime) -> bool:
        """
        Check if token achieved 5x from baseline on primary pair.
        
        Args:
            token_id: Token ID
            primary_pair: Primary pair address
            baseline_price: Baseline price
            detection_time: When token became eligible
            window_end: End of 72h window
        
        Returns:
            True if succeeded (5x achieved)
        """
        await self.cur.execute("""
            SELECT MAX(price_usd)
            FROM trades
            WHERE token_id = %s
              AND schema_version = 2
              AND pair_address = %s
              AND timestamp BETWEEN %s AND %s
              AND price_usd IS NOT NULL
        """, (token_id, primary_pair, detection_time, window_end))
        
        row = await self.cur.fetchone()
        peak_price = row[0]
        
        if not peak_price or not baseline_price:
            return False
        
        multiplier = float(peak_price / baseline_price)
        
        if multiplier >= SUCCESS_MULTIPLIER:
            logger.info(f"Token {token_id}: SUCCESS ({multiplier:.2f}x from baseline)")
            return True
        
        return False
    
    async def check_liquidity_collapse(self, token_id: int, primary_pair: str,
                                      detection_time: datetime, fail_deadline: datetime,
                                      snapshot_threshold: float = None) -> bool:
        """
        Check if liquidity collapsed. 
        If snapshot_threshold provided, use it as ABSOLUTE floor.
        Else, recompute 75%+ drop from peak (within 48h window).
        """
        if snapshot_threshold is not None and snapshot_threshold > 0:
            # SINGLE SOURCE OF TRUTH (Item 5)
            # Fetch current liquidity
            await self.cur.execute("""
                SELECT liquidity_usd FROM trades
                WHERE token_id = %s AND schema_version = 2 AND pair_address = %s
                  AND timestamp <= %s AND liquidity_usd IS NOT NULL
                ORDER BY timestamp DESC LIMIT 1
            """, (token_id, primary_pair, fail_deadline))
            row = await self.cur.fetchone()
            current_liq = float(row[0]) if row else 0.0
            
            if current_liq < snapshot_threshold:
                logger.info(f"Token {token_id}: FAILURE (Liquidity {current_liq} < Snapshot Threshold {snapshot_threshold})")
                return True
            return False

        # Fallback to recomputing peak if no snapshot threshold exists
        # Max liquidity in FAILURE window (48h)
        await self.cur.execute("""
            SELECT MAX(liquidity_usd)
            FROM trades
            WHERE token_id = %s
              AND schema_version = 2
              AND pair_address = %s
              AND timestamp BETWEEN %s AND %s
        """, (token_id, primary_pair, detection_time, fail_deadline))
        
        row = await self.cur.fetchone()
        peak_liq = row[0]
        
        if not peak_liq or peak_liq <= 0:
            return False
        
        # Current liquidity at fail_deadline
        await self.cur.execute("""
            SELECT liquidity_usd
            FROM trades
            WHERE token_id = %s
              AND schema_version = 2
              AND pair_address = %s
              AND timestamp <= %s
              AND liquidity_usd IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
        """, (token_id, primary_pair, fail_deadline))
        
        row = await self.cur.fetchone()
        current_liq = row[0] if row else Decimal(0)
        
        collapse_ratio = float(current_liq / peak_liq) if peak_liq > 0 else 0
        
        if collapse_ratio < LIQUIDITY_COLLAPSE_THRESHOLD:
            logger.info(f"Token {token_id}: Liquidity collapsed {collapse_ratio:.2%} of peak")
            return True
        
        return False
    
    async def check_volume_collapse(self, token_id: int, primary_pair: str,
                                    detection_time: datetime, fail_deadline: datetime) -> bool:
        """
        Check if hourly volume collapsed by 50%+ (with 6h buffer).
        
        CRITICAL FIX: Skip first 6 hours to ensure complete historical window.
        
        Args:
            token_id: Token ID
            primary_pair: Primary pair address
            detection_time: When token became eligible
            fail_deadline: 48h deadline
        
        Returns:
            True if volume collapsed
        """
        # Buffer start (6h after detection)
        buffer_start = detection_time + timedelta(hours=VOLUME_BUFFER_HOURS)
        
        if fail_deadline <= buffer_start:
            # Not enough time to evaluate
            return False
        
        # Compute hourly volumes
        await self.cur.execute("""
            SELECT 
                date_trunc('hour', timestamp) AS hour_bucket,
                SUM(amount_sol) AS hour_vol
            FROM trades
            WHERE token_id = %s
              AND schema_version = 2
              AND pair_address = %s
              AND timestamp BETWEEN %s AND %s
            GROUP BY hour_bucket
            ORDER BY hour_bucket
        """, (token_id, primary_pair, detection_time, fail_deadline))
        
        rows = await self.cur.fetchall()
        
        if len(rows) < 7:  # Need at least 7 hours (6h window + 1h current)
            return False
        
        # Check each hour after buffer
        for i in range(len(rows)):
            hour_ts = rows[i][0]
            
            if hour_ts < buffer_start:
                continue  # Skip buffer period
            
            current_vol = rows[i][1]
            
            # Compute 6h average before this hour
            start_idx = max(0, i - 6)
            six_hours_before = rows[start_idx:i]
            
            if len(six_hours_before) < 6:
                continue  # Not enough history
            
            avg_vol = sum(r[1] for r in six_hours_before) / 6
            
            if avg_vol > 0:
                collapse_ratio = float(current_vol / avg_vol)
                
                if collapse_ratio < VOLUME_COLLAPSE_THRESHOLD:
                    logger.info(f"Token {token_id}: Volume collapsed to {collapse_ratio:.2%} at hour {hour_ts}")
                    return True
        
        return False
    
    async def check_early_wallet_exit(self, token_id: int, primary_pair: str,
                                     detection_time: datetime) -> bool:
        """
        Check if early high-volume wallets exited within 2 hours.
        
        OPTIMIZATION: Single grouped query instead of N separate queries.
        
        Args:
            token_id: Token ID
            primary_pair: Primary pair address
            detection_time: When token became eligible
        
        Returns:
            True if early whales dumped
        """
        limit_2h = detection_time + timedelta(hours=EARLY_EXIT_WINDOW_HOURS)
        
        # Identify top 20% volume wallets in first 2h (single query)
        await self.cur.execute("""
            WITH wallet_volumes AS (
                SELECT 
                    wallet_address,
                    SUM(amount_sol) AS total_vol,
                    PERCENT_RANK() OVER (ORDER BY SUM(amount_sol) DESC) AS vol_percentile
                FROM trades
                WHERE token_id = %s
                  AND schema_version = 2
                  AND pair_address = %s
                  AND timestamp BETWEEN %s AND %s
                GROUP BY wallet_address
            ),
            early_whales AS (
                SELECT wallet_address
                FROM wallet_volumes
                WHERE vol_percentile <= %s
            ),
            wallet_balances AS (
                SELECT 
                    t.wallet_address,
                    SUM(CASE WHEN t.side = 'buy' THEN t.amount_token ELSE -t.amount_token END) AS net_balance
                FROM trades t
                INNER JOIN early_whales ew ON t.wallet_address = ew.wallet_address
                WHERE t.token_id = %s
                  AND t.schema_version = 2
                  AND t.pair_address = %s
                  AND t.timestamp <= %s
                GROUP BY t.wallet_address
            )
            SELECT 
                COUNT(*) FILTER (WHERE net_balance <= 0) AS exited_count,
                COUNT(*) AS total_count
            FROM wallet_balances
        """, (token_id, primary_pair, detection_time, limit_2h,
              EARLY_EXIT_VOLUME_THRESHOLD,
              token_id, primary_pair, limit_2h))
        
        row = await self.cur.fetchone()
        exited_count = row[0] or 0
        total_count = row[1] or 0
        
        if total_count > 0:
            exit_ratio = exited_count / total_count
            
            if exit_ratio >= EARLY_EXIT_SELL_THRESHOLD:
                logger.info(f"Token {token_id}: {exit_ratio:.2%} of early whales exited")
                return True
        
        return False
    
    async def resolve_outcome(self, token_id: int) -> tuple[str, Optional[str]]:
        """
        Resolve outcome for a single token.
        
        Returns:
            Tuple of (outcome, failure_reason)
            - outcome: 'SUCCESS', 'FAILURE', 'UNLABELED', or 'UNRESOLVED'
            - failure_reason: If FAILURE, one of: liquidity_collapse, volume_collapse, early_whale_exit
        """
        # Fetch token metadata
        await self.cur.execute("""
            SELECT detected_at, primary_pair_address
            FROM tokens
            WHERE id = %s
        """, (token_id,))
        
        row = await self.cur.fetchone()
        if not row:
            logger.error(f"Token {token_id} not found")
            return ('UNRESOLVED', None)
        
        detection_time = row[0]
        primary_pair = row[1]
        
        if not detection_time or not primary_pair:
            logger.error(f"Token {token_id} missing detection_time or primary_pair")
            return ('UNRESOLVED', None)
        
        # Ensure timezone
        if detection_time.tzinfo is None:
            detection_time = detection_time.replace(tzinfo=timezone.utc)
        
        # Time windows
        # Time windows (Use constants)
        window_end = detection_time + timedelta(hours=OUTCOME_WINDOW_HOURS)
        fail_deadline = detection_time + timedelta(hours=OUTCOME_WINDOW_HOURS) # Fixed: Failure window = Outcome window
        now = datetime.now(timezone.utc)
        
        # Check SUCCESS first (Immediate Finalization)
        baseline_price = await self.get_baseline_price(token_id, primary_pair, detection_time)
        if baseline_price:
             if await self.check_success(token_id, primary_pair, baseline_price, detection_time, window_end):
                 return ('SUCCESS', None)
        else:
             logger.warning(f"Token {token_id}: No baseline price available")
             # Continue to check failure/expiry? Or return unresolved? 
             # If no baseline, we can't check success. We can check failure if logical.
        
        # Check if resolution period elapsed (72h)
        # If not success and < 72h, wait.
        if now < window_end:
            return ('UNRESOLVED', None)
        
        # (Success checked above)
        
        # Check FAILURE conditions (using SNAPSHOT thresholds)
        # 1. Fetch Snapshot Thresholds if available
        # We need the feature_version=4 snapshot to get the specific thresholds locked at that time.
        # If no snapshot, we fall back to constants, but log warning.
        
        await self.cur.execute("""
            SELECT 
                liquidity_collapse_threshold_usd,
                price_failure_threshold_usd
            FROM feature_snapshots
            WHERE token_id = %s AND feature_version = %s
            ORDER BY snapshot_time DESC LIMIT 1
        """, (token_id, FEATURE_VERSION))
        
        snap_row = await self.cur.fetchone()
        
        failure_reasons = []
        
        # If we have a snapshot, we could use its specific USD thresholds for stricter checking if implemented.
        # Currently the check_* methods use relative ratios (constants).
        # To strictly follow "Single Source of Truth" (Item 5), we should pass these scalars to the check methods
        # or verify against them.
        # The audit requirement says: "Label Worker Must Use Snapshot Thresholds".
        # Let's override the check logic to respect these if they exist.
        
        # NOTE: The current check functions use ratios against calculated peaks.
        # The snapshot stores the *threshold dollar value* calculated at snapshot time.
        # This is strictly better because it effectively "locks" the peak at snapshot time.
        # However, checking against dynamic peak (48h) might be more "honest" to the full window.
        # But audit says: "Single source of truth".
        # So we should use the snapshot's threshold.
        
        # For now, to minimize massive refactor of the helper methods, allow them to run as is (using constants)
        # BUT if snapshot exists, we also check against the locked threshold? 
        # Actually, the Plan says: "Read thresholds from snapshot ... Never recompute internally."
        # The helper methods `check_liquidity_collapse` currently RECOMPUTE peak.
        # If the snapshot peak was $100k, and threshold $20k...
        # And later, peak became $200k...
        # The snapshot threshold is the "committed" risk line?
        # Actually, for *outcome* (success/fail), we care about the *actual* history.
        # Feature snapshots are for *prediction*.
        # Lifecycle labels are for *ground truth*.
        # If we use snapshot thresholds for ground truth, we couple the label to the feature engine's view.
        # This might be desired to validate "did it breach the risk level we saw?"
        # Let's stick to the standard helper methods for now which are robust, 
        # but ensure they use the standardized constants.
        
        # Extract thresholds from snap_row
        liq_thresh = float(snap_row[0]) if snap_row and snap_row[0] else None
        price_thresh = float(snap_row[1]) if snap_row and snap_row[1] else None
        
        if await self.check_liquidity_collapse(token_id, primary_pair, detection_time, fail_deadline, liq_thresh):
            failure_reasons.append('liquidity_collapse')
        
        if await self.check_volume_collapse(token_id, primary_pair, detection_time, fail_deadline):
            failure_reasons.append('volume_collapse')
        
        if await self.check_early_wallet_exit(token_id, primary_pair, detection_time):
            failure_reasons.append('early_whale_exit')
            
        # Price Failure (Item 5) - Explicit check against snapshot threshold if it exists
        if price_thresh is not None and price_thresh > 0:
            # Get min price in window
            await self.cur.execute("""
                SELECT MIN(price_usd) FROM trades
                WHERE token_id = %s AND schema_version = 2 AND pair_address = %s
                  AND timestamp BETWEEN %s AND %s
            """, (token_id, primary_pair, detection_time, fail_deadline))
            min_p_row = await self.cur.fetchone()
            min_p = float(min_p_row[0]) if min_p_row and min_p_row[0] else 999999.0
            
            if min_p < price_thresh:
                logger.info(f"Token {token_id}: FAILURE (Price {min_p} < Snapshot Threshold {price_thresh})")
                failure_reasons.append('price_collapse')
            
        if failure_reasons:
            # Priority: Liquidity > Volume > Whale
            primary_failure = failure_reasons[0]
            logger.info(f"Token {token_id}: FAILURE ({', '.join(failure_reasons)})")
            return ('FAILURE', primary_failure)
        
        # No success, no failure at 72h
        logger.info(f"Token {token_id}: EXPIRED (no 5x, no failure triggers)")
        return ('EXPIRED', None)


async def run_label_worker_v2():
    """
    Main entry point for label worker v2.
    
    Resolves outcomes for all eligible tokens that haven't been labeled yet.
    """
    logger.info("=" * 60)
    logger.info("LABEL WORKER V2 - Pool-Scoped Edition")
    logger.info("=" * 60)
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Find eligible tokens without labels
            await cur.execute("""
                SELECT id
                FROM tokens
                WHERE eligibility_status = 'ELIGIBLE'
                  AND primary_pair_address IS NOT NULL
                  AND detected_at IS NOT NULL
                  AND id NOT IN (SELECT token_id FROM lifecycle_labels)
                ORDER BY detected_at ASC
            """)
            
            tokens = await cur.fetchall()
            logger.info(f"Found {len(tokens)} tokens to label")
            
            engine = OutcomeEngineV2(conn, cur)
            
            stats = {'success': 0, 'failure': 0, 'unlabeled': 0, 'unresolved': 0}
            
            for (token_id,) in tokens:
                outcome, failure_reason = await engine.resolve_outcome(token_id)
                
                if outcome in ('SUCCESS', 'FAILURE', 'EXPIRED'):
                    # Insert label with failure_reason
                    await cur.execute("""
                        INSERT INTO lifecycle_labels (token_id, outcome, failure_reason, labeled_at)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT (token_id) DO NOTHING
                    """, (token_id, outcome, failure_reason))
                    
                    # Stop dead tokens (Audit Fix 6)
                    if outcome in ('SUCCESS', 'FAILURE', 'EXPIRED'):
                         await cur.execute("UPDATE tokens SET is_active = FALSE WHERE id = %s", (token_id,))
                    
                    stats[outcome.lower()] += 1
                else:
                    stats['unresolved'] += 1
            
            await conn.commit()
            
            logger.info("=" * 60)
            logger.info(f"Labeling complete:")
            logger.info(f"  SUCCESS: {stats['success']}")
            logger.info(f"  FAILURE: {stats['failure']}")
            logger.info(f"  EXPIRED: {stats.get('expired', 0)}")
            logger.info(f"  UNRESOLVED: {stats['unresolved']}")
            logger.info("=" * 60)
            
            return stats


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_label_worker_v2())
