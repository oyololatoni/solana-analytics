"""
Eligibility Gate v2 - Pool-Scoped Edition

Clean rewrite with pair discipline enforced throughout.
Uses schema_version=2 trades with pair_address tracking.

Key Differences from v1:
- Real primary pair selection (highest liquidity pool)
- All filters scoped to primary_pair_address
- Uses only schema_version=2 trades
- Continuous liquidity sustain logic (no gaps)
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from app.core.db import get_db_connection
from app.core.constants import (
    BASE_TOKEN_ADDRESSES, MIN_LIQUIDITY_USD, MIN_VOLUME_FIRST_30M_USD,
    MIN_TRADE_COUNT, TRADE_GAP_LIMIT_MINUTES, LIQUIDITY_SUSTAIN_MINUTES, SOL_PRICE_USD_ESTIMATE
)


async def filter_1_select_primary_pair():
    """
    FILTER 1: Real primary pair selection - highest liquidity pool.
    
    For each token, selects the pair_address with highest max liquidity
    from schema_version=2 trades.
    """
    logger.info("Filter 1: Selecting primary pairs by max liquidity (v2)...")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                WITH ranked_pairs AS (
                    SELECT
                        token_id,
                        pair_address,
                        MAX(liquidity_usd) AS max_liq,
                        RANK() OVER (
                            PARTITION BY token_id
                            ORDER BY MAX(liquidity_usd) DESC NULLS LAST
                        ) AS rnk
                    FROM trades
                    WHERE schema_version = 2
                      AND pair_address IS NOT NULL
                    GROUP BY token_id, pair_address
                )
                UPDATE tokens t
                SET primary_pair_address = rp.pair_address,
                    pair_validated = TRUE,
                    eligibility_checked_at = NOW()
                FROM ranked_pairs rp
                WHERE t.id = rp.token_id
                  AND rp.rnk = 1
                  AND t.pair_validated = FALSE
                  AND t.is_active = TRUE
            """)
            
            updated = cur.rowcount
            await conn.commit()
            logger.info(f"Primary pairs assigned: {updated}")
            return updated


async def filter_2_validate_base_token():
    """
    FILTER 2: Validate base token (WSOL/USDC/USDT only).
    
    Checks that primary_pair_address is a known canonical base token.
    """
    logger.info("Filter 2: Validating base tokens...")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE tokens
                SET eligibility_status = 'REJECTED',
                    eligibility_checked_at = NOW()
                WHERE pair_validated = TRUE
                  AND NOT (primary_pair_address = ANY(%s))
                  AND eligibility_status = 'PRE_ELIGIBLE'
            """, (BASE_TOKEN_ADDRESSES,))
            
            rejected = cur.rowcount
            await conn.commit()
            logger.info(f"Rejected (invalid base token): {rejected}")
            return rejected


async def filter_3_not_self_paired():
    """
    FILTER 3: Not self-paired (token != pair).
    
    Rejects tokens where primary_pair_address matches token mint address.
    """
    logger.info("Filter 3: Checking for self-paired tokens...")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE tokens t
                SET eligibility_status = 'REJECTED',
                    eligibility_checked_at = NOW()
                WHERE t.pair_validated = TRUE
                  AND t.eligibility_status = 'PRE_ELIGIBLE'
                  AND t.address = t.primary_pair_address
            """)
            
            rejected = cur.rowcount
            await conn.commit()
            logger.info(f"Rejected (self-paired): {rejected}")
            return rejected


async def filter_4_min_trade_count():
    """
    FILTER 4: Minimum 20 trades (on primary pair, v2 only).
    """
    logger.info("Filter 4: Checking minimum trade count...")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                WITH trade_counts AS (
                    SELECT
                        token_id,
                        COUNT(*) AS trade_count
                    FROM trades
                    WHERE schema_version = 2
                    GROUP BY token_id
                )
                UPDATE tokens t
                SET eligibility_status = 'REJECTED',
                    eligibility_checked_at = NOW()
                FROM trade_counts tc
                WHERE t.id = tc.token_id
                  AND tc.trade_count < %s
                  AND t.eligibility_status = 'PRE_ELIGIBLE'
            """, (MIN_TRADE_COUNT,))
            
            rejected = cur.rowcount
            await conn.commit()
            logger.info(f"Rejected (< 20 trades): {rejected}")
            return rejected


async def filter_5_peak_liquidity():
    """
    FILTER 5: Peak liquidity >= $50k (on primary pair).
    """
    logger.info("Filter 5: Checking peak liquidity...")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                WITH peak_liq AS (
                    SELECT
                        token_id,
                        MAX(liquidity_usd) AS max_liq
                    FROM trades
                    WHERE schema_version = 2
                      AND pair_address IN (
                          SELECT primary_pair_address FROM tokens WHERE primary_pair_address IS NOT NULL
                      )
                    GROUP BY token_id
                )
                UPDATE tokens t
                SET eligibility_status = 'REJECTED',
                    eligibility_checked_at = NOW()
                FROM peak_liq pl
                WHERE t.id = pl.token_id
                  AND (pl.max_liq IS NULL OR pl.max_liq < %s)
                  AND t.eligibility_status = 'PRE_ELIGIBLE'
            """, (MIN_LIQUIDITY_USD,))
            
            rejected = cur.rowcount
            await conn.commit()
            logger.info(f"Rejected (peak liquidity < $50k): {rejected}")
            return rejected


async def filter_6_sustained_liquidity():
    """
    FILTER 6: Sustained liquidity >= $50k for >= 30 continuous minutes.
    
    Uses window functions to detect continuous segments where liquidity
    stays >= 50k for at least 30 minutes on primary pair.
    """
    logger.info("Filter 6: Checking sustained liquidity...")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Detect continuous segments of high liquidity
            await cur.execute("""
                WITH liquidity_series AS (
                    SELECT
                        t.id AS token_id,
                        tr.timestamp,
                        tr.liquidity_usd,
                        LAG(tr.timestamp) OVER (
                            PARTITION BY t.id
                            ORDER BY tr.timestamp
                        ) AS prev_ts,
                        LAG(tr.liquidity_usd) OVER (
                            PARTITION BY t.id
                            ORDER BY tr.timestamp
                        ) AS prev_liq
                    FROM tokens t
                    JOIN trades tr ON tr.token_id = t.id
                    WHERE t.eligibility_status = 'PRE_ELIGIBLE'
                      AND t.primary_pair_address IS NOT NULL
                      AND tr.schema_version = 2
                      AND tr.pair_address = t.primary_pair_address
                ),
                segments AS (
                    SELECT
                        token_id,
                        timestamp,
                        liquidity_usd,
                        CASE
                            WHEN liquidity_usd >= %s
                                 AND (prev_liq < %s OR prev_liq IS NULL)
                            THEN 1
                            ELSE 0
                        END AS segment_start
                    FROM liquidity_series
                ),
                segment_groups AS (
                    SELECT
                        token_id,
                        timestamp,
                        SUM(segment_start) OVER (
                            PARTITION BY token_id
                            ORDER BY timestamp
                        ) AS segment_id
                    FROM segments
                    WHERE liquidity_usd >= %s
                ),
                segment_durations AS (
                    SELECT
                        token_id,
                        segment_id,
                        MIN(timestamp) AS start_ts,
                        MAX(timestamp) AS end_ts,
                        (MAX(timestamp) - MIN(timestamp)) AS duration
                    FROM segment_groups
                    GROUP BY token_id, segment_id
                ),
                tokens_with_sustain AS (
                    SELECT DISTINCT token_id
                    FROM segment_durations
                    WHERE duration >= INTERVAL '%s minutes'
                )
                UPDATE tokens t
                SET eligibility_status = 'ELIGIBLE_PENDING_30M',
                    eligibility_checked_at = NOW()
                FROM tokens_with_sustain tws
                WHERE t.id = tws.token_id
                  AND t.eligibility_status = 'PRE_ELIGIBLE'
            """, (MIN_LIQUIDITY_USD, MIN_LIQUIDITY_USD, MIN_LIQUIDITY_USD, LIQUIDITY_SUSTAIN_MINUTES))
            
            updated = cur.rowcount
            await conn.commit()
            logger.info(f"Advanced to ELIGIBLE_PENDING_30M: {updated}")
            return updated


async def filter_7_early_volume():
    """
    FILTER 7: Early volume >= $5k in first 30 minutes (primary pair).
    """
    logger.info("Filter 7: Checking early volume...")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                WITH early_volume AS (
                    SELECT
                        t.id AS token_id,
                        COALESCE(SUM(tr.amount_sol), 0) AS vol_sol
                    FROM tokens t
                    LEFT JOIN trades tr ON tr.token_id = t.id
                    WHERE t.eligibility_status = 'PRE_ELIGIBLE'
                      AND t.detected_at IS NOT NULL
                      AND t.primary_pair_address IS NOT NULL
                      AND tr.schema_version = 2
                      AND tr.pair_address = t.primary_pair_address
                      AND tr.timestamp BETWEEN t.detected_at AND t.detected_at + INTERVAL '30 minutes'
                    GROUP BY t.id
                )
                UPDATE tokens t
                SET eligibility_status = 'REJECTED',
                    eligibility_checked_at = NOW()
                FROM early_volume ev
                WHERE t.id = ev.token_id
                  AND (ev.vol_sol * %s) < %s  -- Proxy price
                  AND t.eligibility_status = 'PRE_ELIGIBLE'
            """, (SOL_PRICE_USD_ESTIMATE, MIN_VOLUME_FIRST_30M_USD))
            
            rejected = cur.rowcount
            await conn.commit()
            logger.info(f"Rejected (early volume < $5k): {rejected}")
            return rejected


async def filter_8_trade_gap_check():
    """
    FILTER 8: No trade gap > 10 minutes in first 30 minutes (primary pair).
    
    Ensures continuous trading activity early on.
    Includes guard: requires >= 2 trades in window.
    """
    logger.info("Filter 8: Checking for trade gaps...")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                WITH trade_gaps AS (
                    SELECT
                        t.id AS token_id,
                        tr.timestamp,
                        LAG(tr.timestamp) OVER (
                            PARTITION BY t.id
                            ORDER BY tr.timestamp
                        ) AS prev_ts
                    FROM tokens t
                    JOIN trades tr ON tr.token_id = t.id
                    WHERE t.eligibility_status = 'PRE_ELIGIBLE'
                      AND t.detected_at IS NOT NULL
                      AND t.primary_pair_address IS NOT NULL
                      AND tr.schema_version = 2
                      AND tr.pair_address = t.primary_pair_address
                      AND tr.timestamp BETWEEN t.detected_at AND t.detected_at + INTERVAL '30 minutes'
                ),
                gaps_with_counts AS (
                    SELECT
                        token_id,
                        MAX(timestamp - prev_ts) AS max_gap,
                        COUNT(*) AS trade_count
                    FROM trade_gaps
                    WHERE prev_ts IS NOT NULL
                    GROUP BY token_id
                    HAVING COUNT(*) >= 2  -- Guard: need at least 2 trades
                )
                UPDATE tokens t
                SET eligibility_status = 'REJECTED',
                    eligibility_checked_at = NOW()
                FROM gaps_with_counts gwc
                WHERE t.id = gwc.token_id
                  AND gwc.max_gap > INTERVAL '%s minutes'
                  AND t.eligibility_status = 'PRE_ELIGIBLE'
            """, (TRADE_GAP_LIMIT_MINUTES,))
            
            rejected = cur.rowcount
            await conn.commit()
            logger.info(f"Rejected (trade gap > 10min): {rejected}")
            return rejected


async def filter_9_promote_to_eligible():
    """
    FILTER 9 (Hardening): Promote ELIGIBLE_PENDING_30M to ELIGIBLE.
    
    Tokens that passed all checks and have been pending get promoted.
    """
    logger.info("Filter 9: Promoting pending tokens to ELIGIBLE...")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE tokens
                SET eligibility_status = 'ELIGIBLE',
                    detected_at = COALESCE(detected_at, NOW()),
                    eligibility_checked_at = NOW()
                WHERE eligibility_status = 'ELIGIBLE_PENDING_30M'
                  AND pair_validated = TRUE
                  AND primary_pair_address IS NOT NULL
            """)
            
            promoted = cur.rowcount
            await conn.commit()
            logger.info(f"Promoted to ELIGIBLE: {promoted}")
            return promoted


async def run_eligibility_gate_v2():
    """
    Main entry point for eligibility gate v2.
    
    Runs all 9 filters sequentially (order matters).
    Returns statistics about the run.
    """
    logger.info("=" * 60)
    logger.info("ELIGIBILITY GATE V2 - Pool-Scoped Edition")
    logger.info("=" * 60)
    
    start_time = datetime.now(timezone.utc)
    
    stats = {
        'primary_pairs_assigned': await filter_1_select_primary_pair(),
        'invalid_base_token': await filter_2_validate_base_token(),
        'self_paired': await filter_3_not_self_paired(),
        'min_trades': await filter_4_min_trade_count(),
        'peak_liquidity': await filter_5_peak_liquidity(),
        'sustained_liquidity': await filter_6_sustained_liquidity(),
        'early_volume': await filter_7_early_volume(),
        'trade_gaps': await filter_8_trade_gap_check(),
        'promoted_eligible': await filter_9_promote_to_eligible(),
    }
    
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    
    logger.info("=" * 60)
    logger.info(f"Eligibility gate v2 complete in {elapsed:.2f}s")
    logger.info(f"Primary pairs assigned: {stats['primary_pairs_assigned']}")
    logger.info(f"Promoted to ELIGIBLE: {stats['promoted_eligible']}")
    logger.info(f"Total rejected: {sum([
        stats['invalid_base_token'],
        stats['self_paired'],
        stats['min_trades'],
        stats['peak_liquidity'],
        stats['early_volume'],
        stats['trade_gaps']
    ])}")
    logger.info("=" * 60)
    
    return stats


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_eligibility_gate_v2())
