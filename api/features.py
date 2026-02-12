
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json

from api.db import get_db_connection

logger = logging.getLogger("solana-analytics")

async def compute_snapshot(token_id: int):
    """
    Computes a feature snapshot for the given token ID.
    Aggregates data from trades, wallet_token_interactions, and (future) liquidity_events.
    """
    logger.info(f"Computing snapshot for token_id={token_id}")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            now = datetime.now(timezone.utc)
            
            # 1. Volume Acceleration (1h vs 6h) - Using SOL Amount
            # Fetch Volume 1h
            await cur.execute("""
                SELECT SUM(amount_sol) 
                FROM trades 
                WHERE token_id = %s AND timestamp > %s
            """, (token_id, now - timedelta(hours=1)))
            row = await cur.fetchone()
            vol_1h = row[0] or Decimal(0)

            # Fetch Volume 6h (avg per hour)
            await cur.execute("""
                SELECT SUM(amount_sol) 
                FROM trades 
                WHERE token_id = %s AND timestamp > %s AND timestamp <= %s
            """, (token_id, now - timedelta(hours=7), now - timedelta(hours=1)))
            row = await cur.fetchone()
            vol_6h = row[0] or Decimal(0)
            vol_6h_avg = vol_6h / 6 if vol_6h > 0 else Decimal(0)
            
            vol_accel = (vol_1h / vol_6h_avg) if vol_6h_avg > 0 else (Decimal(100) if vol_1h > 0 else Decimal(0))

            # 2. Buy/Sell Ratio (1h) - Using SOL Amount
            await cur.execute("""
                SELECT 
                    SUM(CASE WHEN side = 'buy' THEN amount_sol ELSE 0 END),
                    SUM(CASE WHEN side = 'sell' THEN amount_sol ELSE 0 END)
                FROM trades
                WHERE token_id = %s AND timestamp > %s
            """, (token_id, now - timedelta(hours=1)))
            row = await cur.fetchone()
            buy_vol = row[0] or Decimal(0)
            sell_vol = row[1] or Decimal(0)
            buy_sell_ratio = (buy_vol / sell_vol) if sell_vol > 0 else (Decimal(100) if buy_vol > 0 else Decimal(0))

            # 3. Holder Growth Rate (1h)
            # Count wallets with first_interaction < 1h ago vs prior
            await cur.execute("""
                SELECT COUNT(*) FROM wallet_token_interactions WHERE token_id = %s AND first_interaction <= %s
            """, (token_id, now - timedelta(hours=1)))
            count_prev = (await cur.fetchone())[0]

            await cur.execute("""
                SELECT COUNT(*) FROM wallet_token_interactions WHERE token_id = %s
            """, (token_id,))
            count_now = (await cur.fetchone())[0]
            
            holder_growth = ((Decimal(count_now) - Decimal(count_prev)) / Decimal(count_prev)) if count_prev > 0 else Decimal(0)

            # 4. Early Wallet Retention
            # Get first 100 wallets
            await cur.execute("""
                SELECT id, last_balance_token 
                FROM wallet_token_interactions 
                WHERE token_id = %s
                ORDER BY first_interaction ASC
                LIMIT 100
            """, (token_id,))
            early_wallets = await cur.fetchall()
            
            if early_wallets:
                retained = sum(1 for w in early_wallets if w[1] > 0)
                retention_rate = Decimal(retained) / Decimal(len(early_wallets))
                
                # Net Accumulation
                net_accumulation = sum(w[1] for w in early_wallets)
            else:
                retention_rate = Decimal(0)
                net_accumulation = Decimal(0)

            # 5. Volatility (StdDev of Price: SOL/Token)
            # Fetch last 100 trades to compute StdDev of price
            await cur.execute("""
                SELECT amount_sol, amount_token 
                FROM trades 
                WHERE token_id = %s AND amount_sol > 0 AND amount_token > 0
                ORDER BY timestamp DESC LIMIT 100
            """, (token_id,))
            trades = await cur.fetchall()
            
            if trades and len(trades) > 2:
                prices = [float(t[0] / t[1]) for t in trades]
                # Manual StdDev
                mean_price = sum(prices) / len(prices)
                variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
                volatility = Decimal(variance ** 0.5)
                # Normalize volatility?
                # Maybe Coefficient of Variation (StdDev / Mean)?
                if mean_price > 0:
                    volatility = Decimal(volatility) / Decimal(mean_price) 
            else:
                volatility = Decimal(0)


            # Insert Snapshot
            await cur.execute("""
                INSERT INTO feature_snapshots (
                    token_id, feature_version, detection_timestamp,
                    volume_acceleration, liquidity_growth_rate, holder_growth_rate,
                    buy_sell_ratio, early_wallet_retention, early_wallet_net_accumulation,
                    volatility_score
                )
                VALUES (%s, 1, %s, %s, 0, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                token_id, now,
                vol_accel, holder_growth, buy_sell_ratio,
                retention_rate, net_accumulation, volatility
            ))
            snapshot_id = (await cur.fetchone())[0]
            logger.info(f"Snapshot created: {snapshot_id}")
            return snapshot_id

if __name__ == "__main__":
    from api.db import init_db, close_db
    
    # Test run
    async def main():
        await init_db()
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur: 
                    await cur.execute("SELECT id FROM tokens LIMIT 1")
                    row = await cur.fetchone()
                    if row:
                        sid = await compute_snapshot(row[0])
                        print(f"Snapshot created: {sid}")
                    else:
                        print("No tokens found")
        finally:
            await close_db()
    
    asyncio.run(main())
