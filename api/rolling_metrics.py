"""
Rolling Metrics Worker — Populates token_rolling_metrics table.

Computes windowed aggregates (5m, 30m, 1h, 6h) for each active token
from the raw `trades` and `wallet_token_interactions` tables.

Run periodically (e.g., every 60s) via cron or background loop.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from api.db import init_db, close_db, get_db_connection

logger = logging.getLogger("solana-analytics")

WINDOWS = [
    ("5m",  timedelta(minutes=5)),
    ("30m", timedelta(minutes=30)),
    ("1h",  timedelta(hours=1)),
    ("6h",  timedelta(hours=6)),
]


async def compute_rolling_metrics():
    """Compute rolling window metrics for all active tokens."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            now = datetime.now(timezone.utc)

            # Get active tokens (any trade in last 24h AND not in terminal lifecycle)
            await cur.execute("""
                SELECT DISTINCT t2.token_id FROM trades t2
                JOIN tokens tk ON tk.id = t2.token_id
                WHERE t2.timestamp > %s
                  AND tk.lifecycle_stage NOT IN ('SUCCESS', 'FAILED', 'EXPIRED')
            """, (now - timedelta(hours=24),))
            token_ids = [r[0] for r in await cur.fetchall()]

            if not token_ids:
                logger.info("Rolling metrics: no active tokens.")
                return 0

            count = 0
            for token_id in token_ids:
                for window_type, window_delta in WINDOWS:
                    start = now - window_delta

                    # Volume, trade count, buy/sell, unique wallets
                    await cur.execute("""
                        SELECT
                            COALESCE(SUM(amount_sol), 0),
                            COUNT(*),
                            COUNT(DISTINCT wallet_address),
                            COALESCE(SUM(CASE WHEN side='buy' THEN amount_sol ELSE 0 END), 0),
                            COALESCE(SUM(CASE WHEN side='sell' THEN amount_sol ELSE 0 END), 0)
                        FROM trades
                        WHERE token_id = %s AND timestamp > %s
                    """, (token_id, start))
                    row = await cur.fetchone()
                    volume = row[0]
                    trade_count = row[1]
                    unique_wallets = row[2]
                    buy_volume = row[3]
                    sell_volume = row[4]

                    # Volatility (stddev of price proxy: amount_sol/amount_token)
                    await cur.execute("""
                        SELECT amount_sol, amount_token FROM trades
                        WHERE token_id = %s AND timestamp > %s
                          AND amount_sol > 0 AND amount_token > 0
                    """, (token_id, start))
                    price_rows = await cur.fetchall()
                    volatility = Decimal(0)
                    if len(price_rows) > 1:
                        prices = [float(r[0] / r[1]) for r in price_rows]
                        mean_p = sum(prices) / len(prices)
                        var = sum((p - mean_p) ** 2 for p in prices) / len(prices)
                        volatility = Decimal(str(var ** 0.5))

                    # Upsert
                    await cur.execute("""
                        INSERT INTO token_rolling_metrics (
                            token_id, window_type, computed_at,
                            volume_usd, trade_count, unique_wallets,
                            buy_volume_usd, sell_volume_usd,
                            liquidity_avg_usd, volatility
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, %s)
                        ON CONFLICT (token_id, window_type, computed_at)
                        DO UPDATE SET
                            volume_usd = EXCLUDED.volume_usd,
                            trade_count = EXCLUDED.trade_count,
                            unique_wallets = EXCLUDED.unique_wallets,
                            buy_volume_usd = EXCLUDED.buy_volume_usd,
                            sell_volume_usd = EXCLUDED.sell_volume_usd,
                            volatility = EXCLUDED.volatility
                    """, (
                        token_id, window_type, now,
                        volume, trade_count, unique_wallets,
                        buy_volume, sell_volume, volatility
                    ))
                    count += 1

            logger.info(f"Rolling metrics: computed {count} windows for {len(token_ids)} tokens.")
            return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def main():
        await init_db()
        try:
            n = await compute_rolling_metrics()
            print(f"Computed {n} rolling metric rows.")
        finally:
            await close_db()

    asyncio.run(main())
