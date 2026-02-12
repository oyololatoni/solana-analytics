"""
Label Worker (Stub) — Checklist F.

After 72h, labels each snapshot with an outcome:
  hit_5x | price_failure | liquidity_collapse | volume_collapse | early_wallet_exit | expired

This is a stub. The full implementation will query price/volume history
72h after detection_timestamp and assign the correct outcome.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from api.db import init_db, close_db, get_db_connection

logger = logging.getLogger("solana-analytics")


async def label_expired_snapshots():
    """
    Find snapshots older than 72h that have no label yet.
    Assign 'expired' as the default outcome (stub logic).
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=72)

            await cur.execute("""
                SELECT fs.id, fs.token_id, fs.detection_timestamp
                FROM feature_snapshots fs
                LEFT JOIN lifecycle_labels ll ON ll.snapshot_id = fs.id
                WHERE fs.detection_timestamp < %s
                  AND ll.id IS NULL
            """, (cutoff,))
            unlabeled = await cur.fetchall()

            if not unlabeled:
                logger.info("Label worker: no unlabeled snapshots older than 72h.")
                return 0

            count = 0
            for snapshot_id, token_id, det_ts in unlabeled:
                # TODO: Replace with real outcome logic:
                #   - Query price 72h after det_ts vs price at det_ts
                #   - If max_price / det_price >= 5 -> 'hit_5x'
                #   - If price dropped > 90% -> 'price_failure'
                #   - If liquidity collapsed -> 'liquidity_collapse'
                #   - If volume collapsed -> 'volume_collapse'
                #   - If early wallets exited -> 'early_wallet_exit'
                #   - Else -> 'expired'
                outcome = "expired"
                max_multiplier = None

                await cur.execute("""
                    INSERT INTO lifecycle_labels (
                        token_id, snapshot_id, outcome, max_multiplier
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (token_id, snapshot_id, outcome, max_multiplier))
                count += 1

            logger.info(f"Label worker: labeled {count} snapshots.")
            return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def main():
        await init_db()
        try:
            n = await label_expired_snapshots()
            print(f"Labeled {n} snapshots.")
        finally:
            await close_db()

    asyncio.run(main())
