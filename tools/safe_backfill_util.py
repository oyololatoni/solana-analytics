import asyncio
import logging
from typing import List, Optional
from app.core.db import get_db_connection, init_db, close_db
from app.engines.v2.batch_features import BatchFeatureEngine
from app.core.constants import FEATURE_VERSION
from app.core.logger import get_logger, log_event

logger = get_logger("tools.safe_backfill")

async def safe_backfill_snapshots(target_version: int = FEATURE_VERSION, batch_size: int = 50, dry_run: bool = False):
    """
    Safely backfills missing snapshots for ELIGIBLE tokens.
    
    Guardrails:
    1. SELECT eligibility_status = 'ELIGIBLE' only.
    2. SELECT NOT EXISTS (snapshot with target_version).
    3. INSERT only (via BatchFeatureEngine), no UPDATES.
    4. Strict Version Check.
    """
    logger.info(f"Starting Safe Backfill to Feature Version {target_version}")
    
    if target_version != FEATURE_VERSION:
        logger.error(f"Target version {target_version} does not match codebase version {FEATURE_VERSION}")
        return

    await init_db()
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 1. Identify missing snapshots
            logger.info("Identifying eligible tokens missing snapshots...")
            await cur.execute("""
                SELECT t.id 
                FROM tokens t
                WHERE t.eligibility_status = 'ELIGIBLE' 
                  AND t.is_active = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM feature_snapshots fs 
                      WHERE fs.token_id = t.id 
                        AND fs.feature_version = %s
                  )
                ORDER BY t.detected_at DESC
            """, (target_version,))
            
            rows = await cur.fetchall()
            tokens_to_backfill = [r[0] for r in rows]
            total = len(tokens_to_backfill)
            
            logger.info(f"Found {total} tokens needing backfill.")
            
            if dry_run:
                logger.info("Dry Run: Exiting without changes.")
                return

            if total == 0:
                logger.info("No tokens to backfill.")
                return

            # 2. Process in Batches
            engine = BatchFeatureEngine(conn, cur)
            
            for i in range(0, total, batch_size):
                batch = tokens_to_backfill[i:i+batch_size]
                logger.info(f"Processing batch {i}-{i+len(batch)} of {total}...")
                
                try:
                    # Engine handles INSERT. 
                    # If lazy, it might skip computation? 
                    # Engine.compute_snapshots computes and INSERTS.
                    # It relies on DB constraints to fail if duplicate?
                    # But we filtered out existing ones above, so should be safe.
                    # We wrap in transaction block per batch.
                    
                    await engine.compute_snapshots(batch)
                    await conn.commit()
                    logger.info(f"Batch {i} committed.")
                    
                except Exception as e:
                    logger.error(f"Failed batch {i}: {e}")
                    await conn.rollback()
                    # Continue to next batch? or Stop?
                    # Stop to investigate
                    raise e
                    
    logger.info("Backfill Complete.")
    await close_db()

if __name__ == "__main__":
    import sys
    # Optional arg: --dry-run
    dry_run = "--dry-run" in sys.argv
    asyncio.run(safe_backfill_snapshots(dry_run=dry_run))
