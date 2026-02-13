import asyncio
import logging
import sys
import asyncio
import logging
import sys
from app.core.db import get_db_connection, init_db, close_db

# Import V2 modules (from isolated engine)
from app.engines.v2.eligibility import run_eligibility_gate_v2
from app.engines.v2.features import compute_v2_snapshot
from app.engines.v2.label_worker import run_label_worker_v2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("reprocess-all")

async def run_pipeline():
    logger.info("STARTING FULL REPROCESS PIPELINE")
    await init_db()
    try:
        # 1. Eligibility Gate
        logger.info("--- Step 1: Eligibility Gate ---")
        try:
            stats = await run_eligibility_gate_v2()
            logger.info(f"Gate Stats: {stats}")
        except Exception as e:
            logger.error(f"Eligibility Gate Failed: {e}")
            # Continue? Maybe not.
        
        # 2. Features/Snapshots
        logger.info("--- Step 2: Feature Snapshots ---")
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, address FROM tokens WHERE eligibility_status = 'ELIGIBLE'")
                rows = await cur.fetchall()
                
        logger.info(f"Found {len(rows)} eligible tokens for snapshotting")
        
        count = 0
        errors = 0
        skipped = 0
        
        for token_id, address in rows:
            try:
               # Attempt to create snapshot
               sid = await compute_v2_snapshot(token_id)
               if sid:
                   logger.info(f"Snapshot created for {address} (ID: {sid})")
                   count += 1
               else:
                   logger.warning(f"No snapshot created for {address}")
                   # if sid is None: errors += 1 # Only count error if exception or explicit None with no log? 
                   # compute_v2_snapshot returns None on error and logs it.
            except Exception as e:
                # Check for unique constraint violation which might happen if detected_at hasn't changed
                # Since we renamed detection_timestamp to snapshot_time, the unique index is on detection_timestamp 
                # (which is now snapshot_time).
                if "unique constraint" in str(e).lower():
                    logger.info(f"Snapshot already exists for {address}, skipping")
                    skipped += 1
                else:
                    logger.error(f"Failed to snapshot {token_id} ({address}): {e}")
                    errors += 1
    
        logger.info(f"Snapshots complete: {count} created, {skipped} skipped, {errors} errors")
    
        # 3. Labeling (for Risk and Outcomes)
        logger.info("--- Step 3: Label Worker ---")
        try:
            lstats = await run_label_worker_v2()
            logger.info(f"Label Stats: {lstats}")
        except Exception as e:
            logger.error(f"Label Worker Failed: {e}")
        
        logger.info("PIPELINE COMPLETE")
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(run_pipeline())
