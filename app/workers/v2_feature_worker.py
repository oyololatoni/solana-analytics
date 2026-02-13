import asyncio
import logging
from app.core.db import get_db_connection, init_db, close_db
from app.engines.v2.eligibility import run_eligibility_gate_v2
from app.engines.v2.batch_features import BatchFeatureEngine
from app.engines.v2.label_worker import run_label_worker_v2
import signal

from app.core.logger import get_logger, log_event

logger = get_logger("workers.v2")

async def run_worker_loop():
    logger.info("Starting V2 Worker Loop (Full Pipeline)")
    await init_db()
    
    try:
        while True:
            logger.info("--- Starting Pipeline Cycle ---")
            
            # 1. Eligibility
            await run_eligibility_gate_v2()
            
            # 2. Features (Snapshotting) - Concurrency Safe
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                   # Select ELIGIBLE tokens that DO NOT have a v4 snapshot yet
                   # Lock rows to prevent race conditions with other workers
                   await cur.execute("""
                       SELECT id 
                       FROM tokens t
                       WHERE t.eligibility_status = 'ELIGIBLE'
                         AND t.is_active = TRUE
                         AND NOT EXISTS (
                             SELECT 1 FROM feature_snapshots fs 
                             WHERE fs.token_id = t.id AND fs.feature_version = 4
                         )
                       LIMIT 50
                       FOR UPDATE SKIP LOCKED
                   """)
                   rows = await cur.fetchall()
                   tokens = [r[0] for r in rows]
                   
                   if tokens:
                       logger.info(f"Computing snapshots for {len(tokens)} tokens (Batch Mode)")
                       engine = BatchFeatureEngine(conn, cur)
                       
                       try:
                           await engine.compute_snapshots(tokens)
                           await conn.commit()
                       except Exception as e:
                           logger.error(f"Batch snapshot failed for tokens {tokens[0]}...: {e}")
                           await conn.rollback()

            # 3. Labeling
            await run_label_worker_v2()
            
            logger.info("--- Cycle Complete. Sleeping 60s ---")
            await asyncio.sleep(60)

    except asyncio.CancelledError:
        logger.info("Worker cancelled")
    finally:
        await close_db()
        logger.info("Worker shutdown")

if __name__ == "__main__":
    try:
        asyncio.run(run_worker_loop())
    except KeyboardInterrupt:
        pass
