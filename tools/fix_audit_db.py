import asyncio
import logging
import sys
import os

sys.path.insert(0, os.getcwd())
from app.core.db import get_db_connection, init_db, close_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix_audit_db")

async def fix_database():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            logger.info("--- Step 1: Remove Duplicate Snapshots ---")
            # Keep the earliest snapshot_time for each (token_id, feature_version)
            await cur.execute("""
                DELETE FROM feature_snapshots fs
                USING (
                    SELECT id
                    FROM (
                        SELECT id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY token_id, feature_version
                                   ORDER BY snapshot_time ASC
                               ) AS rn
                        FROM feature_snapshots
                    ) t
                    WHERE t.rn > 1
                ) dup
                WHERE fs.id = dup.id
            """)
            logger.info(f"Deleted {cur.rowcount} duplicate snapshots.")
            
            logger.info("--- Step 2: Add Unique Constraint ---")
            # Drop existing index if it exists to be safe, or just add logic
            await cur.execute("DROP INDEX IF EXISTS idx_snapshot_unique") 
            await cur.execute("DROP INDEX IF EXISTS uq_snapshot_token_version")
            await cur.execute("""
                CREATE UNIQUE INDEX uq_snapshot_token_version
                ON feature_snapshots(token_id, feature_version)
            """)
            logger.info("Unique constraint 'uq_snapshot_token_version' created.")

            logger.info("--- Step 3: Standardize snapshot_time Naming ---")
            # Check if column needs renaming
            await cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'feature_snapshots' AND column_name = 'detection_timestamp'
            """)
            if await cur.fetchone():
                await cur.execute("""
                    ALTER TABLE feature_snapshots
                    RENAME COLUMN detection_timestamp TO snapshot_time
                """)
                logger.info("Renamed detection_timestamp to snapshot_time.")
            else:
                logger.info("Column detection_timestamp not found (already renamed?).")

            logger.info("--- Step 4: Add score_breakdown JSONB ---")
            await cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'feature_snapshots' AND column_name = 'score_breakdown'
            """)
            if not await cur.fetchone():
                await cur.execute("""
                    ALTER TABLE feature_snapshots
                    ADD COLUMN score_breakdown JSONB NOT NULL DEFAULT '{}'
                """)
                await cur.execute("""
                    ALTER TABLE feature_snapshots
                    ALTER COLUMN score_breakdown DROP DEFAULT
                """)
                logger.info("Added score_breakdown column.")
            else:
                logger.info("Column score_breakdown already exists.")

            await conn.commit()

    await close_db()
    logger.info("Database fixes applied successfully.")

if __name__ == "__main__":
    asyncio.run(fix_database())
