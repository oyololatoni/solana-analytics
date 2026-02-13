import asyncio
import logging
import os
import sys

# Add project root to path
sys.path.insert(0, os.getcwd())

# Load .env manually if not set
if not os.environ.get("DATABASE_URL"):
    env_file = ".env" if os.path.exists(".env") else ".env.local"
    try:
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    os.environ[key] = value.strip('"').strip("'")
        print(f"✅ Loaded {env_file} file")
    except FileNotFoundError:
        print("⚠️ No .env or .env.local file found, relying on system env vars")

from app.core.db import get_db_connection, init_db, close_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("verify_hardening")

async def verify_db_hardening():
    logger.info("--- Starting Database Hardening Verification ---")
    await init_db()
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 1. Verify Trigger (Immutability)
            logger.info("1. Checking 'prevent_snapshot_update' trigger...")
            await cur.execute("""
                SELECT trigger_name 
                FROM information_schema.triggers 
                WHERE event_object_table = 'feature_snapshots' 
                  AND trigger_name = 'snapshot_update_block'
            """)
            if await cur.fetchone():
                logger.info("✅ Trigger 'snapshot_update_block' EXISTS.")
            else:
                logger.error("❌ Trigger 'snapshot_update_block' MISSING.")

            # 2. Verify System Config
            logger.info("2. Checking 'system_config' table...")
            try:
                await cur.execute("SELECT value FROM system_config WHERE key = 'feature_version'")
                row = await cur.fetchone()
                if row and row[0] == '4':
                    logger.info("✅ system_config exists and feature_version = 4.")
                else:
                    logger.error(f"❌ system_config found but value is {row}.")
            except Exception as e:
                logger.error(f"❌ system_config table MISSING or error: {e}")

            # 3. Verify Indexes
            logger.info("3. Checking Critical Indexes...")
            indexes_to_check = [
                ('feature_snapshots', 'uq_snapshot_token_version'),
                ('feature_snapshots', 'idx_snapshot_token'),
                ('feature_snapshots', 'idx_snapshot_time'),
                ('tokens', 'idx_tokens_eligibility'),
                ('lifecycle_labels', 'idx_lifecycle_token')
            ]
            
            await cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
            existing_indexes = {row[0] for row in await cur.fetchall()}
            
            for table, idx in indexes_to_check:
                if idx in existing_indexes:
                    logger.info(f"✅ Index '{idx}' on {table} EXISTS.")
                else:
                    logger.error(f"❌ Index '{idx}' on {table} MISSING.")

    await close_db()

if __name__ == "__main__":
    asyncio.run(verify_db_hardening())
