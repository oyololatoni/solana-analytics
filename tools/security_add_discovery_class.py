
import os
import sys
import asyncio
import logging

# Add project root
sys.path.insert(0, os.getcwd())
try:
    from app.core.db import get_db_connection, init_db
except ImportError:
    # Just in case run from root
    sys.path.append(os.path.join(os.getcwd(), 'app'))
    from app.core.db import get_db_connection, init_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("db_patch")

async def add_column():
    token_added = False
    try:
        await init_db()  # Fix: Initialize pool
        async with get_db_connection() as conn:
            logger.info("Adding 'discovery_class' column...")
            async with conn.cursor() as cur:
                await cur.execute("""
                    ALTER TABLE tokens 
                    ADD COLUMN IF NOT EXISTS discovery_class TEXT DEFAULT 'UNKNOWN';
                """)
            await conn.commit()
            token_added = True
            logger.info("✅ Column 'discovery_class' added.")
    except Exception as e:
        logger.error(f"❌ Failed: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(add_column())
    except KeyboardInterrupt:
        pass
