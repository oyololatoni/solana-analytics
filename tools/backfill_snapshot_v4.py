import asyncio
import os
import sys
import logging

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
from app.engines.v2.batch_features import BatchFeatureEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tools.backfill")

async def backfill():
    print("🚀 Starting Snapshot V4 Backfill...")
    await init_db()
    
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                # 1. Fetch Eligible Tokens
                print("Fetching eligible tokens...")
                await cur.execute("SELECT id FROM tokens WHERE eligibility_status = 'ELIGIBLE' AND is_active = TRUE")
                rows = await cur.fetchall()
                token_ids = [r[0] for r in rows]
                
                print(f"Found {len(token_ids)} eligible tokens.")
                
                if not token_ids:
                    print("No tokens to backfill.")
                    return

                # 2. Run Batch Engine
                engine = BatchFeatureEngine(conn, cur)
                batch_size = 50
                
                for i in range(0, len(token_ids), batch_size):
                    batch = token_ids[i:i + batch_size]
                    print(f"Processing batch {i}-{i+len(batch)}...")
                    await engine.compute_snapshots(batch)
                    await conn.commit()
                    
                print("✅ Backfill Complete.")
                
    except Exception as e:
        print(f"❌ Backfill Failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(backfill())
