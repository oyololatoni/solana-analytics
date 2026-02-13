
import os
import sys
import asyncio
import logging

# Add project root
sys.path.insert(0, os.getcwd())

# Load .env explicitly
if not os.environ.get("DATABASE_URL"):
    env_path = ".env.local"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")

try:
    from app.engines.v2.batch_features import BatchFeatureEngine
    from app.core.db import get_db_connection, init_db
    from app.core.constants import FEATURE_VERSION
except ImportError:
    sys.path.append(os.path.join(os.getcwd(), 'app'))
    from app.engines.v2.batch_features import BatchFeatureEngine
    from app.core.db import get_db_connection, init_db
    # Mock constant if needed, but imports should work
    FEATURE_VERSION = 2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("stage6_features")

async def run_stage6():
    print("Running Stage 6: Feature Extraction...")
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Select tokens that are ELIGIBLE but DO NOT have a feature snapshot for this version
            await cur.execute("""
                SELECT t.id 
                FROM tokens t
                LEFT JOIN feature_snapshots fs ON t.id = fs.token_id AND fs.feature_version = %s
                WHERE t.eligibility_status = 'ELIGIBLE'
                  AND fs.id IS NULL
            """, (FEATURE_VERSION,))
            
            tokens = await cur.fetchall()
            token_ids = [t[0] for t in tokens]
            
            if not token_ids:
                print("No new eligible tokens to process for features.")
                return

            print(f"Generating features for {len(token_ids)} tokens...")
            
            engine = BatchFeatureEngine(conn, cur, FEATURE_VERSION)
            await engine.process_batch(token_ids)
            
            print(f"Features generated for {len(token_ids)} tokens.")

if __name__ == "__main__":
    try:
        asyncio.run(run_stage6())
    except KeyboardInterrupt:
        pass
