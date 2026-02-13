import asyncio
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

async def verify():
    print("🔎 Verifying Snapshot V4 Data...")
    await init_db()
    
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                # Fetch latest snapshot
                await cur.execute("""
                    SELECT 
                        token_id, 
                        feature_version, 
                        volume_5m_usd, 
                        max_trade_gap_30m_minutes,
                        feature_version,
                        probability_5x,
                        model_version_id
                    FROM feature_snapshots 
                    WHERE feature_version = 4
                    LIMIT 1
                """)
                row = await cur.fetchone()
                
                if not row:
                    print("❌ No snapshots found.")
                    return
                
                print(f"Token ID: {row[0]}")
                print(f"Feature Version: {row[1]}")
                print(f"Vol 5m USD: {row[2]}")
                print(f"Max Trade Gap: {row[3]}")
                print(f"Probability: {row[5]}")
                print(f"Model Version: {row[6]}")
                
                if row[1] == 4:
                    print("✅ Schema Version Matches (4)")
                else:
                    print(f"❌ Schema Version Mismatch: Expected 4, Got {row[1]}")
                
                if row[2] is not None:
                     print("✅ Volume USD populated")
                else:
                     print("❌ Volume USD is NULL")
    except Exception as e:
        print(f"❌ Verification Failed: {e}")
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(verify())
