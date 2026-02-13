import asyncio
import os
import sys

# Add project root to path
sys.path.insert(0, os.getcwd())

# Load .env
if not os.environ.get("DATABASE_URL"):
    env_file = ".env" if os.path.exists(".env") else ".env.local"
    try:
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    os.environ[key] = value.strip('"').strip("'")
    except FileNotFoundError:
        pass

from app.core.db import get_db_connection, init_db, close_db

async def align_scores():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 1. Drop trigger
            print("Dropping immutability trigger...")
            await cur.execute("DROP TRIGGER IF EXISTS snapshot_update_block ON feature_snapshots")
            
            # 2. Update scores
            print("Aligning scores to 2 decimal places...")
            await cur.execute("UPDATE feature_snapshots SET score_total = ROUND(score_total::numeric, 2)")
            print(f"✅ Aligned {cur.rowcount} snapshots")
            
            # 3. Restore trigger
            print("Restoring immutability trigger...")
            await cur.execute("""
                CREATE TRIGGER snapshot_update_block
                BEFORE UPDATE ON feature_snapshots
                FOR EACH ROW
                EXECUTE FUNCTION prevent_snapshot_update()
            """)
            
            await conn.commit()
    await close_db()
    print("Alignment complete.")

if __name__ == "__main__":
    asyncio.run(align_scores())
