import asyncio
import os
import sys
import glob

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

from app.core.db import get_db_connection, init_db

async def run():
    # 1. Check File Count
    files = glob.glob("backfill_cache/tokens/*.json")
    print(f"Files Downloaded: {len(files)}")

    # 2. Check DB Count
    try:
        await init_db()
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM trades")
                row = await cur.fetchone()
                print(f"Total Trades Ingested: {row[0]}")
    except Exception as e:
        print(f"DB Error: {e}")

if __name__ == "__main__":
    asyncio.run(run())
