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

from app.core.db import get_db_connection, init_db

async def run():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT indexname, indexdef FROM pg_indexes WHERE tablename='trades'")
            rows = await cur.fetchall()
            for r in rows:
                print(f"Index: {r[0]}, Def: {r[1]}")

if __name__ == "__main__":
    asyncio.run(run())
