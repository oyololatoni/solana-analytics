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

async def check_schema():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'tokens'")
            print("tokens:", sorted([r[0] for r in await cur.fetchall()]))

            await cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'trades'")
            print("trades:", sorted([r[0] for r in await cur.fetchall()]))
            
            await cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'liquidity_events'")
            print("liquidity_events:", sorted([r[0] for r in await cur.fetchall()]))
            
    await close_db()

if __name__ == "__main__":
    asyncio.run(check_schema())
