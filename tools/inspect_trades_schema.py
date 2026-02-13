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

async def inspect():
    await init_db()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                print("Checking columns in 'trades' table...")
                await cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'trades'")
                cols = [row[0] for row in await cur.fetchall()]
                print(f"Columns: {cols}")
                if 'liquidity_usd' in cols:
                    print("✅ Found 'liquidity_usd'")
                elif 'liquidity' in cols:
                    print("⚠️ Found 'liquidity' (no _usd suffix)")
                else:
                    print("❌ Neither 'liquidity_usd' nor 'liquidity' found")
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(inspect())
