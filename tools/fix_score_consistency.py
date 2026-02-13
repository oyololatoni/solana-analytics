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

async def fix():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Align score_total with rounded breakdown total
            await cur.execute("UPDATE feature_snapshots SET score_total = ROUND(score_total::numeric, 2)")
            print(f"✅ Aligned {cur.rowcount} snapshots' score_total with rounded precision")
            await conn.commit()
    await close_db()

if __name__ == "__main__":
    asyncio.run(fix())
