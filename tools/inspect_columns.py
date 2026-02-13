import asyncio
import sys
import os

sys.path.insert(0, os.getcwd())
from app.core.db import get_db_connection, init_db

async def main():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'feature_snapshots'")
            rows = await cur.fetchall()
            print("Columns in feature_snapshots:")
            for row in rows:
                print(f"- {row[0]}")

if __name__ == "__main__":
    asyncio.run(main())
