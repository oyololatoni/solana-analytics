
import os
import sys
import asyncio
from app.core.db import get_db_connection, init_db

TARGET_RESOLVED = 400
TARGET_SUCCESS = 20

async def check_stop_condition():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM lifecycle_labels")
            resolved = (await cur.fetchone())[0]
            
            await cur.execute("SELECT COUNT(*) FROM lifecycle_labels WHERE outcome = 'SUCCESS'")
            successes = (await cur.fetchone())[0]
            
            print(f"Resolved: {resolved}/{TARGET_RESOLVED} | Successes: {successes}/{TARGET_SUCCESS}")
            
            if resolved >= TARGET_RESOLVED:
                if successes >= TARGET_SUCCESS:
                    print("🛑 STOP CONDITION MET. Stopping Backfill.")
                    return True
                else:
                    print(f"⚠️ Resolved Met, but Successes Low ({successes}). Continuing.")
                    return False
            return False

if __name__ == "__main__":
    is_stop = asyncio.run(check_stop_condition())
    if is_stop:
        sys.exit(100)
