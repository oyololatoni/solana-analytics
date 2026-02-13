import asyncio
import logging
from app.core.db import init_db, close_db, get_db_connection

logging.basicConfig(level=logging.INFO)

async def check_integrity():
    await init_db()
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                print("Checking A2. Label Integrity...")
                # 1. Duplicate Labels?
                await cur.execute("""
                    SELECT snapshot_id, COUNT(*)
                    FROM lifecycle_labels
                    GROUP BY snapshot_id
                    HAVING COUNT(*) > 1
                """)
                dupes = await cur.fetchall()
                if dupes:
                    print(f"❌ A2 FALIED: Found duplicate labels for snapshots: {dupes}")
                else:
                    print("✅ A2 PASSED: No duplicate labels.")

                # 2. Labeled tokens are inactive?
                await cur.execute("""
                    SELECT t.id, t.address 
                    FROM tokens t
                    JOIN lifecycle_labels l ON l.token_id = t.id
                    WHERE t.is_active = TRUE
                """)
                active_labeled = await cur.fetchall()
                if active_labeled:
                     print(f"❌ A2 FALIED: Found {len(active_labeled)} labeled tokens that are still active!")
                else:
                     print("✅ A2 PASSED: All labeled tokens are inactive.")

    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(check_integrity())
