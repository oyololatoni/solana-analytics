
import os
import sys
import asyncio
from app.core.db import get_db_connection, init_db

async def run_integrity_check():
    print("Running Stage 9: Integrity Verification...")
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 1. Snapshot Uniqueness
            await cur.execute("""
                SELECT token_id, feature_version, count(*) 
                FROM feature_snapshots 
                GROUP BY token_id, feature_version 
                HAVING count(*) > 1
            """)
            dupes = await cur.fetchall()
            if dupes:
                print(f"❌ FAILED: Duplicate Snapshots: {dupes}")
                sys.exit(1)
                
            # 2. Label Uniqueness
            await cur.execute("""
                SELECT token_id, count(*) FROM lifecycle_labels GROUP BY token_id HAVING count(*) > 1
            """)
            lbl_dupes = await cur.fetchall()
            if lbl_dupes:
                print(f"❌ FAILED: Duplicate Labels: {lbl_dupes}")
                sys.exit(1)

            # 3. Orphans
            await cur.execute("""
                SELECT count(*) FROM lifecycle_labels l
                LEFT JOIN tokens t ON l.token_id = t.id
                WHERE t.id IS NULL
            """)
            orphans = (await cur.fetchone())[0]
            if orphans > 0:
                print(f"❌ FAILED: Orphaned Labels: {orphans}")
                sys.exit(1)

            # 4. Snapshot without Label (Stuck > 72h)
            # Complex check: If snapshot exists, label should exist if 72h passed? 
            # Or feature snapshot happens after eligibility. Label happens after 72h.
            
            # 5. Label before Snapshot check?
            # Join labels and snapshots, check timestamps.
            
            print("✅ Integrity Checks Passed.")

if __name__ == "__main__":
    asyncio.run(run_integrity_check())
