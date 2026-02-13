
import os
import sys
import asyncio
from app.core.db import get_db_connection, init_db

async def check_calibration_readiness():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT outcome, count(*) FROM lifecycle_labels GROUP BY outcome")
            rows = await cur.fetchall()
            
            stats = {row[0]: row[1] for row in rows}
            total = sum(stats.values())
            success = stats.get('SUCCESS', 0)
            
            print("-" * 30)
            print(f"Total Resolved: {total}")
            print(f"Successes: {success}")
            
            if total > 0:
                rate = (success / total) * 100
                print(f"Success Rate: {rate:.2f}%")
                
                if total >= 400 and success >= 20 and 3 <= rate <= 20:
                    print("✅ CALIBRATION READY")
                else:
                    print("⚠️  NOT READY (Need 400 total, 20 success, 3-20% rate)")
            else:
                print("⚠️  No data.")

if __name__ == "__main__":
    asyncio.run(check_calibration_readiness())
