import sys
import asyncio
from pathlib import Path

# Fix path to import app
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.db import get_db_connection, init_db, close_db

async def fix():
    await init_db()
    try:
        print("Deleting invalid snapshot 16...")
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                # Check if it has labels first
                await cur.execute("SELECT COUNT(*) FROM lifecycle_labels WHERE snapshot_id = 16")
                count = (await cur.fetchone())[0]
                if count > 0:
                    print(f"Snapshot 16 has {count} labels, deleting labels first...")
                    await cur.execute("DELETE FROM lifecycle_labels WHERE snapshot_id = 16")
                
                await cur.execute("DELETE FROM feature_snapshots WHERE id = 16")
                await conn.commit()
                print("Deleted snapshot 16.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(fix())
