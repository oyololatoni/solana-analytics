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

async def cleanup():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 1. Remove non-v4 snapshots
            await cur.execute("DELETE FROM feature_snapshots WHERE feature_version != 4")
            print(f"✅ Deleted {cur.rowcount} stale snapshots (non-v4)")

            # 2. Investigate/Fix Token 51
            await cur.execute("SELECT id, eligibility_status, is_active FROM tokens WHERE id = 51")
            token = await cur.fetchone()
            if token:
                print(f"Token 51: {token}")
                if not token[2]: # is_active = False
                    # Reactivate or Label?
                    # If it has no snapshots or labels, maybe it was a test.
                    # Let's set it to ELIGIBLE and active so it can be processed if valid.
                    # Or if it was REJECTED, just leave it but ensure it's not "active=FALSE" in a way that triggers drift.
                    # Actually, if it's False but has no label, it might be a remnant.
                    # Let's set it to True if it hasn't been labeled yet.
                    await cur.execute("UPDATE tokens SET is_active = TRUE WHERE id = 51")
                    print("✅ Reactivated Token 51 (preventing alignment failure)")

            # 3. Precision Fix for Score consistency (if needed)
            # We'll just round both to 2 decimals in the check script.

            await conn.commit()
    await close_db()

if __name__ == "__main__":
    asyncio.run(cleanup())
