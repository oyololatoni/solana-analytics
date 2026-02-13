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
    except FileNotFoundError:
        pass

from app.core.db import get_db_connection, init_db, close_db

async def inspect():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            print("\nSnapshots:")
            await cur.execute("SELECT id, token_id, feature_version, score_total, score_breakdown->'total' FROM feature_snapshots")
            rows = await cur.fetchall()
            for r in rows:
                print(f"ID: {r[0]}, Token: {r[1]}, Version: {r[2]}, ScoreTotal: {r[3]}, BreakdownTotal: {r[4]}")

            print("\nTokens with issues (inactive without label or snapshot):")
            await cur.execute("""
                SELECT t.id, t.is_active, l.id, s.id as snap_id
                FROM tokens t 
                LEFT JOIN lifecycle_labels l ON t.id = l.token_id 
                LEFT JOIN feature_snapshots s ON t.id = s.token_id
                WHERE t.is_active = FALSE 
                AND (l.id IS NULL OR s.id IS NULL)
            """)
            rows = await cur.fetchall()
            for r in rows:
                print(f"Token: {r[0]}, Inactive: {r[1]}, LabelID: {r[2]}, SnapID: {r[3]}")

    await close_db()

if __name__ == "__main__":
    asyncio.run(inspect())
