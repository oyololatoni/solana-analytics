import asyncio
import os
import sys
import logging

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

async def apply_migration():
    print("🚀 Applying Migration 022: Critical Hardening...")
    await init_db()
    
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                with open("schema/022_critical_hardening.sql", "r") as f:
                    sql = f.read()
                    
                # Split commands if needed, or run as block
                # Psycopg3 execute supports blocks usually
                await cur.execute(sql)
                await conn.commit()
                print("✅ Migration 022 Applied Successfully.")
                
    except Exception as e:
        print(f"❌ Migration Failed: {e}")
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(apply_migration())
