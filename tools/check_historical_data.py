import asyncio
import os
import sys
from datetime import datetime

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

async def check_data():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Check Trades
            await cur.execute("SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM trades")
            trades = await cur.fetchone()
            print(f"TRADES RANGE: {trades[0]} to {trades[1]} (Count: {trades[2]})")

            # Check Tokens
            await cur.execute("SELECT MIN(detected_at), MAX(detected_at), COUNT(*) FROM tokens")
            tokens = await cur.fetchone()
            print(f"TOKENS RANGE: {tokens[0]} to {tokens[1]} (Count: {tokens[2]})")
            
            # Check v2 trades if relevant (canonical_trades?)
            # Just in case migration 026/028 changed table usage
            # But earlier code used 'trades'
            
    await close_db()

if __name__ == "__main__":
    asyncio.run(check_data())
