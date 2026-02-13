import asyncio
from app.core.db import init_db, close_db, get_db_connection

async def check():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM tokens WHERE address = '9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump'")
            row = await cur.fetchone()
            print(f"Token Found: {row}")
            
            # Check ingestion stats
            await cur.execute("SELECT * FROM ingestion_stats ORDER BY created_at DESC LIMIT 1")
            stats = await cur.fetchone()
            print(f"Latest Stats: {stats}")
            
    await close_db()

if __name__ == "__main__":
    asyncio.run(check())
