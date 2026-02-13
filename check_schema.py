import asyncio
from app.core.db import get_db_connection

async def check():
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'tokens' AND column_name = 'chain_id';")
            print(f"Schema: {await cur.fetchone()}")
            await cur.execute("SELECT address, chain_id FROM tokens LIMIT 5;")
            rows = await cur.fetchall()
            print(f"Rows: {rows}")

if __name__ == '__main__':
    asyncio.run(check())
