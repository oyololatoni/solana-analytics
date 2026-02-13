import asyncio
from app.core.db import init_db, close_db, get_db_connection

async def apply():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            with open("schema/020_add_token_outcomes.sql", "r") as f:
                sql = f.read()
                print("Applying schema 020...")
                await cur.execute(sql)
                await conn.commit()
                print("Applied.")
    await close_db()

if __name__ == "__main__":
    asyncio.run(apply())
