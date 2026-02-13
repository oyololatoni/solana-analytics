import asyncio
import json
from app.core.db import get_db_connection

async def fix():
    with open("candidate_tokens.json", "r") as f:
        candidates = json.load(f)
    mints = [c["mint"] for c in candidates]
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            for mint in mints:
                await cur.execute("UPDATE tokens SET discovery_class = 'NEW_LISTING_CALIBRATION' WHERE address = %s", (mint,))
                print(f"Updated {mint}")
        await conn.commit()

if __name__ == "__main__":
    asyncio.run(fix())
