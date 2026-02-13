import asyncio
from app.core.db import get_db_connection

async def test():
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Test 1: Check existing value
            await cur.execute("SELECT address, discovery_class FROM tokens WHERE address = 'coqRkaaKeUygDPhuS3mrmrj6DiHjeQJc2rFbT2YfxWn';")
            row = await cur.fetchone()
            print(f"Current Row: {row}")
            
            # Test 2: Try update with CASE
            d_class = "TEST_OVERWRITE"
            mint = 'coqRkaaKeUygDPhuS3mrmrj6DiHjeQJc2rFbT2YfxWn'
            
            # First set to CALIBRATION for test
            await cur.execute("UPDATE tokens SET discovery_class = 'NEW_LISTING_CALIBRATION' WHERE address = %s", (mint,))
            print("Set to CALIBRATION")
            
            await cur.execute("""
                UPDATE tokens 
                SET discovery_class = CASE 
                    WHEN discovery_class LIKE 'NEW_LISTING%' THEN discovery_class 
                    ELSE %s 
                END 
                WHERE address = %s
                RETURNING discovery_class
            """, (d_class, mint))
            new_val = await cur.fetchone()
            print(f"Value after CASE update: {new_val[0]}")
        await conn.commit()

if __name__ == "__main__":
    asyncio.run(test())
