
import asyncio
import json
import time
from app.core.db import init_db, close_db, get_db_connection

async def inject():
    payload = [{
        "signature": "TEST_SIG_" + str(int(time.time())),
        "slot": 123456789,
        "timestamp": int(time.time()),
        "events": {
            "swap": {
                "nativeInput": None,
                "nativeOutput": None,
                "tokenInputs": [],
                "tokenOutputs": [
                    {
                        "userAccount": "Wallet123",
                        "mint": "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump",
                        "rawTokenAmount": {"tokenAmount": "1000000", "decimals": 6},
                    }
                ],
                "program": "JUP6Lkq"
            }
        },
        "tokenBalanceChanges": [
            {
                "mint": "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump",
                "userAccount": "Wallet123",
                "rawTokenAmount": {"tokenAmount": "2000000", "decimals": 6}
            }
        ]
    }]
    
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO raw_webhooks (payload, source, payload_hash, status)
                VALUES (%s, 'test', %s, 'pending')
                RETURNING id
                """,
                (json.dumps(payload), "hash_" + str(int(time.time())))
            )
            job_id = (await cur.fetchone())[0]
            print(f"Injected job {job_id}")
            await conn.commit()
    await close_db()

if __name__ == "__main__":
    asyncio.run(inject())
