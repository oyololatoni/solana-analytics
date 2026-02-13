
import asyncio
import logging
from datetime import datetime, timezone
from app.core.db import init_db, get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def ensure_token_id(cur, chain_id, address, timestamp, cache):
    if address in cache:
        return cache[address]
    
    try:
        await cur.execute(
            """
            INSERT INTO tokens (chain_id, address, created_at_chain)
            VALUES (%s, %s, %s)
            ON CONFLICT (chain_id, address) DO UPDATE 
            SET address = EXCLUDED.address
            RETURNING id
            """,
            (chain_id, address, timestamp)
        )
        row = await cur.fetchone()
        if row:
            return row[0]
    except Exception as e:
        logger.error(f"ensure_token_id fail: {e}")
        raise e
    return None

async def main():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
             # Chain
            await cur.execute("SELECT id FROM chains WHERE name = 'solana'")
            chain_row = await cur.fetchone()
            chain_id = chain_row[0]
            print(f"Chain ID: {chain_id}")

            # Test Token Insert
            ts = datetime.now(timezone.utc)
            token_id = await ensure_token_id(
                cur, chain_id, "TestTokenAddress123", ts, {}
            )
            print(f"Token ID: {token_id}")

            # Test Wallet Profile Insert
            wallet_addr = "TestWalletAbc123"
            await cur.execute(
                """
                INSERT INTO wallet_profiles (chain_id, address, first_seen, last_seen)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (chain_id, address) DO UPDATE
                SET last_seen = GREATEST(wallet_profiles.last_seen, EXCLUDED.last_seen)
                RETURNING id
                """,
                (chain_id, wallet_addr, ts, ts)
            )
            wallet_id = (await cur.fetchone())[0]
            print(f"Wallet ID: {wallet_id}")

            # Test Wallet Interaction Insert
            await cur.execute(
                """
                INSERT INTO wallet_token_interactions (
                    chain_id, token_id, wallet_id, first_interaction, last_interaction,
                    last_balance_token, interaction_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (token_id, wallet_id) DO UPDATE
                SET 
                    last_interaction = EXCLUDED.last_interaction,
                    last_balance_token = EXCLUDED.last_balance_token,
                    interaction_count = wallet_token_interactions.interaction_count + 1
                """,
                (chain_id, token_id, wallet_id, ts, ts, 100.5, 1)
            )
            print("Interaction Inserted")

            # Test Trade Insert
            await cur.execute(
                """
                INSERT INTO trades (
                    chain_id, token_id, tx_signature, wallet_address,
                    side, amount_token, amount_sol, slot, timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chain_id, tx_signature, timestamp) DO NOTHING
                """,
                (
                    chain_id, token_id, f"TestSig_{ts.timestamp()}", wallet_addr,
                    'buy', 10.0, 0.1, 12345, ts
                )
            )
            print("Trade Inserted")

if __name__ == "__main__":
    asyncio.run(main())
