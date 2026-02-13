
import asyncio
from datetime import datetime, timezone
from app.core.db import init_db, close_db, get_db_connection

async def debug():
    await init_db()
    async with get_db_connection() as conn:
        print("Connected.")
        async with conn.cursor() as cur:
            # 1. Get Chain ID
            await cur.execute("SELECT id FROM chains WHERE name = 'solana'")
            chain_row = await cur.fetchone()
            chain_id = chain_row[0]
            print(f"Chain ID: {chain_id}")
            
            mint = "DEBUG_MINT_123"
            block_time = datetime.now(timezone.utc)
            
            async with conn.transaction():
                print("Transaction started.")
                
                # 2. Insert Token
                print("Inserting token...")
                await cur.execute(
                    """
                    INSERT INTO tokens (chain_id, address, created_at_chain)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (chain_id, address) DO UPDATE 
                    SET address = EXCLUDED.address
                    RETURNING id
                    """,
                    (chain_id, mint, block_time)
                )
                row = await cur.fetchone()
                print(f"Token Insert Result: {row}")
                token_id = row[0]
                
                # 3. Insert Trade
                print(f"Inserting trade for token_id {token_id}...")
                await cur.execute(
                    """
                    INSERT INTO trades (
                        chain_id, token_id, tx_signature, wallet_address,
                        side, amount_token, slot, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chain_id, tx_signature) DO NOTHING
                    """,
                    (
                        chain_id, token_id, "DEBUG_SIG_1", "WalletDebug",
                        "buy", 100, 123, block_time
                    )
                )
                print("Trade inserted.")
                
                # 4. Insert Event
                print("Inserting event...")
                await cur.execute(
                    """
                    INSERT INTO events (
                        tx_signature, slot, event_type, wallet,
                        token_mint, amount, block_time, program_id, metadata, direction
                    )
                    VALUES (%s, %s, 'swap', 'WalletDebug', %s, 100, %s, 'prog', '{}', 'in')
                    ON CONFLICT (tx_signature, event_type, wallet) DO NOTHING
                    """,
                    ("DEBUG_SIG_1", mint, block_time)
                )
                print("Event inserted.")
        
        await conn.commit()
        print("Commited.")
        
    await close_db()

if __name__ == "__main__":
    asyncio.run(debug())
