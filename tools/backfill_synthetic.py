import asyncio
import json
import random
from datetime import datetime, timedelta, timezone
from api.db import get_db_connection, init_db, close_db

TOKEN_MINT = "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump"

async def generate_synthetic_data(days=7):
    await init_db()
    now = datetime.now(timezone.utc)
    
    # Track a pool of wallets for stickiness
    wallet_pool = [f"Wallet_{i:03d}_{random.getrandbits(64):x}" for i in range(200)]
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Clear old Fartcoin data to ensure clean stats
            await cur.execute("DELETE FROM events WHERE token_mint = %s", (TOKEN_MINT,))
            print(f"Cleared existing data for {TOKEN_MINT}")

            for day_offset in range(days, -1, -1):
                day_date = (now - timedelta(days=day_offset)).date()
                print(f"Generating data for {day_date}...")
                
                # Growth pattern: Start slow, peak 3 days ago, slight decline
                if day_offset > 5: # Early
                    num_swaps = random.randint(50, 100)
                    unique_ratio = 0.8
                elif day_offset > 2: # Peak
                    num_swaps = random.randint(300, 500)
                    unique_ratio = 0.4
                else: # Decline
                    num_swaps = random.randint(150, 250)
                    unique_ratio = 0.6

                # Wallet Cohorts
                active_wallets = random.sample(wallet_pool, min(int(num_swaps * unique_ratio), len(wallet_pool)))
                
                for i in range(num_swaps):
                    wallet = random.choice(active_wallets)
                    block_time = datetime.combine(day_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(seconds=random.randint(0, 86399))
                    
                    # Direction: Random buy/sell but biased towards buys during peak
                    direction = "in" if random.random() < (0.7 if day_offset > 2 else 0.4) else "out"
                    amount = random.uniform(1000, 50000)
                    signature = f"SynthTx_{day_offset}_{i}_{random.getrandbits(64):x}"
                    
                    await cur.execute(
                        """
                        INSERT INTO events (
                            tx_signature, slot, event_type, wallet,
                            token_mint, amount, direction, block_time, program_id, metadata
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            signature, 1000000, "swap", wallet, TOKEN_MINT, amount,
                            direction, block_time, "ProgSynth1111111111111111111111111111111111",
                            json.dumps({"synthetic": True})
                        )
                    )
            
            await conn.commit()
            print("Synthetic backfill complete.")
    await close_db()

if __name__ == "__main__":
    asyncio.run(generate_synthetic_data())
