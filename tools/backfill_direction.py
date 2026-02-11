import asyncio
import json
import os
import psycopg
# Removed unused import

# Re-use tracked tokens list processing
# But wait, tracked token might have changed?
# No, events are stored with `token_mint`.
# We just need to check metadata for THAT token_mint.

async def backfill():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set")
        return

    print("Connecting to DB...")
    async with await psycopg.AsyncConnection.connect(db_url) as conn:
        async with conn.cursor() as cur:
            # 1. Fetch rows needing backfill
            await cur.execute(
                """
                SELECT id, metadata, token_mint, wallet 
                FROM events 
                WHERE direction IS NULL
                """
            )
            rows = await cur.fetchall()
            print(f"Found {len(rows)} rows to backfill.")

            updates = 0
            for row in rows:
                event_id, metadata_json, mint, wallet = row
                
                try:
                    # metadata is JSONB, psycopg returns it as dict or str?
                    # Start with dict if jsonb.
                    if isinstance(metadata_json, str):
                        tx = json.loads(metadata_json)
                    else:
                        tx = metadata_json
                    
                    # Locate Swap
                    swap = tx.get("events", {}).get("swap")
                    if not swap:
                        print(f"Skipping {event_id}: No swap in metadata")
                        continue

                    direction = None
                    
                    # Check Outputs (Buy / In)
                    for leg in swap.get("tokenOutputs", []):
                        if leg.get("mint") == mint and leg.get("userAccount") == wallet:
                            direction = 'in'
                            break
                    
                    # Check Inputs (Sell / Out) - Only if not found (Prioritize IN if both?)
                    # Current worker logic prioritizes IN insertion (first).
                    if not direction:
                        for leg in swap.get("tokenInputs", []):
                            if leg.get("mint") == mint and leg.get("userAccount") == wallet:
                                direction = 'out'
                                break
                    
                    if direction:
                        await cur.execute(
                            "UPDATE events SET direction = %s WHERE id = %s",
                            (direction, event_id)
                        )
                        updates += 1
                        if updates % 100 == 0:
                            print(f"Updated {updates} rows...")
                    else:
                        print(f"Skipping {event_id}: Could not determine direction for {mint}/{wallet}")

                except Exception as e:
                    print(f"Error processing {event_id}: {e}")

            await conn.commit()
            print(f"Backfill complete. Updated {updates}/{len(rows)} rows.")

if __name__ == "__main__":
    asyncio.run(backfill())
