import asyncio
import json
from app.core.db import init_db, close_db, get_db_connection

async def inspect():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Token Count
            await cur.execute("SELECT count(*) FROM tokens")
            token_count = (await cur.fetchone())[0]
            print(f"Total Tokens: {token_count}")
            
            # Raw Webhooks
            await cur.execute("SELECT count(*) FROM raw_webhooks")
            total_hooks = (await cur.fetchone())[0]
            await cur.execute("SELECT count(*) FROM raw_webhooks WHERE status='pending'")
            pending_hooks = (await cur.fetchone())[0]
            print(f"Raw Webhooks: {total_hooks} (Pending: {pending_hooks})")
            
            # Ingestion Stats (Last 10)
            print("\nRecent Ingestion Stats:")
            await cur.execute("""
                SELECT created_at, events_received, swaps_inserted, 
                       ignored_no_tracked_tokens, ignored_no_swap_event 
                FROM ingestion_stats 
                ORDER BY created_at DESC LIMIT 10
            """)
            stats = await cur.fetchall()
            print(f"{'Time':<30} {'Rx':<10} {'Ins':<10} {'Ign(Track)':<15} {'Ign(Swap)'}")
            for s in stats:
                print(f"{str(s[0]):<30} {s[1]:<10} {s[2]:<10} {s[3]:<15} {s[4]}")
                
            # Iterate to find a non-TITAN payload
            print("Searching for Jupiter/Raydium payloads...", flush=True)
            await cur.execute("SELECT payload FROM raw_webhooks ORDER BY created_at DESC LIMIT 100")
            rows = await cur.fetchall()
            print(f"Scanned {len(rows)} rows.", flush=True)
            
            target_payload = None
            for r in rows:
                try:
                    p = r[0] if not isinstance(r[0], str) else json.loads(r[0])
                    if isinstance(p, list) and len(p) > 0:
                        src = p[0].get('source')
                        if src in ['JUPITER', 'RAYDIUM']:
                            target_payload = p
                            break
                except:
                    continue
            
            if target_payload:
                evt = target_payload[0]
                print(f"\n--- Targeted Payload ({evt.get('source')}) ---")
                print(f"Signature Present: {'signature' in evt}")
                print(f"Token Balance Changes: {len(evt.get('tokenBalanceChanges', []))}")
                
                swap = evt.get('events', {}).get('swap', {})
                print(f"Token Inputs: {len(swap.get('tokenInputs', []))}")
                print(f"Token Outputs: {len(swap.get('tokenOutputs', []))}")
                
                 # TEST NORMALIZATION
                print("\n--- Adapter Test ---")
                try:
                    from app.ingestion.solana_adapter import SolanaAdapter
                    adapter = SolanaAdapter()
                    events = adapter.normalize_tx(target_payload[0])
                    print(f"Normalized Events: {len(events)}")
                    for i, e in enumerate(events):
                        print(f"Event {i}: {e}")
                except Exception as ae:
                    print(f"Adapter Test Failed: {ae}")
            else:
                print("\nNo JUPITER/RAYDIUM payloads found in last 100.")

    await close_db()

if __name__ == "__main__":
    asyncio.run(inspect())
