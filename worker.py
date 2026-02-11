
import asyncio
import json
import os
import signal
import psycopg
from datetime import datetime, timezone
from decimal import Decimal

from api import logger
from api.db import init_db, close_db, get_db_connection
from config import TRACKED_TOKENS

# Chain Abstraction
from chains.registry import registry
from chains.solana_adapter import SolanaAdapter
from chains.models import CanonicalTrade, CanonicalWalletInteraction

# Register Adapters
registry.register("solana", SolanaAdapter())

# Configuration
BATCH_SIZE = 50
POLL_INTERVAL = 1.0  # seconds

# Global shutdown event
shutdown_event = asyncio.Event()

def handle_signal():
    logger.info("Shutdown signal received, stopping worker...")
    shutdown_event.set()

async def ensure_token_id(cur, chain_id, address, timestamp, cache):
    if address in cache:
        return cache[address]
    
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
        token_id = row[0]
        cache[address] = token_id
        return token_id
    return None

async def upsert_wallet_interaction(cur, chain_id, token_id, event):
    # Upsert Profile
    await cur.execute(
        """
        INSERT INTO wallet_profiles (chain_id, address, first_seen, last_seen)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (chain_id, address) DO UPDATE
        SET last_seen = GREATEST(wallet_profiles.last_seen, EXCLUDED.last_seen)
        RETURNING id
        """,
        (chain_id, event.wallet_address, event.timestamp, event.timestamp)
    )
    wallet_row = await cur.fetchone()
    if not wallet_row: return
    
    wallet_id = wallet_row[0]
    
    # Upsert Interaction
    await cur.execute(
        """
        INSERT INTO wallet_token_interactions (
            chain_id, token_id, wallet_id, first_interaction, last_interaction,
            last_balance_token
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (token_id, wallet_id) DO UPDATE
        SET 
            last_interaction = EXCLUDED.last_interaction,
            last_balance_token = EXCLUDED.last_balance_token,
            interaction_count = wallet_token_interactions.interaction_count + 1
        """,
        (chain_id, token_id, wallet_id, event.timestamp, event.timestamp, event.last_balance_token)
    )

async def insert_trade(cur, chain_id, token_id, event):
    # Upsert Profile
    await cur.execute(
        """
        INSERT INTO wallet_profiles (chain_id, address, first_seen, last_seen)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (chain_id, address) DO UPDATE
        SET last_seen = GREATEST(wallet_profiles.last_seen, EXCLUDED.last_seen)
        """,
        (chain_id, event.wallet_address, event.timestamp, event.timestamp)
    )
    
    # Insert Trade
    try:
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
                chain_id, token_id, event.tx_signature, event.wallet_address,
                event.side, event.amount_token, event.amount_sol, event.slot, event.timestamp
            )
        )
        return cur.rowcount > 0
    except psycopg.Error as e:
        logger.error(f"Failed to insert trade {event.tx_signature}: {e}")
        return False

async def process_batch():
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Fetch pending jobs
            await cur.execute(
                """
                SELECT id, payload, created_at, source 
                FROM raw_webhooks 
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (BATCH_SIZE,)
            )
            jobs = await cur.fetchall()

            if not jobs:
                return 0

            logger.info(f"Worker processing {len(jobs)} jobs...")
            
            # --- Chain Setup ---
            # Assume 'solana' exists or seed it
            await cur.execute("SELECT id FROM chains WHERE name = 'solana'")
            chain_row = await cur.fetchone()
            if not chain_row:
                await cur.execute("INSERT INTO chains (name) VALUES ('solana') RETURNING id")
                chain_id = (await cur.fetchone())[0]
            else:
                chain_id = chain_row[0]

            adapter = registry.get("solana")
            batch_token_ids = {}

            # Process Jobs
            async with conn.transaction():
                for job in jobs:
                    job_id = job[0]
                    payload = job[1]
                    source = job[3]
                    
                    # Stats tracking
                    events_received = 0
                    swaps_inserted = 0
                    ignored_no_tracked_tokens = 0
                    ignored_exception = 0
                    
                    try:
                        if isinstance(payload, list):
                            events_received = len(payload)
                            for raw_tx in payload:
                                try:
                                    # 1. Normalize via Adapter
                                    canonical_events = adapter.normalize_tx(raw_tx)
                                    
                                    # 2. Process Canonical Events
                                    found_tracked_token = False
                                    
                                    for event in canonical_events:
                                        if event.token_address not in TRACKED_TOKENS:
                                            continue
                                        
                                        found_tracked_token = True
                                        
                                        token_id = await ensure_token_id(
                                            cur, chain_id, event.token_address, event.timestamp, batch_token_ids
                                        )
                                        if not token_id: continue

                                        if isinstance(event, CanonicalWalletInteraction):
                                            await upsert_wallet_interaction(cur, chain_id, token_id, event)
                                            
                                        elif isinstance(event, CanonicalTrade):
                                            inserted = await insert_trade(cur, chain_id, token_id, event)
                                            if inserted:
                                                swaps_inserted += 1

                                    if not found_tracked_token:
                                        ignored_no_tracked_tokens += 1

                                    # 3. Legacy Write (Raw JSON) - Optional/Safety
                                    swap = raw_tx.get("events", {}).get("swap")
                                    if swap:
                                        signature = raw_tx.get("signature")
                                        if signature:
                                            try:
                                                # Use block_time from tx if avail, else now
                                                ts_raw = raw_tx.get("timestamp")
                                                bt = datetime.fromtimestamp(ts_raw, tz=timezone.utc) if ts_raw else datetime.now(timezone.utc)
                                                
                                                await cur.execute(
                                                    """
                                                    INSERT INTO events (
                                                        tx_signature, slot, event_type, wallet,
                                                        token_mint, amount, block_time, program_id, metadata, direction
                                                    )
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                    ON CONFLICT (tx_signature, event_type, wallet) DO NOTHING
                                                    """,
                                                    (
                                                        signature, raw_tx.get("slot"), "swap", 
                                                        raw_tx.get("feePayer"), 
                                                        "legacy", 0, 
                                                        bt,
                                                        swap.get("program", ""), 
                                                        json.dumps(raw_tx), 
                                                        "unknown"
                                                    ),
                                                )
                                            except:
                                                pass 

                                except Exception as e:
                                    ignored_exception += 1
                                    logger.error(f"Tx processing error: {e}")

                        # Insert Stats
                        swaps_ignored = ignored_no_tracked_tokens + ignored_exception
                        try:
                            await cur.execute(
                                """
                                INSERT INTO ingestion_stats (
                                    source, events_received, swaps_inserted, swaps_ignored,
                                    ignored_missing_fields, ignored_no_swap_event, ignored_no_tracked_tokens,
                                    ignored_constraint_violation, ignored_exception
                                )
                                VALUES (%s, %s, %s, %s, 0, 0, %s, 0, %s)
                                """,
                                (
                                    f"{source}-worker", events_received, swaps_inserted, swaps_ignored,
                                    ignored_no_tracked_tokens, ignored_exception
                                ),
                            )
                        except Exception:
                            pass

                        # Mark Job Completed
                        await cur.execute(
                            "UPDATE raw_webhooks SET status = 'processed', processed_at = NOW() WHERE id = %s",
                            (job_id,)
                        )
                
                except Exception as job_err:
                    logger.error(f"Job {job_id} failed: {job_err}")
                    await cur.execute(
                        "UPDATE raw_webhooks SET status = 'failed', error_message = %s, processed_at = NOW() WHERE id = %s",
                        (str(job_err), job_id)
                    )

            await conn.commit()
            return len(jobs)

async def run_worker():
    logger.info("Worker starting up...")
    await init_db()
    
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, handle_signal)
    
    # Try SIGTERM, might fail on some OS/environments if not supported in loop
    try:
        loop.add_signal_handler(signal.SIGTERM, handle_signal)
    except NotImplementedError:
        pass

    logger.info("Worker running. Waiting for jobs...")

    idle_polls = 0
    poll_interval = POLL_INTERVAL

    while not shutdown_event.is_set():
        try:
            count = await process_batch()
            if count == 0:
                idle_polls += 1
                poll_interval = min(POLL_INTERVAL * (1.5 ** (idle_polls - 1)), 10.0)
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    pass
            else:
                idle_polls = 0
                await asyncio.sleep(0.05)
                
        except Exception as e:
            logger.error(f"Worker loop error: {e}")
            await asyncio.sleep(1.0)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
