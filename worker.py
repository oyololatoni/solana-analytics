import asyncio
import json
import os
import signal
import psycopg
from datetime import datetime, timezone

from api import logger
from api.db import init_db, close_db, get_db_connection
from config import TRACKED_TOKENS

# Configuration
BATCH_SIZE = 50
POLL_INTERVAL = 1.0  # seconds

# Global shutdown event
shutdown_event = asyncio.Event()

def handle_signal():
    logger.info("Shutdown signal received, stopping worker...")
    shutdown_event.set()

async def process_batch():
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Fetch pending jobs with locking
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
            
            # --- PRE-FETCH CHAIN ID ---
            # We assume 'solana' exists from migration seeding.
            await cur.execute("SELECT id FROM chains WHERE name = 'solana'")
            chain_row = await cur.fetchone()
            if not chain_row:
                # Fallback if not seeded
                await cur.execute("INSERT INTO chains (name) VALUES ('solana') RETURNING id")
                chain_row = await cur.fetchone()
            chain_id = chain_row[0]
            
            # Local cache for this batch to avoid repeated lookups
            # mint -> token_id
            batch_token_ids = {}

            for job_id, payload, created_at, source in jobs:
                # Stats accumulation for this job
                events_received = len(payload)
                swaps_inserted = 0
                swaps_ignored = 0
                
                # Granular counters
                ignored_missing_fields = 0
                ignored_no_swap_event = 0
                ignored_no_tracked_tokens = 0
                ignored_constraint_violation = 0
                ignored_exception = 0

                try:
                    # Use a nested transaction (SAVEPOINT) for each job
                    async with conn.transaction():
                        for tx in payload:
                            try:
                                # 1. Basic Fields
                                signature = tx.get("signature")
                                slot = tx.get("slot")
                                timestamp = tx.get("timestamp")

                                if not signature or slot is None or timestamp is None:
                                    ignored_missing_fields += 1
                                    continue

                                # 2. Swap Event
                                swap = tx.get("events", {}).get("swap")
                                if not swap:
                                    ignored_no_swap_event += 1
                                    continue
                                
                                block_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                                found_tracked_token = False
                                inserted_for_tx = False

                                # 3. Directional Swap Detection
                                legs_to_process = []
                                
                                # Token Outputs = Wallet RECEIVED token (Buy / 'in')
                                for leg in swap.get("tokenOutputs", []):
                                    legs_to_process.append((leg, 'in'))
                                    
                                # Token Inputs = Wallet SENT token (Sell / 'out')
                                for leg in swap.get("tokenInputs", []):
                                    legs_to_process.append((leg, 'out'))
                                
                                for leg, direction in legs_to_process:
                                    mint = leg.get("mint")
                                    if mint not in TRACKED_TOKENS:
                                        continue
                                    
                                    found_tracked_token = True
                                    wallet = leg.get("userAccount")
                                    # Handle rawTokenAmount potentially being None or nested
                                    # Helius sometimes sends it as String or Object
                                    raw_amount_obj = leg.get("rawTokenAmount")
                                    amount_str = None
                                    if isinstance(raw_amount_obj, dict):
                                        amount_str = raw_amount_obj.get("tokenAmount")
                                    elif isinstance(raw_amount_obj, str):
                                        amount_str = raw_amount_obj
                                    
                                    try:
                                        amount = float(amount_str) if amount_str else 0.0
                                    except ValueError:
                                        amount = 0.0

                                    if amount <= 0:
                                        continue

                                    # --- 4. PRODUCTION SCHEMA WRITE (trades, tokens, wallet_profiles) ---
                                    
                                    # A. Resolve Token ID
                                    token_id = batch_token_ids.get(mint)
                                    if not token_id:
                                        # Deduplicate token insertion
                                        # Use ON CONFLICT DO UPDATE to get ID reliably
                                        logger.info(f"Resolving token ID for {mint} on chain {chain_id}")
                                        await cur.execute(
                                            """
                                            INSERT INTO tokens (chain_id, address, created_at_chain)
                                            VALUES (%s, %s, %s)
                                            ON CONFLICT (chain_id, address) DO UPDATE 
                                            SET address = EXCLUDED.address -- No-op to return ID
                                            RETURNING id
                                            """,
                                            (chain_id, mint, block_time)
                                        )
                                        row = await cur.fetchone()
                                        if row:
                                            token_id = row[0]
                                            batch_token_ids[mint] = token_id
                                            logger.info(f"Resolved token ID: {token_id}")
                                        else:
                                            logger.error(f"Failed to resolve token ID for {mint}")
                                            continue

                                    # B. Upsert Wallet Profile
                                    # Only update last_seen if newer
                                    # We don't need ID for trades table (it uses address text)
                                    if wallet:
                                        await cur.execute(
                                            """
                                            INSERT INTO wallet_profiles (chain_id, address, first_seen, last_seen)
                                            VALUES (%s, %s, %s, %s)
                                            ON CONFLICT (chain_id, address) DO UPDATE
                                            SET last_seen = GREATEST(wallet_profiles.last_seen, EXCLUDED.last_seen)
                                            """,
                                            (chain_id, wallet, block_time, block_time)
                                        )

                                    # C. Insert Trade
                                    side = 'buy' if direction == 'in' else 'sell'
                                    try:
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
                                                chain_id, token_id, signature, wallet,
                                                side, amount, slot, block_time
                                            )
                                        )
                                    except psycopg.Error as e:
                                        # Log constraint violations or other errors
                                        logger.error(f"Failed to insert trade {signature}: {e}")
                                        pass


                                    # --- 5. LEGACY WRITE (events) ---
                                    try:
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
                                                signature, slot, "swap", wallet, mint, amount,
                                                block_time, swap.get("program", ""), json.dumps(tx), direction
                                            ),
                                        )
                                        
                                        if cur.rowcount == 1:
                                            swaps_inserted += 1
                                            inserted_for_tx = True
                                        else:
                                            # If events table rejects it (duplicate), we count it as ignored constraint
                                            # But we successfully tried writing to trades.
                                            pass
                                            
                                    except psycopg.IntegrityError:
                                        ignored_constraint_violation += 1
                                        pass

                                if not found_tracked_token:
                                    ignored_no_tracked_tokens += 1

                            except Exception as tx_err:
                                ignored_exception += 1
                                logger.error(f"Error processing tx in job {job_id}: {tx_err}")

                        # Total ignored calculation
                        swaps_ignored = (
                            ignored_missing_fields + 
                            ignored_no_swap_event + 
                            ignored_no_tracked_tokens + 
                            ignored_constraint_violation + 
                            ignored_exception
                        )

                        # Insert Stats
                        try:
                            await cur.execute(
                                """
                                INSERT INTO ingestion_stats (
                                    source, events_received, swaps_inserted, swaps_ignored,
                                    ignored_missing_fields, ignored_no_swap_event, ignored_no_tracked_tokens,
                                    ignored_constraint_violation, ignored_exception
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """,
                                (
                                    f"{source}-worker", events_received, swaps_inserted, swaps_ignored,
                                    ignored_missing_fields, ignored_no_swap_event, ignored_no_tracked_tokens,
                                    ignored_constraint_violation, ignored_exception
                                ),
                            )
                        except Exception as stats_err:
                            logger.warning(f"Failed to insert worker stats: {stats_err}")

                        # Mark Job Completed
                        await cur.execute(
                            """
                            UPDATE raw_webhooks 
                            SET status = 'processed', processed_at = NOW() 
                            WHERE id = %s
                            """,
                            (job_id,)
                        )
                
                except Exception as job_err:
                    # Savepoint rolled back automatically. Lock is held by outer TX.
                    logger.error(f"Job {job_id} failed: {job_err}")
                    # Mark status as failed in the outer transaction
                    try:
                        await cur.execute(
                            """
                            UPDATE raw_webhooks 
                            SET status = 'failed', error_message = %s, processed_at = NOW() 
                            WHERE id = %s
                            """,
                            (str(job_err), job_id)
                        )
                    except Exception as update_err:
                         # If this fails, the whole batch fails, but logic is sound.
                        logger.error(f"Failed to update job {job_id} failure status: {update_err}")

            await conn.commit()
            return len(jobs)

async def run_worker():
    logger.info("Worker starting up...")
    await init_db()
    
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, handle_signal)
    loop.add_signal_handler(signal.SIGTERM, handle_signal)

    logger.info("Worker running. Waiting for jobs...")

    idle_polls = 0
    poll_interval = POLL_INTERVAL

    while not shutdown_event.is_set():
        try:
            count = await process_batch()
            if count == 0:
                idle_polls += 1
                # Exponential backoff: 1s -> 1.5s -> 2.25s -> ... -> 10s
                poll_interval = min(POLL_INTERVAL * (1.5 ** (idle_polls - 1)), 10.0)
                
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=poll_interval)
                except asyncio.TimeoutError:
                    pass
            else:
                idle_polls = 0
                # Yield briefly to let other tasks run (e.g. signal handlers)
                await asyncio.sleep(0.05)
                
        except Exception as e:
            logger.error(f"Worker loop error: {e}")
            await asyncio.sleep(5)  # Backoff on DB error

    logger.info("Worker shutting down...")
    await close_db()
    logger.info("Worker stopped.")

if __name__ == "__main__":
    asyncio.run(run_worker())
