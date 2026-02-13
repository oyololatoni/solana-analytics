
import asyncio
import json
import os
import signal
import psycopg
import logging
from datetime import datetime, timezone
from decimal import Decimal

logger = logging.getLogger("worker")
from app.core.db import init_db, close_db, get_db_connection
from app.core.config import TRACKED_TOKENS

# Feature Pipeline
from app.engines.v1.features import check_snapshot_trigger
from app.engines.v1.label_worker import run_resolution_engine
from app.engines.v1.eligibility import run_eligibility_check

# Chain Abstraction
from app.ingestion.registry import registry
from app.ingestion.solana_adapter import SolanaAdapter
from app.ingestion.models import CanonicalTrade, CanonicalWalletInteraction

# Register Adapters
registry.register("solana", SolanaAdapter())

# Configuration
BATCH_SIZE = 500
POLL_INTERVAL = 1.0  # seconds

# Global shutdown event
shutdown_event = asyncio.Event()

def handle_signal():
    logger.info("Shutdown signal received, stopping worker...")
    shutdown_event.set()

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
            token_id = row[0]
            cache[address] = token_id
            return token_id
    except Exception as e:
        logger.error(f"ensure_token_id FAILED for {address}: {e}")
        raise e
    return None

async def upsert_wallet_interaction(cur, chain_id, token_id, event):
    try:
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
    except Exception as e:
        logger.error(f"upsert_wallet_interaction FAILED for {event.wallet_address} Token {token_id}: {e}")
        raise e

async def insert_trade(cur, chain_id, token_id, event):
    try:
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
        
        # Insert Trade with pair tracking (schema_version=2)
        # Reject trade if pair_address is None (cannot determine pool)
        if not hasattr(event, 'pair_address') or event.pair_address is None:
            logger.warning(f"Skipping trade {event.tx_signature}: cannot extract pair_address")
            return False
        
        await cur.execute(
            """
            INSERT INTO trades (
                chain_id, token_id, tx_signature, wallet_address,
                side, amount_token, amount_sol, pair_address, schema_version, slot, timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 2, %s, %s)
            ON CONFLICT (chain_id, tx_signature, timestamp) DO NOTHING
            """,
            (
                chain_id, token_id, event.tx_signature, event.wallet_address,
                event.side, event.amount_token, event.amount_sol, event.pair_address, event.slot, event.timestamp
            )
        )
        return cur.rowcount > 0
    except psycopg.Error as e:
        logger.error(f"Failed to insert trade {event.tx_signature}: {e}")
        raise e

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
                    
                    # Stats
                    events_received = 0
                    swaps_inserted = 0
                    ignored_no_tracked_tokens = 0
                    ignored_exception = 0
                    
                    try:
                        # Job Savepoint
                        async with conn.transaction():
                            if isinstance(payload, list):
                                events_received = len(payload)
                                for raw_tx in payload:
                                    try:
                                        # 1. Normalize
                                        canonical_events = adapter.normalize_tx(raw_tx)
                                        
                                        # 2. Process
                                        found_tracked_token = False
                                        
                                        for event in canonical_events:
                                            # Allow all tokens for calibration phase
                                            # if event.token_address not in TRACKED_TOKENS:
                                            #     continue
                                            
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
                                            # This only happens if canonical_events was empty or ensure_token_id failed for all
                                            ignored_no_tracked_tokens += 1

                                        # 3. Legacy Write
                                        swap = raw_tx.get("events", {}).get("swap")
                                        if swap:
                                            signature = raw_tx.get("signature")
                                            if signature:
                                                try:
                                                    # SAVEPOINT for Legacy Write
                                                    async with conn.transaction():
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
                                                                None
                                                            ),
                                                        )
                                                except Exception as legacy_err:
                                                    logger.error(f"Legacy Write FAILED for {signature}: {legacy_err}")
                                                    # Savepoint handles rollback, so we can continue.

                                    except Exception as e:
                                        ignored_exception += 1
                                        logger.error(f"Tx processing error (Tx Sig: {raw_tx.get('signature')}): {e}")
                                        raise e 

                            # Insert Stats
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
                                        f"{source}-worker", events_received, swaps_inserted, 
                                        ignored_no_tracked_tokens + ignored_exception,
                                        ignored_no_tracked_tokens, ignored_exception
                                    ),
                                )
                            except Exception:
                                pass

                            # Mark Success
                            await cur.execute(
                                "UPDATE raw_webhooks SET status = 'processed', processed_at = NOW() WHERE id = %s",
                                (job_id,)
                            )
                
                    except Exception as job_err:
                        logger.error(f"Job {job_id} failed completely: {job_err}")
                        try:
                            await cur.execute(
                                "UPDATE raw_webhooks SET status = 'failed', error_message = %s, processed_at = NOW() WHERE id = %s",
                                (str(job_err), job_id)
                            )
                        except Exception as update_err:
                            logger.error(f"Failed to update job {job_id} failure status: {update_err}")

            await conn.commit()
            
            # Post-batch: Check Snapshot Triggers for each token in this batch
            if batch_token_ids:
                for token_address, token_id in batch_token_ids.items():
                    try:
                        triggered = await check_snapshot_trigger(token_id)
                        if triggered:
                            logger.info(f"Snapshot triggered for token {token_id} ({token_address})")
                    except Exception as trigger_err:
                        logger.error(f"Snapshot trigger error for {token_id}: {trigger_err}")

            return len(jobs)

async def run_worker():
    logger.info("Worker starting up...")
    await init_db()
    
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, handle_signal)
    
    try:
        loop.add_signal_handler(signal.SIGTERM, handle_signal)
    except NotImplementedError:
        pass

    logger.info("Worker running. Waiting for jobs...")

    idle_polls = 0
    poll_interval = POLL_INTERVAL
    last_rolling_metrics = 0.0  # epoch
    last_label_check = 0.0
    last_eligibility = 0.0
    ROLLING_INTERVAL = 60.0    # Compute rolling metrics every 60s
    LABEL_INTERVAL = 300.0     # Check labels every 5 minutes
    ELIGIBILITY_INTERVAL = 300.0 # Run eligibility gate every 5 minutes
    # NOTE: Calibration/training runs OFFLINE (local machine), not here.

    while not shutdown_event.is_set():
        try:
            count = await process_batch()
            now_epoch = asyncio.get_event_loop().time()

            # Periodic: Rolling Metrics (every 60s)
            if now_epoch - last_rolling_metrics > ROLLING_INTERVAL:
                try:
                    await compute_rolling_metrics()
                    last_rolling_metrics = now_epoch
                except Exception as rm_err:
                    logger.error(f"Rolling metrics error: {rm_err}")

            # Periodic: Label Worker (every 5 min)
            if now_epoch - last_label_check > LABEL_INTERVAL:
                try:
                    await run_resolution_engine()
                    last_label_check = now_epoch
                except Exception as lw_err:
                    logger.error(f"Label worker error: {lw_err}")
            
            # Periodic: Eligibility Gate (every 5 min)
            if now_epoch - last_eligibility > ELIGIBILITY_INTERVAL:
                try:
                    stats = await run_eligibility_check()
                    last_eligibility = now_epoch
                except Exception as eg_err:
                    logger.error(f"Eligibility gate error: {eg_err}")
            


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
