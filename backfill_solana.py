import os
import time
import json
import requests
import asyncio
import psycopg
from datetime import datetime, timezone

from api import logger
from api.db import init_db, close_db, get_db_connection

# ------------------
# CONFIG
# ------------------

TOKEN_MINT = "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump"
BASE_URL = f"https://api.helius.xyz/v0/addresses/{TOKEN_MINT}/transactions"

HELIUS_API_KEY = os.environ["HELIUS_API_KEY"]
DRY_RUN = os.environ.get("DRY_RUN") == "1"

async def insert_event(cur, signature, slot, wallet, amount, raw_amount, decimals, direction, block_time, swap, tx):
    """
    Helper to insert a single event into the DB.
    """
    try:
        await cur.execute(
            """
            INSERT INTO events (
                tx_signature, slot, event_type, wallet,
                token_mint, amount, raw_amount, decimals, direction, block_time, program_id, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tx_signature, event_type, wallet) DO NOTHING
            """,
            (
                signature, slot, "swap", wallet, TOKEN_MINT, amount,
                raw_amount, decimals, direction, block_time, swap.get("program", ""), json.dumps(tx),
            ),
        )
    except psycopg.IntegrityError:
        pass
    except Exception as e:
        print(f"Error inserting {signature}: {e}")

# ------------------
# CORE LOGIC
# ------------------

async def backfill(limit_per_page=100, max_pages=50):
    if not DRY_RUN:
        await init_db()

    try:
        gt_time = None
        if not DRY_RUN:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT MAX(block_time) FROM events WHERE token_mint = %s",
                        (TOKEN_MINT,)
                    )
                    row = await cur.fetchone()
                    if row and row[0]:
                        last_ts = row[0]
                        gt_time = int(last_ts.timestamp())
        
        params = {
            "api-key": HELIUS_API_KEY,
            "type": "SWAP",
            "limit": limit_per_page,
        }

        pages = 0
        before = None

        while pages < max_pages:
            print(f"Fetching page {pages + 1} (before={before})...")

            if before:
                params["before"] = before
            if gt_time:
                params["gt-time"] = gt_time

            # Blocking request is acceptable for a backfill script
            r = requests.get(BASE_URL, params=params, timeout=15)
            r.raise_for_status()
            txs = r.json()

            if not txs:
                print("No more transactions found.")
                break

            events_received = len(txs)
            swaps_inserted = 0
            
            # Granular ignore counters
            ignored_missing_fields = 0
            ignored_no_swap_event = 0
            ignored_no_tracked_tokens = 0
            ignored_constraint_violation = 0
            ignored_exception = 0

            # DB Context per page
            if not DRY_RUN:
                async with get_db_connection() as conn:
                    async with conn.cursor() as cur:
                        for tx in txs:
                            signature = tx.get("signature")
                            try:
                                slot = tx.get("slot")
                                timestamp = tx.get("timestamp")

                                if not signature or slot is None or timestamp is None:
                                    ignored_missing_fields += 1
                                    continue

                                swap = tx.get("events", {}).get("swap")
                                if not swap:
                                    ignored_no_swap_event += 1
                                    continue

                                block_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                                found_token = False
                                
                                for leg in swap.get("tokenInputs", []):
                                    if leg.get("mint") == TOKEN_MINT:
                                        found_token = True
                                        wallet = leg.get("userAccount")
                                        amt_obj = leg.get("rawTokenAmount", {})
                                        amount = amt_obj.get("tokenAmount")
                                        raw_amount = amt_obj.get("amount")
                                        decimals = amt_obj.get("decimals")
                                        direction = "out" # Wallet sent token out = Sell
                                        
                                        if amount:
                                            await insert_event(cur, signature, slot, wallet, amount, raw_amount, decimals, direction, block_time, swap, tx)
                                            swaps_inserted += 1

                                for leg in swap.get("tokenOutputs", []):
                                    if leg.get("mint") == TOKEN_MINT:
                                        found_token = True
                                        wallet = leg.get("userAccount")
                                        amt_obj = leg.get("rawTokenAmount", {})
                                        amount = amt_obj.get("tokenAmount")
                                        raw_amount = amt_obj.get("amount")
                                        decimals = amt_obj.get("decimals")
                                        direction = "in" # Wallet received token in = Buy
                                        
                                        if amount:
                                            await insert_event(cur, signature, slot, wallet, amount, raw_amount, decimals, direction, block_time, swap, tx)
                                            swaps_inserted += 1
                                
                                if not found_token:
                                    ignored_no_tracked_tokens += 1

                            except Exception as e:
                                ignored_exception += 1
                                print(f"Error processing {signature}: {e}")

                        # Stats Insert
                        total_ignored = (
                            ignored_missing_fields + 
                            ignored_no_swap_event + 
                            ignored_no_tracked_tokens + 
                            ignored_constraint_violation + 
                            ignored_exception
                        )
                        
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
                                "backfill", events_received, swaps_inserted, total_ignored,
                                ignored_missing_fields, ignored_no_swap_event, ignored_no_tracked_tokens,
                                ignored_constraint_violation, ignored_exception
                            ),
                        )
                        await conn.commit()
                        
                        print(f"Page stats: Recv={events_received} Ins={swaps_inserted} Ign={total_ignored} (Dup={ignored_constraint_violation})")

            else:
                # DRY RUN
                print(f"[DRY RUN] Page fetched with {len(txs)} txs")

            before = txs[-1]["signature"]
            pages += 1
            await asyncio.sleep(0.3)

        print("Backfill complete")

    finally:
        if not DRY_RUN:
            await close_db()

# ------------------
# ENTRYPOINT
# ------------------

if __name__ == "__main__":
    asyncio.run(backfill())

