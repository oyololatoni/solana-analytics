from fastapi import APIRouter, Request, HTTPException
from datetime import datetime, timezone, timedelta
import json
import hashlib
import psycopg

from api import logger
from api.db import get_db_connection
from config import (
    HELIUS_WEBHOOK_SECRET,
    TRACKED_TOKENS,
    INGESTION_ENABLED,
)

router = APIRouter(prefix="/webhooks")


@router.post("/helius")
async def helius_webhook(request: Request):
    # ====================
    # AUTH
    # ====================
    auth_header = request.headers.get("authorization")
    if not auth_header:
        logger.warning("Webhook received without authorization header")
        raise HTTPException(status_code=401, detail="missing authorization header")

    prefix = "x-helius-signature:"
    if not auth_header.lower().startswith(prefix):
        logger.warning(f"Invalid auth header format: {auth_header[:20]}...")
        raise HTTPException(status_code=401, detail="invalid authorization format")

    received_secret = auth_header[len(prefix):].strip()
    if received_secret != HELIUS_WEBHOOK_SECRET:
        logger.error("Unauthorized webhook secret")
        raise HTTPException(status_code=401, detail="unauthorized")

    # ====================
    # RAW BODY (REPLAY HASH)
    # ====================
    raw_body = await request.body()
    payload_hash = hashlib.sha256(raw_body).hexdigest()

    try:
        payload = json.loads(raw_body)
    except Exception as e:
        logger.error(f"Failed to parse webhook JSON: {e}")
        return {"status": "ignored", "reason": "invalid_json"}

    if not isinstance(payload, list):
        logger.warning("Webhook payload is not a list")
        return {"status": "ignored", "reason": "not_a_list"}

    events_received = len(payload)

    # ====================
    # SAFE MODE
    # ====================
    if not INGESTION_ENABLED:
        logger.info(f"Ingestion disabled, ignoring {events_received} events")
        # Try to record stats if DB is reachable
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO ingestion_stats (
                            source, events_received, swaps_inserted, swaps_ignored,
                            ignored_ingestion_disabled
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        ("helius", events_received, 0, events_received, events_received)
                    )
                    await conn.commit()
        except Exception as e:
            logger.warning(f"Could not record ingestion stats (safe mode): {e}")

        return {
            "status": "ok",
            "ingestion": "disabled",
            "events_received": events_received,
        }

    # ====================
    # DB + REPLAY PROTECTION
    # ====================
    
    # 🛑 GUARDRAIL: Retry on connection failure
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                
                # ---- webhook-level replay guard ----
                try:
                    await cur.execute(
                        "INSERT INTO webhook_replays (payload_hash) VALUES (%s)",
                        (payload_hash,),
                    )
                except psycopg.IntegrityError:
                    await conn.rollback()
                    logger.info(f"Ignored replay payload: {payload_hash}")
                    
                    # Record stats for replay
                    try:
                        await cur.execute(
                            """
                            INSERT INTO ingestion_stats (
                                source, events_received, swaps_inserted, swaps_ignored,
                                ignored_replay
                            )
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            ("helius", events_received, 0, events_received, events_received)
                        )
                        await conn.commit()
                    except Exception:
                        pass # Squelch error in replay handler

                    return {
                        "status": "ok",
                        "replay": "ignored",
                        "events_received": events_received,
                    }
                except psycopg.OperationalError as op_err:
                    # Transient error -> Raise 500 for retry
                    logger.error(f"DB Operational Error (Replay Check): {op_err}")
                    raise HTTPException(status_code=500, detail="database constraint check failed")

                # ====================
                # TIME WINDOW GUARD
                # ====================
                now = datetime.now(timezone.utc)
                max_age = timedelta(minutes=10)

                valid_events = []
                for tx in payload:
                    ts = tx.get("timestamp")
                    if not ts:
                        continue
                    try:
                        event_time = datetime.fromtimestamp(ts, tz=timezone.utc)
                    except Exception:
                        continue
                    if now - event_time <= max_age:
                        valid_events.append(tx)

                if not valid_events:
                    # Record stats for expired? (Maybe treated as 'ignored_missing_fields' or just logged?)
                    # For now just log, as schema has no specific column for 'expired'. 
                    # User asked for 'missing_required_fields' or generic.
                    # We'll skip stats for strictly expired payloads to avoid noise, or add column later.
                    # Actually, let's treat it as 'swaps_ignored' with specific reason if we had one.
                    # Given constraint, we just log.
                    await conn.commit()
                    logger.info("All events in payload expired")
                    return {
                        "status": "ok",
                        "expired": True,
                        "events_received": events_received,
                    }

                # ====================
                # INGESTION
                # ====================
                swaps_inserted = 0
                
                # Granular ignore counters
                ignored_missing_fields = 0
                ignored_no_swap_event = 0
                ignored_no_tracked_tokens = 0
                ignored_constraint_violation = 0
                ignored_exception = 0

                for tx in valid_events:
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
                        found_tracked_token = False

                        # Broad Swap Detection
                        all_legs = swap.get("tokenInputs", []) + swap.get("tokenOutputs", [])
                        
                        for leg in all_legs:
                            mint = leg.get("mint")
                            if mint not in TRACKED_TOKENS:
                                continue
                            
                            found_tracked_token = True
                            wallet = leg.get("userAccount")
                            amount = leg.get("rawTokenAmount", {}).get("tokenAmount")
                            
                            if not amount:
                                continue

                            # Try Insert
                            try:
                                await cur.execute(
                                    """
                                    INSERT INTO events (
                                        tx_signature, slot, event_type, wallet,
                                        token_mint, amount, block_time, program_id, metadata
                                    )
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (tx_signature, event_type, wallet) DO NOTHING
                                    """,
                                    (
                                        signature, slot, "swap", wallet, mint, amount,
                                        block_time, swap.get("program", ""), json.dumps(tx),
                                    ),
                                )
                                
                                if cur.rowcount == 1:
                                    swaps_inserted += 1
                                else:
                                    ignored_constraint_violation += 1
                                    
                            except psycopg.IntegrityError:
                                # This handles concurrent inserts that ON CONFLICT might race with
                                ignored_constraint_violation += 1
                                pass

                        if not found_tracked_token:
                            ignored_no_tracked_tokens += 1
                            
                    except psycopg.OperationalError as op_err:
                        # 🛑 Critical: Connectivity lost mid-processing -> Raise 500
                        logger.error(f"DB Connectivity Lost processing tx {signature}: {op_err}")
                        raise HTTPException(status_code=500, detail="database connection lost")
                    except Exception as tx_err:
                        # Data-level error (parsing, type error) -> Logs and Ignore
                        ignored_exception += 1
                        logger.error(f"Error processing tx {signature}: {tx_err}")

                # Total ignored
                total_ignored = (
                    ignored_missing_fields + 
                    ignored_no_swap_event + 
                    ignored_no_tracked_tokens + 
                    ignored_constraint_violation + 
                    ignored_exception
                )

                # ---- stats (non-fatal) ----
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
                            "helius", events_received, swaps_inserted, total_ignored,
                            ignored_missing_fields, ignored_no_swap_event, ignored_no_tracked_tokens,
                            ignored_constraint_violation, ignored_exception
                        ),
                    )
                except Exception as stats_err:
                    logger.warning(f"Failed to insert ingestion stats: {stats_err}")

                await conn.commit()

                logger.info(
                    f"HELIOUS WEBHOOK | received: {events_received} | inserted: {swaps_inserted} | ignored: {total_ignored} "
                    f"(tracked: {ignored_no_tracked_tokens}, exists: {ignored_constraint_violation})"
                )

                return {
                    "status": "ok",
                    "events_received": events_received,
                    "inserted": swaps_inserted,
                    "ignored": total_ignored,
                    "details": {
                        "no_tracked_tokens": ignored_no_tracked_tokens,
                        "constraint_violations": ignored_constraint_violation,
                        "missing_fields": ignored_missing_fields,
                        "no_swap_event": ignored_no_swap_event,
                        "exceptions": ignored_exception
                    }
                }

    except psycopg.OperationalError as e:
        # 🛑 OUTER CATCH: Connection error during connection acquisition or commit
        logger.error(f"Database unavailable: {e}")
        raise HTTPException(status_code=500, detail="database unavailable")
    except Exception as e:
        # Unexpected error (e.g. pool exhausted)
        # Check if it's an HTTPException already raised
        if isinstance(e, HTTPException):
            raise e
        logger.error(f"Unexpected webhook error: {e}")
        raise HTTPException(status_code=500, detail="internal server error")


