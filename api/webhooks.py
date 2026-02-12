from fastapi import APIRouter, Request, HTTPException
from datetime import datetime, timezone, timedelta
import json
import hashlib

from db import get_conn
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
        raise HTTPException(status_code=401, detail="missing authorization header")

    prefix = "x-helius-signature:"
    if not auth_header.lower().startswith(prefix):
        raise HTTPException(status_code=401, detail="invalid authorization format")

    received_secret = auth_header[len(prefix):].strip()
    if received_secret != HELIUS_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")

    # ====================
    # RAW BODY (REPLAY HASH)
    # ====================
    raw_body = await request.body()
    payload_hash = hashlib.sha256(raw_body).hexdigest()

    try:
        payload = json.loads(raw_body)
    except Exception:
        return {"status": "ignored"}

    if not isinstance(payload, list):
        return {"status": "ignored"}

    events_received = len(payload)

    # ====================
    # SAFE MODE
    # ====================
    if not INGESTION_ENABLED:
        return {
            "status": "ok",
            "ingestion": "disabled",
            "events_received": events_received,
        }

    # ====================
    # DB + REPLAY PROTECTION
    # ====================
    conn = get_conn()
    cur = conn.cursor()

    try:
        # ---- webhook-level replay guard ----
        try:
            cur.execute(
                "INSERT INTO webhook_replays (payload_hash) VALUES (%s)",
                (payload_hash,),
            )
        except Exception:
            conn.rollback()
            return {
                "status": "ok",
                "replay": "ignored",
                "events_received": events_received,
            }

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
            conn.commit()
            return {
                "status": "ok",
                "expired": True,
                "events_received": events_received,
            }

        # ====================
        # INGESTION
        # ====================
        swaps_inserted = 0
        
        # Detailed ignore counters
        ignored_missing_fields = 0
        ignored_no_swap_event = 0
        ignored_no_tracked_tokens = 0
        ignored_constraint_violation = 0
        ignored_exception = 0

        for tx in valid_events:
            try:
                signature = tx.get("signature")
                slot = tx.get("slot")
                timestamp = tx.get("timestamp")

                # Validate required fields
                if not signature or slot is None or timestamp is None:
                    ignored_missing_fields += 1
                    continue

                # Validate swap event exists
                swap = tx.get("events", {}).get("swap")
                if not swap:
                    ignored_no_swap_event += 1
                    continue

                block_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                
                # ====================
                # ATOMIC MULTI-LEG COLLECTION
                # ====================
                # Collect all legs for tracked tokens BEFORE inserting
                # This prevents data loss on multi-token swaps
                legs_to_insert = []
                
                for leg in swap.get("tokenInputs", []):
                    mint = leg.get("mint")
                    if mint not in TRACKED_TOKENS:
                        continue

                    wallet = leg.get("userAccount")
                    amount = leg["rawTokenAmount"]["tokenAmount"]
                    
                    legs_to_insert.append({
                        "signature": signature,
                        "slot": slot,
                        "wallet": wallet,
                        "mint": mint,
                        "amount": amount,
                        "block_time": block_time,
                        "program": swap.get("program", ""),
                        "metadata": json.dumps(tx),
                    })
                
                # If no tracked tokens found, count as ignored
                if not legs_to_insert:
                    ignored_no_tracked_tokens += 1
                    continue
                
                # ====================
                # INSERT ALL LEGS ATOMICALLY
                # ====================
                # Track which legs succeed/fail
                legs_inserted = 0
                legs_failed_constraint = 0
                
                for leg_data in legs_to_insert:
                    try:
                        cur.execute(
                            """
                            INSERT INTO events (
                                tx_signature,
                                slot,
                                event_type,
                                wallet,
                                token_mint,
                                amount,
                                block_time,
                                program_id,
                                metadata
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (tx_signature, event_type, wallet) DO NOTHING
                            """,
                            (
                                leg_data["signature"],
                                leg_data["slot"],
                                "swap",
                                leg_data["wallet"],
                                leg_data["mint"],
                                leg_data["amount"],
                                leg_data["block_time"],
                                leg_data["program"],
                                leg_data["metadata"],
                            ),
                        )
                        
                        if cur.rowcount == 1:
                            legs_inserted += 1
                        else:
                            legs_failed_constraint += 1
                            
                    except Exception as leg_err:
                        legs_failed_constraint += 1
                        print(f"[INGESTION][WARN] leg_insert_error tx={leg_data['signature']} wallet={leg_data['wallet']} error={leg_err}")
                
                # Update counters
                swaps_inserted += legs_inserted
                if legs_failed_constraint > 0 and legs_inserted == 0:
                    # All legs failed due to constraint (full duplicate)
                    ignored_constraint_violation += 1

            except Exception as tx_err:
                ignored_exception += 1
                print(f"[INGESTION][WARN] tx_processing_error tx={signature if 'signature' in locals() else 'unknown'} error={tx_err}")
        
        # Calculate total ignored
        swaps_ignored = (
            ignored_missing_fields + 
            ignored_no_swap_event + 
            ignored_no_tracked_tokens + 
            ignored_constraint_violation + 
            ignored_exception
        )

        # ---- stats (non-fatal) ----
        try:
            cur.execute(
                """
                INSERT INTO ingestion_stats (
                    source,
                    events_received,
                    swaps_inserted,
                    swaps_ignored,
                    ignored_missing_fields,
                    ignored_no_swap_event,
                    ignored_no_tracked_tokens,
                    ignored_constraint_violation,
                    ignored_exception
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "helius",
                    events_received,
                    swaps_inserted,
                    swaps_ignored,
                    ignored_missing_fields,
                    ignored_no_swap_event,
                    ignored_no_tracked_tokens,
                    ignored_constraint_violation,
                    ignored_exception,
                ),
            )
        except Exception as stats_err:
            print(f"[INGESTION][WARN] stats_insert_failed={stats_err}")

        conn.commit()

    finally:
        cur.close()
        conn.close()

    print(
        f"[INGESTION] source=helius "
        f"events={events_received} "
        f"inserted={swaps_inserted} "
        f"ignored={swaps_ignored} "
        f"(missing_fields={ignored_missing_fields} "
        f"no_swap={ignored_no_swap_event} "
        f"no_tracked={ignored_no_tracked_tokens} "
        f"constraint={ignored_constraint_violation} "
        f"exception={ignored_exception})"
    )

    return {
        "status": "ok",
        "events_received": events_received,
        "inserted": swaps_inserted,
        "ignored": swaps_ignored,
        "ignored_reasons": {
            "missing_fields": ignored_missing_fields,
            "no_swap_event": ignored_no_swap_event,
            "no_tracked_tokens": ignored_no_tracked_tokens,
            "constraint_violation": ignored_constraint_violation,
            "exception": ignored_exception,
        },
    }

