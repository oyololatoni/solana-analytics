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
        swaps_ignored = 0

        for tx in valid_events:
            try:
                signature = tx.get("signature")
                slot = tx.get("slot")
                timestamp = tx.get("timestamp")

                if not signature or slot is None or timestamp is None:
                    swaps_ignored += 1
                    continue

                swap = tx.get("events", {}).get("swap")
                if not swap:
                    swaps_ignored += 1
                    continue

                block_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                inserted_for_tx = False

                for leg in swap.get("tokenInputs", []):
                    mint = leg.get("mint")
                    if mint not in TRACKED_TOKENS:
                        continue

                    wallet = leg.get("userAccount")
                    amount = leg["rawTokenAmount"]["tokenAmount"]

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
                        ON CONFLICT (tx_signature) DO NOTHING
                        """,
                        (
                            signature,
                            slot,
                            "swap",
                            wallet,
                            mint,
                            amount,
                            block_time,
                            swap.get("program", ""),
                            json.dumps(tx),
                        ),
                    )

                    if cur.rowcount == 1:
                        swaps_inserted += 1
                        inserted_for_tx = True

                if not inserted_for_tx:
                    swaps_ignored += 1

            except Exception as tx_err:
                swaps_ignored += 1
                print(f"[INGESTION][WARN] tx_error={tx_err}")

        # ---- stats (non-fatal) ----
        try:
            cur.execute(
                """
                INSERT INTO ingestion_stats (
                    source,
                    events_received,
                    swaps_inserted,
                    swaps_ignored
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "helius",
                    events_received,
                    swaps_inserted,
                    swaps_ignored,
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
        f"ignored={swaps_ignored}"
    )

    return {
        "status": "ok",
        "events_received": events_received,
        "inserted": swaps_inserted,
        "ignored": swaps_ignored,
    }

