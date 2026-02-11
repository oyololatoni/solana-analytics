from fastapi import APIRouter, Request, HTTPException
import json
import hashlib
import psycopg
from datetime import datetime, timezone

from api import logger
from api.db import get_db_connection
from config import (
    HELIUS_WEBHOOK_SECRET,
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
    # RAW BODY
    # ====================
    raw_body = await request.body()
    payload_hash = hashlib.sha256(raw_body).hexdigest()

    try:
        # Validate JSON structure only
        payload = json.loads(raw_body)
    except Exception as e:
        logger.error(f"Failed to parse webhook JSON: {e}")
        return {"status": "ignored", "reason": "invalid_json"}

    if not isinstance(payload, list):
        logger.warning("Webhook payload is not a list")
        return {"status": "ignored", "reason": "not_a_list"}

    events_received = len(payload)

    # ====================
    # QUEUEING
    # ====================
    status = "pending"
    if not INGESTION_ENABLED:
        status = "ignored"
        logger.info(f"Ingestion disabled, queuing as ignored: {events_received} events")

    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        """
                        INSERT INTO raw_webhooks (payload, source, payload_hash, status)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (json.dumps(payload), "helius", payload_hash, status)
                    )
                    await conn.commit()
                    
                    return {
                        "status": "ok",
                        "queued": True,
                        "events_received": events_received
                    }

                except psycopg.IntegrityError:
                    # Duplicate payload hash -> Replay detected
                    # We treat this as success (idempotency)
                    await conn.rollback()
                    logger.info(f"Ignored replay payload (hash collision): {payload_hash}")
                    return {
                        "status": "ok",
                        "replay": "ignored",
                        "events_received": events_received
                    }

    except psycopg.OperationalError as e:
        logger.error(f"Database unavailable for queueing: {e}")
        raise HTTPException(status_code=500, detail="database queue unavailable")
    except Exception as e:
        logger.error(f"Unexpected queueing error: {e}")
        raise HTTPException(status_code=500, detail="internal server error")


