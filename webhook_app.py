from fastapi import FastAPI, Request, HTTPException
from ingest import ingest_event
import os

app = FastAPI()

# Hardcoded for now (later we’ll move to env vars)
HELIUS_SECRET = "46391c692139d66b060306058c42c50ccba563f7241c05ea0e646c366a890e68"
HELIUS_HEADER = "x-helius-secret"


@app.post("/webhook/solana")
async def helius_webhook(request: Request):
    # 1. Authenticate
    header_value = request.headers.get(HELIUS_HEADER)
    if header_value != HELIUS_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # 2. Parse payload
    payload = await request.json()

    # Helius sends a list of transactions
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="Invalid payload format")

    # 3. Ingest each transaction
    for tx in payload:
        try:
            ingest_event(tx)
        except Exception as e:
            # Do not fail the whole webhook for one bad tx
            print("Ingest error:", e)

    return {"status": "ok"}

