import os
import time
import json
import requests
import psycopg
from datetime import datetime, timezone

# ------------------
# CONFIG
# ------------------

TOKEN_MINT = "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump"
BASE_URL = f"https://api.helius.xyz/v0/addresses/{TOKEN_MINT}/transactions"

HELIUS_API_KEY = os.environ["HELIUS_API_KEY"]
DATABASE_URL = os.environ.get("DATABASE_URL")

DRY_RUN = os.environ.get("DRY_RUN") == "1"

# ------------------
# DB
# ------------------

def get_conn():
    if DRY_RUN:
        return None
    return psycopg.connect(DATABASE_URL, connect_timeout=10)

# ------------------
# CORE LOGIC
# ------------------

def backfill(limit_per_page=100, max_pages=50):
    conn = get_conn()
    cur = conn.cursor() if conn else None

    gt_time = None
    if not DRY_RUN:
        cur.execute(
            "SELECT MAX(block_time) FROM events WHERE token_mint = %s",
            (TOKEN_MINT,)
        )
        last_ts = cur.fetchone()[0]
        gt_time = int(last_ts.timestamp()) if last_ts else None

    params = {
        "api-key": HELIUS_API_KEY,
        "type": "SWAP",
        "limit": limit_per_page,
    }

    pages = 0
    before = None

    while pages < max_pages:
        print(f"Fetching page {pages + 1}")

        if before:
            params["before"] = before
        if gt_time:
            params["gt-time"] = gt_time

        r = requests.get(BASE_URL, params=params, timeout=15)
        r.raise_for_status()
        txs = r.json()

        if not txs:
            break

        for tx in txs:
            signature = tx.get("signature")
            slot = tx.get("slot")
            timestamp = tx.get("timestamp")

            if not signature or slot is None or timestamp is None:
                continue

            swap = tx.get("events", {}).get("swap")
            if not swap:
                continue

            wallet = None
            amount = None

            for leg in swap.get("tokenInputs", []):
                if leg.get("mint") == TOKEN_MINT:
                    wallet = leg.get("userAccount")
                    amount = leg["rawTokenAmount"]["tokenAmount"]
                    break

            if wallet is None or amount is None:
                continue

            block_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)

            if DRY_RUN:
                print({
                    "tx_signature": signature,
                    "slot": slot,
                    "event_type": "swap",
                    "wallet": wallet,
                    "token_mint": TOKEN_MINT,
                    "amount": amount,
                    "block_time": block_time.isoformat(),
                })
            else:
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
                        TOKEN_MINT,
                        amount,
                        block_time,
                        swap.get("program", ""),
                        json.dumps(tx),
                    ),
                )

        if not DRY_RUN:
            conn.commit()

        before = txs[-1]["signature"]
        pages += 1
        time.sleep(0.3)

    if cur:
        cur.close()
        conn.close()

    print("Backfill complete")

# ------------------
# ENTRYPOINT
# ------------------

if __name__ == "__main__":
    backfill()

