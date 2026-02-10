from psycopg.types.json import Json
from db import get_conn

INSERT_EVENT_SQL = """
INSERT INTO events (
    tx_signature,
    slot,
    block_time,
    event_type,
    program_id,
    wallet,
    counterparty,
    token_mint,
    amount,
    raw_amount,
    decimals,
    metadata
)
VALUES (
    %(tx_signature)s,
    %(slot)s,
    %(block_time)s,
    %(event_type)s,
    %(program_id)s,
    %(wallet)s,
    %(counterparty)s,
    %(token_mint)s,
    %(amount)s,
    %(raw_amount)s,
    %(decimals)s,
    %(metadata)s
)
ON CONFLICT (tx_signature, event_type, wallet)
DO NOTHING;
"""

def ingest_event(event: dict):
    event = dict(event)
    event["metadata"] = Json(event.get("metadata"))

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(INSERT_EVENT_SQL, event)
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    fake_event = {
        "tx_signature": "FAKE_TX_123",
        "slot": 123456789,
        "block_time": "2026-02-02T12:00:00Z",
        "event_type": "swap",
        "program_id": "FAKE_DEX_PROGRAM",
        "wallet": "WALLET_ABC",
        "counterparty": "POOL_XYZ",
        "token_mint": "TOKEN_PENGUIN",
        "amount": 100.5,
        "raw_amount": 100500000,
        "decimals": 6,
        "metadata": {"price": 0.42}
    }

    ingest_event(fake_event)
    ingest_event(fake_event)

