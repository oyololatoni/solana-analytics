from datetime import datetime, timezone

def normalize_enhanced_swap(payload: dict):
    """
    Convert a Helius enhanced SWAP payload
    into one or more event dicts for ingest_event().
    """

    events = []

    base = {
        "tx_signature": payload["signature"],
        "slot": payload.get("slot"),
        "block_time": payload.get("timestamp"),
        "event_type": "swap",
        "program_id": payload.get("programId"),
        "wallet": payload.get("feePayer"),
    }

    inputs = payload.get("tokenInputs", [])
    outputs = payload.get("tokenOutputs", [])

    # Pair inputs to outputs if possible, otherwise emit separately
    for inp in inputs:
        event = dict(base)

        event.update({
            "counterparty": payload.get("source"),
            "token_mint": inp.get("mint"),
            "amount": float(inp.get("tokenAmount", 0)),
            "raw_amount": int(inp.get("rawTokenAmount", 0)),
            "decimals": int(inp.get("decimals", 0)),
            "metadata": {
                "direction": "in",
                "source": payload.get("source"),
                "raw": payload,
            },
        })

        events.append(event)

    for out in outputs:
        event = dict(base)

        event.update({
            "counterparty": payload.get("source"),
            "token_mint": out.get("mint"),
            "amount": float(out.get("tokenAmount", 0)),
            "raw_amount": int(out.get("rawTokenAmount", 0)),
            "decimals": int(out.get("decimals", 0)),
            "metadata": {
                "direction": "out",
                "source": payload.get("source"),
                "raw": payload,
            },
        })

        events.append(event)

    return events

