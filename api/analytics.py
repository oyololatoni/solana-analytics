from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta, timezone
from api.db import get_db_connection
from typing import Optional
from config import TRACKED_TOKENS, get_token_name
from api.helius import fetch_token_metadata

router = APIRouter(prefix="/analytics")

@router.get("/tokens")
async def get_tracked_tokens():
    """Returns list of {mint, name} via Helius DAS."""
    result = []
    for t in TRACKED_TOKENS:
        # Prefer "TOKEN_LABELS" from config (manual override), else fetch
        manual_name = get_token_name(t)
        if manual_name != f"{t[:4]}...{t[-4:]}":
             # User manually set a name in config/env
             name = manual_name
        else:
             # Fetch from Helius
             meta = fetch_token_metadata(t)
             name = meta.get("name", manual_name)
        
        result.append({"mint": t, "name": name})

    return sorted(result, key=lambda x: x["name"])

@router.get("/health")
async def get_health():
    """
    Returns system health metrics:
    - ingestion_status (enabled/disabled)
    - ignore_ratio_24h (swaps_ignored / events_received)
    - last_insert_age_seconds (freshness)
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 1. Ignore Ratio (Last 24h)
            await cur.execute(
                """
                SELECT 
                    COALESCE(SUM(events_received), 0), 
                    COALESCE(SUM(swaps_ignored), 0)
                FROM ingestion_stats
                WHERE created_at > NOW() - INTERVAL '24 hours'
                """
            )
            total, ignored = await cur.fetchone()
            ignore_ratio = (ignored / total) if total > 0 else 0.0

            # 2. Freshness
            await cur.execute("SELECT MAX(block_time) FROM events")
            row = await cur.fetchone()
            last_event_time = row[0]
            
            freshness_seconds = None
            if last_event_time:
                # Ensure UTC awareness
                now = datetime.now(timezone.utc)
                last_event_time = last_event_time.replace(tzinfo=timezone.utc)
                freshness_seconds = (now - last_event_time).total_seconds()

            return {
                "status": "ok",
                "ignore_ratio_24h": round(ignore_ratio, 4),
                "last_insert_age_seconds": int(freshness_seconds) if freshness_seconds is not None else None,
                "total_events_24h": total,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

@router.get("/token/{mint}")
async def get_token_stats(mint: str, window: str = "24h"):
    """
    Canonical Metrics for a token:
    - swap_count
    - volume_total
    - volume_buy (in)
    - volume_sell (out)
    - unique_makers
    """
    # Map window to interval
    intervals = {"1h": "1 hour", "24h": "24 hours", "7d": "7 days"}
    if window not in intervals:
        raise HTTPException(status_code=400, detail="Invalid window. Use 1h, 24h, 7d.")
    
    interval = intervals[window]
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT 
                    COUNT(*) as swap_count,
                    COALESCE(SUM(amount), 0) as volume_total,
                    COALESCE(SUM(CASE WHEN direction = 'in' THEN amount ELSE 0 END), 0) as volume_buy,
                    COALESCE(SUM(CASE WHEN direction = 'out' THEN amount ELSE 0 END), 0) as volume_sell,
                    COUNT(DISTINCT wallet) as unique_makers
                FROM events 
                WHERE token_mint = %s 
                  AND block_time > NOW() - INTERVAL '{interval}'
                """,
                (mint,)
            )
            row = await cur.fetchone()
            
            return {
                "mint": mint,
                "window": window,
                "swap_count": row[0],
                "volume_total": float(row[1]),
                "volume_buy": float(row[2]),
                "volume_sell": float(row[3]),
                "unique_makers": row[4],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

@router.get("/timeseries")
async def get_timeseries(mint: str = Query(..., description="Token mint"),
                         window: str = "24h",
                         bucket: str = "1h"):
    """
    Returns aggregated time-series data for charting.
    Params:
    - mint: Token Address
    - window: 24h, 7d, 30d
    - bucket: 1h, 4h, 1d
    """
    valid_windows = {"24h": "24 hours", "7d": "7 days", "30d": "30 days"}
    
    if window not in valid_windows:
        raise HTTPException(400, "Invalid window. Use 24h, 7d, 30d")
        
    window_interval = valid_windows[window]
    
    # Bucket Logic
    if bucket == "1h":
        bucket_expr = "date_trunc('hour', block_time)"
    elif bucket == "1d":
        bucket_expr = "date_trunc('day', block_time)"
    elif bucket == "4h":
        # round down to 4h block: epoch / 14400 * 14400
        bucket_expr = "to_timestamp(floor((extract('epoch' from block_time) / 14400 )) * 14400) AT TIME ZONE 'UTC'"
    else:
        raise HTTPException(400, "Invalid bucket. Use 1h, 4h, 1d")

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # We use f-string for interval/bucket which is safe as they are validated against strict allowlist above
            await cur.execute(
                f"""
                SELECT 
                    {bucket_expr} as ts,
                    COUNT(*) as swap_count,
                    COALESCE(SUM(CASE WHEN direction = 'in' THEN amount ELSE 0 END), 0) as vol_buy,
                    COALESCE(SUM(CASE WHEN direction = 'out' THEN amount ELSE 0 END), 0) as vol_sell,
                    COUNT(DISTINCT wallet) as unique_makers
                FROM events 
                WHERE token_mint = %s 
                  AND block_time > NOW() - INTERVAL '{window_interval}'
                GROUP BY 1
                ORDER BY 1 ASC
                """,
                (mint,)
            )
            rows = await cur.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    "start": row[0].isoformat() if row[0] else None,
                    "swap_count": row[1],
                    "volume_buy": float(row[2]),
                    "volume_sell": float(row[3]),
                    "unique_makers": row[4]
                })

            return {
                "mint": mint,
                "window": window,
                "bucket": bucket,
                "data": data
            }

# ---------------------------------------------------------------------------
# Phase Analysis Endpoints (Snapshot Architecture)
# ---------------------------------------------------------------------------
from api.phase_engine import get_all_states, analyze_all_tokens, analyze_token

@router.get("/phase/all")
async def get_all_phases():
    """
    Returns phase state for all tokens from the snapshot table.
    Fast read — no computation. Call POST /analytics/refresh to update.
    """
    from config import get_token_name
    from api.helius import fetch_token_metadata

    states = await get_all_states()

    # If snapshot table is empty, trigger a refresh
    if not states:
        results = await analyze_all_tokens(days=7)
        for r in results:
            mint = r["mint"]
            name = get_token_name(mint)
            if name == f"{mint[:4]}...{mint[-4:]}":
                meta = fetch_token_metadata(mint)
                if meta and meta.get("name"):
                    name = meta["name"]
            r["name"] = name
        return results

    # Attach names
    for s in states:
        mint = s["mint"]
        name = get_token_name(mint)
        if name == f"{mint[:4]}...{mint[-4:]}":
            meta = fetch_token_metadata(mint)
            if meta and meta.get("name"):
                name = meta["name"]
        s["name"] = name

    return states

@router.get("/phase/{mint}")
async def get_token_phase(mint: str):
    """Returns full phase analysis for a single token (live computation)."""
    from config import get_token_name
    result = await analyze_token(mint)
    result["name"] = get_token_name(mint)
    return result

@router.post("/refresh")
async def refresh_all_phases():
    """
    Triggers full re-analysis of all active tokens.
    Updates token_state and token_scores_history tables.
    """
    results = await analyze_all_tokens(days=7)
    return {
        "status": "ok",
        "tokens_analyzed": len(results),
        "top_ev": results[0] if results else None,
    }

