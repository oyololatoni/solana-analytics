from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta, timezone
from app.core.db import get_db_connection
from typing import Optional
from app.core.config import TRACKED_TOKENS, get_token_name
import logging

logger = logging.getLogger("api.v1")
router = APIRouter(prefix="/analytics", tags=["analytics-v1"])

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
# LEGACY v1 - Commented out (use analytics_v2.py /snapshots instead)
# from api.phase_engine import get_all_states, analyze_all_tokens, analyze_token

@router.get("/phase/all")
async def get_all_phases():
    """
    Returns generic phase state + V1 Scoring Engine results.
    Joins 'token_state' (for phase info) with 'feature_snapshots' (for V1 scores).
    """
    from app.core.config import get_token_name

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT 
                    ts.token_mint, 
                    ts.current_phase as phase, 
                    ts.days_since_peak,
                    ts.decision_bias as decision,
                    -- V1 Scores
                    fs.score_total as ev_score, 
                    fs.score_momentum,
                    fs.score_liquidity,
                    fs.score_participation,
                    fs.score_wallet,
                    fs.score_label,
                    fs.is_sniper_candidate,
                    -- Participation
                    ts.unique_makers,
                    ts.usr,
                    ts.vpu,
                    ts.vpu_cv,
                    ts.unique_growth,
                    ts.volume_growth,
                    ts.decline_from_peak,
                    t.id,
                    -- Extended fields for enriched table
                    fs.score_risk_penalty,
                    fs.volume_acceleration,
                    fs.buy_sell_ratio,
                    fs.unique_wallet_growth_rate,
                    fs.early_wallet_retention,
                    fs.top10_concentration_delta,
                    fs.drawdown_depth_1h,
                    fs.volume_collapse_ratio,
                    fs.liquidity_stability_score,
                    fs.snapshot_time,
                    fs.feature_version,
                    t.lifecycle_stage,
                    fs.wallet_entropy_score,
                    fs.liquidity_growth_rate,
                    t.current_price,
                    t.baseline_price,
                    t.peak_price,
                    t.current_liquidity_usd,
                    t.outcome,
                    t.completed_at
                FROM token_state ts
                JOIN tokens t ON t.address = ts.token_mint
                LEFT JOIN LATERAL (
                    SELECT * 
                    FROM feature_snapshots fs 
                    WHERE fs.token_id = t.id 
                      AND fs.feature_version = 1
                    ORDER BY fs.snapshot_time DESC
                    LIMIT 1
                ) fs ON true
                ORDER BY fs.score_total DESC NULLS LAST
            """)
            
            rows = await cur.fetchall()
            results = []
            for r in rows:
                results.append({
                    "mint": r[0],
                    "phase": r[1],
                    "days_since_peak": r[2],
                    "decision": r[3],
                    "ev_score": float(r[4]) if r[4] is not None else 0.0,
                    "structural_score": float(r[5]) if r[5] is not None else 0.0,
                    "capital_score": float(r[6]) if r[6] is not None else 0.0,
                    "lifecycle_score": float(r[7]) if r[7] is not None else 0.0,
                    "wallet_score": float(r[8]) if r[8] is not None else 0.0,
                    "score_label": r[9],
                    "is_sniper_candidate": r[10],
                    "unique_makers": r[11],
                    "usr": float(r[12]) if r[12] is not None else 0.0,
                    "vpu": float(r[13]) if r[13] is not None else 0.0,
                    "vpu_cv": float(r[14]) if r[14] is not None else 0.0,
                    "unique_growth": float(r[15]) if r[15] is not None else 0.0,
                    "volume_growth": float(r[16]) if r[16] is not None else 0.0,
                    "decline_from_peak": float(r[17]) if r[17] is not None else 0.0,
                    "token_id": r[18],
                    # Extended fields
                    "risk_penalty": float(r[19]) if r[19] is not None else 0.0,
                    "volume_acceleration": float(r[20]) if r[20] is not None else 0.0,
                    "buy_sell_ratio": float(r[21]) if r[21] is not None else 0.0,
                    "unique_wallet_growth": float(r[22]) if r[22] is not None else 0.0,
                    "early_wallet_retention": float(r[23]) if r[23] is not None else 0.0,
                    "top10_concentration_delta": float(r[24]) if r[24] is not None else 0.0,
                    "drawdown_depth": float(r[25]) if r[25] is not None else 0.0,
                    "volume_collapse_ratio": float(r[26]) if r[26] is not None else 0.0,
                    "liquidity_stability": float(r[27]) if r[27] is not None else 0.0,
                    "snapshot_time": str(r[28]) if r[28] else None,
                    "feature_version": r[29],
                    "lifecycle_stage": r[30] or "UNKNOWN",
                    "wallet_entropy": float(r[31]) if r[31] is not None else 0.0,
                    "liquidity_growth": float(r[32]) if r[32] is not None else 0.0,
                    "current_price": float(r[33]) if r[33] is not None else 0.0,
                    "baseline_price": float(r[34]) if r[34] is not None else 0.0,
                    "peak_price": float(r[35]) if r[35] is not None else 0.0,
                    "liquidity_usd": float(r[36]) if r[36] is not None else 0.0,
                    "outcome": r[37],
                    "completed_at": str(r[38]) if r[38] else None,
                    "multiplier": round(float(r[33]) / float(r[34]), 2) if r[33] and r[34] and float(r[34]) > 0 else None,
                    "peak_multiplier": round(float(r[35]) / float(r[34]), 2) if r[35] and r[34] and float(r[34]) > 0 else None,
                    "name": get_token_name(r[0])
                })
        
    return results

@router.get("/phase/{mint}")
async def get_token_phase(mint: str):
    """Returns full phase analysis for a single token (live computation)."""
    from app.core.config import get_token_name
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


@router.get("/discovery")
async def get_discovery_tokens():
    """
    Lightweight read-only view of ALL ingested tokens with basic trade stats.
    No heavy computation — single efficient query.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT 
                    t.id,
                    t.address,
                    t.symbol,
                    t.name,
                    t.lifecycle_stage,
                    t.created_at,
                    t.created_at_chain,
                    COALESCE(stats.trade_count, 0) as trade_count,
                    COALESCE(stats.volume_sol, 0) as volume_sol,
                    COALESCE(stats.unique_wallets, 0) as unique_wallets,
                    stats.first_trade,
                    stats.last_trade,
                    t.eligibility_status
                FROM tokens t
                LEFT JOIN LATERAL (
                    SELECT 
                        COUNT(*) as trade_count,
                        SUM(amount_sol) as volume_sol,
                        COUNT(DISTINCT wallet_address) as unique_wallets,
                        MIN(timestamp) as first_trade,
                        MAX(timestamp) as last_trade
                    FROM trades
                    WHERE token_id = t.id
                ) stats ON true
                ORDER BY stats.last_trade DESC NULLS LAST
                LIMIT 200
            """)
            rows = await cur.fetchall()

            result = []
            for r in rows:
                age_str = ""
                if r[6]:  # created_at_chain
                    age = datetime.now(timezone.utc) - r[6].replace(tzinfo=timezone.utc) if r[6].tzinfo is None else datetime.now(timezone.utc) - r[6]
                    hours = age.total_seconds() / 3600
                    if hours < 1:
                        age_str = f"{int(hours * 60)}m"
                    elif hours < 24:
                        age_str = f"{hours:.1f}h"
                    else:
                        age_str = f"{hours / 24:.1f}d"

                result.append({
                    "id": r[0],
                    "address": r[1],
                    "symbol": r[2] or "",
                    "name": r[3] or f"{r[1][:6]}...{r[1][-4:]}",
                    "stage": r[4] or "PRE_ELIGIBLE",
                    "age": age_str,
                    "trade_count": r[7],
                    "volume_sol": float(r[8]) if r[8] else 0,
                    "unique_wallets": r[9],
                    "last_trade": r[11].isoformat() if r[11] else None,
                    "eligibility_status": r[12] or "PRE_ELIGIBLE",
                })

            return result
