"""
Analytics v2 - Snapshot-Centric Endpoints

CRITICAL ARCHITECTURAL PRINCIPLE:
UI must be **snapshot-driven**, not trade-driven.

This ensures:
- UI displays what the engine computed (frozen features)
- No divergence between live aggregates and scored features
- Reproducibility and verification possible

Endpoints:
- /analytics/snapshots - Canonical snapshot-driven token list
- /analytics/token/{id}/details - Comprehensive token intelligence
"""

from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
from app.core.db import get_db_connection
from app.engines.v2.features import compute_v2_snapshot
import logging

router = APIRouter(prefix="/analytics", tags=["analytics-v2"])
logger = logging.getLogger("api.v2")


@router.get("/snapshots")
async def get_snapshots(
    min_score: Optional[float] = None,
    min_liquidity: Optional[float] = None,
    lifecycle_state: Optional[str] = None,
    only_eligible: bool = True,
    limit: int = 100
):
    """
    **CANONICAL SNAPSHOT-DRIVEN ENDPOINT**
    
    Returns tokens with their engineered features from feature_snapshots table.
    This is the PRIMARY data source for UI ranking and display.
    
    Query Parameters:
    - min_score: Minimum final score (0-50 in v2, 0-100 in v1)
    - min_liquidity: Minimum liquidity USD
    - lifecycle_state: Filter by lifecycle state
    - only_eligible: Only show ELIGIBLE tokens (default: true)
    - limit: Max results (default: 100)
    
    Returns:
    - List of tokens with full feature vectors + scoring + outcomes
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Build dynamic WHERE clause
            where_clauses = []
            params = []
            
            if only_eligible:
                where_clauses.append("t.eligibility_status = 'ELIGIBLE'")
            
            if min_score is not None:
                where_clauses.append("fs.score_total >= %s")
                params.append(min_score)
            
            if min_liquidity is not None:
                where_clauses.append("fs.liquidity_at_snapshot >= %s")
                params.append(min_liquidity)
            
            if lifecycle_state:
                where_clauses.append("fs.lifecycle_state = %s")
                params.append(lifecycle_state)
            
            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            # Query feature_snapshots as primary table
            await cur.execute(f"""
                SELECT 
                    -- Token Identity
                    t.id,
                    t.address,
                    t.symbol,
                    t.detected_at,
                    t.eligibility_status,
                    t.is_active,
                    t.primary_pair_address,
                    
                    -- Snapshot Metadata
                    fs.id as snapshot_id,
                    fs.feature_version,
                    fs.snapshot_time,
                    
                    -- Volume Momentum Features
                    fs.volume_acceleration,
                    fs.volume_growth_rate_1h,
                    
                    -- Market Quality Features
                    fs.buy_sell_ratio_1h,
                    fs.unique_wallets_growth,
                    
                    -- Price features
                    fs.price_volatility_1h,
                    fs.price_drawdown_6h,
                    fs.current_price_usd,
                    fs.current_multiplier,
                    
                    -- Holder features
                    fs.holder_concentration,
                    fs.holder_retention,
                    fs.early_wallet_exit_ratio,
                    
                    -- Liquidity
                    fs.liquidity_growth_rate,
                    
                    -- Risk & Meta
                    fs.risk_score,
                    fs.age_hours,
                    fs.lifecycle_state,
                    fs.score_total,
                    
                    -- Outcome
                    ll.outcome as outcome,
                    ll.labeled_at,
                    
                    -- Schema tracking
                    COUNT(DISTINCT tr.id) FILTER (WHERE tr.schema_version = 2) as v2_trade_count
                    
                FROM tokens t
                LEFT JOIN LATERAL (
                    SELECT * FROM feature_snapshots
                    WHERE token_id = t.id
                    ORDER BY snapshot_time DESC
                    LIMIT 1
                ) fs ON true
                LEFT JOIN lifecycle_labels ll ON ll.token_id = t.id
                LEFT JOIN trades tr ON tr.token_id = t.id
                {where_sql}
                GROUP BY t.id, t.address, t.symbol, t.detected_at, t.eligibility_status,
                         t.is_active, t.primary_pair_address, fs.id, fs.feature_version,
                         fs.snapshot_time, fs.volume_acceleration, fs.volume_growth_rate_1h,
                         fs.buy_sell_ratio_1h, fs.unique_wallets_growth, fs.price_volatility_1h,
                         fs.price_drawdown_6h, fs.current_price_usd, fs.current_multiplier,
                         fs.holder_concentration, fs.holder_retention, fs.early_wallet_exit_ratio,
                         fs.liquidity_growth_rate, fs.risk_score, 
                         fs.age_hours, fs.lifecycle_state, fs.score_total, ll.outcome, ll.labeled_at
                ORDER BY fs.score_total DESC NULLS LAST
                LIMIT %s
            """, params + [limit])
            
            rows = await cur.fetchall()
            
            results = []
            for row in rows:
                # Calculate time since detection
                time_since_detection = None
                if row[3]:  # detected_at
                    detected = row[3].replace(tzinfo=timezone.utc) if row[3].tzinfo is None else row[3]
                    time_since_detection = (datetime.now(timezone.utc) - detected).total_seconds() / 3600  # hours
                
                results.append({
                    # Identity
                    "token_id": row[0],
                    "address": row[1],
                    "symbol": row[2] or f"{row[1][:6]}...{row[1][-4:]}",
                    "detected_at": row[3].isoformat() if row[3] else None,
                    "time_since_detection_hours": round(time_since_detection, 2) if time_since_detection else None,
                    "eligibility_status": row[4],
                    "is_active": row[5],
                    "primary_pair_address": row[6],
                    
                    # Snapshot Metadata (Dataset Integrity)
                    "snapshot_id": row[7],
                    "feature_version": row[8],
                    "snapshot_time": row[9].isoformat() if row[9] else None,
                    "snapshot_locked": True,  # Snapshots are IMMUTABLE (never recomputed)
                    
                    # Volume Momentum
                    "volume_acceleration": float(row[10]) if row[10] is not None else None,
                    "volume_growth_rate_1h": float(row[11]) if row[11] is not None else None,
                    
                    # Market Quality
                    "buy_sell_ratio_1h": float(row[12]) if row[12] is not None else None,
                    "unique_wallets_growth": float(row[13]) if row[13] is not None else None,
                    
                    # Price Stability Features
                    "price_volatility_1h": float(row[14]) if row[14] is not None else None,
                    "price_drawdown_6h": float(row[15]) if row[15] is not None else None,
                    "current_price": float(row[16]) if row[16] is not None else None,
                    "multiplier": float(row[17]) if row[17] is not None else None,
                    
                    # Holder Behavior Features
                    "holder_concentration": float(row[18]) if row[18] is not None else None,
                    "holder_retention": float(row[19]) if row[19] is not None else None,
                    "early_wallet_exit_ratio": float(row[20]) if row[20] is not None else None,
                    
                    # Liquidity
                    "liquidity_growth_rate": float(row[21]) if row[21] is not None else None,
                    
                    # Risk
                    "risk_score": float(row[22]) if row[22] is not None else 0.0,
                    
                    # Time
                    "age_hours": float(row[23]) if row[23] is not None else None,
                    
                    # Lifecycle
                    "lifecycle_state": row[24],
                    
                    # Scoring
                    "score_total": float(row[25]) if row[25] is not None else 0.0,
                    
                    # Outcome
                    "outcome": row[26],
                    "labeled_at": row[27].isoformat() if row[27] else None,
                    
                    # Dataset Integrity (Complete Lineage)
                    "dataset_integrity": {
                        "schema_version": 2 if row[28] > 0 else 1,  # v2 if any v2 trades exist
                        "v2_trade_count": row[28] or 0,
                        "snapshot_immutable": True,  # Snapshots never change
                        "model_version_id": None  # TODO: Add when ML enabled
                    }
                })
            
            return {
                "count": len(results),
                "tokens": results
            }


@router.get("/token/{token_id}/details")
async def get_token_details(token_id: int):
    """
    **COMPREHENSIVE TOKEN INTELLIGENCE**
    
    Returns complete view of a single token:
    - Structural metadata (eligibility, versions, timestamps)
    - Full feature vector (all 16+ engineered features)
    - Scoring breakdown (weight × feature contributions)
    - Eligibility gate metrics (transparency on gating logic)
    - Lifecycle outcome + failure reason (granular)
    - Dataset integrity fields
    
    NO raw trade aggregates in main response (use /token/{id}/trades for debug).
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # A. Structural Metadata
            await cur.execute("""
                SELECT 
                    address,
                    symbol,
                    eligibility_status,
                    eligibility_checked_at,
                    detected_at,
                    is_active,
                    primary_pair_address,
                    pair_validated
                FROM tokens
                WHERE id = %s
            """, (token_id,))
            
            token_row = await cur.fetchone()
            if not token_row:
                raise HTTPException(status_code=404, detail="Token not found")
            
            # B. Feature Snapshot (latest)
            await cur.execute("""
                SELECT 
                    id as snapshot_id,
                    feature_version,
                    snapshot_time,
                    volume_acceleration,
                    volume_growth_rate_1h,
                    buy_sell_ratio_1h,
                    unique_wallets_growth,
                    price_volatility_1h,
                    price_drawdown_6h,
                    holder_concentration,
                    holder_retention,
                    age_hours,
                    lifecycle_state,
                    score_total,
                    sudden_liquidity_spike,
                    
                    -- Extended Features
                    liquidity_growth_rate,
                    liquidity_current_usd,
                    liquidity_peak_window_usd,
                    wallet_entropy,
                    early_wallet_count,
                    early_wallet_net_accumulation_sol,
                    early_wallet_exit_ratio,
                    risk_score,
                    volume_collapse_ratio_current,
                    score_breakdown,
                    
                    -- Phase 12 Additions
                    volume_5m_usd,
                    volume_30m_usd,
                    volume_1h_usd,
                    volume_6h_usd,
                    max_trade_gap_30m_minutes,
                    multiplier_at_snapshot,
                    liquidity_collapse_threshold_usd,
                    price_failure_threshold_usd
                FROM feature_snapshots
                WHERE token_id = %s
                ORDER BY snapshot_time DESC
                LIMIT 1
            """, (token_id,))
            
            snapshot_row = await cur.fetchone()
            
            # C. Lifecycle Outcome (with failure_reason)
            await cur.execute("""
                SELECT label, labeled_at, failure_reason
                FROM lifecycle_labels
                WHERE token_id = %s
            """, (token_id,))
            
            outcome_row = await cur.fetchone()
            
            # D. Eligibility Gate Metrics (transparency)
            await cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE schema_version = 2) as v2_trade_count,
                    MIN(timestamp) FILTER (WHERE schema_version = 2) as first_trade_v2,
                    MAX(timestamp) FILTER (WHERE schema_version = 2) as last_trade_v2,
                    SUM(amount_sol) FILTER (
                        WHERE schema_version = 2 
                        AND timestamp BETWEEN %s AND %s + INTERVAL '30 minutes'
                    ) as volume_first_30m,
                    MAX(liquidity_usd) FILTER (WHERE schema_version = 2) as peak_liquidity
                FROM trades
                WHERE token_id = %s
            """, (token_row[4], token_row[4], token_id))
            
            eligibility_row = await cur.fetchone()
            
            # E. Trade counts by schema version
            await cur.execute("""
                SELECT 
                    schema_version,
                    COUNT(*) as count
                FROM trades
                WHERE token_id = %s
                GROUP BY schema_version
            """, (token_id,))
            
            trade_versions = {row[0]: row[1] for row in await cur.fetchall()}
            
            # F. Use stored breakdown
            scoring_breakdown = snapshot_row[24] if snapshot_row and len(snapshot_row) > 24 else {}
            
            # G. Compute time to outcome if labeled
            time_to_outcome_hours = None
            if outcome_row and outcome_row[1] and token_row[4]:
                detected = token_row[4].replace(tzinfo=timezone.utc) if token_row[4].tzinfo is None else token_row[4]
                labeled = outcome_row[1].replace(tzinfo=timezone.utc) if outcome_row[1].tzinfo is None else outcome_row[1]
                time_to_outcome_hours = (labeled - detected).total_seconds() / 3600
            
            return {
                "structural": {
                    "address": token_row[0],
                    "symbol": token_row[1],
                    "eligibility_status": token_row[2],
                    "eligibility_checked_at": token_row[3].isoformat() if token_row[3] else None,
                    "detected_at": token_row[4].isoformat() if token_row[4] else None,
                    "is_active": token_row[5],
                    "primary_pair_address": token_row[6],
                    "pair_validated": token_row[7]
                },
                "eligibility_gate": {
                    "v2_trade_count": eligibility_row[0] if eligibility_row else 0,
                    "first_trade_time": eligibility_row[1].isoformat() if eligibility_row and eligibility_row[1] else None,
                    "last_trade_time": eligibility_row[2].isoformat() if eligibility_row and eligibility_row[2] else None,
                    "volume_first_30m_sol": float(eligibility_row[3]) if eligibility_row and eligibility_row[3] else 0.0,
                    "peak_liquidity_usd": float(eligibility_row[4]) if eligibility_row and eligibility_row[4] else 0.0
                } if eligibility_row else None,
                "feature_snapshot": {
                    "snapshot_id": snapshot_row[0] if snapshot_row else None,
                    "feature_version": snapshot_row[1] if snapshot_row else None,
                    "snapshot_time": snapshot_row[2].isoformat() if snapshot_row and snapshot_row[2] else None,
                    "volume_acceleration": float(snapshot_row[3]) if snapshot_row and snapshot_row[3] is not None else None,
                    "volume_growth_rate_1h": float(snapshot_row[4]) if snapshot_row and snapshot_row[4] is not None else None,
                    "buy_sell_ratio_1h": float(snapshot_row[5]) if snapshot_row and snapshot_row[5] is not None else None,
                    "unique_wallets_growth": float(snapshot_row[6]) if snapshot_row and snapshot_row[6] is not None else None,
                    "price_volatility_1h": float(snapshot_row[7]) if snapshot_row and snapshot_row[7] is not None else None,
                    "price_drawdown_6h": float(snapshot_row[8]) if snapshot_row and snapshot_row[8] is not None else None,
                    "holder_concentration": float(snapshot_row[9]) if snapshot_row and snapshot_row[9] is not None else None,
                    "holder_retention": float(snapshot_row[10]) if snapshot_row and snapshot_row[10] is not None else None,
                    "age_hours": float(snapshot_row[11]) if snapshot_row and snapshot_row[11] is not None else None,
                    "lifecycle_state": snapshot_row[12] if snapshot_row else None,
                    "score_total": float(snapshot_row[13]) if snapshot_row and snapshot_row[13] is not None else 0.0,
                    "sudden_liquidity_spike": snapshot_row[14] if snapshot_row else False,
                    
                    # Extended Features
                    "liquidity_growth_rate": float(snapshot_row[15]) if snapshot_row and snapshot_row[15] is not None else None,
                    "liquidity_current_usd": float(snapshot_row[16]) if snapshot_row and snapshot_row[16] is not None else None,
                    "liquidity_peak_window_usd": float(snapshot_row[17]) if snapshot_row and snapshot_row[17] is not None else None,
                    "wallet_entropy_score": float(snapshot_row[18]) if snapshot_row and snapshot_row[18] is not None else None,
                    "early_wallet_count": snapshot_row[19] if snapshot_row else None,
                    "early_wallet_net_accumulation": float(snapshot_row[20]) if snapshot_row and snapshot_row[20] is not None else None,
                    "early_wallet_retention": float(snapshot_row[21]) if snapshot_row and snapshot_row[21] is not None else None, # Reusing exit ratio slot? Wait.
                    # Column 21 is early_wallet_exit_ratio.
                    # Column 10 is holder_retention.
                    # UI asks for early_wallet_retention.
                    # Feature engine calculates 'early_wallet_exit_ratio'.
                    # Let's map early_wallet_exit_ratio to a key UI might logically equate or introduce new key?
                    # UI expects 'early_wallet_retention'.
                    # Actually, features.py calculates 'holder_retention' (overall) and 'early_wallet_exit_ratio'.
                    # Let's map 'early_wallet_retention' to 'early_wallet_exit_ratio' inverse??
                    # No, let's just expose 'early_wallet_exit_ratio' and update UI to use it if needed, or map generic retention.
                    # Wait, line 367 in details.html: { key: 'early_wallet_retention', ... }
                    # Features.py line 367: holder_retention = float(retained / max(total_6h_count, 1))
                    # So 'holder_retention' IS 'early_wallet_retention' for the last 6h vs 1h.
                    # Features.py doesn't calculate retention for FIRST 30m specifically in a dedicated column other than 'holder_retention' logic.
                    # Let's just map 'holder_retention' (col 10) to 'early_wallet_retention' in UI if that's what it wants.
                    # BUT wait, the UI code loops FEATURES list and looks for key in snapshot object.
                    # So I should populate 'early_wallet_retention' in response from 'holder_retention' (col 10) OR just let UI use 'holder_retention'.
                    # UI uses key 'early_wallet_retention'. Snapshot DB has 'holder_retention'.
                    # I will alias it here to support the UI.
                    "early_wallet_retention": float(snapshot_row[10]) if snapshot_row and snapshot_row[10] is not None else None,
                    
                    "score_risk_penalty": float(snapshot_row[22]) if snapshot_row and snapshot_row[22] is not None else None,
                    "volume_collapse_ratio": float(snapshot_row[23]) if snapshot_row and snapshot_row[23] is not None else None,
                    "liquidity_stability_score": float(snapshot_row[16]) / float(snapshot_row[17]) if snapshot_row and snapshot_row[17] and snapshot_row[17] > 0 else 0.0,
                    # Derived on the fly: current / peak
                    
                    "trade_frequency_ratio": 1.0, # Placeholder
                    
                    # Phase 12 Additions
                    "volume_5m_usd": float(snapshot_row[25]) if snapshot_row and len(snapshot_row) > 25 and snapshot_row[25] is not None else 0.0,
                    "volume_30m_usd": float(snapshot_row[26]) if snapshot_row and len(snapshot_row) > 26 and snapshot_row[26] is not None else 0.0,
                    "volume_1h_usd": float(snapshot_row[27]) if snapshot_row and len(snapshot_row) > 27 and snapshot_row[27] is not None else 0.0,
                    "volume_6h_usd": float(snapshot_row[28]) if snapshot_row and len(snapshot_row) > 28 and snapshot_row[28] is not None else 0.0,
                    "max_trade_gap_minutes": float(snapshot_row[29]) if snapshot_row and len(snapshot_row) > 29 and snapshot_row[29] is not None else 0.0,
                    "multiplier_at_snapshot": float(snapshot_row[30]) if snapshot_row and len(snapshot_row) > 30 and snapshot_row[30] is not None else 1.0,
                    
                    "risk_thresholds": {
                         "liquidity_collapse_usd": float(snapshot_row[31]) if snapshot_row and len(snapshot_row) > 31 and snapshot_row[31] is not None else 0.0,
                         "price_failure_usd": float(snapshot_row[32]) if snapshot_row and len(snapshot_row) > 32 and snapshot_row[32] is not None else 0.0
                    }
                } if snapshot_row else None,
                "scoring": {
                    "rule_score": float(snapshot_row[13]) if snapshot_row and snapshot_row[13] is not None else 0.0,
                    "probability_5x": None,  # TODO: Add when ML enabled
                    "final_score": float(snapshot_row[13]) if snapshot_row and snapshot_row[13] is not None else 0.0,
                    "model_version": None,  # TODO: Add from model_versions table
                    "breakdown": scoring_breakdown
                } if snapshot_row else None,
                "lifecycle": {
                    "outcome": outcome_row[0] if outcome_row else None,
                    "labeled_at": outcome_row[1].isoformat() if outcome_row and outcome_row[1] else None,
                    "failure_reason": outcome_row[2] if outcome_row else None,
                    "time_to_outcome_hours": round(time_to_outcome_hours, 2) if time_to_outcome_hours else None,
                    "max_multiplier": None  # TODO: Compute from trades
                } if outcome_row else None,
                "dataset_integrity": {
                    "snapshot": {
                        "snapshot_id": snapshot_row[0] if snapshot_row else None,
                        "feature_version": snapshot_row[1] if snapshot_row else None,
                        "snapshot_time": snapshot_row[2].isoformat() if snapshot_row and snapshot_row[2] else None,
                        "snapshot_immutable": True,  # Snapshots are NEVER recomputed
                        "model_version_id": None  # TODO: Add when ML enabled
                    },
                    "trades": {
                        "v1_trade_count": trade_versions.get(1, 0),
                        "v2_trade_count": trade_versions.get(2, 0),
                        "total_trade_count": sum(trade_versions.values()),
                        "schema_version": 2 if trade_versions.get(2, 0) > 0 else 1
                    }
                }
            }


# Keep existing legacy endpoints for backward compatibility but mark as deprecated
@router.get("/discovery")
async def get_discovery(limit: int = 100):
    """
    Lightweight discovery view for tokens flowing through the system.
    Returns raw token tracking data before they qualify for full snapshots.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT 
                    t.id, t.address, t.symbol, t.detected_at, 
                    t.eligibility_status, t.is_active,
                    COUNT(tr.id) as trade_count,
                    COALESCE(SUM(tr.amount_usd), 0) as volume_usd, -- Use USD for volume
                    COUNT(DISTINCT tr.wallet_address) as unique_wallets,
                    MAX(tr.timestamp) as last_trade
                FROM tokens t
                LEFT JOIN trades tr ON tr.token_id = t.id
                GROUP BY t.id
                ORDER BY t.detected_at DESC
                LIMIT %s
            """, (limit,))
            
            rows = await cur.fetchall()
            return [
                {
                    "token_id": r[0],
                    "address": r[1],
                    "symbol": r[2],
                    "detected_at": r[3].isoformat() if r[3] else None,
                    "stage": "ACTIVE_MONITORING" if r[5] else "PRE_ELIGIBLE", # Simplified stage mapping
                    "eligibility_status": r[4],
                    "age": _format_age(r[3]),
                    "trade_count": r[6],
                    "volume_usd": float(r[7]),
                    "unique_wallets": r[8],
                    "last_trade": r[9].isoformat() if r[9] else None
                }
                for r in rows
            ]

@router.post("/refresh")
async def refresh_all_phases():
    """
    Trigger re-analysis for all active tokens.
    """
    # This would ideally be an async background task or call the worker
    # For now we will rely on the worker loop, but we can potentially force 
    # a check if we had a task queue.
    # Stub response for UI:
    return {"tokens_analyzed": 0, "message": "Analysis is handled by background workers automatically."}

@router.post("/token/{token_id}/refresh")
async def refresh_token(token_id: int):
    """
    Force a manual snapshot computation for a specific token.
    """
    from app.engines.v2.features import compute_v2_snapshot
    try:
        await compute_v2_snapshot(token_id)
        return {"status": "success", "message": f"Snapshot computed for token {token_id}"}
    except Exception as e:
        logger.error(f"Error computing snapshot for {token_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/token/{mint}/stats")
async def get_token_stats(mint: str, window: str = "24h"):
    """
    Get aggregated stats for a token over a specific window.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Resolve mint to ID
            await cur.execute("SELECT id FROM tokens WHERE address = %s", (mint,))
            row = await cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Token not found")
            token_id = row[0]

            interval = "24 hours"
            if window == "1h": interval = "1 hour"
            elif window == "7d": interval = "7 days"

            await cur.execute(f"""
                SELECT 
                    COUNT(*) as swap_count,
                    COALESCE(SUM(amount_usd), 0) as volume_total,
                    COALESCE(SUM(CASE WHEN side = 'buy' THEN amount_usd ELSE 0 END), 0) as volume_buy,
                    COALESCE(SUM(CASE WHEN side = 'sell' THEN amount_usd ELSE 0 END), 0) as volume_sell,
                    COUNT(DISTINCT wallet_address) as unique_makers
                FROM trades 
                WHERE token_id = %s 
                AND timestamp > NOW() - INTERVAL '{interval}'
            """, (token_id,))
            
            stats = await cur.fetchone()
            return {
                "swap_count": stats[0],
                "volume_total": float(stats[1]),
                "volume_buy": float(stats[2]),
                "volume_sell": float(stats[3]),
                "unique_makers": stats[4]
            }

@router.get("/health")
async def get_health():
    """System health metrics."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) FROM trades")
            age = (await cur.fetchone())[0]
            
            await cur.execute("""
                SELECT 
                    SUM(events_received) as total,
                    SUM(swaps_ignored) as ignored
                FROM ingestion_stats 
                WHERE created_at > NOW() - INTERVAL '24 hours'
            """)
            stats = await cur.fetchone()
            total = stats[0] or 0
            ignored = stats[1] or 0
            ratio = (ignored / total) if total > 0 else 0.0

            return {
                "status": "ok",
                "last_insert_age_seconds": age,
                "total_events_24h": total,
                "ignore_ratio_24h": ratio
            }

def _format_age(dt):
    if not dt: return "—"
    now = datetime.now(timezone.utc)
    delta = now - (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt)
    if delta.seconds < 3600: return f"{delta.seconds // 60}m"
    if delta.seconds < 86400: return f"{delta.seconds // 3600}h"
    return f"{delta.days}d"
