from fastapi import APIRouter, HTTPException
from api.features_v1 import compute_v1_snapshot
from api.scoring_engine import compute_score
from api.db import get_db_connection

router = APIRouter()

@router.post("/{token_id}/compute")
async def compute_token_features(token_id: int):
    """Computes Feature Snapshot v1 for the token."""
    try:
        snapshot_id = await compute_v1_snapshot(token_id)
        return {"status": "success", "snapshot_id": snapshot_id, "version": 1}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{token_id}/score")
async def get_token_score(token_id: int):
    """Returns the latest score breakdown for the token."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT 
                    fs.id, fs.detection_timestamp, fs.lifecycle_state,
                    fs.score_momentum, fs.score_liquidity, 
                    fs.score_participation, fs.score_wallet,
                    fs.score_risk_penalty, fs.score_total,
                    fs.score_label, fs.is_sniper_candidate,
                    fs.volume_acceleration, fs.volume_growth_rate_1h,
                    fs.trade_frequency_ratio, fs.liquidity_growth_rate,
                    fs.liquidity_stability_score, fs.unique_wallet_growth_rate,
                    fs.buy_sell_ratio, fs.wallet_entropy_score,
                    fs.early_wallet_retention, fs.early_wallet_net_accumulation,
                    fs.top10_concentration_delta, fs.drawdown_depth_1h,
                    fs.volume_collapse_ratio, fs.liquidity_volatility
                FROM feature_snapshots fs
                WHERE fs.token_id = %s AND fs.feature_version = 1
                ORDER BY fs.detection_timestamp DESC
                LIMIT 1
            """, (token_id,))
            row = await cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="No snapshot found for token")
            
            (snap_id, det_ts, lifecycle,
             s_mom, s_liq, s_part, s_wal,
             s_risk, s_total, s_label, s_sniper,
             vol_acc, vol_gr, trade_fr, liq_gr,
             liq_stab, uw_gr, bs_ratio, entropy,
             ew_ret, ew_acc, t10_delta, dd, vc, liq_vol) = row
            
            # If score not yet computed, compute it on the fly
            if s_total is None:
                feature_dict = {
                    "volume_acceleration": float(vol_acc or 0),
                    "volume_growth_rate_1h": float(vol_gr or 0),
                    "trade_frequency_ratio": float(trade_fr or 0),
                    "liquidity_growth_rate": float(liq_gr or 0),
                    "liquidity_stability_score": float(liq_stab or 0),
                    "unique_wallet_growth_rate": float(uw_gr or 0),
                    "buy_sell_ratio": float(bs_ratio or 0),
                    "wallet_entropy_score": float(entropy or 0),
                    "early_wallet_retention": float(ew_ret or 0),
                    "early_wallet_net_accumulation": float(ew_acc or 0),
                    "top10_concentration_delta": float(t10_delta or 0),
                    "drawdown_depth_1h": float(dd or 0),
                    "volume_collapse_ratio": float(vc or 0),
                    "liquidity_volatility": float(liq_vol or 0),
                    "lifecycle_state": lifecycle or "dormant",
                }
                scores = compute_score(feature_dict)
                
                # Persist
                await cur.execute("""
                    UPDATE feature_snapshots
                    SET score_momentum = %s, score_liquidity = %s,
                        score_participation = %s, score_wallet = %s,
                        score_risk_penalty = %s, score_total = %s,
                        score_label = %s, is_sniper_candidate = %s
                    WHERE id = %s
                """, (
                    scores["score_momentum"], scores["score_liquidity"],
                    scores["score_participation"], scores["score_wallet"],
                    scores["score_risk_penalty"], scores["score_total"],
                    scores["score_label"], scores["is_sniper_candidate"],
                    snap_id,
                ))
                await conn.commit()
            else:
                scores = {
                    "score_momentum": float(s_mom),
                    "score_liquidity": float(s_liq),
                    "score_participation": float(s_part),
                    "score_wallet": float(s_wal),
                    "score_risk_penalty": float(s_risk),
                    "score_total": float(s_total),
                    "score_label": s_label,
                    "is_sniper_candidate": s_sniper,
                }
            
            return {
                "token_id": token_id,
                "snapshot_id": snap_id,
                "detection_timestamp": str(det_ts),
                "lifecycle_state": lifecycle,
                **scores,
            }
