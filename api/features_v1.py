
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import math

from api.db import get_db_connection

logger = logging.getLogger("solana-analytics")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EPSILON = Decimal("0.000001")

# Lifecycle Thresholds (v1 Deterministic)
THRESHOLDS = {
    "ignition": {
        "vol_accel": 1.5,
        "liq_growth": 0,
        "age_hours": 2
    },
    "expansion": {
        "buy_sell": 1.2,
        "unique_growth": 0,
        "liq_stable": 0.7
    },
    "unstable": {
        "liq_vol": "high", # Relative check
        "drawdown": 0.3
    },
    "distribution": {
        "buy_sell": 0.8,
        "conc_delta": 0 # rising
    },
    "fragile": {
        "vol_collapse": 0.4
    }
}

async def compute_v1_snapshot(token_id: int):
    """
    Computes Feature Snapshot v1 (16 features + lifecycle state).
    Stores result in feature_snapshots table with feature_version=1.
    """
    logger.info(f"Computing v1 snapshot for token_id={token_id}")
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            now = datetime.now(timezone.utc)
            
            # --- 1. Market Momentum ---
            
            # 1. volume_acceleration (V5m / V30m/6)
            await cur.execute("""
                SELECT 
                    SUM(CASE WHEN timestamp > %s THEN amount_sol ELSE 0 END) as v_5m,
                    SUM(amount_sol) as v_30m
                FROM trades 
                WHERE token_id = %s AND timestamp > %s
            """, (now - timedelta(minutes=5), token_id, now - timedelta(minutes=30)))
            row = await cur.fetchone()
            v_5m = row[0] or Decimal(0)
            v_30m = row[1] or Decimal(0)
            
            v_30m_avg = v_30m / Decimal(6)
            vol_accel = (v_5m / max(v_30m_avg, EPSILON))
            
            # 2. volume_growth_rate_1h ((V1h - V6h/6) / (V6h/6))
            await cur.execute("""
                SELECT 
                    SUM(CASE WHEN timestamp > %s THEN amount_sol ELSE 0 END) as v_1h,
                    SUM(amount_sol) as v_6h
                FROM trades 
                WHERE token_id = %s AND timestamp > %s
            """, (now - timedelta(hours=1), token_id, now - timedelta(hours=6)))
            row = await cur.fetchone()
            v_1h = row[0] or Decimal(0)
            v_6h = row[1] or Decimal(0)
            
            v_6h_avg = v_6h / Decimal(6)
            vol_growth_1h = (v_1h - v_6h_avg) / max(v_6h_avg, EPSILON)
            
            # 3. trade_frequency_ratio (T5m / (T30m/6))
            await cur.execute("""
                SELECT 
                    COUNT(CASE WHEN timestamp > %s THEN 1 END) as t_5m,
                    COUNT(*) as t_30m
                FROM trades 
                WHERE token_id = %s AND timestamp > %s
            """, (now - timedelta(minutes=5), token_id, now - timedelta(minutes=30)))
            row = await cur.fetchone()
            t_5m = row[0] or 0
            t_30m = row[1] or 0
            
            t_30m_avg = Decimal(t_30m) / Decimal(6)
            trade_freq_ratio = Decimal(t_5m) / max(t_30m_avg, EPSILON)

            # --- 2. Liquidity Stability (PLACEHOLDERS) ---
            # TODO: Implement when Liquidity Events are tracked
            liq_growth_rate = Decimal(0)
            liq_volatility = Decimal(0)
            liq_stability_score = Decimal(0)

            # --- 3. Participation Structure ---
            
            # 7. unique_wallet_growth_rate
            # Unique Wallets 1h vs 6h avg
            await cur.execute("""
                SELECT COUNT(DISTINCT wallet_id) 
                FROM wallet_token_interactions 
                WHERE token_id = %s AND first_interaction > %s
            """, (token_id, now - timedelta(hours=1)))
            new_wallets_1h = (await cur.fetchone())[0] or 0
            
            await cur.execute("""
                SELECT COUNT(DISTINCT wallet_id) 
                FROM wallet_token_interactions 
                WHERE token_id = %s AND first_interaction > %s AND first_interaction <= %s
            """, (token_id, now - timedelta(hours=6), now - timedelta(hours=1)))
            new_wallets_prev_5h = (await cur.fetchone())[0] or 0
            
            # Avg per hour for previous 5h window? Spec says "Unique Wallets 6h / 6"
            # Actually, "Unique Wallets 6h" usually means Cumulative Unique Wallets appearing in 6h window?
            # Or "New Wallets"?
            # Formula: (UniqueWallets_1h - UniqueWallets_6h/6) / ...
            # UniqueWallets_1h is count of distinct wallets active in last 1h.
            # UniqueWallets_6h is count of distinct wallets active in last 6h.
            
            await cur.execute("""
                SELECT COUNT(DISTINCT wallet_address)
                FROM trades
                WHERE token_id = %s AND timestamp > %s
            """, (token_id, now - timedelta(hours=1)))
            uw_1h = (await cur.fetchone())[0] or 0
            
            await cur.execute("""
                SELECT COUNT(DISTINCT wallet_address)
                FROM trades
                WHERE token_id = %s AND timestamp > %s
            """, (token_id, now - timedelta(hours=6)))
            uw_6h = (await cur.fetchone())[0] or 0
            
            uw_6h_avg = Decimal(uw_6h) / Decimal(6)
            unique_wallet_growth = (Decimal(uw_1h) - uw_6h_avg) / max(uw_6h_avg, EPSILON)
            
            # 8. buy_sell_ratio (1h Volume)
            await cur.execute("""
                SELECT 
                    SUM(CASE WHEN side = 'buy' THEN amount_sol ELSE 0 END),
                    SUM(CASE WHEN side = 'sell' THEN amount_sol ELSE 0 END)
                FROM trades
                WHERE token_id = %s AND timestamp > %s
            """, (token_id, now - timedelta(hours=1)))
            row = await cur.fetchone()
            buy_vol = row[0] or Decimal(0)
            sell_vol = row[1] or Decimal(0)
            buy_sell_ratio = buy_vol / max(sell_vol, EPSILON)
            
            # 9. holder_concentration_top10 (Snapshot)
            # & 10. top10_concentration_delta
            # We need Top 10 balances NOW and 1h AGO.
            # This is hard without balance history snapshot.
            # We have `last_balance_token` in `wallet_token_interactions`.
            # This is CURRENT balance.
            
            # Current Top 10
            await cur.execute("""
                SELECT SUM(last_balance_token)
                FROM (
                    SELECT last_balance_token 
                    FROM wallet_token_interactions 
                    WHERE token_id = %s
                    ORDER BY last_balance_token DESC
                    LIMIT 10
                ) sub
            """, (token_id,))
            top10_bal = (await cur.fetchone())[0] or Decimal(0)
            
            # Total Supply? We don't track total supply.
            # We can sum all balances we know of.
            await cur.execute("SELECT SUM(last_balance_token) FROM wallet_token_interactions WHERE token_id = %s", (token_id,))
            total_supply_tracked = (await cur.fetchone())[0] or Decimal(0)
            
            holder_conc_top10 = top10_bal / max(total_supply_tracked, EPSILON)
            
            # Delta? We can't compute 1h ago easily without replaying trades.
            # For v1, we will set delta = 0 (TODO)
            top10_conc_delta = Decimal(0)

            # --- 4. Wallet Intelligence ---
            
            # 11. early_wallet_retention
            # Early = First 30m of token life.
            # We need `created_at_chain` from tokens table.
            await cur.execute("SELECT created_at_chain FROM tokens WHERE id = %s", (token_id,))
            created_at = (await cur.fetchone())[0]
            
            early_retention = Decimal(0)
            early_acc = Decimal(0)
            
            if created_at:
                cutoff = created_at + timedelta(minutes=30)
                await cur.execute("""
                    SELECT last_balance_token 
                    FROM wallet_token_interactions 
                    WHERE token_id = %s AND first_interaction <= %s
                """, (token_id, cutoff))
                early_wallets = await cur.fetchall()
                
                if early_wallets:
                    total_early = len(early_wallets)
                    still_holding = sum(1 for w in early_wallets if w[0] > 0)
                    early_acc = sum(w[0] for w in early_wallets) # Net position (balance)
                    
                    early_retention = Decimal(still_holding) / Decimal(total_early)
            
            # 13. wallet_entropy_score
            # Shannon Entropy of balances
            await cur.execute("""
                SELECT last_balance_token 
                FROM wallet_token_interactions 
                WHERE token_id = %s AND last_balance_token > 0
            """, (token_id,))
            balances = [r[0] for r in await cur.fetchall()]
            
            entropy = Decimal(0)
            if balances:
                total_bal = sum(balances)
                if total_bal > 0:
                    probs = [float(b / total_bal) for b in balances]
                    entropy_val = -sum(p * math.log(p) for p in probs if p > 0)
                    entropy = Decimal(entropy_val)

            # --- 5. Risk & Instability ---
            
            # 14. price_volatility_1h (StdDev of Price)
            # 15. drawdown_depth_1h (Peak - Curr / Peak)
            await cur.execute("""
                SELECT amount_sol, amount_token 
                FROM trades 
                WHERE token_id = %s AND timestamp > %s AND amount_token > 0
                ORDER BY timestamp ASC
            """, (token_id, now - timedelta(hours=1)))
            trades_1h = await cur.fetchall()
            
            price_vol_1h = Decimal(0)
            drawdown_1h = Decimal(0)
            
            if trades_1h:
                prices = [float(t[0] / t[1]) for t in trades_1h]
                if len(prices) > 1:
                    mean_p = sum(prices) / len(prices)
                    variance = sum((p - mean_p) ** 2 for p in prices) / len(prices)
                    price_vol_1h = Decimal(variance ** 0.5)
                
                peak_price = max(prices)
                curr_price = prices[-1]
                if peak_price > 0:
                    drawdown_1h = Decimal(peak_price - curr_price) / Decimal(peak_price)

            # 16. volume_collapse_ratio (V 1h / V prev 6h avg)
            # Reuse v_1h and v_6h_avg calculated earlier
            vol_collapse_ratio = v_1h / max(v_6h_avg, EPSILON)

            # --- Lifecycle Classification ---
            lifecycle_state = "dormant"
            
            age_hours = 0
            if created_at:
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                age_hours = (now - created_at).total_seconds() / 3600

            # 1. Fragile (Collapse < 0.4)
            if vol_collapse_ratio < Decimal("0.4"):
                lifecycle_state = "fragile"
            
            # 2. Distribution (Sell Pressure + Rising Concentration)
            elif buy_sell_ratio < Decimal("0.8") and top10_conc_delta > 0:
                lifecycle_state = "distribution"
            
            # 3. Unstable (High Vol + Drawdown > 0.3)
            elif drawdown_1h > Decimal("0.3"):
                lifecycle_state = "unstable"
            
            # 4. Expansion (Buy Pressure + New Wallets)
            elif buy_sell_ratio > Decimal("1.2") and unique_wallet_growth > 0:
                lifecycle_state = "expansion"
                
            # 5. Ignition (Accel > 1.5 + Young)
            elif vol_accel > Decimal("1.5") and age_hours < 2:
                lifecycle_state = "ignition"

            # Insert Snapshot
            await cur.execute("""
                INSERT INTO feature_snapshots (
                    token_id, feature_version, detection_timestamp,
                    
                    volume_acceleration, volume_growth_rate_1h, trade_frequency_ratio,
                    liquidity_growth_rate, liquidity_volatility, liquidity_stability_score,
                    unique_wallet_growth_rate, buy_sell_ratio,
                    holder_concentration_top10, top10_concentration_delta,
                    early_wallet_retention, early_wallet_net_accumulation, wallet_entropy_score,
                    volatility_score, drawdown_depth_1h, volume_collapse_ratio,
                    
                    holder_growth_rate,
                    lifecycle_state
                )
                VALUES (
                    %s, 1, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    
                    0, 
                    %s
                )
                RETURNING id
            """, (
                token_id, now,
                vol_accel, vol_growth_1h, trade_freq_ratio,
                liq_growth_rate, liq_volatility, liq_stability_score,
                unique_wallet_growth, buy_sell_ratio,
                holder_conc_top10, top10_conc_delta,
                early_retention, early_acc, entropy,
                price_vol_1h, drawdown_1h, vol_collapse_ratio,
                lifecycle_state
            ))
            
            snapshot_id = (await cur.fetchone())[0]
            logger.info(f"Created v1 snapshot {snapshot_id} for token {token_id} (State: {lifecycle_state})")
            
            # --- Compute Score ---
            from api.scoring_engine import compute_score
            
            feature_dict = {
                "volume_acceleration": float(vol_accel),
                "volume_growth_rate_1h": float(vol_growth_1h),
                "trade_frequency_ratio": float(trade_freq_ratio),
                "liquidity_growth_rate": float(liq_growth_rate),
                "liquidity_stability_score": float(liq_stability_score),
                "unique_wallet_growth_rate": float(unique_wallet_growth),
                "buy_sell_ratio": float(buy_sell_ratio),
                "wallet_entropy_score": float(entropy),
                "early_wallet_retention": float(early_retention),
                "early_wallet_net_accumulation": float(early_acc),
                "top10_concentration_delta": float(top10_conc_delta),
                "drawdown_depth_1h": float(drawdown_1h),
                "volume_collapse_ratio": float(vol_collapse_ratio),
                "liquidity_volatility": float(liq_volatility),
                "lifecycle_state": lifecycle_state,
            }
            
            scores = compute_score(feature_dict)
            
            await cur.execute("""
                UPDATE feature_snapshots
                SET score_momentum = %s,
                    score_liquidity = %s,
                    score_participation = %s,
                    score_wallet = %s,
                    score_risk_penalty = %s,
                    score_total = %s,
                    score_label = %s,
                    is_sniper_candidate = %s
                WHERE id = %s
            """, (
                scores["score_momentum"],
                scores["score_liquidity"],
                scores["score_participation"],
                scores["score_wallet"],
                scores["score_risk_penalty"],
                scores["score_total"],
                scores["score_label"],
                scores["is_sniper_candidate"],
                snapshot_id,
            ))
            await conn.commit()
            
            logger.info(f"Snapshot {snapshot_id} scored: {scores['score_total']}/100 ({scores['score_label']})"
                        f" | M={scores['score_momentum']} L={scores['score_liquidity']}"
                        f" P={scores['score_participation']} W={scores['score_wallet']}"
                        f" R=-{scores['score_risk_penalty']}"
                        f" | Sniper={scores['is_sniper_candidate']}")
            
            return snapshot_id


# ---------------------------------------------------------------------------
# D. Snapshot Trigger Logic
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# D. Snapshot Trigger Logic (Enhanced Step 4)
# ---------------------------------------------------------------------------
LIQUIDITY_THRESHOLD_USD = Decimal("50000")
SUSTAIN_MINUTES = 30


async def check_snapshot_trigger(token_id: int) -> bool:
    """
    Step 4: Enhanced Trigger Logic with Lifecycle State Tracking.
    
    Transitions:
      - PRE_ELIGIBLE -> ELIGIBLE_PENDING_30M (Liquidity >= 50k)
      - ELIGIBLE_PENDING_30M -> PRE_ELIGIBLE (Liquidity drops < 50k)
      - ELIGIBLE_PENDING_30M -> ACTIVE_MONITORING (Sustained > 30m) -> SNAPSHOT
    
    Returns True if a snapshot was created.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 1. Get current lifecycle state & candidate start
            await cur.execute("""
                SELECT lifecycle_stage, liquidity_candidate_start, address 
                FROM tokens WHERE id = %s
            """, (token_id,))
            row = await cur.fetchone()
            if not row: return False
            
            stage, candidate_start, address = row
            
            # If already active or finished, don't trigger again
            if stage in ('ACTIVE_MONITORING', 'SUCCESS', 'FAILED', 'EXPIRED'):
                return False

            now = datetime.now(timezone.utc)

            # 2. Get Current Liquidity (or Proxy)
            # Use 5m rolling window for current check (more responsive than 30m)
            await cur.execute("""
                SELECT volume_usd FROM token_rolling_metrics
                WHERE token_id = %s AND window_type = '5m'
                ORDER BY computed_at DESC LIMIT 1
            """, (token_id,))
            metric_row = await cur.fetchone()
            
            current_liquidity = Decimal(0)
            if metric_row and metric_row[0]:
                 # PROXY: 5m Volume * scalar? Or just 5m Volume? 
                 # User spec says "Liquidity". We don't have it.
                 # We will use 5m volume * 12 (to approximate hourly) * some factor?
                 # No, let's use the explicit fallback logic we had check earlier:
                 current_liquidity = metric_row[0]

            # Fallback if no rolling metric
            if current_liquidity == 0:
                 await cur.execute("""
                    SELECT COALESCE(SUM(amount_sol), 0) FROM trades
                    WHERE token_id = %s AND timestamp > %s
                """, (token_id, now - timedelta(minutes=5)))
                 vol_sol = (await cur.fetchone())[0]
                 current_liquidity = vol_sol * Decimal("150") # Proxy $150/SOL

            # 3. State Machine Logic
            
            is_above_threshold = current_liquidity >= LIQUIDITY_THRESHOLD_USD
            
            if not is_above_threshold:
                # Reset if it was pending
                if stage == 'ELIGIBLE_PENDING_30M':
                    logger.info(f"Token {token_id}: Liquidity dropped below $50k (${current_liquidity:.2f}). Resetting candidate timer.")
                    await cur.execute("""
                        UPDATE tokens 
                        SET lifecycle_stage = 'PRE_ELIGIBLE', 
                            liquidity_candidate_start = NULL 
                        WHERE id = %s
                    """, (token_id,))
                    await conn.commit()
                return False
            
            # It IS above threshold
            if stage == 'PRE_ELIGIBLE' or candidate_start is None:
                # Start Timer
                logger.info(f"Token {token_id}: Liquidity >= $50k (${current_liquidity:.2f}). Starting 30m timer.")
                await cur.execute("""
                    UPDATE tokens 
                    SET lifecycle_stage = 'ELIGIBLE_PENDING_30M', 
                        liquidity_candidate_start = %s 
                    WHERE id = %s
                """, (now, token_id))
                await conn.commit()
                return False
            
            elif stage == 'ELIGIBLE_PENDING_30M':
                # Check Duration
                if candidate_start.tzinfo is None:
                    candidate_start = candidate_start.replace(tzinfo=timezone.utc)
                
                elapsed = (now - candidate_start).total_seconds() / 60.0
                
                if elapsed >= SUSTAIN_MINUTES:
                    # A3: Detection timestamp = candidate_start + 30m (NOT now)
                    detection_ts = candidate_start + timedelta(minutes=SUSTAIN_MINUTES)
                    
                    logger.info(f"Token {token_id}: Sustained liquidity > $50k for {elapsed:.1f}m. "
                                f"Detection timestamp = {detection_ts}. Triggering Snapshot.")
                    
                    # Protection 3 — Data Gap: Check rolling metrics freshness
                    await cur.execute("""
                        SELECT MAX(computed_at) FROM token_rolling_metrics
                        WHERE token_id = %s
                    """, (token_id,))
                    last_metric = await cur.fetchone()
                    if last_metric and last_metric[0]:
                        metric_ts = last_metric[0]
                        if metric_ts.tzinfo is None:
                            metric_ts = metric_ts.replace(tzinfo=timezone.utc)
                        gap = (now - metric_ts).total_seconds() / 60.0
                        if gap > 10:
                            logger.warning(f"Token {token_id}: Rolling metrics stale ({gap:.1f}m). Delaying snapshot.")
                            return False
                    
                    # Protection 2 — Rapid Liquidity Pump Detection
                    sudden_spike = False
                    await cur.execute("""
                        SELECT volume_usd FROM token_rolling_metrics
                        WHERE token_id = %s AND window_type = '5m'
                          AND computed_at > %s
                        ORDER BY computed_at ASC LIMIT 1
                    """, (token_id, now - timedelta(minutes=10)))
                    early_vol = await cur.fetchone()
                    if early_vol and early_vol[0] and early_vol[0] < Decimal("10000"):
                        if current_liquidity > Decimal("200000"):
                            sudden_spike = True
                            logger.warning(f"Token {token_id}: Rapid liquidity spike detected (<10k -> >200k in 5m). Flagging.")
                    
                    # Compute Snapshot
                    snapshot_id = await compute_v1_snapshot(token_id)
                    
                    if snapshot_id:
                        # Flag spike if detected
                        if sudden_spike:
                            await cur.execute("""
                                UPDATE feature_snapshots SET sudden_liquidity_spike = TRUE WHERE id = %s
                            """, (snapshot_id,))
                        
                        # Update Lifecycle to ACTIVE_MONITORING
                        await cur.execute("""
                            UPDATE tokens 
                            SET lifecycle_stage = 'ACTIVE_MONITORING',
                                detected_at = %s,
                                is_active = TRUE,
                                peak_liquidity_usd = %s
                            WHERE id = %s
                        """, (detection_ts, current_liquidity, token_id))
                        await conn.commit()
                        logger.info(f"Token {token_id} enters ACTIVE_MONITORING (detected_at={detection_ts}).")
                        return True
            
            return False



if __name__ == "__main__":
    from api.db import init_db, close_db
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        await init_db()
        try:
             async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT id FROM tokens LIMIT 1")
                    row = await cur.fetchone()
                    if row:
                        triggered = await check_snapshot_trigger(row[0])
                        print(f"Trigger result: {triggered}")
        finally:
            await close_db()
            
    asyncio.run(main())
