"""
Phase Detection Engine
======================
Classifies tokens into lifecycle phases (Initial / Destructive / Post-Destructive)
and computes a continuous Expected Value (EV) score based on participation structure.

Core question: "Is this token statistically positioned for asymmetric upside right now?"
"""

from api.db import get_db_connection
from typing import List, Dict, Optional
import statistics
import asyncio


# ---------------------------------------------------------------------------
# 1. Data Layer — Pull daily snapshots from events table
# ---------------------------------------------------------------------------

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Enhanced query to get repeat makers (wallets active today that were active yesterday)
            await cur.execute(
                """
                WITH daily_wallets AS (
                    SELECT DATE(block_time) as day, wallet
                    FROM events
                    WHERE token_mint = %s AND block_time > NOW() - INTERVAL '%s days'
                    GROUP BY 1, 2
                )
                SELECT 
                    curr.day, 
                    COUNT(DISTINCT curr.wallet) as unique_makers,
                    COUNT(*) FILTER (WHERE prev.wallet IS NOT NULL) as repeat_makers,
                    (SELECT COUNT(*) FROM events e WHERE e.token_mint = %s AND DATE(e.block_time) = curr.day) as swap_count,
                    (SELECT COALESCE(SUM(amount), 0) FROM events e WHERE e.token_mint = %s AND DATE(e.block_time) = curr.day) as volume
                FROM daily_wallets curr
                LEFT JOIN daily_wallets prev ON curr.wallet = prev.wallet AND prev.day = curr.day - INTERVAL '1 day'
                GROUP BY curr.day
                ORDER BY curr.day ASC
                """,
                (mint, days + 1, mint, mint),
            )
            rows = await cur.fetchall()

    return [
        {
            "day": str(row[0]),
            "unique_makers": row[1],
            "repeat_makers": row[2],
            "swap_count": row[3],
            "volume": float(row[4]),
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# 2. Compute Derived Metrics for each snapshot row
# ---------------------------------------------------------------------------

def enrich_snapshots(snapshots: List[Dict]) -> List[Dict]:
    """
    Adds computed ratios to each daily snapshot:
      - unique_to_swap_ratio
      - volume_per_unique
    """
    for s in snapshots:
        swaps = s["swap_count"] or 1  # avoid division by zero
        uniques = s["unique_makers"] or 1

        s["unique_to_swap_ratio"] = round(uniques / swaps, 4)
        s["volume_per_unique"] = round(s["volume"] / uniques, 2)

    return snapshots


# ---------------------------------------------------------------------------
# 3. Compute Derived Signals (rolling window comparisons)
# ---------------------------------------------------------------------------

def compute_signals(snapshots: List[Dict]) -> Dict:
    """
    Takes enriched snapshots (>= 3 days) and computes change signals.
    Returns a dict of all signals needed for phase classification + EV scoring.
    """
    if len(snapshots) < 3:
        return {"insufficient_data": True}

    # Latest day
    today = snapshots[-1]
    # Previous 2 days for acceleration
    prev_2d = snapshots[-3:-1]

    # --- A) Unique Growth Rate (2-day) ---
    avg_prev_uniques = statistics.mean([d["unique_makers"] for d in prev_2d]) or 1
    unique_growth_rate = (today["unique_makers"] - avg_prev_uniques) / avg_prev_uniques

    # --- B) Swap Acceleration (2-day) ---
    avg_prev_swaps = statistics.mean([d["swap_count"] for d in prev_2d]) or 1
    swap_acceleration = (today["swap_count"] - avg_prev_swaps) / avg_prev_swaps

    # --- C) Volume Acceleration (2-day) ---
    avg_prev_vol = statistics.mean([d["volume"] for d in prev_2d]) or 1
    volume_acceleration = (today["volume"] - avg_prev_vol) / avg_prev_vol

    # --- D) Ratio Health Trend (slope over last 3 days) ---
    last_3_ratios = [d["unique_to_swap_ratio"] for d in snapshots[-3:]]
    # Simple slope: (last - first) / 2
    ratio_health_trend = (last_3_ratios[-1] - last_3_ratios[0]) / 2

    # --- E) Volume Per Unique Stability (coefficient of variation over 3 days) ---
    last_3_vpu = [d["volume_per_unique"] for d in snapshots[-3:]]
    vpu_mean = statistics.mean(last_3_vpu) or 1
    vpu_stdev = statistics.stdev(last_3_vpu) if len(last_3_vpu) > 1 else 0
    vpu_cv = vpu_stdev / vpu_mean  # lower = more stable

    # --- F) Peak & Decline metrics (contextual) ---
    all_uniques = [d["unique_makers"] for d in snapshots]
    peak_uniques = max(all_uniques)
    decline_from_peak = (peak_uniques - today["unique_makers"]) / peak_uniques if peak_uniques > 0 else 0

    # Days since peak
    peak_day_idx = all_uniques.index(peak_uniques)
    days_since_peak = len(snapshots) - 1 - peak_day_idx

    # Swap reduction from peak
    all_swaps = [d["swap_count"] for d in snapshots]
    peak_swaps = max(all_swaps)
    swap_reduction_from_peak = (peak_swaps - today["swap_count"]) / peak_swaps if peak_swaps > 0 else 0

    return {
        "insufficient_data": False,

        # Core metrics (latest day)
        "unique_makers": today["unique_makers"],
        "swap_count": today["swap_count"],
        "volume": today["volume"],
        "unique_to_swap_ratio": today["unique_to_swap_ratio"],
        "volume_per_unique": today["volume_per_unique"],

        # Derived signals
        "unique_growth_rate": round(unique_growth_rate, 4),
        "swap_acceleration": round(swap_acceleration, 4),
        "volume_acceleration": round(volume_acceleration, 4),
        "ratio_health_trend": round(ratio_health_trend, 4),
        "vpu_cv": round(vpu_cv, 4),  # volume-per-unique coefficient of variation

        # Contextual
        "peak_uniques": peak_uniques,
        "decline_from_peak": round(decline_from_peak, 4),
        "days_since_peak": days_since_peak,
        "swap_reduction_from_peak": round(swap_reduction_from_peak, 4),
        
        # New: Growth State (Actionable Text)
        "growth_state": "Accelerating" if unique_growth_rate > 0.1 else "Stalling" if unique_growth_rate < -0.1 else "Stable",
        "exhaustion": "High" if decline_from_peak > 0.3 else "Low", 
        
        # New: Stickiness (Cohort health)
        "stickiness": round(today["repeat_makers"] / today["unique_makers"], 4) if today["unique_makers"] > 0 else 0,
    }


# ---------------------------------------------------------------------------
# 4. Phase Classification
# ---------------------------------------------------------------------------

def classify_phase(signals: Dict) -> str:
    """
    Classifies token into one of:
      EARLY_EXPANSION  — Organic expansion beginning (Blue)
      EXPANSION        — Strong growth, healthy (Green)
      LATE_HYPE        — Growth stalling, volume rising (Yellow)
      DESTRUCTIVE      — Distribution / collapse (Red)
      POST_DESTRUCTIVE — Base forming (highest EV) (Green/Blue)
      INSUFFICIENT_DATA — Not enough history
    """
    if signals.get("insufficient_data"):
        return "INSUFFICIENT_DATA"

    ugr = signals["unique_growth_rate"]
    rht = signals["ratio_health_trend"]
    vpu_cv = signals["vpu_cv"]
    decline = signals["decline_from_peak"]
    swap_reduction = signals["swap_reduction_from_peak"]
    days_since_peak = signals["days_since_peak"]

    # 1. POST-DESTRUCTIVE (Highest EV)
    # Must have had a significant prior decline (not just flat from birth)
    if (
        -0.1 <= ugr <= 0.1             # uniques stabilized
        and decline > 0.3               # had a significant drop from peak
        and swap_reduction > 0.3        # swaps have cooled off
        and vpu_cv < 1.0                # volume per unique not wildly volatile
    ):
        return "POST_DESTRUCTIVE"

    # 2. DESTRUCTIVE (Worst EV)
    if ugr < -0.2:
        return "DESTRUCTIVE"

    # 3. EXPANSION (Growth)
    if ugr > 0.1:
        # Check if Early or Late
        if rht >= 0 and decline < 0.1:
            # Ratio improving/healthy + near peak = Early/Strong
            return "EARLY_EXPANSION" if days_since_peak <= 1 else "EXPANSION"
        else:
            # Ratio deteriorating or decline setting in = Late Hype
            return "LATE_HYPE"

    # Default: Stalling/Choppy
    return "DESTRUCTIVE" if rht < -0.1 else "ACCUMULATING"


# ---------------------------------------------------------------------------
# 5. EV Score & Decision Bias
# ---------------------------------------------------------------------------

def _normalize(value: float, low: float, high: float) -> float:
    """Clamp and normalize a value to 0-1 range."""
    if high == low:
        return 0.5
    return max(0.0, min(1.0, (value - low) / (high - low)))


def compute_ev_score(signals: Dict, phase: str) -> float:
    """
    Continuous expected value score (0.0 = worst, 1.0 = best).
    """
    if signals.get("insufficient_data"):
        return 0.0

    ugr = signals["unique_growth_rate"]
    rht = signals["ratio_health_trend"]
    vpu_cv = signals["vpu_cv"]
    swap_reduction = signals["swap_reduction_from_peak"]
    days_since_peak = signals["days_since_peak"]

    # A) Unique Growth Score
    if ugr >= 0:
        unique_score = 1.0 - abs(ugr - 0.05) * 1.5
    else:
        unique_score = max(0, 1.0 + ugr)
    unique_score = max(0.0, min(1.0, unique_score))

    # B) Ratio Health Score
    ratio_score = _normalize(rht, -0.1, 0.1)

    # C) Volume Stability Score
    volume_stability_score = _normalize(1.0 - vpu_cv, 0.0, 1.0)

    # D) Compression Score
    compression_score = _normalize(swap_reduction, 0.0, 0.8)

    # E) Recovery Time Score
    recovery_score = _normalize(days_since_peak, 0, 5)
    
    # Phase Bonus/Penalty
    phase_bonus = 0.0
    if phase == "POST_DESTRUCTIVE": phase_bonus = 0.2
    if phase == "EARLY_EXPANSION": phase_bonus = 0.1
    if phase == "DESTRUCTIVE": phase_bonus = -0.3

    ev = (
        0.30 * unique_score
        + 0.25 * ratio_score
        + 0.20 * volume_stability_score
        + 0.15 * compression_score
        + 0.10 * recovery_score
        + phase_bonus
    )

    return max(0.0, min(1.0, round(ev, 4)))

def get_decision_bias(ev_score: float, phase: str) -> str:
    """Returns opinionated 1-word bias."""
    if phase == "INSUFFICIENT_DATA": return "WAIT"
    if phase == "DESTRUCTIVE": return "AVOID"
    
    if ev_score > 0.75: return "BUY"
    if ev_score > 0.6: return "CONSIDER"
    if ev_score > 0.4: return "WATCH"
    return "AVOID"


# ---------------------------------------------------------------------------
# 6. Top-Level API: Analyze a single token
# ---------------------------------------------------------------------------

async def analyze_token(mint: str, days: int = 7) -> Dict:
    """
    Full pipeline for a single token:
      events table → daily snapshots → enriched → signals → phase + EV
    """
    snapshots = await get_daily_snapshots(mint, days)
    snapshots = enrich_snapshots(snapshots)
    signals = compute_signals(snapshots)
    phase = classify_phase(signals)
    ev_score = compute_ev_score(signals, phase)
    decision = get_decision_bias(ev_score, phase)

    # Simple Time-in-Phase logic (reverse scan snapshots and classify each)
    # In a real app we'd store phase history. Here we re-compute.
    time_in_phase = 1
    if len(snapshots) > 1:
        # Check backward from yesterday
        for i in range(len(snapshots)-2, -1, -1):
            prev_signals = compute_signals(snapshots[:i+2])
            if prev_signals.get("insufficient_data"): break
            prev_phase = classify_phase(prev_signals)
            if prev_phase == phase:
                time_in_phase += 1
            else:
                break

    return {
        "mint": mint,
        "phase": phase,
        "time_in_phase": time_in_phase,
        "ev_score": ev_score,
        "decision": decision,
        "signals": signals,
        "snapshots": snapshots,
        "days_of_data": len(snapshots),
    }


async def get_active_mints(days: int = 7) -> List[str]:
    """
    Returns a list of all distinct token mints that have had activity
    in the last N days.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT DISTINCT token_mint FROM events WHERE block_time > NOW() - INTERVAL '%s days'",
                (days,),
            )
            rows = await cur.fetchall()
    return [row[0] for row in rows]


async def analyze_all_tokens(mints: Optional[List[str]] = None, days: int = 7) -> List[Dict]:
    """
    Analyze tokens and return sorted by EV score descending.
    If mints is None, discovers all active mints from the database.
    Runs analysis in parallel.
    """
    if mints is None:
        mints = await get_active_mints(days)

    if not mints:
        return []

    # Run analysis in parallel
    tasks = [analyze_token(mint, days) for mint in mints]
    results = await asyncio.gather(*tasks)

    # Sort by EV score descending (best opportunities first)
    results.sort(key=lambda x: x["ev_score"], reverse=True)
    return results
