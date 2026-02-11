"""
Phase Detection Engine — Production Spec
==========================================
Deterministic 7-phase classification + 3-layer EV scoring (0-100).
Dashboard reads from token_state table, never raw events.

Phases (priority order):
  DESTRUCTIVE → DISTRIBUTION → POST_DESTRUCTIVE → ACCELERATION →
  EXPANSION → MATURE → INITIAL → DORMANT

EV Score (0-100):
  Structural (0-40) + Capital Quality (0-30) + Lifecycle Bias (0-30)
"""

from api.db import get_db_connection
from typing import List, Dict, Optional
import statistics
import asyncio
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# 1. Base Window Metrics (from raw events, 48h window)
# ---------------------------------------------------------------------------

async def compute_window_metrics(mint: str, offset_days: int = 0) -> Dict:
    """
    Compute base metrics for a 48-hour window.
    offset_days=0 → current window (now - 48h to now)
    offset_days=2 → previous window (now - 96h to now - 48h)
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    COUNT(DISTINCT wallet) as unique_makers,
                    COUNT(*) as swap_count,
                    COALESCE(SUM(amount), 0) as volume,
                    COALESCE(SUM(CASE WHEN direction = 'in' THEN amount ELSE 0 END), 0) as vol_buy,
                    COALESCE(SUM(CASE WHEN direction = 'out' THEN amount ELSE 0 END), 0) as vol_sell
                FROM events
                WHERE token_mint = %s
                  AND block_time > NOW() - INTERVAL '%s days'
                  AND block_time <= NOW() - INTERVAL '%s days'
                  AND event_type != 'init'
                """,
                (mint, offset_days + 2, offset_days),
            )
            row = await cur.fetchone()

    U = row[0] or 0
    S = row[1] or 0
    V = float(row[2] or 0)
    vol_buy = float(row[3] or 0)
    vol_sell = float(row[4] or 0)

    VPU = V / U if U > 0 else 0
    USR = U / S if S > 0 else 0

    return {
        "U": U,
        "S": S,
        "V": V,
        "VPU": VPU,
        "USR": USR,
        "vol_buy": vol_buy,
        "vol_sell": vol_sell,
    }


# ---------------------------------------------------------------------------
# 2. Historical Derivatives (2-day delta)
# ---------------------------------------------------------------------------

def compute_deltas(current: Dict, previous: Dict) -> Dict:
    """Compute percentage change between two 48h windows."""
    def delta(curr, prev):
        if prev == 0:
            return 0
        return (curr - prev) / prev

    return {
        "dU": round(delta(current["U"], previous["U"]), 4),
        "dS": round(delta(current["S"], previous["S"]), 4),
        "dV": round(delta(current["V"], previous["V"]), 4),
    }


# ---------------------------------------------------------------------------
# 3. Peak Calculations (from daily VPU history)
# ---------------------------------------------------------------------------

async def compute_peak_metrics(mint: str, days: int = 14) -> Dict:
    """
    Compute peak VPU, decline from peak, and days since peak
    using daily snapshots over the last N days.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    DATE(block_time) as day,
                    COUNT(DISTINCT wallet) as unique_makers,
                    COUNT(*) as swap_count,
                    COALESCE(SUM(amount), 0) as volume
                FROM events
                WHERE token_mint = %s
                  AND block_time > NOW() - INTERVAL '%s days'
                  AND event_type != 'init'
                GROUP BY DATE(block_time)
                ORDER BY day ASC
                """,
                (mint, days),
            )
            rows = await cur.fetchall()

    if not rows:
        return {
            "peak_vpu": 0, "decline_from_peak": 0,
            "days_since_peak": 0, "vpu_cv": 1.0,
            "vpu_history": [], "median_vpu": 0,
            "usr_recovering": False, "vpu_rising": False,
            "usr_drop": 0, "vpu_stable": False,
            "usr_healthy": False,
        }

    vpu_history = []
    usr_history = []
    for row in rows:
        u = row[1] or 1
        s = row[2] or 1
        v = float(row[3] or 0)
        vpu_history.append(v / u)
        usr_history.append(u / s)

    peak_vpu = max(vpu_history) if vpu_history else 0
    current_vpu = vpu_history[-1] if vpu_history else 0

    decline_from_peak = 0
    if peak_vpu > 0:
        decline_from_peak = (current_vpu - peak_vpu) / peak_vpu  # negative when below peak

    peak_idx = vpu_history.index(peak_vpu) if peak_vpu > 0 else 0
    days_since_peak = len(vpu_history) - 1 - peak_idx

    # VPU coefficient of variation (stability measure)
    vpu_cv = 1.0
    if len(vpu_history) >= 3:
        recent = vpu_history[-3:]
        mean_v = statistics.mean(recent) or 1
        vpu_cv = (statistics.stdev(recent) / mean_v) if len(recent) > 1 else 0

    median_vpu = statistics.median(vpu_history) if vpu_history else 0

    # USR recovering? (last 2 days trending up)
    usr_recovering = False
    if len(usr_history) >= 3:
        usr_recovering = usr_history[-1] > usr_history[-2] > usr_history[-3]

    # VPU rising?
    vpu_rising = False
    if len(vpu_history) >= 2:
        vpu_rising = vpu_history[-1] > vpu_history[-2]

    # USR drop (max drop from recent peak)
    usr_drop = 0
    if usr_history:
        usr_peak = max(usr_history)
        if usr_peak > 0:
            usr_drop = (usr_peak - usr_history[-1]) / usr_peak

    # VPU stable = CV < 0.3
    vpu_stable = vpu_cv < 0.3

    # USR healthy = above 0.2
    usr_healthy = usr_history[-1] > 0.2 if usr_history else False

    return {
        "peak_vpu": round(peak_vpu, 4),
        "decline_from_peak": round(decline_from_peak, 4),
        "days_since_peak": days_since_peak,
        "vpu_cv": round(vpu_cv, 4),
        "vpu_history": vpu_history,
        "median_vpu": round(median_vpu, 4),
        "usr_recovering": usr_recovering,
        "vpu_rising": vpu_rising,
        "usr_drop": round(usr_drop, 4),
        "vpu_stable": vpu_stable,
        "usr_healthy": usr_healthy,
    }


# ---------------------------------------------------------------------------
# 4. Deterministic Phase Classification (7 phases, priority order)
# ---------------------------------------------------------------------------

def classify_phase(metrics: Dict) -> str:
    """
    Deterministic phase classification.
    Priority: Destructive > Distribution > Post-Destructive >
              Acceleration > Expansion > Mature > Initial > Dormant
    """
    U = metrics.get("U", 0)
    dU = metrics.get("dU", 0)
    dV = metrics.get("dV", 0)
    decline = metrics.get("decline_from_peak", 0)
    days_since_peak = metrics.get("days_since_peak", 0)
    vpu_cv = metrics.get("vpu_cv", 1.0)
    median_vpu = metrics.get("median_vpu", 0)
    VPU = metrics.get("VPU", 0)
    usr_recovering = metrics.get("usr_recovering", False)
    vpu_rising = metrics.get("vpu_rising", False)
    usr_drop = metrics.get("usr_drop", 0)

    # 1. DESTRUCTIVE — sharp collapse
    if dU < -0.40 and decline <= -0.50 and days_since_peak > 3:
        return "DESTRUCTIVE"

    # 2. DISTRIBUTION — smart money exiting
    if (
        dU < 0.2
        and dV >= 0
        and VPU >= 1.5 * median_vpu if median_vpu > 0 else False
        and usr_drop > 0.3
        and days_since_peak <= 3
    ):
        return "DISTRIBUTION"

    # 3. POST_DESTRUCTIVE — base forming (highest EV)
    if (
        decline <= -0.6
        and days_since_peak >= 3
        and dU >= 0
        and -0.2 <= dV <= 0.2
        and vpu_cv < 0.25
        and usr_recovering
    ):
        return "POST_DESTRUCTIVE"

    # 4. ACCELERATION — strong momentum
    if (
        dU >= 1.0
        and dV >= 1.0
        and dV >= dU
        and vpu_rising
        and usr_drop < 0.25
        and abs(decline) < 0.1
    ):
        return "ACCELERATION"

    # 5. EXPANSION — healthy growth
    if (
        dU >= 0.5
        and dV >= 0.5
        and abs(dU - dV) <= 0.2
        and usr_drop < 0.2
        and decline >= -0.3
    ):
        return "EXPANSION"

    # 6. MATURE — plateau
    if -0.1 <= dU <= 0.1 and decline >= -0.4:
        return "MATURE"

    # 7. INITIAL — early traction
    if dU > 1.0 and U < 500:
        return "INITIAL"

    # Default
    return "DORMANT"


# ---------------------------------------------------------------------------
# 5. Three-Layer EV Scoring (0-100)
# ---------------------------------------------------------------------------

def structural_score(dU: float, dV: float, usr_deviation: float) -> float:
    """Layer 1 — Structural participation (0-40)."""
    score_u = min(max(dU / 2, 0), 1)
    score_v = min(max(dV / 2, 0), 1)
    score_usr = max(0, 1 - abs(usr_deviation))
    return round((score_u * 15) + (score_v * 15) + (score_usr * 10), 2)


def capital_quality_score(vpu_stable: bool, usr_healthy: bool, cv: float) -> float:
    """Layer 2 — Capital quality (0-30)."""
    score = 0.0
    if vpu_stable:
        score += 10
    if usr_healthy:
        score += 10
    score += max(0, 10 - (cv * 20))
    return round(min(score, 30), 2)


def lifecycle_score(phase: str) -> float:
    """Layer 3 — Phase lifecycle bias (0-30)."""
    weights = {
        "DORMANT": 5,
        "INITIAL": 10,
        "MATURE": 8,
        "EXPANSION": 20,
        "ACCELERATION": 15,
        "DISTRIBUTION": 5,
        "DESTRUCTIVE": 0,
        "POST_DESTRUCTIVE": 30,
    }
    return float(weights.get(phase, 0))


def compute_ev_score(struct: float, capital: float, lifecycle: float) -> float:
    """Final EV = sum of all three layers (0-100)."""
    return round(struct + capital + lifecycle, 2)


def get_decision_bias(ev_score: float, phase: str) -> str:
    """Interpret EV score into actionable bias."""
    if phase == "DESTRUCTIVE":
        return "AVOID"
    if phase == "DISTRIBUTION":
        return "CAUTION"
    if ev_score >= 80:
        return "BUY"
    if ev_score >= 65:
        return "CONSIDER"
    if ev_score >= 50:
        return "WATCH"
    if ev_score >= 35:
        return "WEAK"
    return "AVOID"


# ---------------------------------------------------------------------------
# 6. Full Analysis Pipeline (single token)
# ---------------------------------------------------------------------------

async def analyze_token(mint: str) -> Dict:
    """
    Full pipeline for a single token:
      events → window metrics → deltas → peak → phase → EV → persist
    """
    # 1. Current and previous window metrics
    current = await compute_window_metrics(mint, offset_days=0)
    previous = await compute_window_metrics(mint, offset_days=2)

    # 2. Deltas
    deltas = compute_deltas(current, previous)

    # 3. Peak metrics
    peaks = await compute_peak_metrics(mint, days=14)

    # 4. Merge all metrics
    metrics = {
        **current,
        **deltas,
        **peaks,
    }

    # 5. Classify phase
    phase = classify_phase(metrics)

    # 6. Compute EV score (3 layers)
    # USR deviation from optimal (0.3 is ideal)
    usr_dev = current["USR"] - 0.3

    struct = structural_score(deltas["dU"], deltas["dV"], usr_dev)
    capital = capital_quality_score(peaks["vpu_stable"], peaks["usr_healthy"], peaks["vpu_cv"])
    lifecycle = lifecycle_score(phase)
    ev = compute_ev_score(struct, capital, lifecycle)

    # 7. Decision
    decision = get_decision_bias(ev, phase)

    result = {
        "mint": mint,
        "phase": phase,
        "ev_score": ev,
        "structural_score": struct,
        "capital_score": capital,
        "lifecycle_score": lifecycle,
        "decision": decision,
        # Raw metrics for display
        "unique_makers": current["U"],
        "swap_count": current["S"],
        "volume": current["V"],
        "unique_growth": deltas["dU"],
        "volume_growth": deltas["dV"],
        "vpu": current["VPU"],
        "usr": current["USR"],
        "vpu_cv": peaks["vpu_cv"],
        "decline_from_peak": peaks["decline_from_peak"],
        "days_since_peak": peaks["days_since_peak"],
    }

    return result


# ---------------------------------------------------------------------------
# 7. State Persistence (writes to token_state + token_scores_history)
# ---------------------------------------------------------------------------

async def persist_state(result: Dict):
    """Upsert into token_state and append to token_scores_history."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Upsert token_state
            await cur.execute(
                """
                INSERT INTO token_state (
                    token_mint, current_phase, ev_score,
                    structural_score, capital_score, lifecycle_score,
                    unique_makers, swap_count, volume,
                    unique_growth, volume_growth, vpu, usr, vpu_cv,
                    decline_from_peak, days_since_peak, decision_bias,
                    last_updated
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, NOW()
                )
                ON CONFLICT (token_mint) DO UPDATE SET
                    current_phase = EXCLUDED.current_phase,
                    ev_score = EXCLUDED.ev_score,
                    structural_score = EXCLUDED.structural_score,
                    capital_score = EXCLUDED.capital_score,
                    lifecycle_score = EXCLUDED.lifecycle_score,
                    unique_makers = EXCLUDED.unique_makers,
                    swap_count = EXCLUDED.swap_count,
                    volume = EXCLUDED.volume,
                    unique_growth = EXCLUDED.unique_growth,
                    volume_growth = EXCLUDED.volume_growth,
                    vpu = EXCLUDED.vpu,
                    usr = EXCLUDED.usr,
                    vpu_cv = EXCLUDED.vpu_cv,
                    decline_from_peak = EXCLUDED.decline_from_peak,
                    days_since_peak = EXCLUDED.days_since_peak,
                    decision_bias = EXCLUDED.decision_bias,
                    last_updated = NOW()
                """,
                (
                    result["mint"], result["phase"], result["ev_score"],
                    result["structural_score"], result["capital_score"],
                    result["lifecycle_score"],
                    result["unique_makers"], result["swap_count"],
                    result["volume"], result["unique_growth"],
                    result["volume_growth"], result["vpu"], result["usr"],
                    result["vpu_cv"], result["decline_from_peak"],
                    result["days_since_peak"], result["decision"],
                ),
            )

            # Append to history
            await cur.execute(
                """
                INSERT INTO token_scores_history (
                    token_mint, phase, ev_score,
                    structural_score, capital_score, lifecycle_score
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    result["mint"], result["phase"], result["ev_score"],
                    result["structural_score"], result["capital_score"],
                    result["lifecycle_score"],
                ),
            )
            await conn.commit()


# ---------------------------------------------------------------------------
# 8. Discovery: Active mints from DB
# ---------------------------------------------------------------------------

async def get_active_mints(days: int = 7) -> List[str]:
    """Returns all distinct token mints with activity in last N days."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT DISTINCT token_mint FROM events
                WHERE block_time > NOW() - INTERVAL '%s days'
                  AND event_type != 'init'
                """,
                (days,),
            )
            rows = await cur.fetchall()
    return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# 9. Bulk Analysis + Persist
# ---------------------------------------------------------------------------

async def analyze_all_tokens(days: int = 7) -> List[Dict]:
    """
    Analyze all active tokens, persist state, return sorted by EV desc.
    """
    mints = await get_active_mints(days)
    if not mints:
        return []

    tasks = [analyze_token(mint) for mint in mints]
    results = await asyncio.gather(*tasks)

    # Persist all states
    for r in results:
        await persist_state(r)

    results.sort(key=lambda x: x["ev_score"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# 10. Read from snapshot (fast path for dashboard)
# ---------------------------------------------------------------------------

async def get_all_states() -> List[Dict]:
    """Read directly from token_state table. No computation."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    token_mint, current_phase, ev_score,
                    structural_score, capital_score, lifecycle_score,
                    unique_makers, swap_count, volume,
                    unique_growth, volume_growth, vpu, usr, vpu_cv,
                    decline_from_peak, days_since_peak, decision_bias,
                    last_updated
                FROM token_state
                ORDER BY ev_score DESC
                """
            )
            rows = await cur.fetchall()

    return [
        {
            "mint": r[0],
            "phase": r[1],
            "ev_score": float(r[2]),
            "structural_score": float(r[3]),
            "capital_score": float(r[4]),
            "lifecycle_score": float(r[5]),
            "unique_makers": r[6],
            "swap_count": r[7],
            "volume": float(r[8]),
            "unique_growth": float(r[9]),
            "volume_growth": float(r[10]),
            "vpu": float(r[11]),
            "usr": float(r[12]),
            "vpu_cv": float(r[13]),
            "decline_from_peak": float(r[14]),
            "days_since_peak": r[15],
            "decision": r[16],
            "last_updated": r[17].isoformat() if r[17] else None,
        }
        for r in rows
    ]
