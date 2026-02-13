
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import statistics

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.db import get_db_connection
# NOTE: phase_engine was V1-only; these functions may need to be
# re-implemented in app.engines.v1 or removed if no longer needed.
# For now, provide stubs to prevent ImportError.
try:
    from app.engines.v1.scoring import compute_score
except ImportError:
    pass

def classify_phase(metrics): return "UNKNOWN"
def structural_score(du, dv, usr_dev): return 0.0
def capital_quality_score(vpu_stable, usr_healthy, vpu_cv): return 0.0
def lifecycle_score(phase): return 0.0
def compute_ev_score(s, c, l): return s + c + l

# ---------------------------------------------------------------------------
# "Time Travel" Metrics Computation
# ---------------------------------------------------------------------------

async def compute_metrics_at(mint: str, ref_time: datetime) -> Dict:
    """
    Computes metrics as if 'now' was 'ref_time'.
    """
    # 1. Current Window (ref_time - 48h to ref_time)
    current = await fetch_window(mint, ref_time, hours=48)
    
    # 2. Previous Window (ref_time - 96h to ref_time - 48h)
    prev_time = ref_time - timedelta(hours=48)
    previous = await fetch_window(mint, ref_time, hours=48, offset_hours=48)
    
    # 3. Peak Metrics (last 14 days from ref_time)
    peaks = await fetch_peak_metrics(mint, ref_time, days=14)
    
    # Deltas
    deltas = compute_deltas(current, previous)
    
    return {
        **current,
        "U_t": current["U"],
        "S_t": current["S"],
        "V_t": current["V"],
        "VPU_t": current["VPU"],
        "USR_t": current["USR"],
        "previous_USR": previous["USR"],
        "dU_2d": deltas["dU"],
        "dS_2d": deltas["dS"],
        "dV_2d": deltas["dV"],
        "DeclineFromPeak": peaks["decline_from_peak"],
        "DaysSincePeak": peaks["days_since_peak"],
        "median_VPU": peaks["median_vpu"],
        "VPU_CV": peaks["vpu_cv"],
        "vpu_stable": peaks["vpu_stable"],
        "usr_healthy": peaks["usr_healthy"],
        **deltas,
        **peaks
    }

async def fetch_window(mint: str, ref_time: datetime, hours: int = 48, offset_hours: int = 0) -> Dict:
    start = ref_time - timedelta(hours=offset_hours + hours)
    end = ref_time - timedelta(hours=offset_hours)
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    COUNT(DISTINCT wallet) as unique_makers,
                    COUNT(*) as swap_count,
                    COALESCE(SUM(amount), 0) as volume
                FROM events
                WHERE token_mint = %s
                  AND block_time > %s
                  AND block_time <= %s
                  AND event_type != 'init'
                """,
                (mint, start, end)
            )
            row = await cur.fetchone()
            
    U = row[0] or 0
    S = row[1] or 0
    V = float(row[2] or 0)
    VPU = V / U if U > 0 else 0
    USR = U / S if S > 0 else 0
    
    return {"U": U, "S": S, "V": V, "VPU": VPU, "USR": USR}

def compute_deltas(current, previous):
    def d(c, p): return (c - p) / p if p > 0 else 0
    return {
        "dU": d(current["U"], previous["U"]),
        "dS": d(current["S"], previous["S"]),
        "dV": d(current["V"], previous["V"])
    }

async def fetch_peak_metrics(mint: str, ref_time: datetime, days: int = 14) -> Dict:
    start = ref_time - timedelta(days=days)
    
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    DATE(block_time) as day,
                    COUNT(DISTINCT wallet),
                    COUNT(*),
                    COALESCE(SUM(amount), 0)
                FROM events
                WHERE token_mint = %s
                  AND block_time > %s
                  AND block_time <= %s
                  AND event_type != 'init'
                GROUP BY DATE(block_time)
                ORDER BY day ASC
                """,
                (mint, start, ref_time)
            )
            rows = await cur.fetchall()
            
    if not rows:
        return {
            "decline_from_peak": 0, "days_since_peak": 0, 
            "median_vpu": 0, "vpu_cv": 0, "vpu_stable": False, "usr_healthy": False
        }

    vpu_hist = []
    usr_hist = []
    for r in rows:
        u, s, v = r[1] or 1, r[2] or 1, float(r[3] or 0)
        vpu_hist.append(v/u)
        usr_hist.append(u/s)
        
    peak_vpu = max(vpu_hist) if vpu_hist else 0
    curr_vpu = vpu_hist[-1] if vpu_hist else 0
    decline = (curr_vpu - peak_vpu)/peak_vpu if peak_vpu > 0 else 0
    days_since = len(vpu_hist) - 1 - (vpu_hist.index(peak_vpu) if vpu_hist else 0)
    
    median_vpu = statistics.median(vpu_hist) if vpu_hist else 0
    
    cv = 0
    if len(vpu_hist) >= 3:
        rec = vpu_hist[-3:]
        cv = statistics.stdev(rec)/statistics.mean(rec) if statistics.mean(rec) > 0 else 0
        
    return {
        "decline_from_peak": decline,
        "days_since_peak": days_since,
        "median_vpu": median_vpu,
        "vpu_cv": cv,
        "vpu_stable": cv < 0.3,
        "usr_healthy": usr_hist[-1] > 0.2 if usr_hist else False
    }

# ---------------------------------------------------------------------------
# Main Backtest Loop
# ---------------------------------------------------------------------------

async def run_backtest(mint: str, days: int = 7):
    from app.core.db import init_db, close_db
    await init_db()
    try:
        print(f"Running backtest for {mint} over last {days} days...")
        print(f"{'Time':<20} | {'Phase':<16} | {'EV':<5} | {'U_48h':<5} | {'V_48h':<8} | {'VPU':<6} | {'USR':<4}")
        print("-" * 80)
        
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(days=days)
        curr = start_time
        
        while curr <= now:
            metrics = await compute_metrics_at(mint, curr)
            phase = classify_phase(metrics)
            
            # EV
            usr_dev = metrics["USR"] - 0.3
            s = structural_score(metrics["dU_2d"], metrics["dV_2d"], usr_dev)
            c = capital_quality_score(metrics["vpu_stable"], metrics["usr_healthy"], metrics["VPU_CV"])
            l = lifecycle_score(phase)
            ev = compute_ev_score(s, c, l)
            
            print(f"{curr.strftime('%Y-%m-%d %H:%M'):<20} | {phase:<16} | {ev:<5.1f} | {metrics['U']:<5} | {metrics['V']:<8.0f} | {metrics['VPU']:<6.2f} | {metrics['USR']:<4.2f}")
            
            curr += timedelta(hours=4)
    finally:
        await close_db()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 tools/backtest_engine.py <mint> [days]")
        sys.exit(1)
        
    mint = sys.argv[1]
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    asyncio.run(run_backtest(mint, days))
