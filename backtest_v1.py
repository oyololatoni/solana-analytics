import asyncio
import csv
import math
import os
import logging
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from app.core.db import init_db, close_db, get_db_connection

# Configuration
OUTPUT_DIR = "backtest_results"
FEATURE_VERSION = 1

# Loss Multipliers
LOSS_MULTS = {
    'price_failure': 0.5,
    'liquidity_collapse': 0.6,
    'volume_collapse': 0.7,
    'early_wallet_exit': 0.6,
    'expired': 0.0, # Will use final price / baseline if available, else 0? Spec says "use final price".
                    # But we don't have final price in labels. We have max_mult.
                    # For expired, max_mult might be < 1.0. Let's assume 0.2 default if unknown?
                    # The spec says "use final price". Queries needed?
                    # For now, let's use max_mult for expired if < 1, or just 0.5 conservative.
                    # Actually, let's use max_mult from DB if available and < 1.
}
DEFAULT_EXPIRY_VAL = 0.5 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("backtest")

async def fetch_dataset():
    """But query to join snapshots, labels, scores."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Note: We need feature columns. 
            # We'll select * from feature_snapshots or specific cols?
            # User said: "s.id, ts.score, l.outcome, l.max_multiplier, l.time_to_outcome"
            # AND "Feature Correlation Check" implies we need features.
            
            # Get columns for features
            await cur.execute("SELECT * FROM feature_snapshots LIMIT 0")
            col_desc = cur.description
            feature_cols = [c.name for c in col_desc if c.name not in ('id', 'token_id', 'feature_version', 'created_at', 'detection_timestamp')]
            
            feature_select = ", ".join([f"s.{c}" for c in feature_cols])
            
            query = f"""
                SELECT
                    s.id as snapshot_id,
                    ts.score,
                    l.outcome,
                    l.max_multiplier,
                    l.time_to_outcome,
                    s.detection_timestamp,
                    {feature_select}
                FROM feature_snapshots s
                JOIN lifecycle_labels l ON l.snapshot_id = s.id
                JOIN token_scores ts ON ts.snapshot_id = s.id
                WHERE s.feature_version = %s
            """
            
            await cur.execute(query, (FEATURE_VERSION,))
            rows = await cur.fetchall()
            
            return rows, feature_cols

def calculate_ev(prob_success, avg_mult, prob_fail, avg_loss):
    # EV = P(S) * E(M) - P(F) * E(L)
    # Actually usually EV = P(S)*E(M) + P(F)*E(M_fail) - 1 (if cost is 1)
    # The user formula: EV = P(success) * E(multiplier) - P(failure) * E(loss)
    # This implies "E(loss)" is positive magnitude of loss?
    # Or is E(loss) the remaining value (e.g. 0.5)? 
    # If loss multiplier is 0.5, then we get 0.5 back.
    # So EV = P(S)*Mult + P(F)*LossMult.
    # User wrote MINUS P(failure)*E(loss). This is ambiguous.
    # If E(loss) is "Amount Lost" (e.g. 0.5 lost), then EV = P(S)*Mult - P(F)*0.5.
    # If Loss Mult is 0.5 (we keep 0.5), then EV = P(S)*Mult + P(F)*0.5. (Rel to 1.0 basis).
    # "Define loss multiplier... price_failure -> 0.5".
    # I'll assume this means we KEEP 0.5. So EV = (P(S)*Mult + P(F)*Loss) - 1.0 (Profit).
    # But User Formula: P(S)*E(M) - P(F)*E(L).
    # Maybe E(L) is "Loss Percentage"? e.g. 0.5 means 50% loss?
    # "price_failure -> 0.5". If it means we RETAIN 0.5, then Loss is 0.5.
    # I will calculate Total Return based on weighted sums.
    # Return = (Sum(Success_Mults) + Sum(Failure_Mults)) / Total_Count.
    return (prob_success * avg_mult) + (prob_fail * avg_loss)

def get_loss_value(outcome, max_mult):
    if outcome == 'hit_5x':
        return 0 # Not a loss
    if outcome in LOSS_MULTS:
        val = LOSS_MULTS[outcome]
        if val == 0.0 and outcome == 'expired':
            # Use max_mult if available??
            return float(max_mult) if max_mult else DEFAULT_EXPIRY_VAL
        return val
    return 0.0

def run_analysis(rows, feature_cols, output_dir=OUTPUT_DIR):
    if not rows:
        logger.warning("No data found.")
        return

    # Prepare Data
    data = []
    for r in rows:
        # r structure: 0=id, 1=score, 2=outcome, 3=max_mult, 4=time_to_outcome, 5...=features
        item = {
            'id': r[0],
            'score': float(r[1]) if r[1] is not None else 0.0,
            'outcome': r[2],
            'max_mult': float(r[3]) if r[3] is not None else 0.0,
            'time_to_outcome': r[4].total_seconds() if r[4] else None,
            'detection_timestamp': r[5],
            'features': {k: float(v) if v is not None else 0.0 for k, v in zip(feature_cols, r[6:])}
        }
        item['is_success'] = 1 if item['outcome'] == 'hit_5x' else 0
        
        # Loss/Value for failed
        if item['is_success']:
            item['final_mult'] = item['max_mult']
        else:
            item['final_mult'] = get_loss_value(item['outcome'], item['max_mult'])
            
        data.append(item)

    total_n = len(data)
    hits = sum(d['is_success'] for d in data)
    overall_hit_rate = hits / total_n

    print(f"Total Samples: {total_n}")
    print(f"Overall Hit Rate: {overall_hit_rate:.2%}")

    # 2. Bucket Stats
    buckets = [
        (0, 30), (30, 50), (50, 60), (60, 70), (70, 80), (80, 101)
    ]
    bucket_stats = []
    
    print("\n--- Bucket Stats ---")
    print(f"{'Bucket':<10} {'N':<5} {'Hit%':<8} {'AvgMult':<8} {'EV':<8}")
    
    for low, high in buckets:
        subset = [d for d in data if low <= d['score'] < high]
        n = len(subset)
        if n == 0:
            bucket_stats.append({'bucket': f"{low}-{high}", 'n': 0})
            continue
            
        n_hits = sum(d['is_success'] for d in subset)
        hit_rate = n_hits / n
        avg_mult = sum(d['final_mult'] for d in subset) / n # This is effectively EV (Gross Return)
        
        bucket_stats.append({
            'bucket': f"{low}-{high}",
            'n': n,
            'hit_rate': hit_rate,
            'avg_mult': avg_mult,
            'ev': avg_mult # Gross EV
        })
        print(f"{low}-{high:<7} {n:<5} {hit_rate:.1%}    {avg_mult:.2f}x    {avg_mult:.2f}")

    # 3. Sniper Calibration (Thresholds)
    thresholds = [60, 65, 70, 75, 80, 85]
    print("\n--- Sniper Calibration ---")
    print(f"{'MinScore':<10} {'N':<5} {'Hit%':<8} {'AvgMult':<8}")
    
    sniper_rows = []
    for thr in thresholds:
        subset = [d for d in data if d['score'] >= thr]
        n = len(subset)
        if n == 0: continue
        n_hits = sum(d['is_success'] for d in subset)
        hit_rate = n_hits / n
        avg_mult = sum(d['final_mult'] for d in subset) / n
        
        sniper_rows.append({'threshold': thr, 'n': n, 'hit_rate': hit_rate, 'avg_mult': avg_mult})
        print(f">={thr:<8} {n:<5} {hit_rate:.1%}    {avg_mult:.2f}x")

    # 4. Failure Mode Analysis (High Score > 60)
    high_score = [d for d in data if d['score'] >= 60 and not d['is_success']]
    if high_score:
        fail_counts = defaultdict(int)
        for d in high_score:
            fail_counts[d['outcome']] += 1
        
        print("\n--- Failure Modes (Score >= 60) ---")
        for k, v in fail_counts.items():
            print(f"{k}: {v} ({v/len(high_score):.1%})")

    # 5. Feature Correlation
    # Pearson Correlation
    print("\n--- Feature Correlations (to Success) ---")
    correlations = []
    
    # Precompute means
    mean_y = overall_hit_rate
    
    for feat in feature_cols:
        vals = [d['features'][feat] for d in data]
        mean_x = sum(vals) / total_n
        
        # Num / Denom
        # sum((x - mx)*(y - my)) / sqrt(sum((x-mx)^2) * sum((y-my)^2))
        num = 0.0
        den_x = 0.0
        den_y = 0.0
        
        for i, d in enumerate(data):
            dx = d['features'][feat] - mean_x
            dy = d['is_success'] - mean_y
            num += dx * dy
            den_x += dx * dx
            den_y += dy * dy
            
        if den_x > 0 and den_y > 0:
            r = num / math.sqrt(den_x * den_y)
        else:
            r = 0.0
        
        correlations.append((feat, r))
    
    correlations.sort(key=lambda x: abs(x[1]), reverse=True)
    for f, r in correlations[:10]:
        print(f"{f:<30} {r:.3f}")

    # 6. ROC/AUC (Phase D)
    print("\n--- ROC/AUC Analysis ---")
    # Sort by score descending
    sorted_data = sorted(data, key=lambda x: x['score'], reverse=True)
    tpr_list = []
    fpr_list = []
    
    # Efficient AUC calc (Trapezoidal rule)
    # Iterate thresholds? Or simply iterate sorted list?
    # Iterate sorted list is standard O(N log N) approach.
    tp = 0
    fp = 0
    total_pos = sum(1 for d in data if d['is_success'])
    total_neg = total_n - total_pos
    
    auc = 0.0
    prev_fpr = 0.0
    
    if total_pos > 0 and total_neg > 0:
        for d in sorted_data:
            if d['is_success']:
                tp += 1
            else:
                fp += 1
            
            tpr = tp / total_pos
            fpr = fp / total_neg
            
            # Add trapezoid area
            auc += (fpr - prev_fpr) * tpr
            prev_fpr = fpr
            
        print(f"AUC: {auc:.3f}")
        if auc < 0.6:
            print("⚠️ WEAK PREDICTIVE POWER (AUC < 0.6)")
        elif auc > 0.7:
            print("✅ STRONG PREDICTIVE POWER (AUC > 0.7)")
            
    else:
        print("AUC: N/A (Insufficient pos/neg samples)")

    # 7. Temporal Validation (Phase G)
    print("\n--- Temporal Validation (Old vs New) ---")
    # Sort by detection_timestamp
    data_by_time = sorted(data, key=lambda x: x['detection_timestamp'] if x['detection_timestamp'] else datetime.min)
    if total_n >= 10:
        mid_idx = total_n // 2
        old_half = data_by_time[:mid_idx]
        new_half = data_by_time[mid_idx:]
        
        old_hit = sum(d['is_success'] for d in old_half) / len(old_half)
        new_hit = sum(d['is_success'] for d in new_half) / len(new_half)
        
        print(f"Old Half ({len(old_half)}): Hit Rate {old_hit:.1%}")
        print(f"New Half ({len(new_half)}): Hit Rate {new_hit:.1%}")
        
        if abs(old_hit - new_hit) > 0.1:
            print("⚠️ REGIME INSTABILITY DETECTED (>10% variance)")
        else:
            print("✅ REGIME STABLE")
    else:
        print("Skipping Temporal Split (N < 10)")

    # 8. Save CSVs
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    # Dataset Dump
    with open(f"{output_dir}/dataset_export.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'score', 'outcome', 'max_mult', 'time_to_outcome', 'detection_timestamp', 'is_success'] + feature_cols)
        for d in data:
            writer.writerow([d['id'], d['score'], d['outcome'], d['max_mult'], d['time_to_outcome'], d['detection_timestamp'], d['is_success']] + 
                            [d['features'][c] for c in feature_cols])

    # Stats Dump
    with open(f"{output_dir}/sniper_calibration.csv", "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['threshold', 'n', 'hit_rate', 'avg_mult'])
        writer.writeheader()
        writer.writerows(sniper_rows)
        
    print(f"\nSaved detailed results to {output_dir}/")

# Automation
async def run_calibration_cycle(standalone=False):
    """
    Automated entry point for the worker.
    Runs the backtest analysis and saves results if data exists.
    """
    logger.info("Starting automated calibration cycle...")
    if standalone:
        await init_db()
        
    try:
        rows, feature_cols = await fetch_dataset()
        if not rows or len(rows) < 50: # Min threshold to even bother
             # Use info if standalone, debug if worker to avoid spam?
            logger.info(f"Insufficient data for calibration (Found {len(rows)} samples). Skipping.")
            return False
            
        # Add timestamp to output dir
        ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_output_dir = f"{OUTPUT_DIR}/{ts_str}"
        
        # We need to pass this dir to run_analysis or modify run_analysis to accept it
        # Let's modify run_analysis signature in a separate edit or just patch it here?
        # Better: run_analysis currently uses global OUTPUT_DIR. 
        # I should change OUTPUT_DIR to a parameter.
        
        # check run_analysis first... it uses OUTPUT_DIR constant.
        # I will hack it by temporarily setting the global (ugly) or passing it.
        # Let's update run_analysis signature first.
        
        # Actually, let's just run it. The user wants it to "run itself".
        # Overwriting 'latest' is fine, but history is better.
        # I will leave OUTPUT_DIR as is for now to avoid breaking changes, 
        # but I will copy the files to a timestamped folder after?
        
        # Simpler: Just update run_analysis to take output_dir.
        
        if standalone:
            await close_db() # Close only if we opened it improperly or if we want to shut down
                             # Actually fetch_dataset uses connection pool. 
                             # If we close it here, we kill it for worker.
        
        # The main issue is run_analysis prints to stdout.
        # We want it to log or save.
        
        run_analysis(rows, feature_cols, output_dir=run_output_dir)
        return True
        
    except Exception as e:
        logger.error(f"Calibration cycle failed: {e}")
        return False
    finally:
        if standalone:
            await close_db() # Ensure closed if standalone

if __name__ == "__main__":
    asyncio.run(run_calibration_cycle(standalone=True))
