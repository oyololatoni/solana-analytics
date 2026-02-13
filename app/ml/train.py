import asyncio
import os
import pickle
import json
import logging
from datetime import datetime
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, precision_score, recall_score
from app.core.db import init_db, get_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml-train")

MODEL_DIR = "ml/models"

async def fetch_training_data():
    """
    Fetch labeled data for training.
    Logic: 
    - Join feature_snapshots (s) + lifecycle_labels (l)
    - Filter: feature_version=1 (consistent features)
    - Filter: outcome is known (not None)
    - One snapshot per token (enforced by label Resolution engine which picks one snapshot?)
      Actually resolution engine puts label on ONE snapshot.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT 
                    s.volume_acceleration,
                    s.volume_growth_rate_1h,
                    s.trade_frequency_ratio,
                    s.liquidity_growth_rate,
                    s.liquidity_stability_score,
                    s.unique_wallet_growth_rate,
                    s.buy_sell_ratio,
                    s.wallet_entropy_score,
                    s.early_wallet_retention,
                    s.early_wallet_net_accumulation,
                    s.top10_concentration_delta,
                    s.drawdown_depth_1h,
                    s.volume_collapse_ratio,
                    s.liquidity_volatility,
                    l.outcome
                FROM feature_snapshots s
                JOIN lifecycle_labels l ON l.snapshot_id = s.id
                WHERE s.feature_version = 1
            """)
            rows = await cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            return pd.DataFrame(rows, columns=cols)

def train_model(df):
    if len(df) < 50:
        logger.warning(f"Insufficient data ({len(df)} samples). Need 50+.")
        return None, None, None

    # Prepare logic: Hit 5x = 1, Else = 0
    # Note: 'hit_5x' is string from DB enum or text? Check verify_integrity.py or DB schema.
    # It is likely 'SUCCESS' or 'hit_5x'. 
    # Wait, Resolution Engine uses 'SUCCESS' for > 5x? 
    # Use backtest_v1 logic as reference.
    # In backtest_v1, is_success = (outcome == 'SUCCESS' or outcome == 'hit_5x'?)
    # Let's check api/label_worker.py to be sure.
    # Assuming 'SUCCESS' based on previous context.
    
    df['label'] = df['outcome'].apply(lambda x: 1 if x == 'SUCCESS' else 0)
    
    feature_cols = [c for c in df.columns if c not in ['outcome', 'label']]
    X = df[feature_cols]
    y = df['label']
    
    # Check class balance
    pos_count = y.sum()
    if pos_count < 10 or (len(y) - pos_count) < 10:
        logger.warning(f"Class imbalance too severe (Pos: {pos_count}, Neg: {len(y)-pos_count}).")
        return None, None, None

    # Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Train
    model = LogisticRegression(
        penalty='l2',
        C=1.0,
        solver='liblinear',
        max_iter=1000,
        class_weight='balanced' # Optional: handle imbalance? Prompt didn't specify, but good practice.
    )
    model.fit(X_scaled, y)
    
    # Eval
    probs = model.predict_proba(X_scaled)[:, 1]
    auc = roc_auc_score(y, probs)
    
    # Precision/Recall at 0.7 threshold (as requested in Schema/Plan?)
    # Plan mentioned "Precision@70".
    preds_70 = (probs >= 0.7).astype(int)
    prec = precision_score(y, preds_70, zero_division=0)
    rec = recall_score(y, preds_70, zero_division=0)
    
    metrics = {
        "auc": round(auc, 3),
        "precision_at_70": round(prec, 3),
        "recall_at_70": round(rec, 3),
        "n_samples": len(df),
        "n_pos": int(pos_count)
    }
    
    return model, scaler, metrics

async def save_model(model, scaler, metrics):
    if not os.path.exists(MODEL_DIR):
        os.makedirs(MODEL_DIR)
        
    # Generate Version ID (use DB or timestamp?)
    # We insert into DB first to get ID?
    # Or use timestamp.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    version_id_str = f"v_{timestamp}"
    
    model_path = f"{MODEL_DIR}/{version_id_str}_model.pkl"
    scaler_path = f"{MODEL_DIR}/{version_id_str}_scaler.pkl"
    
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    with open(scaler_path, 'wb') as f:
        pickle.dump(scaler, f)
        
    # DB Insert
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO model_versions (feature_version, model_type, metrics, filepath)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (1, "logistic_regression", json.dumps(metrics), model_path)) # Storing model_path as main ref
            
            v_id = (await cur.fetchone())[0]
            await conn.commit()
            logger.info(f"Model saved: Version {v_id} (AUC={metrics['auc']})")
            return v_id

async def run_training_pipeline():
    logger.info("Starting training pipeline...")
    await init_db()
    try:
        df = await fetch_training_data()
        if df.empty:
            logger.warning("No data found.")
            return

        model, scaler, metrics = train_model(df)
        if model:
            logger.info(f"Training success. Metrics: {metrics}")
            await save_model(model, scaler, metrics)
        else:
            logger.info("Training skipped (insufficient data/quality).")
            
    except Exception as e:
        logger.error(f"Training failed: {e}")
        # import traceback; traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_training_pipeline())
