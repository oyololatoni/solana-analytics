import asyncio
import pickle
import logging
import os
import pandas as pd
from app.core.db import get_db_connection
from app.engines.v2.features import compute_v2_snapshot

logger = logging.getLogger("ml-inference")

class ModelLoader:
    _instance = None
    _model = None
    _scaler = None
    _version_id = None
    _last_check = 0
    REFRESH_INTERVAL = 3600  # Check for new model every hour

    @classmethod
    async def get_instance(cls):
        import time
        if cls._instance is None:
            cls._instance = ModelLoader()
            await cls._instance._load_latest_model()
        
        # Periodic refresh
        now = time.time()
        if now - cls._instance._last_check > cls.REFRESH_INTERVAL:
            await cls._instance._load_latest_model()
            cls._instance._last_check = now
            
        return cls._instance

    async def _load_latest_model(self):
        """
        Fetch latest model path from DB.
        Load pickle.
        """
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cur:
                    # Get latest successful model
                    await cur.execute("""
                        SELECT id, filepath 
                        FROM model_versions 
                        ORDER BY trained_at DESC 
                        LIMIT 1
                    """)
                    row = await cur.fetchone()
                    
                    if not row:
                        logger.warning("No trained model found in DB.")
                        return

                    version_id, model_path = row
                    
                    if version_id == self._version_id:
                        return # Already loaded

                    # Load artifacts
                    if not os.path.exists(model_path):
                        logger.error(f"Model file missing at {model_path}")
                        return

                    # Assume scaler path is model path with _scaler.pkl suffix?
                    # My train.py logic: f"{MODEL_DIR}/{version_id_str}_model.pkl"
                    # scaler: f"{MODEL_DIR}/{version_id_str}_scaler.pkl"
                    scaler_path = model_path.replace("_model.pkl", "_scaler.pkl")

                    with open(model_path, 'rb') as f:
                        self._model = pickle.load(f)
                    
                    if os.path.exists(scaler_path):
                        with open(scaler_path, 'rb') as f:
                            self._scaler = pickle.load(f)
                    else:
                        logger.warning("Scaler file missing!")
                        self._model = None # Safety
                        return

                    self._version_id = version_id
                    logger.info(f"Loaded ML Model Version {version_id}")

        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            self._model = None

    def predict_probability(self, features: dict) -> float:
        """
        Compute P(Success) for a single feature vector.
        """
        if not self._model or not self._scaler:
            return 0.0 # Default if no model

        # Convert dict to DataFrame (1 row) ensuring correct column order
        # We need the columns used during training.
        # Ideally we store column names in model_versions too?
        # For now, rely on hardcoded list matching `ml/train.py` query.
        
        feature_cols = [
            "volume_acceleration",
            "volume_growth_rate_1h",
            "trade_frequency_ratio",
            "liquidity_growth_rate",
            "liquidity_stability_score",
            "unique_wallet_growth_rate",
            "buy_sell_ratio",
            "wallet_entropy_score",
            "early_wallet_retention",
            "early_wallet_net_accumulation",
            "top10_concentration_delta",
            "drawdown_depth_1h",
            "volume_collapse_ratio",
            "liquidity_volatility"
        ]
        
        try:
            # Extract values in order
            data = []
            for col in feature_cols:
                 val = features.get(col, 0.0)
                 if val is None: val = 0.0
                 data.append(float(val))
            
            # Scale
            X_scaled = self._scaler.transform([data])
            
            # Predict
            prob = self._model.predict_proba(X_scaled)[0][1] # P(1)
            return float(prob)
            
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return 0.0
            
# Global Helper
async def predict_probability(features):
    loader = await ModelLoader.get_instance()
    # Refresh periodically? For now, load once.
    # Worker runs continuously. We might want to refresh model if new one avail.
    # But for "Phase 5", simple load is fine.
    return loader.predict_probability(features)
