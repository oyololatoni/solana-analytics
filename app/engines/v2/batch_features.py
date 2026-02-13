
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import math
import json
from typing import List

from app.core.db import get_db_connection
from app.core.constants import (
    SCORE_WEIGHTS_V3, LIFECYCLE_THRESHOLDS, RISK_PARAMS, EPSILON, ML_ENABLED, FEATURE_VERSION
)

logger = logging.getLogger("engines.v2.batch_features")

class BatchFeatureEngine:
    def __init__(self, conn, cur, feature_version=2):
        self.conn = conn
        self.cur = cur
        self.feature_version = feature_version

    async def process_batch(self, token_ids: list):
        if not token_ids: return
        
        for token_id in token_ids:
            try:
                await self.generate_snapshot(token_id)
            except Exception as e:
                logger.error(f"Error processing token {token_id}: {e}")

    async def generate_snapshot(self, token_id: int):
        from app.engines.v2.features import compute_v2_snapshot
        try:
            # We call the full logic which handles its own connection/transaction
            # This is less "batch-efficient" but much safer and reuses the Gold Standard logic.
            snapshot_id = await compute_v2_snapshot(token_id)
            if snapshot_id:
                logger.info(f"Generated full snapshot {snapshot_id} for token {token_id}")
            return snapshot_id
        except Exception as e:
            logger.error(f"Failed to generate snapshot for {token_id}: {e}")
            return None
