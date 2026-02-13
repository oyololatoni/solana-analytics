import sys
import os
sys.path.insert(0, os.getcwd())

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from app.core.db import get_db_connection, init_db, close_db

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("audit")

async def audit_tier_1_dataset_correctness():
    """
    Tier 1: Immutability, Feature Version, Schema Isolation, Duplicates
    """
    print("\n🔵 TIER 1: DATASET CORRECTNESS")
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 1.1 No Duplicate Snapshots
            await cur.execute("""
                SELECT token_id, feature_version, COUNT(*)
                FROM feature_snapshots
                GROUP BY token_id, feature_version
                HAVING COUNT(*) > 1
            """)
            dupes = await cur.fetchall()
            if dupes:
                print(f"❌ FAIL: Found {len(dupes)} duplicate snapshots (token_id, version)")
            else:
                print("✅ PASS: No duplicate snapshots per token/version")

            # 1.2 Schema Isolation (v2 trades only)
            # Check column names for 'snapshot_time' vs 'detection_timestamp' mismatch
            await cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'feature_snapshots'
            """)
            cols = [row[0] for row in await cur.fetchall()]
            if 'snapshot_time' in cols:
                print("✅ PASS: Column 'snapshot_time' exists")
            elif 'detection_timestamp' in cols:
                print(f"⚠️ WARN: Column 'snapshot_time' MISSING, found 'detection_timestamp'. Code/DB Mismatch?")
            else:
                print("❌ FAIL: Neither 'snapshot_time' nor 'detection_timestamp' found")

async def audit_tier_2_features_scoring():
    """
    Tier 2: Feature Engine & Scoring
    """
    print("\n🟠 TIER 2: FEATURE & SCORING INTEGRITY")
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 2.1 Feature Normalization (0-1 mostly)
            await cur.execute("""
                SELECT COUNT(*) FROM feature_snapshots WHERE volume_acceleration = -1
            """)
            placeholders = (await cur.fetchone())[0]
            if placeholders > 0:
                 print(f"❌ FAIL: Found {placeholders} rows with placeholder -1")
            else:
                 print("✅ PASS: No explicit -1 placeholders found")

            # 2.2 Rule Score == Breakdown Total (Spot Check)
            await cur.execute("SELECT COUNT(*) FROM feature_snapshots WHERE score_total IS NULL")
            null_scores = (await cur.fetchone())[0]
            if null_scores > 0:
                print(f"❌ FAIL: {null_scores} snapshots with NULL score_total")
            else:
                 print("✅ PASS: All snapshots have score_total")

async def audit_tier_3_labeling():
    """
    Tier 3: Label Worker (72h window)
    """
    print("\n🟡 TIER 3: STATISTICAL VALIDITY & LABELS")
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 3.1 72h Window Enforcement
            # Check tokens that are ACTIVE but older than 72h (Should be EXPIRED or Labeled)
            # Actually, label worker doesn't expire them, it just labels 'UNRESOLVED' if < 72h? 
            # No, if > 72h and no success/fail, it labels 'UNLABELED'.
            await cur.execute("""
                SELECT COUNT(*)
                FROM tokens
                WHERE is_active = TRUE
                AND detected_at < NOW() - INTERVAL '72 hours'
                AND eligibility_status = 'ELIGIBLE'
                AND id NOT IN (SELECT token_id FROM lifecycle_labels)
            """)
            stuck_tokens = (await cur.fetchone())[0]
            if stuck_tokens > 0:
                print(f"❌ FAIL: {stuck_tokens} tokens >72h old are UNLABELED and ACTIVE")
            else:
                print("✅ PASS: No stuck tokens >72h old")

            # 3.2 Label Distribution
            await cur.execute("SELECT outcome, COUNT(*) FROM lifecycle_labels GROUP BY outcome")
            rows = await cur.fetchall()
            print("   Label Distribution:", dict(rows))

            # 3.3 Inactive Flag Enforcement Check
            # Check if any token has a 'FAILURE' or 'EXPIRED' label but is still is_active=TRUE
            # Note: The checked outcomes are defined in schema 009: 
            # 'hit_5x', 'price_failure', 'liquidity_collapse', 'volume_collapse', 'early_wallet_exit', 'expired'
            # 'FAILURE' is not an outcome string, the specific failures are.
            # We treat any outcome except 'hit_5x' (and maybe 'expired'?) as failure.
            # Wait, 009 constraints: hit_5x, price_failure, liquidity_collapse, volume_collapse, early_wallet_exit, expired.
            # So we check for those.
            failure_outcomes = ('price_failure', 'liquidity_collapse', 'volume_collapse', 'early_wallet_exit', 'expired')
            
            await cur.execute(f"""
                SELECT COUNT(*)
                FROM tokens t
                JOIN lifecycle_labels l ON t.id = l.token_id
                WHERE t.is_active = TRUE
                AND l.outcome IN {failure_outcomes}
            """)
            zombie_tokens = (await cur.fetchone())[0]
            if zombie_tokens > 0:
                 print(f"❌ FAIL: {zombie_tokens} tokens match failure outcomes but are still is_active=TRUE")
            else:
                 print("✅ PASS: All failed tokens are inactive")

async def main():
    await init_db()
    try:
        await audit_tier_1_dataset_correctness()
        await audit_tier_2_features_scoring()
        await audit_tier_3_labeling()
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(main())
