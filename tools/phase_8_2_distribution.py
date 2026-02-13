import asyncio
import os
import sys
import logging
from decimal import Decimal

# Add project root to path
sys.path.insert(0, os.getcwd())

# Load .env
if not os.environ.get("DATABASE_URL"):
    env_file = ".env" if os.path.exists(".env") else ".env.local"
    try:
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    os.environ[key] = value.strip('"').strip("'")
    except FileNotFoundError:
        pass

from app.core.db import get_db_connection, init_db, close_db

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("phase_8_2")

async def analyze_distribution():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            print("\n" + "="*50)
            print("PHASE 8.2 — LABEL DISTRIBUTION ANALYSIS")
            print("="*50)

            # 8.2.1 Basic Outcome Distribution
            print("\n🔍 8.2.1 — Basic Outcome Distribution")
            await cur.execute("""
                SELECT
                    outcome,
                    COUNT(*) AS count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
                FROM lifecycle_labels
                GROUP BY outcome
                ORDER BY count DESC;
            """)
            rows = await cur.fetchall()
            if not rows:
                print("No labels found.")
            else:
                print(f"{'OUTCOME':<20} {'COUNT':<10} {'PERCENT':<10}")
                print("-" * 40)
                for r in rows:
                    print(f"{r[0]:<20} {r[1]:<10} {r[2]}%")

            # 8.2.2 Failure Mode Breakdown
            print("\n🔍 8.2.2 — Failure Mode Breakdown")
            await cur.execute("""
                SELECT
                    failure_reason,
                    COUNT(*) AS count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
                FROM lifecycle_labels
                WHERE outcome = 'FAILURE'
                GROUP BY failure_reason
                ORDER BY count DESC;
            """)
            rows = await cur.fetchall()
            if not rows:
                print("No failures found.")
            else:
                print(f"{'REASON':<25} {'COUNT':<10} {'PERCENT':<10}")
                print("-" * 45)
                for r in rows:
                    reason = r[0] if r[0] else "None"
                    print(f"{reason:<25} {r[1]:<10} {r[2]}%")

            # 8.2.3 Time-to-Outcome Distribution
            # Note: lifecycle_labels might not have time_to_outcome_hours? 
            # Let's check if column exists. Migration 027 added failure_reason.
            # Schema usually has labeled_at. We need to join with tokens.detected_at?
            # Or feature_snapshots.snapshot_time?
            # User query assumes time_to_outcome_hours column exists in lifecycle_labels.
            # If not, we compute it: EXTRACT(EPOCH FROM (labeled_at - s.snapshot_time))/3600
            
            # Let's check columns first to be safe, or just try the join query which is safer.
            print("\n🔍 8.2.3 — Time-to-Outcome Distribution (Success)")
            # Try computing it
            await cur.execute("""
                SELECT
                    ROUND(EXTRACT(EPOCH FROM (l.labeled_at - t.detected_at))/3600) AS hour_bucket,
                    COUNT(*) AS count
                FROM lifecycle_labels l
                JOIN tokens t ON t.id = l.token_id
                WHERE l.outcome = 'SUCCESS'
                GROUP BY hour_bucket
                ORDER BY hour_bucket;
            """)
            rows = await cur.fetchall()
            if not rows:
                print("No success outcomes to analyze.")
            else:
                print(f"{'HOUR BUCKET':<15} {'COUNT':<10}")
                print("-" * 25)
                for r in rows:
                    print(f"{r[0]:<15} {r[1]:<10}")
                    
            # 8.2.4 Snapshot Count vs Resolved Count
            print("\n🔍 8.2.4 — Snapshot Count vs Resolved Count")
            await cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM feature_snapshots WHERE feature_version = 4) AS total_snapshots,
                    (SELECT COUNT(*) FROM lifecycle_labels) AS total_resolved
            """)
            row = await cur.fetchone()
            total_snaps = row[0]
            total_resolved = row[1]
            print(f"Total Snapshots (v4): {total_snaps}")
            print(f"Total Resolved:       {total_resolved}")
            
            if total_resolved < 100:
                print("⚠️  WARNING: Insufficient data (<100 resolved). Calibration will be unstable.")

            # 8.2.5 Base Rate vs Eligibility Rate
            print("\n🔍 8.2.5 — Base Rate vs Eligibility Rate")
            await cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE eligibility_status = 'ELIGIBLE') AS eligible_count,
                    COUNT(*) AS total_tokens
                FROM tokens;
            """)
            row = await cur.fetchone()
            eligible = row[0]
            total_tokens = row[1]
            eligibility_rate = (eligible / total_tokens * 100) if total_tokens > 0 else 0
            print(f"Eligible: {eligible}")
            print(f"Total:    {total_tokens}")
            print(f"Rate:     {eligibility_rate:.2f}%")

            # 8.2.6 Active vs Resolved Balance
            print("\n🔍 8.2.6 — Active vs Resolved Balance")
            await cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE is_active = TRUE) AS active_tokens,
                    COUNT(*) FILTER (WHERE is_active = FALSE) AS resolved_tokens
                FROM tokens;
            """)
            row = await cur.fetchone()
            print(f"Active:   {row[0]}")
            print(f"Resolved: {row[1]}")

            print("\n" + "="*50)

    await close_db()

if __name__ == "__main__":
    asyncio.run(analyze_distribution())
