import asyncio
import logging
from datetime import datetime, timezone
import os
import json
import sys

# Add project root to path
sys.path.insert(0, os.getcwd())

# Load .env manually if not set
if not os.environ.get("DATABASE_URL"):
    env_file = ".env" if os.path.exists(".env") else ".env.local"
    try:
        with open(env_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    os.environ[key] = value.strip('"').strip("'")
        print(f"✅ Loaded {env_file} file")
    except FileNotFoundError:
        print("⚠️ No .env or .env.local file found, relying on system env vars")

from app.core.db import get_db_connection, init_db, close_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("phase_8_1_integrity")

async def run_integrity_checks():
    await init_db()
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # 8.1.1 Snapshot Uniqueness
            logger.info("🔍 8.1.1 — Snapshot Uniqueness")
            await cur.execute("""
                SELECT token_id, COUNT(*)
                FROM feature_snapshots
                GROUP BY token_id, feature_version
                HAVING COUNT(*) > 1;
            """)
            dupes = await cur.fetchall()
            status_8_1_1 = "PASS" if not dupes else "FAIL"
            logger.info(f"Result: {status_8_1_1} ({len(dupes)} duplicates found)")

            # 8.1.2 Snapshot–Label Alignment
            # Note: active -> is_active
            logger.info("🔍 8.1.2 — Snapshot–Label Alignment")
            await cur.execute("""
                SELECT t.id
                FROM tokens t
                LEFT JOIN feature_snapshots s ON s.token_id = t.id
                LEFT JOIN lifecycle_labels l ON l.token_id = t.id
                WHERE t.is_active = FALSE
                AND (s.id IS NULL OR l.id IS NULL);
            """)
            alignment_issues = await cur.fetchall()
            status_8_1_2 = "PASS" if not alignment_issues else "FAIL"
            logger.info(f"Result: {status_8_1_2} ({len(alignment_issues)} alignment issues found)")

            # 8.1.3 No Future Leakage
            # Note: snapshot_time (v2)
            logger.info("🔍 8.1.3 — No Future Leakage")
            await cur.execute("""
                SELECT l.token_id
                FROM lifecycle_labels l
                JOIN feature_snapshots s ON s.token_id = l.token_id
                WHERE l.labeled_at < s.snapshot_time;
            """)
            leakage = await cur.fetchall()
            status_8_1_3 = "PASS" if not leakage else "FAIL"
            logger.info(f"Result: {status_8_1_3} ({len(leakage)} leakage cases found)")

            # 8.1.4 Outcome Completeness
            logger.info("🔍 8.1.4 — Outcome Completeness")
            await cur.execute("""
                SELECT id
                FROM tokens
                WHERE NOW() - detected_at > INTERVAL '72 hours'
                AND eligibility_status = 'ELIGIBLE'
                AND id NOT IN (
                    SELECT token_id FROM lifecycle_labels
                );
            """)
            incomplete = await cur.fetchall()
            status_8_1_4 = "PASS" if not incomplete else "FAIL"
            logger.info(f"Result: {status_8_1_4} ({len(incomplete)} incomplete tokens found)")

            # 8.1.5 Feature Version Purity
            logger.info("🔍 8.1.5 — Feature Version Purity")
            await cur.execute("""
                SELECT DISTINCT feature_version
                FROM feature_snapshots;
            """)
            versions = await cur.fetchall()
            status_8_1_5 = "PASS" if len(versions) <= 1 else "FAIL"
            logger.info(f"Result: {status_8_1_5} (Versions found: {[v[0] for v in versions]})")

            # 8.1.6 Score Consistency
            # Note: My score_breakdown JSON has "total" key for rule_score_total equivalent.
            # And column is named score_breakdown, not rule_breakdown.
            logger.info("🔍 8.1.6 — Score Consistency")
            await cur.execute("""
                SELECT id
                FROM feature_snapshots
                WHERE score_breakdown IS NOT NULL 
                AND ABS(score_total - (score_breakdown->>'total')::numeric) > 0.0001;
            """)
            inconsistent_scores = await cur.fetchall()
            status_8_1_6 = "PASS" if not inconsistent_scores else "FAIL"
            logger.info(f"Result: {status_8_1_6} ({len(inconsistent_scores)} inconsistent scores found)")

            # Summary Stats
            await cur.execute("SELECT COUNT(*) FROM lifecycle_labels")
            total_labels = (await cur.fetchone())[0]
            await cur.execute("SELECT COUNT(*) FROM feature_snapshots")
            total_snapshots = (await cur.fetchone())[0]

            print("\n" + "="*40)
            print("PHASE 8.1 INTEGRITY REPORT")
            print("="*40)
            print(f"8.1.1 Snapshot Uniqueness:     {status_8_1_1}")
            print(f"8.1.2 Snapshot-Label Align:    {status_8_1_2}")
            print(f"8.1.3 No Future Leakage:       {status_8_1_3}")
            print(f"8.1.4 Outcome Completeness:    {status_8_1_4}")
            print(f"8.1.5 Feature Version Purity:  {status_8_1_5}")
            print(f"8.1.6 Score Consistency:       {status_8_1_6}")
            print("-"*40)
            print(f"Total Resolved Tokens:         {total_labels}")
            print(f"Total Snapshots:               {total_snapshots}")
            print("="*40)

    await close_db()

if __name__ == "__main__":
    asyncio.run(run_integrity_checks())
