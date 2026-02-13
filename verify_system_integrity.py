# System Integrity Verification Script
"""
Enforces architectural invariants and detects violations.
Run daily to ensure system discipline.
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from app.core.db import get_db_connection, init_db, close_db

async def check_snapshot_immutability():
    """Verify no duplicate snapshots exist (violation of immutability)"""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT token_id, feature_version, snapshot_time, COUNT(*)
                FROM feature_snapshots
                GROUP BY token_id, feature_version, snapshot_time
                HAVING COUNT(*) > 1
            """)
            duplicates = await cur.fetchall()
            if duplicates:
                print(f"❌ FATAL: Found {len(duplicates)} duplicate snapshots (immutability violated)")
                for dup in duplicates:
                    print(f"   Token {dup[0]}, version {dup[1]}, timestamp {dup[2]}: {dup[3]} copies")
                return False
            print("✅ No duplicate snapshots")
            return True

async def check_eligible_have_detected_at():
    """Verify all ELIGIBLE tokens have detected_at timestamp"""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT id, address
                FROM tokens
                WHERE eligibility_status = 'ELIGIBLE'
                AND detected_at IS NULL
            """)
            missing = await cur.fetchall()
            if missing:
                print(f"❌ WARNING: {len(missing)} ELIGIBLE tokens missing detected_at")
                for m in missing[:5]:
                    print(f"   Token {m[0]}: {m[1]}")
                return False
            print("✅ All ELIGIBLE tokens have detected_at")
            return True

async def check_active_tokens_not_expired():
    """Verify no active tokens are past 72h without trades"""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=72)
            await cur.execute("""
                SELECT t.id, t.address, t.created_at
                FROM tokens t
                WHERE t.is_active = TRUE
                AND t.created_at < %s
                AND NOT EXISTS (
                    SELECT 1 FROM trades tr
                    WHERE tr.token_id = t.id
                    AND tr.timestamp > %s
                )
            """, (cutoff, cutoff))
            expired = await cur.fetchall()
            if expired:
                print(f"❌ WARNING: {len(expired)} tokens marked active but 72h+ expired")
                for e in expired[:5]:
                    print(f"   Token {e[0]}: {e[1]} (created {e[2]})")
                return False
            print("✅ No active tokens past 72h expiry")
            return True

async def check_snapshots_have_scores():
    """Verify all snapshots have corresponding score rows"""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT fs.id, fs.token_id
                FROM feature_snapshots fs
                WHERE fs.score_total IS NULL
                OR fs.score_label IS NULL
            """)
            missing = await cur.fetchall()
            if missing:
                print(f"❌ FATAL: {len(missing)} snapshots missing scores (immutability violated)")
                for m in missing[:5]:
                    print(f"   Snapshot {m[0]} (token {m[1]})")
                return False
            print("✅ All snapshots have scores")
            return True

async def check_resolved_tokens_have_labels():
    """Verify all resolved lifecycle tokens have labels"""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT t.id, t.address, t.outcome
                FROM tokens t
                WHERE t.outcome IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM lifecycle_labels ll
                    JOIN feature_snapshots fs ON fs.id = ll.snapshot_id
                    WHERE fs.token_id = t.id
                )
            """)
            missing = await cur.fetchall()
            if missing:
                print(f"❌ WARNING: {len(missing)} resolved tokens missing labels")
                for m in missing[:5]:
                    print(f"   Token {m[0]}: {m[1]} (outcome: {m[2]})")
                return False
            print("✅ All resolved tokens have labels")
            return True

async def check_primary_pair_enforcement():
    """Verify primary_pair_address is set for all pair_validated tokens"""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT id, address
                FROM tokens
                WHERE pair_validated = TRUE
                AND primary_pair_address IS NULL
            """)
            missing = await cur.fetchall()
            if missing:
                print(f"❌ FATAL: {len(missing)} validated tokens missing primary_pair_address")
                for m in missing[:5]:
                    print(f"   Token {m[0]}: {m[1]}")
                return False
            print("✅ All validated tokens have primary_pair_address")
            return True

async def check_ml_feature_flag():
    """Verify ML is disabled if dataset < 300 labels"""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM lifecycle_labels")
            row = await cur.fetchone()
            if not row:
                print("⚠️  No lifecycle_labels table or empty")
                return True
            label_count = row[0]
            
            # Check if ML_ENABLED flag is set correctly in features_v2.py
            feature_file = '/home/dlip-24/solana-analytics/app/engines/v2/features.py'
            try:
                with open(feature_file, 'r') as f:
                    content = f.read()
                    if 'ML_ENABLED = True' in content and label_count < 300:
                        print(f"❌ FATAL: ML enabled with only {label_count}/300 labels (statistical invalidity)")
                        return False
                    elif 'ML_ENABLED = False' in content:
                        print(f"✅ ML correctly disabled ({label_count}/300 labels)")
                        return True
                    elif label_count >= 300:
                        print(f"⚠️  Dataset mature ({label_count}/300 labels), consider enabling ML")
                        return True
            except FileNotFoundError:
                print(f"⚠️  Could not find {feature_file}")
                return True
            return True

async def main():
    print("=" * 60)
    print("SOLANA ANALYTICS - SYSTEM INTEGRITY VERIFICATION")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    print()
    
    await init_db()
    
    try:
        checks = [
            ("Snapshot Immutability", check_snapshot_immutability()),
            ("ELIGIBLE Tokens Have detected_at", check_eligible_have_detected_at()),
            ("Active Tokens Not Expired", check_active_tokens_not_expired()),
            ("Snapshots Have Scores", check_snapshots_have_scores()),
            ("Resolved Tokens Have Labels", check_resolved_tokens_have_labels()),
            ("Primary Pair Enforcement", check_primary_pair_enforcement()),
            ("ML Feature Flag", check_ml_feature_flag()),
        ]
        
        results = []
        for name, coro in checks:
            print(f"\n[{name}]")
            result = await coro
            results.append((name, result))
            print()
        
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)
        passed = sum(1 for _, r in results if r)
        total = len(results)
        
        for name, result in results:
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status}: {name}")
        
        print()
        print(f"Result: {passed}/{total} checks passed")
        
        if passed < total:
            print("\n⚠️  SYSTEM INTEGRITY COMPROMISED - INVESTIGATE FAILURES")
            sys.exit(1)
        else:
            print("\n✅ SYSTEM INTEGRITY VERIFIED")
            sys.exit(0)
    
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(main())
