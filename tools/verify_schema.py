#!/usr/bin/env python3
"""
Schema verification tool - validates that the database matches expected state.

Usage:
    python tools/verify_schema.py
    
Returns exit code 0 if schema is valid, 1 otherwise.
"""
import os
import sys
import psycopg
from typing import List, Dict, Any

DATABASE_URL = os.environ.get("DATABASE_URL")

# Expected schema state after all migrations
EXPECTED_TABLES = {
    "events": {
        "columns": [
            "id", "tx_signature", "slot", "block_time", "event_type",
            "program_id", "wallet", "counterparty", "token_mint",
            "amount", "raw_amount", "decimals", "metadata", "created_at"
        ],
        "indexes": [
            "idx_events_unique",
            "idx_events_token_mint",
            "idx_events_block_time",
            "idx_events_wallet"
        ]
    },
    "webhook_replays": {
        "columns": ["id", "payload_hash", "created_at"],
        "indexes": ["idx_webhook_replays_hash"]
    },
    "ingestion_stats": {
        "columns": [
            "id", "source", "events_received", "swaps_inserted",
            "swaps_ignored", "created_at",
            # Migration 002 columns
            "ignored_missing_fields", "ignored_no_swap_event",
            "ignored_no_tracked_tokens", "ignored_constraint_violation",
            "ignored_exception"
        ],
        "indexes": [
            "idx_ingestion_stats_source",
            "idx_ingestion_stats_created_at"
        ]
    }
}

def get_table_columns(cur, table_name: str) -> List[str]:
    """Fetch all column names for a given table."""
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    return [row[0] for row in cur.fetchall()]

def get_table_indexes(cur, table_name: str) -> List[str]:
    """Fetch all index names for a given table."""
    cur.execute("""
        SELECT indexname 
        FROM pg_indexes 
        WHERE tablename = %s
    """, (table_name,))
    return [row[0] for row in cur.fetchall()]

def verify_table(cur, table_name: str, expected: Dict[str, Any]) -> List[str]:
    """Verify a single table's structure. Returns list of errors."""
    errors = []
    
    # Check table exists
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        )
    """, (table_name,))
    
    if not cur.fetchone()[0]:
        return [f"Table '{table_name}' does not exist"]
    
    # Check columns
    actual_columns = set(get_table_columns(cur, table_name))
    expected_columns = set(expected["columns"])
    
    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns
    
    if missing_columns:
        errors.append(f"Table '{table_name}' missing columns: {', '.join(missing_columns)}")
    
    if extra_columns:
        errors.append(f"Table '{table_name}' has unexpected columns: {', '.join(extra_columns)}")
    
    # Check indexes
    actual_indexes = set(get_table_indexes(cur, table_name))
    expected_indexes = set(expected["indexes"])
    
    missing_indexes = expected_indexes - actual_indexes
    
    if missing_indexes:
        errors.append(f"Table '{table_name}' missing indexes: {', '.join(missing_indexes)}")
    
    return errors

def verify_composite_constraint(cur) -> List[str]:
    """Verify the events table has the correct composite unique constraint."""
    errors = []
    
    cur.execute("""
        SELECT i.relname as index_name, 
               array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE t.relname = 'events' 
          AND i.relname = 'idx_events_unique'
          AND ix.indisunique = true
        GROUP BY i.relname
    """)
    
    result = cur.fetchone()
    if not result:
        errors.append("Missing unique constraint 'idx_events_unique' on events table")
    else:
        index_name, columns = result
        expected_columns = ["tx_signature", "event_type", "wallet"]
        if sorted(columns) != sorted(expected_columns):
            errors.append(
                f"Unique constraint '{index_name}' has wrong columns. "
                f"Expected: {expected_columns}, Got: {columns}"
            )
    
    return errors

def main():
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL environment variable not set")
        return 1
    
    print("🔍 Verifying database schema...")
    print(f"📍 Database: {DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else 'local'}\n")
    
    try:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=10)
        cur = conn.cursor()
        
        all_errors = []
        
        # Verify each table
        for table_name, expected in EXPECTED_TABLES.items():
            table_errors = verify_table(cur, table_name, expected)
            all_errors.extend(table_errors)
        
        # Verify composite constraint
        constraint_errors = verify_composite_constraint(cur)
        all_errors.extend(constraint_errors)
        
        cur.close()
        conn.close()
        
        if all_errors:
            print("❌ SCHEMA VERIFICATION FAILED\n")
            for error in all_errors:
                print(f"  • {error}")
            print()
            return 1
        else:
            print("✅ Schema verification passed!")
            print("   All tables, columns, and indexes match expected state.\n")
            return 0
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
