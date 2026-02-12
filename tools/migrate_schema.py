#!/usr/bin/env python3
"""
Schema migration runner - safely applies schema migrations to the database.

Usage:
    python tools/migrate_schema.py [--dry-run]
    
Options:
    --dry-run    Show what would be executed without making changes
"""
import os
import sys
import psycopg
from pathlib import Path
from typing import List, Tuple

DATABASE_URL = os.environ.get("DATABASE_URL")
SCHEMA_DIR = Path(__file__).parent.parent / "schema"

def create_migrations_table(cur):
    """Create a table to track applied migrations."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id SERIAL PRIMARY KEY,
            migration_file TEXT NOT NULL UNIQUE,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

def get_applied_migrations(cur) -> List[str]:
    """Get list of already-applied migrations."""
    cur.execute("SELECT migration_file FROM schema_migrations ORDER BY migration_file")
    return [row[0] for row in cur.fetchall()]

def get_pending_migrations(applied: List[str]) -> List[Tuple[str, str]]:
    """Get list of migration files that haven't been applied yet."""
    if not SCHEMA_DIR.exists():
        return []
    
    all_migrations = sorted([
        f for f in SCHEMA_DIR.iterdir() 
        if f.is_file() and f.suffix == ".sql"
    ])
    
    pending = []
    for migration_file in all_migrations:
        filename = migration_file.name
        if filename not in applied:
            with open(migration_file, 'r') as f:
                sql_content = f.read()
            pending.append((filename, sql_content))
    
    return pending

def apply_migration(cur, filename: str, sql: str, dry_run: bool = False):
    """Apply a single migration."""
    print(f"\n📄 {filename}")
    
    if dry_run:
        print("   [DRY RUN] Would execute:")
        for line in sql.split('\n')[:10]:  # Show first 10 lines
            if line.strip():
                print(f"      {line}")
        if len(sql.split('\n')) > 10:
            print("      ...")
        return
    
    print(f"   ▶ Executing migration...")
    
    try:
        # Execute the migration SQL
        cur.execute(sql)
        
        # Record that this migration was applied
        cur.execute(
            "INSERT INTO schema_migrations (migration_file) VALUES (%s)",
            (filename,)
        )
        
        print(f"   ✅ Migration applied successfully")
        
    except Exception as e:
        print(f"   ❌ Migration failed: {e}")
        raise

def main():
    dry_run = "--dry-run" in sys.argv
    
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL environment variable not set")
        return 1
    
    print("🔄 Schema Migration Runner")
    print(f"📍 Database: {DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else 'local'}")
    if dry_run:
        print("🔍 Mode: DRY RUN (no changes will be made)\n")
    else:
        print("⚠️  Mode: LIVE (changes will be committed)\n")
    
    try:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=10)
        cur = conn.cursor()
        
        # Ensure migrations tracking table exists
        create_migrations_table(cur)
        conn.commit()
        
        # Get migration status
        applied = get_applied_migrations(cur)
        pending = get_pending_migrations(applied)
        
        if applied:
            print(f"✅ Already applied: {len(applied)} migrations")
            for m in applied:
                print(f"   • {m}")
        
        if not pending:
            print("\n✨ No pending migrations. Schema is up to date!")
            cur.close()
            conn.close()
            return 0
        
        print(f"\n📋 Pending migrations: {len(pending)}")
        
        # Apply each pending migration
        for filename, sql in pending:
            apply_migration(cur, filename, sql, dry_run)
        
        if not dry_run:
            conn.commit()
            print("\n✅ All migrations applied successfully!")
        else:
            conn.rollback()
            print("\n✅ Dry run complete. Run without --dry-run to apply changes.")
        
        cur.close()
        conn.close()
        return 0
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        if 'conn' in locals():
            conn.rollback()
        return 1

if __name__ == "__main__":
    sys.exit(main())
