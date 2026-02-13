import os
import sys
import psycopg
from pathlib import Path

# Fix path to import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from app.core.config import DATABASE_URL

SCHEMA_DIR = Path(__file__).parent.parent / "schema"

def migrate():
    if not DATABASE_URL:
        print("DATABASE_URL not set.")
        sys.exit(1)

    print(f"Connecting to DB...")
    try:
        conn = psycopg.connect(DATABASE_URL, autocommit=True)
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    cur = conn.cursor()

    # Ensure table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id SERIAL PRIMARY KEY,
            migration_file TEXT NOT NULL UNIQUE,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # Get applied
    cur.execute("SELECT migration_file FROM schema_migrations")
    applied = {row[0] for row in cur.fetchall()}

    # Get pending
    files = sorted(SCHEMA_DIR.glob("*.sql"))
    pending = [f for f in files if f.name not in applied]

    if not pending:
        print("Schema up to date.")
        return

    print(f"Applying {len(pending)} migrations...")
    for f in pending:
        print(f"Applying {f.name}...")
        try:
            cur.execute(f.read_text())
            cur.execute("INSERT INTO schema_migrations (migration_file) VALUES (%s)", (f.name,))
            print("Success.")
        except Exception as e:
            print(f"Failed to apply {f.name}: {e}")
            sys.exit(1)

    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    migrate()
