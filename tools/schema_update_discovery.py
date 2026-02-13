
import psycopg
import os
import sys

# Load env
if not os.environ.get("DATABASE_URL"):
    env_path = ".env.local"
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")

DB_DSN = os.getenv("DATABASE_URL")
if not DB_DSN:
    print("DATABASE_URL missing")
    sys.exit(1)

def run_migration():
    print(f"Connecting to DB...")
    try:
        conn = psycopg.connect(DB_DSN)
        cur = conn.cursor()
        
        # 1. discovery_class
        print("Checking discovery_class...")
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='tokens' AND column_name='discovery_class'")
        if not cur.fetchone():
            print("Adding discovery_class column...")
            cur.execute("ALTER TABLE tokens ADD COLUMN discovery_class VARCHAR(50);")
        else:
            print("discovery_class exists.")

        # 2. base_token_symbol
        print("Checking base_token_symbol...")
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='tokens' AND column_name='base_token_symbol'")
        if not cur.fetchone():
            print("Adding base_token_symbol column...")
            cur.execute("ALTER TABLE tokens ADD COLUMN base_token_symbol VARCHAR(20);")
        else:
             print("base_token_symbol exists.")

        # 3. quote_token_symbol
        print("Checking quote_token_symbol...")
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='tokens' AND column_name='quote_token_symbol'")
        if not cur.fetchone():
            print("Adding quote_token_symbol column...")
            cur.execute("ALTER TABLE tokens ADD COLUMN quote_token_symbol VARCHAR(20);")
        else:
             print("quote_token_symbol exists.")
             
        conn.commit()
        print("Migration Complete.")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    run_migration()
