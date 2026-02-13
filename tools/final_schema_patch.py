
import os
import sys
import logging
import psycopg

# Load .env.local EXPLICITLY
env_path = ".env.local"
if os.path.exists(env_path):
    print(f"Loading {env_path}...")
    with open(env_path, "r") as f:
        for line in f:
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip().strip('"').strip("'")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL not set")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("final_schema_patch")

def apply_patch():
    print(f"Connecting to DB...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
            cur = conn.cursor()
            
            logger.info("🔒 Applying FINAL Schema Specification...")
            
            # 1.1 Ingestion Truncated
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'tokens' AND column_name = 'ingestion_truncated'")
            if not cur.fetchone():
                logger.info("  > Adding tokens.ingestion_truncated")
                cur.execute("ALTER TABLE tokens ADD COLUMN ingestion_truncated BOOLEAN DEFAULT FALSE")

            # 5 Pair Validated
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'tokens' AND column_name = 'pair_validated'")
            if not cur.fetchone():
                logger.info("  > Adding tokens.pair_validated")
                cur.execute("ALTER TABLE tokens ADD COLUMN pair_validated BOOLEAN DEFAULT FALSE")

            # 11 Discovery Class
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'tokens' AND column_name = 'discovery_class'")
            if not cur.fetchone():
                logger.info("  > Adding tokens.discovery_class")
                cur.execute("ALTER TABLE tokens ADD COLUMN discovery_class TEXT")

            # 9 Constraints
            # Snapshot Unique
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'unique_snapshot'")
            if not cur.fetchone():
                # Drop old if exists to rename/ensure correct
                cur.execute("ALTER TABLE feature_snapshots DROP CONSTRAINT IF EXISTS feature_snapshots_token_version_key") 
                logger.info("  > Adding unique_snapshot constraint")
                cur.execute("ALTER TABLE feature_snapshots ADD CONSTRAINT unique_snapshot UNIQUE(token_id, feature_version)")

            # Label Unique
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'unique_label'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE lifecycle_labels DROP CONSTRAINT IF EXISTS lifecycle_labels_token_id_key")
                logger.info("  > Adding unique_label constraint")
                cur.execute("ALTER TABLE lifecycle_labels ADD CONSTRAINT unique_label UNIQUE(token_id)")

            # Trades FK
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'trades_token_id_fkey'")
            if not cur.fetchone():
                logger.info("  > Adding trades FK")
                cur.execute("ALTER TABLE trades ADD CONSTRAINT trades_token_id_fkey FOREIGN KEY (token_id) REFERENCES tokens(id)")

            logger.info("✅ Final Schema Patch Applied.")
        
    except Exception as e:
        logger.error(f"❌ Schema Patch Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    apply_patch()
