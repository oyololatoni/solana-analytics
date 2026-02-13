
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
logger = logging.getLogger("schema_patch_v2")

def apply_patch():
    print(f"Connecting to DB...")
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        cur = conn.cursor()
        
        logger.info("🔒 Applying Risk Remediation Schema Patch...")

        # 1. Trade Unique Constraint (Risk 9)
        # UNIQUE(token_id, tx_signature)
        # Check for duplicates first? 
        # For safety, we try to create index/constraint.
        try:
            logger.info("  > Adding unique_trade_token constraint...")
            cur.execute("""
                ALTER TABLE trades 
                ADD CONSTRAINT unique_trade_token UNIQUE (token_id, tx_signature);
            """)
        except psycopg.errors.UniqueViolation:
            logger.warning("  ⚠️ Could not add unique constraint due to duplicates. Skipping (User warning needed).")
        except psycopg.errors.DuplicateObject:
            logger.info("  ✅ Constraint already exists.")
        except Exception as e:
            logger.warning(f"  ⚠️ Error adding constraint: {e}")

        # 2. Immutability Triggers (Global Risk 1 & 2)
        # Function to prevent updates
        cur.execute("""
            CREATE OR REPLACE FUNCTION prevent_update()
            RETURNS TRIGGER AS $$
            BEGIN
                RAISE EXCEPTION 'Table % is immutable! UPDATE prohibited.', TG_TABLE_NAME;
            END;
            $$ LANGUAGE plpgsql;
        """)

        # Trigger for feature_snapshots
        try:
            cur.execute("DROP TRIGGER IF EXISTS trg_immutable_snapshots ON feature_snapshots")
            cur.execute("""
                CREATE TRIGGER trg_immutable_snapshots
                BEFORE UPDATE ON feature_snapshots
                FOR EACH ROW EXECUTE FUNCTION prevent_update();
            """)
            logger.info("  ✅ Immutability enabled for feature_snapshots.")
        except Exception as e:
            logger.error(f"  ❌ Failed trigger feature_snapshots: {e}")

        # Trigger for lifecycle_labels
        try:
            cur.execute("DROP TRIGGER IF EXISTS trg_immutable_labels ON lifecycle_labels")
            cur.execute("""
                CREATE TRIGGER trg_immutable_labels
                BEFORE UPDATE ON lifecycle_labels
                FOR EACH ROW EXECUTE FUNCTION prevent_update();
            """)
            logger.info("  ✅ Immutability enabled for lifecycle_labels.")
        except Exception as e:
            logger.error(f"  ❌ Failed trigger lifecycle_labels: {e}")
            
        logger.info("✅ Schema Patch V2 Applied.")

if __name__ == "__main__":
    apply_patch()
