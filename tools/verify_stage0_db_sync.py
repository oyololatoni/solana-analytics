
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
logger = logging.getLogger("db_verify_sync")

def verify_constraints_sync():
    print(f"Connecting to DB...")
    try:
        # Connect using psycopg 3
        with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
            cur = conn.cursor()
            
            logger.info("🔒 Verifying Stage 0 Strict Constraints (SYNC)...")
            
            # 1. UNIQUE(tokens.address)
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'tokens_address_key'")
            if not cur.fetchone():
                logger.info("  > Adding UNIQUE(tokens.address)")
                cur.execute("ALTER TABLE tokens ADD CONSTRAINT tokens_address_key UNIQUE (address)")
            
            # 2. UNIQUE(trades.tx_signature, ...)
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'trades_unique_tx_token_wallet'")
            if not cur.fetchone():
                try:
                    logger.info("  > Adding UNIQUE(trades...)")
                    cur.execute("ALTER TABLE trades ADD CONSTRAINT trades_unique_tx_token_wallet UNIQUE (tx_signature, token_id, wallet_address)")
                except Exception as e:
                    logger.warning(f"  ⚠️ Could not add trades unique constraint: {e}")

            # 3. UNIQUE(token_id, feature_version)
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'feature_snapshots_token_version_key'")
            if not cur.fetchone():
                logger.info("  > Adding UNIQUE(feature_snapshots...)")
                cur.execute("ALTER TABLE feature_snapshots ADD CONSTRAINT feature_snapshots_token_version_key UNIQUE (token_id, feature_version)")

            # 4. UNIQUE(token_id) on lifecycle_labels
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'lifecycle_labels_token_id_key'")
            if not cur.fetchone():
                logger.info("  > Adding UNIQUE(lifecycle_labels...)")
                cur.execute("ALTER TABLE lifecycle_labels ADD CONSTRAINT lifecycle_labels_token_id_key UNIQUE (token_id)")

            # 5. Foreign Keys
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'trades_token_id_fkey'")
            if not cur.fetchone():
                logger.info("  > Adding FK trades.token_id")
                cur.execute("ALTER TABLE trades ADD CONSTRAINT trades_token_id_fkey FOREIGN KEY (token_id) REFERENCES tokens(id) ON DELETE CASCADE")

            # liquidity_events table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS liquidity_events (
                    id SERIAL PRIMARY KEY,
                    chain_id INT,
                    token_id INT,
                    tx_signature TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE,
                    amount_usd NUMERIC,
                    liquidity_usd NUMERIC,
                    type TEXT,
                    token_id_fk INT REFERENCES tokens(id) ON DELETE CASCADE
                )
            """)
            # Fix: The CREATE TABLE above has the FK inline. 
            # But if table exists, check constraint.
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'liquidity_events_token_id_fkey'")
            if not cur.fetchone():
                # Check if we can add it (if column exists)
                # It assumes token_id column exists.
                logger.info("  > Adding FK liquidity_events.token_id")
                try:
                    cur.execute("ALTER TABLE liquidity_events ADD CONSTRAINT liquidity_events_token_id_fkey FOREIGN KEY (token_id) REFERENCES tokens(id) ON DELETE CASCADE")
                except Exception as e:
                    logger.warning(f"  ⚠️ Could not add liquidity FK: {e}")

            # 6. tokens.detected_at NOT NULL
            cur.execute("UPDATE tokens SET detected_at = created_at_chain WHERE detected_at IS NULL")
            cur.execute("UPDATE tokens SET detected_at = NOW() WHERE detected_at IS NULL")
            
            cur.execute("SELECT is_nullable FROM information_schema.columns WHERE table_name = 'tokens' AND column_name = 'detected_at'")
            row = cur.fetchone()
            if row and row[0] == 'YES':
                 logger.info("  > Setting tokens.detected_at NOT NULL")
                 cur.execute("ALTER TABLE tokens ALTER COLUMN detected_at SET NOT NULL")

            # 7. Chain Default
            cur.execute("INSERT INTO chains (name) VALUES ('solana') ON CONFLICT (name) DO NOTHING")
            cur.execute("SELECT id FROM chains WHERE name = 'solana'")
            sol_id = cur.fetchone()[0]
            logger.info(f"  > Chain 'solana' confirmed (ID: {sol_id})")

            # 8. Pair Address
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'tokens' AND column_name = 'primary_pair_address'")
            if not cur.fetchone():
                logger.info("  > Adding tokens.primary_pair_address")
                cur.execute("ALTER TABLE tokens ADD COLUMN primary_pair_address TEXT")

            logger.info("✅ Constraints Verified (SYNC).")
        
    except Exception as e:
        logger.error(f"❌ DB Verification Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    verify_constraints_sync()
