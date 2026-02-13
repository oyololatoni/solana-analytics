
import os
import sys
import asyncio
import logging

# Add project root
sys.path.insert(0, os.getcwd())

# Load .env.local EXPLICITLY before importing app components
# This fixes the "role does not exist" error when running outside of a full env wrapper
env_path = ".env.local"
if os.path.exists(env_path):
    print(f"Loading {env_path}...")
    with open(env_path, "r") as f:
        for line in f:
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip().strip('"').strip("'")

# Now import app components
try:
    from app.core.db import get_db_connection, init_db
except ImportError:
    # Just in case run from root
    sys.path.append(os.path.join(os.getcwd(), 'app'))
    from app.core.db import get_db_connection, init_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("db_verify")

async def verify_constraints():
    await init_db()
    conn = await get_db_connection()
    try:
        logger.info("🔒 Verifying Stage 0 Strict Constraints...")
        
        async with conn.cursor() as cur:
            # 1. UNIQUE(tokens.address)
            await cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'tokens_address_key'")
            if not await cur.fetchone():
                logger.info("  > Adding UNIQUE(tokens.address)")
                await cur.execute("ALTER TABLE tokens ADD CONSTRAINT tokens_address_key UNIQUE (address)")
            
            # 2. UNIQUE(trades.tx_signature, ...)
            await cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'trades_unique_tx_token_wallet'")
            if not await cur.fetchone():
                try:
                    logger.info("  > Adding UNIQUE(trades...)")
                    await cur.execute("ALTER TABLE trades ADD CONSTRAINT trades_unique_tx_token_wallet UNIQUE (tx_signature, token_id, wallet_address)")
                except Exception as e:
                    logger.warning(f"  ⚠️ Could not add trades unique constraint (dupes exist?): {e}")

            # 3. UNIQUE(token_id, feature_version) on feature_snapshots
            await cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'feature_snapshots_token_version_key'")
            if not await cur.fetchone():
                logger.info("  > Adding UNIQUE(feature_snapshots...)")
                await cur.execute("ALTER TABLE feature_snapshots ADD CONSTRAINT feature_snapshots_token_version_key UNIQUE (token_id, feature_version)")

            # 4. UNIQUE(token_id) on lifecycle_labels
            await cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'lifecycle_labels_token_id_key'")
            if not await cur.fetchone():
                logger.info("  > Adding UNIQUE(lifecycle_labels...)")
                await cur.execute("ALTER TABLE lifecycle_labels ADD CONSTRAINT lifecycle_labels_token_id_key UNIQUE (token_id)")

            # 5. Foreign Keys
            # trades.token_id -> tokens.id
            await cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'trades_token_id_fkey'")
            if not await cur.fetchone():
                logger.info("  > Adding FK trades.token_id")
                await cur.execute("ALTER TABLE trades ADD CONSTRAINT trades_token_id_fkey FOREIGN KEY (token_id) REFERENCES tokens(id) ON DELETE CASCADE")

            # liquidity_events.token_id -> tokens.id
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS liquidity_events (
                    id SERIAL PRIMARY KEY,
                    chain_id INT,
                    token_id INT,
                    tx_signature TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE,
                    amount_usd NUMERIC,
                    liquidity_usd NUMERIC,
                    type TEXT
                )
            """)
            await cur.execute("SELECT 1 FROM pg_constraint WHERE conname = 'liquidity_events_token_id_fkey'")
            if not await cur.fetchone():
                logger.info("  > Adding FK liquidity_events.token_id")
                await cur.execute("ALTER TABLE liquidity_events ADD CONSTRAINT liquidity_events_token_id_fkey FOREIGN KEY (token_id) REFERENCES tokens(id) ON DELETE CASCADE")

            # 6. tokens.detected_at NOT NULL
            # Update nulls first
            await cur.execute("UPDATE tokens SET detected_at = created_at_chain WHERE detected_at IS NULL")
            await cur.execute("UPDATE tokens SET detected_at = NOW() WHERE detected_at IS NULL")
            
            await cur.execute("SELECT is_nullable FROM information_schema.columns WHERE table_name = 'tokens' AND column_name = 'detected_at'")
            row = await cur.fetchone()
            if row and row[0] == 'YES':
                 logger.info("  > Setting tokens.detected_at NOT NULL")
                 await cur.execute("ALTER TABLE tokens ALTER COLUMN detected_at SET NOT NULL")

            # 7. Chain Default (Ensure 'solana' exists)
            await cur.execute("INSERT INTO chains (name) VALUES ('solana') ON CONFLICT (name) DO NOTHING")
            await cur.execute("SELECT id FROM chains WHERE name = 'solana'")
            sol_id = (await cur.fetchone())[0]
            logger.info(f"  > Chain 'solana' confirmed (ID: {sol_id})")

            # 8. ensure tokens.pair_address column exists if strictly required by new logic
            await cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'tokens' AND column_name = 'primary_pair_address'")
            if not await cur.fetchone():
                logger.info("  > Adding tokens.primary_pair_address")
                await cur.execute("ALTER TABLE tokens ADD COLUMN primary_pair_address TEXT")

            logger.info("✅ Constraints Verified.")
            
        await conn.commit()
        
    except Exception as e:
        logger.error(f"❌ DB Verification Failed: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    try:
        asyncio.run(verify_constraints())
    except KeyboardInterrupt:
        pass
