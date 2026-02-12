-- 012_optimization.sql
-- Optimizes trades and liquidity_events for Neon Free Tier
-- 1. Partitioning by TIMESTAMP (Monthly)
-- 2. Strategic Indexing

-- A. TRADES MIGRATION
ALTER TABLE trades RENAME TO trades_old;

CREATE TABLE trades (
    id BIGSERIAL, -- logical ID, not primary key across partitions easily? 
                  -- Actually, for partitioning, PK must include partition key.
                  -- We will rely on (chain_id, tx_signature, timestamp) as unique.
                  -- We can keep 'id' but it won't be global PK constraint enforced strictly unless we include timestamp.
                  -- Let's just use the same schema columns.
    chain_id INTEGER NOT NULL REFERENCES chains(id),
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    tx_signature TEXT NOT NULL,
    wallet_address TEXT NOT NULL,
    side TEXT CHECK (side IN ('buy','sell')),
    amount_token NUMERIC,
    amount_usd NUMERIC,
    price_usd NUMERIC,
    liquidity_usd NUMERIC,
    slot BIGINT,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    amount_sol NUMERIC,
    
    -- Constraint must include timestamp for partitioning
    UNIQUE(chain_id, tx_signature, timestamp)
) PARTITION BY RANGE (timestamp);

-- Partitions
CREATE TABLE trades_default PARTITION OF trades DEFAULT;

CREATE TABLE trades_y2026m02 PARTITION OF trades
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE trades_y2026m03 PARTITION OF trades
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

-- Migrate Data
INSERT INTO trades (
    id, chain_id, token_id, tx_signature, wallet_address, side, 
    amount_token, amount_usd, price_usd, liquidity_usd, slot, 
    timestamp, created_at, amount_sol
)
SELECT 
    id, chain_id, token_id, tx_signature, wallet_address, side, 
    amount_token, amount_usd, price_usd, liquidity_usd, slot, 
    timestamp, created_at, amount_sol
FROM trades_old;

-- Indexes for TRADES
CREATE INDEX idx_trades_token_timestamp ON trades (token_id, timestamp DESC);
CREATE INDEX idx_trades_timestamp ON trades (timestamp DESC);
-- idx_trades_chain_signature is covered by UNIQUE constraint (mostly), 
-- but UNIQUE is (chain, sig, ts). 
-- If we look up by sig alone, we might need index?
-- Idempotency check usually has timestamp available from payload? Yes.
-- If not, we might need (chain, sig). 
-- But let's stick to user request: "CREATE UNIQUE INDEX idx_trades_chain_signature ON trades (chain_id, tx_signature);"
-- WAIT. User requested `idx_trades_chain_signature`.
-- We CANNOT have unique index on (chain, sig) if partition key (timestamp) is not in it.
-- Postgres limitation.
-- So we MUST use (chain, sig, ts).
-- The User's prompt said: "CREATE UNIQUE INDEX idx_trades_chain_signature ON trades (chain_id, tx_signature);"
-- But also said "Partition these two tables... Partition key: timestamp".
-- These are conflicting requirements in Postgres.
-- I notified user I would modify constraint to include timestamp.
-- So I will create index on (chain, signature, timestamp).


-- B. LIQUIDITY EVENTS MIGRATION
ALTER TABLE liquidity_events RENAME TO liquidity_events_old;

CREATE TABLE liquidity_events (
    id BIGSERIAL,
    chain_id INTEGER NOT NULL REFERENCES chains(id),
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    tx_signature TEXT NOT NULL,
    liquidity_usd NUMERIC,
    delta_liquidity_usd NUMERIC,
    slot BIGINT,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(chain_id, tx_signature, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE TABLE liquidity_events_default PARTITION OF liquidity_events DEFAULT;
CREATE TABLE liquidity_events_y2026m02 PARTITION OF liquidity_events FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE liquidity_events_y2026m03 PARTITION OF liquidity_events FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

INSERT INTO liquidity_events (
    id, chain_id, token_id, tx_signature, liquidity_usd, 
    delta_liquidity_usd, slot, timestamp, created_at
)
SELECT 
    id, chain_id, token_id, tx_signature, liquidity_usd, 
    delta_liquidity_usd, slot, timestamp, created_at
FROM liquidity_events_old;

CREATE INDEX idx_liquidity_token_timestamp ON liquidity_events (token_id, timestamp DESC);


-- C. OTHER INDEXES

-- Wallet Token Interactions
CREATE INDEX IF NOT EXISTS idx_wallet_token ON wallet_token_interactions (token_id);
CREATE INDEX IF NOT EXISTS idx_wallet_token_net_position ON wallet_token_interactions (token_id, net_position_usd DESC);

-- Rolling Metrics
CREATE INDEX IF NOT EXISTS idx_metrics_token_window_time ON token_rolling_metrics (token_id, window_type, computed_at DESC);

-- Snapshots
CREATE INDEX IF NOT EXISTS idx_snapshots_token ON feature_snapshots (token_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_detection_time ON feature_snapshots (detection_timestamp DESC);

-- Labels
CREATE INDEX IF NOT EXISTS idx_labels_snapshot ON lifecycle_labels (snapshot_id);
CREATE INDEX IF NOT EXISTS idx_labels_outcome ON lifecycle_labels (outcome);

-- Scores
CREATE INDEX IF NOT EXISTS idx_scores_token ON token_scores (token_id);
CREATE INDEX IF NOT EXISTS idx_scores_computed_time ON token_scores (computed_at DESC);

-- Tokens
CREATE INDEX IF NOT EXISTS idx_tokens_active ON tokens (is_active);
CREATE INDEX IF NOT EXISTS idx_tokens_detection_time ON tokens (detected_at DESC);

-- Fix sequence sync if needed (since we copied IDs)
-- SELECT setval('trades_id_seq', (SELECT MAX(id) FROM trades));
-- Actually, the new table created a NEW sequence. We inserted IDs from old.
-- We should update the sequence to max(id).
DO $$
BEGIN
    PERFORM setval(pg_get_serial_sequence('trades', 'id'), COALESCE((SELECT MAX(id) FROM trades), 1), false);
    PERFORM setval(pg_get_serial_sequence('liquidity_events', 'id'), COALESCE((SELECT MAX(id) FROM liquidity_events), 1), false);
END $$;
