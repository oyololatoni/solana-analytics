-- 009_production_schema.sql
-- Implements the full production-grade schema (4-plane architecture)
-- Chain-agnostic, snapshot-safe, feature-versioned.

-- 1. Core Entity Tables

CREATE TABLE IF NOT EXISTS chains (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,          -- 'solana', 'bsc'
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tokens (
    id BIGSERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains(id),
    address TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    created_at_chain TIMESTAMP,
    detected_at TIMESTAMP,              -- when liquidity threshold satisfied
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(), -- Added for tracking insertion time
    UNIQUE(chain_id, address)
);

CREATE TABLE IF NOT EXISTS wallet_profiles (
    id BIGSERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains(id),
    address TEXT NOT NULL,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    total_tokens_interacted INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(chain_id, address)
);

-- 2. Raw Event Store (Append-Only)

CREATE TABLE IF NOT EXISTS trades (
    id BIGSERIAL PRIMARY KEY,
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
    UNIQUE(chain_id, tx_signature)
);

CREATE TABLE IF NOT EXISTS liquidity_events (
    id BIGSERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains(id),
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    tx_signature TEXT NOT NULL,
    liquidity_usd NUMERIC,
    delta_liquidity_usd NUMERIC,
    slot BIGINT,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(chain_id, tx_signature)
);

CREATE TABLE IF NOT EXISTS wallet_token_interactions (
    id BIGSERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains(id),
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    wallet_id BIGINT NOT NULL REFERENCES wallet_profiles(id),
    first_interaction TIMESTAMP,
    last_interaction TIMESTAMP,
    total_bought_usd NUMERIC DEFAULT 0,
    total_sold_usd NUMERIC DEFAULT 0,
    net_position_usd NUMERIC DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(token_id, wallet_id)
);

-- 3. Rolling Aggregate Layer

CREATE TABLE IF NOT EXISTS token_rolling_metrics (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    window_type TEXT NOT NULL,  -- '5m','30m','1h','6h','24h'
    computed_at TIMESTAMP NOT NULL,
    volume_usd NUMERIC,
    trade_count INTEGER,
    unique_wallets INTEGER,
    buy_volume_usd NUMERIC,
    sell_volume_usd NUMERIC,
    liquidity_avg_usd NUMERIC,
    volatility NUMERIC,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(token_id, window_type, computed_at)
);

-- 4. Feature Snapshot Layer

CREATE TABLE IF NOT EXISTS feature_snapshots (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    feature_version INTEGER NOT NULL,
    detection_timestamp TIMESTAMP NOT NULL,
    
    -- Core Features
    volume_acceleration NUMERIC,
    liquidity_growth_rate NUMERIC,
    holder_growth_rate NUMERIC,
    buy_sell_ratio NUMERIC,
    early_wallet_retention NUMERIC,
    early_wallet_net_accumulation NUMERIC,
    top10_concentration_delta NUMERIC,
    volatility_score NUMERIC,
    
    -- Risk Flags
    liquidity_stability_score NUMERIC,
    
    created_at TIMESTAMP DEFAULT NOW()
);

-- 5. Lifecycle Label Layer

CREATE TABLE IF NOT EXISTS lifecycle_labels (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    snapshot_id BIGINT NOT NULL REFERENCES feature_snapshots(id),
    
    outcome TEXT CHECK (
        outcome IN (
            'hit_5x',
            'price_failure',
            'liquidity_collapse',
            'volume_collapse',
            'early_wallet_exit',
            'expired'
        )
    ),
    
    max_multiplier NUMERIC,
    time_to_outcome INTERVAL,
    labeled_at TIMESTAMP DEFAULT NOW()
);

-- 6. Intelligence Output Layer

CREATE TABLE IF NOT EXISTS token_scores (
    id BIGSERIAL PRIMARY KEY,
    token_id BIGINT NOT NULL REFERENCES tokens(id),
    snapshot_id BIGINT NOT NULL REFERENCES feature_snapshots(id),
    
    score NUMERIC,
    risk_score NUMERIC,
    wallet_quality NUMERIC,
    lifecycle_state TEXT,
    
    computed_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(token_id, snapshot_id)
);

-- 7. Ingestion State

CREATE TABLE IF NOT EXISTS ingestion_state (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains(id),
    last_processed_slot BIGINT,
    last_processed_signature TEXT,
    last_processed_block_time TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(chain_id)
);

-- SEED DATA
INSERT INTO chains (name) VALUES ('solana') ON CONFLICT DO NOTHING;
