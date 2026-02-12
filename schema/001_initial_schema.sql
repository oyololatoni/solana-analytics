-- Initial Schema (captured state, February 2026)
-- This documents the existing database structure before hardening

-- ======================
-- EVENTS TABLE
-- ======================
-- Stores all swap transactions for tracked tokens
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    tx_signature TEXT NOT NULL,
    slot BIGINT NOT NULL,
    block_time TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    program_id TEXT,
    wallet TEXT NOT NULL,
    counterparty TEXT,
    token_mint TEXT NOT NULL,
    amount NUMERIC,
    raw_amount BIGINT,
    decimals INTEGER,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Composite unique constraint to prevent duplicate swap legs
-- Allows multiple legs per transaction (multi-token swaps)
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_unique 
    ON events(tx_signature, event_type, wallet);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_events_token_mint ON events(token_mint);
CREATE INDEX IF NOT EXISTS idx_events_block_time ON events(block_time);
CREATE INDEX IF NOT EXISTS idx_events_wallet ON events(wallet);

-- ======================
-- WEBHOOK_REPLAYS TABLE
-- ======================
-- Prevents duplicate webhook processing
CREATE TABLE IF NOT EXISTS webhook_replays (
    id SERIAL PRIMARY KEY,
    payload_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_replays_hash ON webhook_replays(payload_hash);

-- ======================
-- INGESTION_STATS TABLE
-- ======================
-- Tracks ingestion metrics over time
CREATE TABLE IF NOT EXISTS ingestion_stats (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    events_received INTEGER DEFAULT 0,
    swaps_inserted INTEGER DEFAULT 0,
    swaps_ignored INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_stats_source ON ingestion_stats(source);
CREATE INDEX IF NOT EXISTS idx_ingestion_stats_created_at ON ingestion_stats(created_at);

-- ======================
-- COMMENTS (DOCUMENTATION)
-- ======================

COMMENT ON TABLE events IS 'All swap events for tracked tokens';
COMMENT ON COLUMN events.tx_signature IS 'Solana transaction signature (unique identifier)';
COMMENT ON COLUMN events.event_type IS 'Event type, typically "swap"';
COMMENT ON COLUMN events.wallet IS 'User wallet that performed the swap';
COMMENT ON COLUMN events.token_mint IS 'Token contract address being tracked';
COMMENT ON COLUMN events.amount IS 'Human-readable token amount (with decimals applied)';
COMMENT ON COLUMN events.raw_amount IS 'Raw token amount (before decimal adjustment)';
COMMENT ON COLUMN events.metadata IS 'Full transaction payload from Helius';

COMMENT ON TABLE webhook_replays IS 'SHA-256 hashes of processed webhook payloads to prevent replays';
COMMENT ON TABLE ingestion_stats IS 'Hourly/daily ingestion metrics for monitoring';
