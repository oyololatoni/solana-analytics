-- ========================================
-- 008: Phase Engine Tables
-- ========================================
-- Aggregation, live state, and audit trail 
-- for the production phase classification engine.

-- 1. Hourly aggregated metrics per token
CREATE TABLE IF NOT EXISTS token_metrics_hourly (
    id SERIAL PRIMARY KEY,
    token_mint TEXT NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    swap_count INTEGER DEFAULT 0,
    buy_count INTEGER DEFAULT 0,
    sell_count INTEGER DEFAULT 0,
    volume NUMERIC DEFAULT 0,
    unique_wallets INTEGER DEFAULT 0,
    net_flow NUMERIC DEFAULT 0,
    buy_sell_ratio NUMERIC DEFAULT 0,
    avg_trade_size NUMERIC DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tmh_mint_bucket
    ON token_metrics_hourly(token_mint, bucket_start);
CREATE INDEX IF NOT EXISTS idx_tmh_bucket
    ON token_metrics_hourly(bucket_start);

-- 2. Live token state (what the dashboard reads)
CREATE TABLE IF NOT EXISTS token_state (
    token_mint TEXT PRIMARY KEY,
    current_phase TEXT NOT NULL DEFAULT 'DORMANT',
    phase_confidence NUMERIC DEFAULT 0,
    ev_score NUMERIC DEFAULT 0,
    structural_score NUMERIC DEFAULT 0,
    capital_score NUMERIC DEFAULT 0,
    lifecycle_score NUMERIC DEFAULT 0,
    unique_makers INTEGER DEFAULT 0,
    swap_count INTEGER DEFAULT 0,
    volume NUMERIC DEFAULT 0,
    unique_growth NUMERIC DEFAULT 0,
    volume_growth NUMERIC DEFAULT 0,
    vpu NUMERIC DEFAULT 0,
    usr NUMERIC DEFAULT 0,
    vpu_cv NUMERIC DEFAULT 0,
    decline_from_peak NUMERIC DEFAULT 0,
    days_since_peak INTEGER DEFAULT 0,
    decision_bias TEXT DEFAULT 'WAIT',
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Score history for backtesting
CREATE TABLE IF NOT EXISTS token_scores_history (
    id SERIAL PRIMARY KEY,
    token_mint TEXT NOT NULL,
    recorded_at TIMESTAMPTZ DEFAULT NOW(),
    phase TEXT NOT NULL,
    ev_score NUMERIC DEFAULT 0,
    structural_score NUMERIC DEFAULT 0,
    capital_score NUMERIC DEFAULT 0,
    lifecycle_score NUMERIC DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tsh_mint_time
    ON token_scores_history(token_mint, recorded_at);
