-- 013_feature_snapshot_v1.sql
-- Implements Feature Snapshot v1 Schema (Additional columns)

ALTER TABLE feature_snapshots
    ADD COLUMN IF NOT EXISTS volume_growth_rate_1h NUMERIC,
    ADD COLUMN IF NOT EXISTS trade_frequency_ratio NUMERIC,
    ADD COLUMN IF NOT EXISTS liquidity_volatility NUMERIC,
    -- liquidity_stability_score already exists
    ADD COLUMN IF NOT EXISTS unique_wallet_growth_rate NUMERIC,
    ADD COLUMN IF NOT EXISTS holder_concentration_top10 NUMERIC,
    ADD COLUMN IF NOT EXISTS wallet_entropy_score NUMERIC,
    -- volatility_score already exists (maps to price_volatility_1h)
    ADD COLUMN IF NOT EXISTS drawdown_depth_1h NUMERIC,
    ADD COLUMN IF NOT EXISTS volume_collapse_ratio NUMERIC,
    ADD COLUMN IF NOT EXISTS lifecycle_state TEXT;
