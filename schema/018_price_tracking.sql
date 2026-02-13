-- 018_price_tracking.sql
-- Step: Add price tracking columns to tokens table
-- Enables: baseline price, current price, peak price, multiplier calculation

ALTER TABLE tokens
ADD COLUMN IF NOT EXISTS baseline_price NUMERIC,
ADD COLUMN IF NOT EXISTS current_price NUMERIC,
ADD COLUMN IF NOT EXISTS peak_price NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS current_liquidity_usd NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS price_updated_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

-- Price history for charting (lightweight)
CREATE TABLE IF NOT EXISTS price_snapshots (
    id SERIAL PRIMARY KEY,
    token_mint TEXT NOT NULL,
    price_usd NUMERIC NOT NULL,
    liquidity_usd NUMERIC DEFAULT 0,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_price_snapshots_mint_time
    ON price_snapshots(token_mint, recorded_at);

COMMENT ON COLUMN tokens.baseline_price IS 'Price at detection time (first snapshot)';
COMMENT ON COLUMN tokens.current_price IS 'Latest price from Jupiter';
COMMENT ON COLUMN tokens.peak_price IS 'Highest price observed after detection';
COMMENT ON TABLE price_snapshots IS 'Time-series price data for charting';
