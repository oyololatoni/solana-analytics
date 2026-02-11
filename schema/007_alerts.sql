CREATE TABLE IF NOT EXISTS alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_mint TEXT NOT NULL,
    metric TEXT NOT NULL, -- 'volume_1h', 'price_usd', 'swap_count_1h'
    condition TEXT NOT NULL CHECK (condition IN ('gt', 'lt')),
    value NUMERIC NOT NULL,
    channel TEXT NOT NULL DEFAULT 'slack',
    last_triggered_at TIMESTAMPTZ,
    cooldown_minutes INTEGER NOT NULL DEFAULT 60,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_mint ON alerts(token_mint);
