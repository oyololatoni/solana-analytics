-- 016_scoring_engine.sql
-- Adds score component columns to feature_snapshots for the
-- Continuous Weighted Scoring Engine v1.

-- Component scores
ALTER TABLE feature_snapshots
    ADD COLUMN IF NOT EXISTS score_momentum     NUMERIC,
    ADD COLUMN IF NOT EXISTS score_liquidity    NUMERIC,
    ADD COLUMN IF NOT EXISTS score_participation NUMERIC,
    ADD COLUMN IF NOT EXISTS score_wallet       NUMERIC,
    ADD COLUMN IF NOT EXISTS score_risk_penalty NUMERIC,
    ADD COLUMN IF NOT EXISTS score_total        NUMERIC,
    ADD COLUMN IF NOT EXISTS score_label        TEXT,
    ADD COLUMN IF NOT EXISTS is_sniper_candidate BOOLEAN DEFAULT FALSE;
