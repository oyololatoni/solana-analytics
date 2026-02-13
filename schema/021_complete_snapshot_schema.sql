-- Migration 033: Complete Snapshot Schema (USD Volumes, Trade Gaps, Risk Metrics)
-- Description: Adds missing raw metrics requested for audit compliance and reproducibility.

-- 1. USD Volume Windows
ALTER TABLE feature_snapshots
ADD COLUMN IF NOT EXISTS volume_5m_usd NUMERIC(20,4) NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS volume_30m_usd NUMERIC(20,4) NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS volume_1h_usd NUMERIC(20,4) NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS volume_6h_usd NUMERIC(20,4) NOT NULL DEFAULT 0;

-- 2. Trade Gap Metric
ALTER TABLE feature_snapshots
ADD COLUMN IF NOT EXISTS max_trade_gap_30m_minutes NUMERIC(10,4) NOT NULL DEFAULT 0;

-- 3. Risk Metrics (Ensuring existence if not already present from batch engine implementation)
-- Note: batch_features.py uses them, so they might exist if manual ALTERs were run, but migration ensures it.
-- We check IF NOT EXISTS to be safe.
ALTER TABLE feature_snapshots
ADD COLUMN IF NOT EXISTS price_failure_threshold_usd NUMERIC(20,10) NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS liquidity_collapse_threshold_usd NUMERIC(20,4) NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS volume_collapse_ratio NUMERIC(10,6) NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS early_wallet_exit_ratio NUMERIC(10,6) NOT NULL DEFAULT 0;

-- 4. Baseline Price (Explicit USD Baseline)
-- Usually computed as baseline_price_usd (which already exists in batch engine inserts), ensuring it.
ALTER TABLE feature_snapshots
ADD COLUMN IF NOT EXISTS baseline_price_usd NUMERIC(20,10) NOT NULL DEFAULT 0;

-- 5. Multiplier at Snapshot
-- Map to current_multiplier if exists, or adding specific column if needed.
-- Batch engine uses current_multiplier. We'll stick to that or alias it if UI strictly needs 'multiplier_at_snapshot'.
-- For now, we assume current_multiplier covers it, but user asked for 'multiplier_at_snapshot'.
-- We'll add it as an alias/explicit column to satisfy the requirement if needed, or update code to populate it.
-- Let's add it to be safe and explicit.
ALTER TABLE feature_snapshots
ADD COLUMN IF NOT EXISTS multiplier_at_snapshot NUMERIC(10,4) NOT NULL DEFAULT 1;

-- 6. Liquidity Peak (USD)
-- Batch engine uses liquidity_peak_window_usd. User asked for liquidity_peak_usd.
-- We'll add liquidity_peak_usd and populate it (or migrate data).
ALTER TABLE feature_snapshots
ADD COLUMN IF NOT EXISTS liquidity_peak_usd NUMERIC(20,4) NOT NULL DEFAULT 0;

-- 7. Early Wallet Net Accumulation (USD/SOL?)
-- User asked for early_wallet_net_accumulation. Batch has _sol.
-- We'll add the generic one (likely USD or native units). Let's assume SOL for now as accumulation is usually native.
-- If user meant USD, we need price.
-- batch_features.py calculates early_wallet_net_accumulation_sol.
-- We'll add early_wallet_net_accumulation (no suffix) as requested, default 0.
ALTER TABLE feature_snapshots
ADD COLUMN IF NOT EXISTS early_wallet_net_accumulation NUMERIC(20,4) NOT NULL DEFAULT 0;
