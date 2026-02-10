-- Migration 002: Add Detailed Ignored Counters
-- Adds granular tracking for why transactions are ignored during ingestion

-- Add 5 new columns to track specific ignore reasons
ALTER TABLE ingestion_stats
    ADD COLUMN IF NOT EXISTS ignored_missing_fields INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ignored_no_swap_event INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ignored_no_tracked_tokens INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ignored_constraint_violation INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ignored_exception INTEGER DEFAULT 0;

-- Add comments for documentation
COMMENT ON COLUMN ingestion_stats.ignored_missing_fields IS 
    'Transactions missing required fields (signature, slot, timestamp)';

COMMENT ON COLUMN ingestion_stats.ignored_no_swap_event IS 
    'Transactions without a swap event in the payload';

COMMENT ON COLUMN ingestion_stats.ignored_no_tracked_tokens IS 
    'Transactions with no legs matching TRACKED_TOKENS';

COMMENT ON COLUMN ingestion_stats.ignored_constraint_violation IS 
    'Transactions that failed due to unique constraint (duplicates)';

COMMENT ON COLUMN ingestion_stats.ignored_exception IS 
    'Transactions that raised unexpected exceptions during processing';

-- The original swaps_ignored column will now represent the sum of all ignored reasons
COMMENT ON COLUMN ingestion_stats.swaps_ignored IS 
    'Total ignored transactions (sum of all ignored_* counters)';
