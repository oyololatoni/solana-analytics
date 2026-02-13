-- 020_add_token_outcomes.sql
-- Adds detailed outcome tracking to the main tokens table for easier UI access.

ALTER TABLE tokens
ADD COLUMN IF NOT EXISTS outcome TEXT,
ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP;

-- Index for querying by detailed outcome if needed
CREATE INDEX IF NOT EXISTS idx_tokens_outcome ON tokens(outcome);
