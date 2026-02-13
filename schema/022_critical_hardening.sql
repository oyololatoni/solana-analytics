-- Migration 022: Critical System Hardening
-- Description: Implements snapshot immutability, system configuration, and performance indexes.

-- 1. Snapshot Immutability Trigger
CREATE OR REPLACE FUNCTION prevent_snapshot_update()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'feature_snapshots rows are immutable';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS snapshot_update_block ON feature_snapshots;

CREATE TRIGGER snapshot_update_block
BEFORE UPDATE ON feature_snapshots
FOR EACH ROW
EXECUTE FUNCTION prevent_snapshot_update();

-- 2. System Configuration Table & Feature Version
CREATE TABLE IF NOT EXISTS system_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Initialize Feature Version (Idempotent)
INSERT INTO system_config (key, value)
VALUES ('feature_version', '4')
ON CONFLICT (key) DO UPDATE SET value = '4', updated_at = NOW();

-- 3. Idempotency Constraint (Snapshot per Token per Version)
-- Note: feature_snapshots already has a unique index on (token_id, feature_version) from migration 021/003?
-- Checking provided schema context, let's ensure it exists safely.
CREATE UNIQUE INDEX IF NOT EXISTS uq_snapshot_token_version 
ON feature_snapshots(token_id, feature_version);

-- 4. Missing Performance Indexes
CREATE INDEX IF NOT EXISTS idx_snapshot_token ON feature_snapshots(token_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_time ON feature_snapshots(snapshot_time);

-- Check if tokens has eligibility_status index
CREATE INDEX IF NOT EXISTS idx_tokens_eligibility ON tokens(eligibility_status);

-- Check if lifecycle_labels has token_id index
CREATE INDEX IF NOT EXISTS idx_lifecycle_token ON lifecycle_labels(token_id);

-- 5. Additional Hardening: Ensure active column index
CREATE INDEX IF NOT EXISTS idx_tokens_active ON tokens(is_active);
