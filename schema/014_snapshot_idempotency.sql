-- 014_snapshot_idempotency.sql
-- G. Idempotency: Prevent duplicate snapshots for the same token+version+timestamp.

CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_unique
    ON feature_snapshots (token_id, feature_version, detection_timestamp);
