-- 017_add_snapshot_index.sql
-- Optimizes the LATERAL JOIN for fetching the latest snapshot per token.
CREATE INDEX IF NOT EXISTS idx_feature_snapshots_latest 
ON feature_snapshots (token_id, feature_version, detection_timestamp DESC);
