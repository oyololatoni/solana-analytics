-- 019_label_unique_constraint.sql
-- G. Idempotency: Duplicate Preventions for Label Worker
-- Ensures a snapshot can only have one outcome label.

CREATE UNIQUE INDEX IF NOT EXISTS idx_labels_outcome_unique 
ON lifecycle_labels (snapshot_id);

-- Check integrity:
-- If duplicates exist, this will fail. But since we are fresh, it should pass.
