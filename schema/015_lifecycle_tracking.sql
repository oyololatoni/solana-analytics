-- 015_lifecycle_tracking.sql
-- Step 4: Snapshot Lifecycle & Monitoring
-- Adds tracking for lifecycle stages, liquidity candidates, and failure conditions.

-- 1. Lifecycle Stage Enum (as text for simplicity/portability, checked via constraint)
ALTER TABLE tokens 
ADD COLUMN IF NOT EXISTS lifecycle_stage TEXT DEFAULT 'PRE_ELIGIBLE';

ALTER TABLE tokens 
ADD CONSTRAINT check_lifecycle_stage 
CHECK (lifecycle_stage IN (
    'PRE_ELIGIBLE', 
    'ELIGIBLE_PENDING_30M', 
    'ACTIVE_MONITORING', 
    'SUCCESS', 
    'FAILED', 
    'EXPIRED'
));

-- 2. Liquidity Candidate Tracking
-- When did it first cross $50k? Null if not currently a candidate.
ALTER TABLE tokens 
ADD COLUMN IF NOT EXISTS liquidity_candidate_start TIMESTAMP WITHOUT TIME ZONE;

-- 3. Peak Liquidity Tracking (for F2 Failure Rule)
-- Max liquidity observed AFTER detection.
ALTER TABLE tokens 
ADD COLUMN IF NOT EXISTS peak_liquidity_usd NUMERIC DEFAULT 0;

-- 4. Sudden Liquidity Spike Flag (for Protection 2)
ALTER TABLE feature_snapshots 
ADD COLUMN IF NOT EXISTS sudden_liquidity_spike BOOLEAN DEFAULT FALSE;

-- Index for lifecycle queries
CREATE INDEX IF NOT EXISTS idx_tokens_lifecycle_stage ON tokens(lifecycle_stage);
