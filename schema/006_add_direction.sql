-- Migration 006: Add direction column to events
-- Needed for "Direction-aware Volume" analytics (Buy vs Sell).

ALTER TABLE events ADD COLUMN IF NOT EXISTS direction VARCHAR(10);

-- Constraint check to ensure data integrity
ALTER TABLE events ADD CONSTRAINT check_direction CHECK (direction IN ('in', 'out'));

-- Index for performance (Volume queries will filter by direction)
CREATE INDEX IF NOT EXISTS idx_events_direction ON events(direction);

COMMENT ON COLUMN events.direction IS 'Swap direction relative to the wallet: "in" (Buy) or "out" (Sell)';
