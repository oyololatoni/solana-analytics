-- Migration 003: Fix Unique Constraint on Events Table
-- Changes from single-column (tx_signature) to composite (tx_signature, event_type, wallet)
-- This is CRITICAL for multi-leg swap support

-- Step 1: Drop the old unique constraint
-- The old constraint may be either a UNIQUE INDEX or a UNIQUE CONSTRAINT
-- We try both approaches safely

-- Drop old unique index on tx_signature (if it exists as an index)
DO $$
BEGIN
    -- Check if there's a unique index on just tx_signature
    IF EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE tablename = 'events' 
          AND indexname = 'events_tx_signature_key'
    ) THEN
        DROP INDEX events_tx_signature_key;
        RAISE NOTICE 'Dropped index: events_tx_signature_key';
    END IF;

    -- Also check for constraint-based unique (ALTER TABLE style)
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name = 'events' 
          AND constraint_name = 'events_tx_signature_key'
          AND constraint_type = 'UNIQUE'
    ) THEN
        ALTER TABLE events DROP CONSTRAINT events_tx_signature_key;
        RAISE NOTICE 'Dropped constraint: events_tx_signature_key';
    END IF;

    -- Also check for any other unique constraint on just tx_signature
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'events'
          AND indexname = 'events_tx_signature_idx'
    ) THEN
        DROP INDEX events_tx_signature_idx;
        RAISE NOTICE 'Dropped index: events_tx_signature_idx';
    END IF;
END
$$;

-- Step 2: Create the new composite unique index
-- This allows multiple legs per transaction (different wallets)
-- while preventing true duplicates (same tx + event_type + wallet)
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_unique 
    ON events(tx_signature, event_type, wallet);

-- Step 3: Add supporting indexes if they don't exist
CREATE INDEX IF NOT EXISTS idx_events_token_mint ON events(token_mint);
CREATE INDEX IF NOT EXISTS idx_events_block_time ON events(block_time);
CREATE INDEX IF NOT EXISTS idx_events_wallet ON events(wallet);
