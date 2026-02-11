-- Migration 004: Add Replay and Ingestion Disabled Counters
-- Extends the granular ignore reasons to cover upstream filter rejections.

ALTER TABLE ingestion_stats
    ADD COLUMN IF NOT EXISTS ignored_replay INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ignored_ingestion_disabled INTEGER DEFAULT 0;

COMMENT ON COLUMN ingestion_stats.ignored_replay IS 
    'Transactions (or payloads) ignored because they were already processed (webhook_replays check)';

COMMENT ON COLUMN ingestion_stats.ignored_ingestion_disabled IS 
    'Transactions ignored because INGESTION_ENABLED=0';
