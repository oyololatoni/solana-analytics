-- Migration 005: Create raw_webhooks table (Queue)
-- Stores incoming webhook payloads before processing by the worker.

CREATE TABLE IF NOT EXISTS raw_webhooks (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL DEFAULT 'helius',
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, processing, processed, failed
    payload_hash VARCHAR(64) UNIQUE, -- Replay protection moved here? Or keep separate?
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    retry_count INT DEFAULT 0
);

-- Index for queue processing
CREATE INDEX idx_raw_webhooks_pending ON raw_webhooks (status, created_at) WHERE status = 'pending';
CREATE INDEX idx_raw_webhooks_payload_hash ON raw_webhooks (payload_hash);

COMMENT ON TABLE raw_webhooks IS 'Queue for incoming webhooks before worker processing';
