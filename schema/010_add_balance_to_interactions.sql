
-- Add missing columns to wallet_token_interactions
ALTER TABLE wallet_token_interactions
ADD COLUMN IF NOT EXISTS last_balance_token NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS interaction_count INTEGER DEFAULT 0;
