
-- Add amount_sol to trades for price/volume calculation
ALTER TABLE trades
ADD COLUMN IF NOT EXISTS amount_sol NUMERIC DEFAULT 0;
