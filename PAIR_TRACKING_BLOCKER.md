# Eligibility Gate Correction Blocker Analysis

## Critical Finding

**Cannot implement real primary pair selection without pair_address tracking**

### Current Schema Status

1. **`liquidity_events` table**:
   - EXISTS but has **0 rows** (unused)
   - NO `pair_address` column in schema
   - Not populated by ingestion pipeline

2. **`trades` table**:
   - Active table with all swap data
   - Has `liquidity_usd` column
   - **NO `pair_address` column**
   - Cannot determine which pair (WSOL/USDC/USDT) each trade is against

### User's Requested Fix (Blocked)

```sql
-- CANNOT RUN: liquidity_events has no pair_address column
WITH ranked_pairs AS (
    SELECT token_id, pair_address, MAX(liquidity_usd) AS max_liq
    FROM liquidity_events  -- TABLE EMPTY + NO pair_address COLUMN
    GROUP BY token_id, pair_address
)
```

### Root Cause

The webhook ingestion (`api/webhooks.py`) doesn't extract or store which quote token (pair) each swap is against. All trades are aggregated without pair tracking.

## Options

### Option A: Add pair_address to trades table

**Migration**:
```sql
ALTER TABLE trades ADD COLUMN pair_address TEXT;
CREATE INDEX idx_trades_pair ON trades(pair_address);
```

**Pros**:
- Enables real primary pair selection
- Proper base token validation

**Cons**:
- Existing trades have NULL pair_address (data loss)
- Requires webhook parser update to extract pair from Helius events
- May need backfill logic

### Option B: Extract pair from Helius webhook going forward

**Code Change**: Update `api/webhooks.py` to parse pair_address from swap instructions

**Pros**:
- Clean data going forward
- Minimal schema changes

**Cons**:
- Historical data remains unpaired
- Gradual adoption (only new tokens benefit)

### Option C: Keep simplified WSOL-only approach (Current)

**Status Quo**:
- Assume all trades are WSOL pairs
- Filter 2 (base validation) becomes cosmetic

**Pros**:
- No migration needed
- Works with current data

**Cons**:
- Violates architectural purity
- Cannot detect USDC/USDT primary pairs
- Base validation is fake

### Option D: Defer until liquidity tracking implemented

**Wait for**:
- Proper `liquidity_events` table population
- Pair tracking in ingestion pipeline
- Then implement full corrections

**Pros**:
- Correct implementation when ready
- No half-measures

**Cons**:
- Delays eligibility gate corrections
- Current system remains architecturally flawed

## Recommendation

**Option B + Option C hybrid**:

1. **Short-term** (immediate):
   - Keep WSOL assumption for existing data
   - Update base token addresses (simple fix)
   - Implement continuous liquidity sustain logic
   - Remove batch rejection side effects

2. **Medium-term** (next sprint):
   - Add `pair_address` to trades table
   - Update `webhooks.py` to extract pair from Helius
   - Backfill recent trades if possible

This allows immediate corrections while building toward proper pair tracking.

## Decision Required

User must choose which option to pursue before eligibility gate corrections can proceed.
