# Label Worker Corrections Summary - Feb 12, 2026

## Applied Corrections (3/6)

### ✅ 1. Liquidity Collapse Peak Window Fix (CRITICAL)
**Problem**: Peak measured over full 72h window, but collapse checked within 48h failure window
**Fixed**: Changed peak query from `window_end` (72h) to `fail_deadline` (48h)
**Impact**: Ensures collapse is evaluated against peak within same timeframe (statistical consistency)

### ✅ 2. Volume Collapse 6h Minimum Buffer
**Problem**: Collapse evaluated immediately after detection, when 6h historical window incomplete
**Fixed**: Added skip condition for first 6 hours after detection:
```python
if curr < start + timedelta(hours=6):
    curr += timedelta(hours=1)
    continue
```
**Impact**: Prevents false positives from incomplete historical data

### ✅ 3. Documentation of Missing pair_address Enforcement
**Fixed**: Added NOTE comments to all affected functions documenting the blocker
**Impact**: Clear technical debt marking for future pair tracking implementation

## Blocked Corrections (3/6)

### ❌ 4. Primary Pair Enforcement in All Trade Queries (CRITICAL)
**Required**:
```python
# At start of resolve_token:
primary_pair = fetch_primary_pair_address(token_id)

# In ALL trade queries:
WHERE token_id = %s
AND pair_address = %s  # BLOCKED: column doesn't exist
```
**Blocker**: `trades` table has no `pair_address` column
**Impact**: 
- Success 5x could occur in secondary pool
- Price failure could reference wrong pool
- Liquidity collapse mixes pools
- Volume aggregates across all pairs
- Early wallet exit mixes pools

### ❌ 5. Baseline Price Primary Pair Enforcement
**Problem**: `get_baseline_price()` doesn't filter by primary_pair_address
**Blocker**: Same - no `pair_address` column
**Impact**: Baseline price could come from wrong pool, corrupting all multiplier calculations

### ❌ 6. Early Wallet Exit Query Optimization
**Problem**: 1 query per early wallet (200 wallets = 200 queries)
**Proposed Fix**:
```python
SELECT wallet_address,
       SUM(CASE WHEN side='buy' THEN amount_token ELSE -amount_token END) AS net
FROM trades
WHERE token_id = %s
AND pair_address = %s  # BLOCKED
AND wallet_address = ANY(%s)
AND timestamp <= %s
GROUP BY wallet_address
```
**Blocker**: Needs `pair_address` column for correctness
**Status**: Can implement without pair_address but would perpetuate cross-pool aggregation issue

## Logic Confirmations (Correct Behavior)

### ✅ Success Overrides All (No Change Needed)
- If price hits 5x at hour 20, even if it dropped to 0.5x at hour 1, SUCCESS is returned
- This is CORRECT per stated priority rules
- User confirmed this is intentional

### ✅ max_mult Usage (No Bug)
- Variable always defined by success check before use
- No undefined variable risk

### ✅ Expiry With No Trades (Acceptable)
- Token remains active until 72h window if no trades
- Correct behavior, no issue

## Performance Notes

### Total Queries Per Token: ~10-200+
For each token resolved:
- 1x baseline price
- 1x success check
- 1x max multiplier
- 1x price failure
- 2x liquidity collapse (peak + min)
- 1x volume hourly
- 1x early wallet buyers
- N x early wallet exits (1 per wallet)

**Impact on Free Tier**: Heavy for 100+ active tokens
**Mitigation**: Correctness first, optimize later

## Strategic Assessment

**Label Worker Status**: 
- ✅ Deterministic ordering correct
- ✅ Idempotency enforced
- ✅ No future data leakage
- ✅ Uses detection_time correctly
- ❌ Missing primary_pair enforcement (systemic flaw)

**Compared to Other Layers**:
- Eligibility gate: needs structural fix (pair tracking + continuous liquidity)
- Feature engine: needs pair scoping + timing fix (timing FIXED in v2)
- Label worker: mostly correct, needs pair scoping

## Remaining Work

### High Priority:
1. **Add `pair_address` column to trades table** (schema change)
2. **Update webhook parser** to extract pair from Helius events
3. **Enforce primary_pair in label_worker.py** (blocked by #1)

### Medium Priority:
4. Optimize early wallet exit to single grouped query (after pair tracking)
5. Consider query batching for multiple tokens

### Low Priority:
6. General performance optimization

## Dataset Integrity Impact

**Current State**: Labels may be based on cross-pool aggregation
- Price movements in secondary pools affect outcomes
- Volume from all pools counted together  
- Early wallet behavior mixed across pools

**After Fix**: Labels will be pool-specific
- Aligns with eligibility gate (when that's fixed)
- Aligns with feature engine (when that's fixed)
- Statistical consistency across all layers

**Recommendation**: Version bump for lifecycle_labels when pair tracking implemented  
(similar to feature_version=2 for snapshots)
