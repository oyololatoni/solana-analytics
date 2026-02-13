# Features V1 Critical Corrections Summary - Feb 12, 2026

## Applied Corrections (6/7)

### ✅ 1. Snapshot Timing Correction (CRITICAL)
**Problem**: Used `now()` instead of `detected_at` for snapshot reference time
**Fixed**: 
- Fetch `detected_at` from tokens table at start of `compute_v1_snapshot`
- Use `detected_at` as `now` variable for all time window calculations
- Abort if `detected_at` is NULL
**Impact**: Ensures statistical reproducibility and backtest determinism

### ✅ 2. Final Score Calculation Fix (CRITICAL)
**Problem**: Score compressed by 0.3x when ML disabled (always)  
- Old: `final_score = (0.0 * 70) + (rule_score * 0.3) = rule_score * 0.3`
**Fixed**:
```python
if ML_ENABLED:
    final_score = (probability_5x * 70.0) + (scores["score_total"] * 0.3)
else:
    final_score = scores["score_total"]  # No compression
```
**Impact**: Correct scoring when ML disabled (current state: 0/300 labels)

### ✅ 3. Logger Undefined Variable Fix
**Problem**: `rule_score` variable doesn't exist, would crash  
**Fixed**: Changed to `scores['score_total']`  
**Impact**: Prevents runtime error in logging

### ✅ 4. Liquidity Placeholder Documentation
**Problem**: Zero values for liquidity features could distort scoring  
**Fixed**: Added TODO comments and note that they're excluded from scoring_engine  
**Impact**: Clarifies intent, documents tech debt

### ✅ 5. Feature Version Increment
**Problem**: Breaking changes without version bump  
**Fixed**: Changed `feature_version` from 1 to 2  
**Impact**: Historical v1 snapshots preserved, new snapshots clearly versioned

### ✅ 6. Remove Liquidity Constants (Partial)
**Problem**: `LIQUIDITY_THRESHOLD_USD` and `SUSTAIN_MINUTES` don't belong in features layer  
**Status**: Constants still exist but deprecated in comments  
**Needs**: Final cleanup to fully remove (low priority)

### ❌ 7. Primary Pair Enforcement (BLOCKED)
**Problem**: All trade queries aggregate across ALL pairs (WSOL, USDC, USDT)  
**Required Fix**:
```sql
WHERE token_id = %s
AND pair_address = primary_pair_address  -- BLOCKED: pair_address column doesn't exist
```
**Blocker**: `trades` table has NO `pair_address` column  
**Impact**: Metric corruption from multi-pair aggregation (volume, buy/sell ratio, volatility, etc.)  
**Resolution**: Requires schema change + webhook parser update (see PAIR_TRACKING_BLOCKER.md)

## Verification

**System Integrity**: ✅ 7/7 checks passed
- No duplicate snapshots
- All ELIGIBLE tokens have detected_at  
- No active tokens past 72h expiry
- All snapshots have scores
- All resolved tokens have labels
- Primary pair addresses set
- ML correctly disabled

## Migration Created

**025_feature_version_2.sql**: Documents version 2 changes

## Statistical Impact

### Before Corrections:
- Snapshot time drifted with worker lag (5-10 min non-determinism)
- Scores compressed to 30% of actual value
- Features potentially mixed across multiple pairs

### After Corrections:
- Snapshot time locked to eligibility flip moment ✅
- Scores calculated correctly (100% rule-based) ✅
- Features still mixed across pairs ❌ (blocked by schema)

## Remaining Work

1. **High Priority**: Add `pair_address` column to trades table
2. **High Priority**: Update webhook parser to extract pair
3. **Medium Priority**: Implement or remove liquidity placeholder features
4. **Low Priority**: Final cleanup of deprecated constants

## Deployment Notes

**Version 2 snapshots will use different timing than v1**:
- v1: Computed at worker run time (variable)
- v2: Computed at detected_at time (deterministic)

This is INTENTIONAL and CORRECT. Do not mix v1 and v2 in same analysis without accounting for this difference.

**Recommendation**: Only use v2 snapshots for new ML training when dataset matures (300+ labels).
