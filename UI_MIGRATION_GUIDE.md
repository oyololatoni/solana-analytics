# UI Migration Guide: Trade-Driven → Snapshot-Driven

## CRITICAL ARCHITECTURAL SHIFT

**The UI must use `/analytics/snapshots` as its PRIMARY data source.**

This is not optional. This is structural integrity.

---

## Current Problem

**UI queries `/analytics/tokens` or computes live from `trades` table**:
```javascript
// ❌ WRONG: UI computing features live
fetch('/analytics/tokens').then(tokens => {
  tokens.forEach(token => {
    const volume5m = computeVolume(token.trades, 5);
    const volume30m = computeVolume(token.trades, 30);
    const acceleration = volume5m / (volume30m / 6);  // This diverges from engine!
  });
});
```

**Engine uses frozen `feature_snapshots`**:
```sql
-- Engine scored this exact snapshot
SELECT volume_acceleration FROM feature_snapshots WHERE token_id = 123;
-- Returns: 2.4 (frozen at snapshot_time)
```

**Result**: UI shows 2.7, engine scored 2.4 → verification impossible

---

## Solution: Snapshot-Driven UI

### Step 1: Replace Primary Data Source

**File**: `static/monitor.html` (or equivalent React/Vue component)

**Before**:
```javascript
async function loadTokens() {
  const response = await fetch('/analytics/tokens');  // ❌ Trade-driven
  const data = await response.json();
  displayTokens(data);
}
```

**After**:
```javascript
async function loadTokens() {
  const response = await fetch('/analytics/snapshots?only_eligible=true&limit=100');  // ✅ Snapshot-driven
  const data = await response.json();
  displayTokens(data.tokens);  // Note: wrapped in {count, tokens}
}
```

### Step 2: Display Snapshot Metadata (Integrity Panel)

**For EVERY token card**, display:

```html
<div class="token-card">
  <div class="token-header">
    <span class="symbol">${token.symbol}</span>
    <span class="score">${token.score_total}</span>
  </div>
  
  <!-- NEW: Dataset Integrity Panel -->
  <div class="integrity-panel">
    <span class="snapshot-id" title="Snapshot ID">#${token.snapshot_id}</span>
    <span class="feature-version" title="Feature Version">v${token.feature_version}</span>
    <span class="snapshot-locked" title="Snapshot Immutable">🔒</span>
    <span class="snapshot-time" title="Snapshot Time">${formatTime(token.snapshot_time)}</span>
  </div>
  
  <!-- Engineered Features (from snapshot) -->
  <div class="features">
    <div>Accel: ${token.volume_acceleration?.toFixed(2)}</div>
    <div>B/S: ${token.buy_sell_ratio_1h?.toFixed(2)}</div>
    <div>Wallet Growth: ${token.unique_wallets_growth?.toFixed(2)}</div>
  </div>
</div>
```

**CSS**:
```css
.integrity-panel {
  font-size: 0.7rem;
  color: var(--text-dim);
  display: flex;
  gap: 0.5rem;
  padding: 0.25rem 0;
  border-top: 1px solid var(--border);
}

.snapshot-locked {
  color: var(--green);  /* Lock icon indicates immutability */
}
```

### Step 3: Token Detail View (Drill-Down)

**On click**, fetch comprehensive intelligence:

```javascript
async function showTokenDetails(tokenId) {
  const response = await fetch(`/analytics/token/${tokenId}/details`);
  const data = await response.json();
  
  // Display ALL panels
  renderStructural(data.structural);
  renderEligibilityGate(data.eligibility_gate);
  renderFeatures(data.feature_snapshot);
  renderScoringBreakdown(data.scoring.breakdown);
  renderLifecycle(data.lifecycle);
  renderIntegrity(data.dataset_integrity);
}

function renderIntegrity(integrity) {
  return `
    <div class="integrity-detail">
      <h3>Dataset Integrity</h3>
      <table>
        <tr>
          <td>Snapshot ID</td>
          <td>${integrity.snapshot.snapshot_id}</td>
        </tr>
        <tr>
          <td>Feature Version</td>
          <td>v${integrity.snapshot.feature_version}</td>
        </tr>
        <tr>
          <td>Snapshot Time</td>
          <td>${integrity.snapshot.snapshot_time}</td>
        </tr>
        <tr>
          <td>Immutable</td>
          <td>🔒 ${integrity.snapshot.snapshot_immutable ? 'YES' : 'NO'}</td>
        </tr>
        <tr>
          <td>Schema Version</td>
          <td>v${integrity.trades.schema_version}</td>
        </tr>
        <tr>
          <td>V2 Trades</td>
          <td>${integrity.trades.v2_trade_count}</td>
        </tr>
      </table>
    </div>
  `;
}
```

### Step 4: Deprecate Trade-Driven Endpoints (Backend)

**Mark as deprecated** in `api/analytics.py`:

```python
@router.get("/tokens")
@deprecated("Use /analytics/snapshots instead - this endpoint computes live aggregates")
async def get_tracked_tokens():
    """
    [DEPRECATED] Returns live trade aggregates.
    
    DO NOT USE for ranking or scoring.
    Use /analytics/snapshots instead (snapshot-driven).
    """
    # ... existing implementation
```

### Step 5: Verify No Live Feature Computation

**Audit UI code** for these anti-patterns:

❌ **BAD** - Computing features in UI:
```javascript
const acceleration = volume5m / (volume30m / 6);
const buyPressure = buyVolume / sellVolume;
const walletGrowth = (wallets1h - wallets6h) / wallets6h;
```

✅ **GOOD** - Using snapshot features:
```javascript
const acceleration = token.volume_acceleration;  // From feature_snapshots
const buyPressure = token.buy_sell_ratio_1h;   // From feature_snapshots
const walletGrowth = token.unique_wallets_growth;  // From feature_snapshots
```

---

## Complete Example: Token Ranking Table

```javascript
async function renderRankedTokens() {
  // Fetch snapshot-driven data
  const response = await fetch('/analytics/snapshots?only_eligible=true&min_score=30&limit=50');
  const { tokens } = await response.json();
  
  // Sort by score (already pre-sorted, but can re-sort client-side)
  const sorted = tokens.sort((a, b) => b.score_total - a.score_total);
  
  const tableHtml = `
    <table class="ranked-tokens">
      <thead>
        <tr>
          <th>Rank</th>
          <th>Token</th>
          <th>Score</th>
          <th>Vol Accel</th>
          <th>B/S Ratio</th>
          <th>Lifecycle</th>
          <th>Snapshot</th>
          <th>🔒</th>
        </tr>
      </thead>
      <tbody>
        ${sorted.map((token, idx) => `
          <tr onclick="showTokenDetails(${token.token_id})">
            <td>${idx + 1}</td>
            <td>${token.symbol}</td>
            <td class="score">${token.score_total.toFixed(1)}</td>
            <td>${token.volume_acceleration?.toFixed(2) || 'N/A'}</td>
            <td>${token.buy_sell_ratio_1h?.toFixed(2) || 'N/A'}</td>
            <td><span class="lifecycle-${token.lifecycle_state}">${token.lifecycle_state}</span></td>
            <td class="snapshot-meta">
              <span title="Snapshot #${token.snapshot_id}">#${token.snapshot_id}</span>
              <span title="Feature v${token.feature_version}">v${token.feature_version}</span>
            </td>
            <td title="Snapshot Immutable">🔒</td>
          </tr>
        `).join('')}
      </tbody>
    </table>
  `;
  
  document.getElementById('ranked-table').innerHTML = tableHtml;
}
```

---

## Verification Checklist

After migration, verify:

- [ ] UI fetches from `/analytics/snapshots` (not `/analytics/tokens`)
- [ ] Token ranking uses `score_total` from snapshots
- [ ] All displayed metrics come from snapshot fields (no live computation)
- [ ] Snapshot metadata visible: `snapshot_id`, `feature_version`, `snapshot_time`
- [ ] Immutability indicator (🔒) shown
- [ ] Detail view shows complete integrity panel
- [ ] NO raw trade aggregations in ranking logic
- [ ] Deprecated endpoints marked or removed

---

## Dataset Integrity Fields Reference

### Required in List View

| Field | Source | Purpose |
|-------|--------|---------|
| `snapshot_id` | feature_snapshots.id | Exact snapshot used |
| `feature_version` | feature_snapshots.feature_version | Which engine version |
| `snapshot_locked` | Always TRUE | Immutability proof |
| `snapshot_time` | feature_snapshots.snapshot_time | When features computed |

### Required in Detail View

| Field | Source | Purpose |
|-------|--------|---------|
| All above | feature_snapshots | Core lineage |
| `schema_version` | trades.schema_version | v1 (cross-pool) or v2 (pair-scoped) |
| `v2_trade_count` | COUNT trades WHERE schema_version=2 | Data quality |
| `model_version_id` | model_versions.id (future) | Which ML model scored |

---

## Critical Rules

1. **Never compute features in UI** - Always fetch from snapshots
2. **Never query trades table for ranking** - Use feature_snapshots
3. **Always show snapshot_time** - Prove data is frozen
4. **Always show snapshot_locked = true** - No recomputation confusion
5. **Deprecate trade-driven endpoints** - Prevent dual surfaces

---

## Summary

**Old Architecture** (Broken):
- UI: trades table → live aggregates → divergence
- Engine: feature_snapshots → frozen features

**New Architecture** (Correct):
- UI: feature_snapshots → frozen features → alignment
- Engine: feature_snapshots → frozen features

**Key Principle**: UI displays intelligence layer, doesn't recompute it.
