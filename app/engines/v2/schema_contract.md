# Snapshot Schema Contract (V2)

## Overview
This document defines the invariant rules for Feature Version 3 / Schema Version 2.
This schema is strictly enforced by `app/engines/v2/snapshot_contract.py`.

## Versioning
- **Engine Version**: 2
- **Feature Version**: 3
- **Schema Version**: 2

## Required Columns

### Identity
- `token_id` (Integer)
- `feature_version` (Integer)
- `schema_version` (Integer)
- `snapshot_time` (Datetime)

### Volume Metrics
- `volume_acceleration` (Float)
- `volume_growth_rate_1h` (Float)
- `volume_5m_sol` (Decimal)
- `volume_30m_sol` (Decimal)
- `volume_1h_sol` (Decimal)
- `volume_6h_sol` (Decimal)

### Price Metrics
- `price_volatility_1h` (Float)
- `price_drawdown_6h` (Float)
- `baseline_price_usd` (Float)
- `current_price_usd` (Float)
- `current_multiplier` (Float)

### Liquidity Metrics
- `liquidity_current_usd` (Float)
- `liquidity_peak_window_usd` (Float)
- `liquidity_growth_rate` (Float)
- `sudden_liquidity_spike` (Boolean)

### Market & Wallet Metrics
- `buy_sell_ratio_1h` (Float)
- `unique_wallets_growth` (Float)
- `holder_concentration` (Float)
- `holder_retention` (Float)
- `wallet_entropy` (Float)
- `early_wallet_count` (Integer)
- `early_wallet_net_accumulation_sol` (Float)
- `early_wallet_exit_ratio` (Float)

### Risk Metrics
- `risk_score` (Float)
- `liquidity_collapse_threshold_usd` (Float)
- `volume_collapse_ratio_current` (Float)
- `price_failure_threshold_usd` (Float)

### Meta
- `age_hours` (Float)
- `lifecycle_state` (String: "ignition", "expansion", "unstable", "distribution", "fragile")
- `score_total` (Float)

## Scoring Weights (Immutable)
- `volume_momentum`: 15.0
- `market_quality`: 15.0
- `price_stability`: 10.0
- `holder_behavior`: 10.0

## Invariants
1. All timestamps are UTC.
2. `risk_score` is calculated PRE-outcome.
3. `lifecycle_state` is deterministic based on thresholds defined in `features.py`.
