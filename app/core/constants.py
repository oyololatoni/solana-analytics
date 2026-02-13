from decimal import Decimal

# ==============================================================================
# SYSTEM CONFIGURATION
# ==============================================================================
FEATURE_VERSION = 4
ML_ENABLED = False  # Disabled until 300+ labeled outcomes
EPSILON = Decimal("0.000001")

# ==============================================================================
# ELIGIBILITY PRE-FILTER CONSTANTS
# ==============================================================================
MIN_TRADE_COUNT = 20
MIN_LIQUIDITY_USD = 50000                # $50k peak liquidity
MIN_VOLUME_FIRST_30M_USD = 5000          # $5k volume in first 30m
LIQUIDITY_SUSTAIN_MINUTES = 30           # Must hold >$50k for 30m
TRADE_GAP_LIMIT_MINUTES = 10             # No gap >10m in first 30m
SOL_PRICE_USD_ESTIMATE = Decimal("150.0") # Fallback if price missing

# ==============================================================================
# LIFECYCLE & LABELING CONSTANTS
# ==============================================================================
# Windows
OUTCOME_WINDOW_HOURS = 72                # Hard limit for success/failure
PEAK_WINDOW_HOURS = 48                   # Window to find success peak
FAILURE_BUFFER_HOURS = 6                 # Buffer after window before labeling expire/fail

# Multipliers & Drawdowns (Success/Failure)
SUCCESS_MULTIPLIER = 5.0                 # 5x from baseline = SUCCESS
FAILURE_DRAWDOWN = 0.5                   # 50% drop from peak = FAILURE (if rug/collapse)

# Lifecycle States
LIFECYCLE_THRESHOLDS = {
    "ignition": {
        "vol_accel": 1.5,        # Volume acceleration ratio (5m / 30m_avg)
        "min_age_hours": 2,      # Minimum age to exit ignition
    },
    "expansion": {
        "buy_sell_ratio": 1.2,   # Minimum buy/sell ratio
        "liq_stable_ratio": 0.7, # Liquidity stability (current / peak)
    },
    "unstable": {
        "drawdown_threshold": 0.3,  # Price drawdown from peak
    },
    "distribution": {
        "buy_sell_ceiling": 0.8,    # Maximum buy/sell ratio (more sells)
    },
    "fragile": {
        "vol_collapse_ratio": 0.4,  # Volume collapse threshold
    }
}

# ==============================================================================
# SCORING & RISK CONSTANTS
# ==============================================================================
SCORE_WEIGHTS_V3 = {
    "volume_momentum": 15.0,     # Volume acceleration + growth
    "market_quality": 15.0,      # Buy/sell ratio + unique wallets
    "price_stability": 10.0,     # Volatility + drawdown
    "holder_behavior": 10.0,     # Concentration + retention
}

RISK_PARAMS = {
    "liquidity_collapse_threshold_ratio": 0.2,  # 20% of peak liquidity
    "volume_collapse_ratio_threshold": 0.4,     # 40% of peak volume (accel < 0.4)
    "price_failure_drawdown": 0.5,              # 50% drawdown
    "early_exit_ratio_threshold": 0.5,          # 50% of early volume sold
}

# Canonical Base Tokens
BASE_TOKENS = {
    'WSOL': 'So11111111111111111111111111111111111111112',
    'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    'USDT': 'Es9vMFrzaCERmZp4pC8F5zw6rH6YhZC8Yz1KJk9gP3Rz',
}
BASE_TOKEN_ADDRESSES = list(BASE_TOKENS.values())

# Program IDs
PUMP_PROGRAM_ID = "6EF8rrecthR5DkzkRgZNpmjDoE7YQDdyCjTiMQuYzfoP"
RAYDIUM_PROGRAM_ID = "RVKd61ztZW9qTMWvHdQWfhKGXT5VErC7E3q6MZBqYg"
