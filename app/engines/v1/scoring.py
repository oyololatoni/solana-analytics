"""
Scoring Engine v1 — Continuous Weighted Scoring Model

Pure-function module. No DB dependency.
Takes a feature dict, returns a score dict.

Architecture:
  - 14 features normalized to [0, 1] via capped linear scaling
  - 85 positive weight points + 15 risk penalty points = 100 max
  - Lifecycle state modifier (×0.8 or ×1.05)
  - Sniper mode filter (score > 75, stability > 0.7, retention > 60%)

All bands and weights are tunable constants.
After 200–500 labeled tokens, recalibrate from empirical distributions.
"""

from decimal import Decimal

# ---------------------------------------------------------------------------
# 1. Feature Bands — Tunable min/max for [0, 1] normalization
# ---------------------------------------------------------------------------
BANDS = {
    # Momentum
    "volume_acceleration":          (1.0, 3.0),
    "volume_growth_rate_1h":        (0.0, 2.0),
    "trade_frequency_ratio":        (1.0, 3.0),
    # Liquidity
    "liquidity_growth_rate":        (0.0, 0.5),
    "liquidity_stability_score":    (0.4, 1.0),
    # Participation
    "unique_wallet_growth_rate":    (0.0, 1.0),
    "buy_sell_ratio":               (1.0, 3.0),
    "wallet_entropy_score":         (1.0, 3.0),
    # Wallet Conviction
    "early_wallet_retention":       (0.3, 0.9),
    "early_wallet_net_accumulation":(0.0, 0.3),
    "top10_concentration_delta":    (-0.2, 0.2),
    # Risk
    "drawdown_depth_1h":            (0.0, 0.5),
    "volume_collapse_ratio":        (0.0, 1.0),
    "liquidity_volatility":         (0.0, 0.5),
}

# ---------------------------------------------------------------------------
# 2. Weights — Positive (85) + Risk (15) = 100 max theoretical
# ---------------------------------------------------------------------------
WEIGHTS = {
    # Momentum (25)
    "volume_acceleration":          10,
    "volume_growth_rate_1h":        10,
    "trade_frequency_ratio":         5,
    # Liquidity (20)
    "liquidity_growth_rate":        12,
    "liquidity_stability_score":     8,
    # Participation (20)
    "unique_wallet_growth_rate":    10,
    "buy_sell_ratio":                5,
    "wallet_entropy_score":          5,
    # Wallet Conviction (20)
    "early_wallet_retention":       10,
    "early_wallet_net_accumulation": 6,
    "top10_concentration_delta":     4,
    # Risk Penalty (15)
    "drawdown_depth_1h":             6,
    "volume_collapse_ratio":         5,
    "liquidity_volatility":          4,
}

# Feature groupings for component score breakdown
MOMENTUM_FEATURES = [
    "volume_acceleration",
    "volume_growth_rate_1h",
    "trade_frequency_ratio",
]
LIQUIDITY_FEATURES = [
    "liquidity_growth_rate",
    "liquidity_stability_score",
]
PARTICIPATION_FEATURES = [
    "unique_wallet_growth_rate",
    "buy_sell_ratio",
    "wallet_entropy_score",
]
WALLET_FEATURES = [
    "early_wallet_retention",
    "early_wallet_net_accumulation",
    "top10_concentration_delta",
]
RISK_FEATURES = [
    "drawdown_depth_1h",
    "volume_collapse_ratio",
    "liquidity_volatility",
]


# ---------------------------------------------------------------------------
# 3. Normalization — Continuous [0, 1] capped linear
# ---------------------------------------------------------------------------
def normalize(value, min_val, max_val, invert=False):
    """
    Linearly scale value into [0, 1], clamped.
    If invert=True, 1 becomes 0 and vice versa (for features where higher is worse).
    """
    v = float(value) if isinstance(value, Decimal) else value
    if max_val == min_val:
        return 0.0
    scaled = (v - min_val) / (max_val - min_val)
    scaled = max(0.0, min(scaled, 1.0))
    return 1.0 - scaled if invert else scaled


# ---------------------------------------------------------------------------
# 4. Component Score Functions
# ---------------------------------------------------------------------------
def _score_component(features, feature_list, invert_set=None):
    """Score a group of features. Returns (total_points, breakdown_dict)."""
    invert_set = invert_set or set()
    total = 0.0
    breakdown = {}
    for feat in feature_list:
        raw = features.get(feat, 0) or 0
        min_v, max_v = BANDS[feat]
        weight = WEIGHTS[feat]
        inv = feat in invert_set
        norm = normalize(raw, min_v, max_v, invert=inv)
        pts = norm * weight
        total += pts
        breakdown[feat] = {"raw": float(raw), "norm": round(norm, 4), "pts": round(pts, 2)}
    return round(total, 2), breakdown


def score_momentum(f):
    return _score_component(f, MOMENTUM_FEATURES)


def score_liquidity(f):
    return _score_component(f, LIQUIDITY_FEATURES)


def score_participation(f):
    return _score_component(f, PARTICIPATION_FEATURES)


def score_wallet(f):
    # top10_concentration_delta: rising concentration is BAD → invert
    return _score_component(f, WALLET_FEATURES, invert_set={"top10_concentration_delta"})


def score_risk(f):
    """
    Risk penalty. Higher raw values → more penalty.
    - drawdown_depth_1h: higher = worse (normal scaling, then subtract)
    - volume_collapse_ratio: LOWER = worse → invert (so low ratio → high penalty)
    - liquidity_volatility: higher = worse (normal scaling)
    """
    logger = logging.getLogger("engines.v1.scoring")
    total = 0.0
    breakdown = {}

    # Drawdown (higher → more penalty)
    for feat in ["drawdown_depth_1h", "liquidity_volatility"]:
        raw = float(f.get(feat, 0) or 0)
        min_v, max_v = BANDS[feat]
        weight = WEIGHTS[feat]
        norm = normalize(raw, min_v, max_v)
        pts = norm * weight
        total += pts
        breakdown[feat] = {"raw": raw, "norm": round(norm, 4), "penalty": round(pts, 2)}

    # Volume collapse (lower ratio → worse → invert)
    feat = "volume_collapse_ratio"
    raw = float(f.get(feat, 0) or 0)
    min_v, max_v = BANDS[feat]
    weight = WEIGHTS[feat]
    norm = normalize(raw, min_v, max_v, invert=True)
    pts = norm * weight
    total += pts
    breakdown[feat] = {"raw": raw, "norm": round(norm, 4), "penalty": round(pts, 2)}

    return round(total, 2), breakdown


# ---------------------------------------------------------------------------
# 5. Score Labels
# ---------------------------------------------------------------------------
def get_score_label(score):
    if score >= 85:
        return "sniper_candidate"
    elif score >= 75:
        return "high_asymmetry"
    elif score >= 60:
        return "structured_opportunity"
    elif score >= 30:
        return "transitional"
    else:
        return "low_probability"


# ---------------------------------------------------------------------------
# 6. Sniper Mode Filter
# ---------------------------------------------------------------------------
def is_sniper_candidate(score, features):
    """
    Sniper mode: score > 75 AND liquidity_stability > 0.7 AND retention > 60%.
    """
    liq_stab = float(features.get("liquidity_stability_score", 0) or 0)
    retention = float(features.get("early_wallet_retention", 0) or 0)
    return score > 75 and liq_stab > 0.7 and retention > 0.6


# ---------------------------------------------------------------------------
# 7. Main Scoring Function
# ---------------------------------------------------------------------------
def compute_score(features):
    """
    Compute the full score breakdown from a feature dict.
    
    Args:
        features: dict with all 16 feature keys + "lifecycle_state"
    
    Returns:
        dict with component scores, total, label, breakdown, sniper flag
    """
    momentum_pts, momentum_bd = score_momentum(features)
    liquidity_pts, liquidity_bd = score_liquidity(features)
    participation_pts, participation_bd = score_participation(features)
    wallet_pts, wallet_bd = score_wallet(features)
    risk_pts, risk_bd = score_risk(features)

    base_score = momentum_pts + liquidity_pts + participation_pts + wallet_pts - risk_pts

    # Lifecycle modifier
    lifecycle = features.get("lifecycle_state", "dormant")
    modifier = 1.0
    if lifecycle in ("unstable", "fragile"):
        modifier = 0.8
    elif lifecycle == "expansion":
        modifier = 1.05

    final = base_score * modifier
    final = max(0.0, min(final, 100.0))
    final = round(final, 2)

    label = get_score_label(final)
    sniper = is_sniper_candidate(final, features)

    return {
        "score_momentum": momentum_pts,
        "score_liquidity": liquidity_pts,
        "score_participation": participation_pts,
        "score_wallet": wallet_pts,
        "score_risk_penalty": risk_pts,
        "score_total": final,
        "score_label": label,
        "is_sniper_candidate": sniper,
        "lifecycle_modifier": modifier,
        "breakdown": {
            "momentum": momentum_bd,
            "liquidity": liquidity_bd,
            "participation": participation_bd,
            "wallet": wallet_bd,
            "risk": risk_bd,
        },
    }
