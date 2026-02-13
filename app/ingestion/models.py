
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any

@dataclass
class CanonicalToken:
    chain: str
    address: str
    symbol: Optional[str] = None
    name: Optional[str] = None
    created_at_chain: Optional[datetime] = None
    
@dataclass
class CanonicalTrade:
    chain: str
    token_address: str
    tx_signature: str
    wallet_address: str
    side: str  # 'buy' | 'sell'
    amount_token: Decimal
    amount_sol: Decimal # For Solana, or native currency
    amount_usd: Optional[Decimal] = None
    price_usd: Optional[Decimal] = None
    liquidity_usd: Optional[Decimal] = None
    pair_address: Optional[str] = None  # Liquidity pool address (Raydium/Orca/Meteora pool ID)
    slot: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
@dataclass
class CanonicalLiquidityEvent:
    chain: str
    token_address: str
    tx_signature: str
    liquidity_usd: Decimal
    delta_liquidity_usd: Decimal
    slot: Optional[int] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)

@dataclass
class CanonicalWalletHistory:
    wallet_address: str
    total_tokens_interacted: int
    first_seen: datetime
    last_seen: datetime
    net_pnl_estimate: Optional[Decimal] = None

@dataclass
class CanonicalWalletInteraction:
    chain: str
    token_address: str
    wallet_address: str
    last_balance_token: Decimal
    last_balance_usd: Optional[Decimal] = None
    interaction_count_delta: int = 1
    timestamp: datetime = field(default_factory=datetime.utcnow)
