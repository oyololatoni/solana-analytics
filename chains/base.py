
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union
from .models import CanonicalToken, CanonicalTrade, CanonicalLiquidityEvent, CanonicalWalletInteraction

class ChainAdapter(ABC):
    """
    Abstract Base Class for Chain Adapters.
    Responsible for fetching data and normalizing it into Canonical schemas.
    """

    @abstractmethod
    async def get_token_creation(self, cursor: Any) -> List[Dict]:
        """Fetch newly created tokens since cursor. Return list of CanonicalToken dicts."""
        pass

    @abstractmethod
    async def get_trades(self, cursor: Any) -> List[Dict]:
        """Fetch trade/swap events since cursor. Return list of CanonicalTrade dicts."""
        pass

    @abstractmethod
    async def get_liquidity_events(self, cursor: Any) -> List[Dict]:
        """Fetch liquidity add/remove events. Return list of CanonicalLiquidityEvent dicts."""
        pass

    @abstractmethod
    async def get_wallet_history(self, wallet_address: str) -> Dict:
        """Fetch wallet historical behavior. Return CanonicalWalletHistory dict."""
        pass

    @abstractmethod
    def normalize_tx(self, raw_tx: Dict) -> List[Union[CanonicalTrade, CanonicalLiquidityEvent, CanonicalWalletInteraction, CanonicalToken]]:
        """
        Convert a raw chain transaction (or event payload) into a list of Canonical objects.
        A single transaction can generate multiple canonical events (e.g. 1 Swap + 2 Balance Updates).
        """
        pass
