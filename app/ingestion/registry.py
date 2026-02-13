
from typing import Dict, Type
from .base import ChainAdapter

class AdapterRegistry:
    _instance = None
    _adapters: Dict[str, ChainAdapter] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AdapterRegistry, cls).__new__(cls)
        return cls._instance

    def register(self, chain_name: str, adapter: ChainAdapter):
        """Register a chain adapter instance."""
        self._adapters[chain_name] = adapter

    def get(self, chain_name: str) -> ChainAdapter:
        """Get a registered adapter instance."""
        adapter = self._adapters.get(chain_name)
        if not adapter:
            raise ValueError(f"No adapter registered for chain: {chain_name}")
        return adapter

# Global instance
registry = AdapterRegistry()
