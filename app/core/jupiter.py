"""
Jupiter Price API Client
========================
Fetches token prices from Jupiter Aggregator API v2.
"""
import os
import logging
import httpx

logger = logging.getLogger("core.jupiter")

JUPITER_PRICE_API = "https://api.jup.ag/price/v2"


async def get_token_prices(mints: list[str]) -> dict:
    """
    Batch fetch prices for multiple token mints from Jupiter.
    Returns dict of {mint: {price, liquidity}} for each token.
    """
    if not mints:
        return {}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Jupiter accepts comma-separated mints
            params = {"ids": ",".join(mints)}
            resp = await client.get(JUPITER_PRICE_API, params=params)
            resp.raise_for_status()
            data = resp.json()

            results = {}
            for mint in mints:
                token_data = data.get("data", {}).get(mint, {})
                if token_data:
                    results[mint] = {
                        "price": float(token_data.get("price", 0)),
                        "liquidity": None,  # Jupiter v2 doesn't return liquidity directly
                    }
            return results

    except Exception as e:
        logger.error(f"Jupiter price fetch error: {e}")
        return {}


async def get_single_price(mint: str) -> dict:
    """Fetch price for a single token from Jupiter."""
    prices = await get_token_prices([mint])
    return prices.get(mint, {"price": 0, "liquidity": None})
