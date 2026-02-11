import requests
import time
from config import HELIUS_API_KEY
from api import logger

# Simple in-memory cache: mint -> {name, symbol, ts}
_metadata_cache = {}
CACHE_TTL = 3600  # 1 hour

def fetch_token_metadata(mint: str):
    """
    Fetches token metadata (name, symbol) from Helius DAS API.
    Uses caching to avoid rate limits.
    """
    # Check cache
    if mint in _metadata_cache:
        item = _metadata_cache[mint]
        if time.time() - item["ts"] < CACHE_TTL:
            return item["data"]

    # Fallback default
    default_data = {"name": f"{mint[:4]}...{mint[-4:]}", "symbol": "UNK"}

    if not HELIUS_API_KEY:
        logger.warning("No HELIUS_API_KEY set. Cannot fetch metadata.")
        return default_data

    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    payload = {
        "jsonrpc": "2.0",
        "id": "antigravity",
        "method": "getAsset",
        "params": {
            "id": mint
        }
    }

    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        if "result" in data:
            content = data["result"].get("content", {})
            metadata = data["result"].get("content", {}).get("metadata", {})
            
            # Prefer symbol, then name
            name = metadata.get("name") or content.get("json_uri", "").split("/")[-1] or default_data["name"]
            symbol = metadata.get("symbol") or "UNK"
            
            # Clean up name if it's too long
            if len(name) > 20:
                name = symbol if symbol != "UNK" else name[:20]

            result = {"name": name, "symbol": symbol}
            
            # Cache it
            _metadata_cache[mint] = {"data": result, "ts": time.time()}
            return result
        else:
            logger.error(f"Helius RPC Error for {mint}: {data.get('error')}")
            return default_data

    except Exception as e:
        logger.error(f"Failed to fetch metadata for {mint}: {e}")
        return default_data
