from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel
from api.db import get_db_connection
from typing import List, Optional, Literal
import requests
from config import HELIUS_API_KEY

router = APIRouter(prefix="/screener", tags=["screener"])

class FilterCriteria(BaseModel):
    metric: Literal['volume_24h', 'swap_count_24h', 'unique_makers_24h', 'price_change_24h', 'unique_growth', 'ratio', 'decline']
    condition: Literal['gt', 'lt']
    value: float

class ScreenerRequest(BaseModel):
    filters: List[FilterCriteria] = []
    source: Literal['local', 'external'] = 'local'
    query: Optional[str] = None
    sort_by: Literal['volume_24h', 'swap_count_24h', 'price_change_24h'] = 'volume_24h'
    sort_direction: Literal['asc', 'desc'] = 'desc'

class TokenResult(BaseModel):
    mint: str
    metric_value: float
    name: Optional[str] = None
    volume_24h: Optional[float] = 0
    swap_count_24h: Optional[int] = 0
    unique_makers_24h: Optional[int] = 0
    price_change_24h: Optional[float] = 0

@router.post("/filter")
async def filter_tokens(req: ScreenerRequest):
    """
    Filters tokens based on aggregated metrics.
    If source is local, it supports both database-level filters (volume/swaps)
    and phase-engine level filters (growth, ratio, decline).
    """
    if req.source == 'external':
        return await fetch_external_tokens(req)

    # Check if we have structural filters that require the phase engine
    structural_metrics = {'unique_growth', 'ratio', 'decline'}
    has_structural = any(f.metric in structural_metrics for f in req.filters)

    if has_structural:
        from api.phase_engine import analyze_all_tokens
        # Analyze all active tokens in DB
        tokens = await analyze_all_tokens(days=7)
        
        filtered = []
        for t in tokens:
            sig = t.get("signals", {})
            if not sig or sig.get("insufficient_data"):
                continue
                
            include = True
            for f in req.filters:
                val = None
                if f.metric == 'unique_growth': val = sig.get("unique_growth_rate")
                elif f.metric == 'ratio': val = sig.get("unique_to_swap_ratio")
                elif f.metric == 'decline': val = sig.get("decline_from_peak")
                elif f.metric == 'volume_24h': val = t.get("signals", {}).get("volume") # use engine's latest day vol
                elif f.metric == 'swap_count_24h': val = t.get("signals", {}).get("swap_count")
                
                if val is None:
                    continue # or skip?
                
                if f.condition == 'gt' and not (val > f.value): include = False
                elif f.condition == 'lt' and not (val < f.value): include = False
            
            if include:
                # Map to TokenResult format
                from config import get_token_name
                filtered.append({
                    "mint": t["mint"],
                    "name": t.get("name") or get_token_name(t["mint"]),
                    "volume_24h": sig.get("volume", 0),
                    "swap_count_24h": sig.get("swap_count", 0),
                    "unique_makers_24h": sig.get("unique_makers", 0),
                    "price_change_24h": sig.get("unique_growth_rate", 0) * 100, # use growth as a proxy for change
                    "metric_value": sig.get("unique_growth_rate", 0) # primary sort key if requested
                })
        
        # Sort
        reverse = (req.sort_direction == 'desc')
        filtered.sort(key=lambda x: x.get(req.sort_by, 0), reverse=reverse)
        return filtered[:50]

    # Fallback to direct DB query for non-structural filters (more efficient)
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # We will build a query that aggregates 24h stats for ALL tracked tokens
            # and then filters them.
            
            # Base query: Get 24h stats for all tokens
            query = """
                WITH stats AS (
                    SELECT 
                        token_mint,
                        COALESCE(SUM(amount), 0) as volume_24h,
                        COUNT(*) as swap_count_24h,
                        COUNT(DISTINCT wallet) as unique_makers_24h
                    FROM events
                    WHERE block_time > NOW() - INTERVAL '24 hours'
                    GROUP BY token_mint
                )
                SELECT * FROM stats
                WHERE 1=1
            """
            
            params = []
            
            # Dynamically add HAVING/WHERE clauses based on filters
            # Since we are selecting from a CTE, we can use WHERE on the columns
            
            for f in req.filters:
                if f.condition == 'gt':
                    query += f" AND {f.metric} > %s"
                elif f.condition == 'lt':
                    query += f" AND {f.metric} < %s"
                
                params.append(f.value)
            
            # Sorting
            sort_map = {
                "volume_24h": "volume_24h",
                "swap_count_24h": "swap_count_24h",
                "unique_makers_24h": "unique_makers_24h",
                 # price_change_24h not supported in local sql for now easily
            }
            sort_col = sort_map.get(req.sort_by, "volume_24h")
            query += f" ORDER BY {sort_col} {req.sort_direction.upper()} LIMIT 50"
            
            await cur.execute(query, tuple(params))
            rows = await cur.fetchall()
            
            results = []
            from config import get_token_name # Lazy import to avoid circular if any
            
            for row in rows:
                # row keys depends on the structure of `stats` CTE which is:
                # token_mint, volume_24h, swap_count_24h, unique_makers_24h
                mint = row[0]
                
                # We return the value of the *first* filter metric as the primary "value" to show,
                # or just volume if mixed.
                # Actually, let's just return all stats or the object.
                # For the UI simplicity, we might just list them.
                
                results.append({
                    "mint": mint,
                    "name": get_token_name(mint),
                    "volume_24h": float(row[1]),
                    "swap_count_24h": row[2],
                    "unique_makers_24h": row[3],
                    "price_change_24h": 0.0  # Placeholder for local tokens
                })
                
            return results

async def fetch_external_tokens(req: ScreenerRequest):
    """
    Fetches tokens from DexScreener (Boosted/Search) and filters them.
    If using boosted endpoint, requires a second call to get market data.
    """
    try:
        if req.query:
            # Search query -> returns pairs directly with volume data
            url = f"https://api.dexscreener.com/latest/dex/search?q={req.query}"
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            pairs = data.get("pairs", [])
        else:
            # Boosted tokens -> returns list of token metadata without volume stats
            # We need to fetch stats for these tokens separately
            url = "https://api.dexscreener.com/token-boosts/top/v1"
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            boosted_data = resp.json()
            
            # Extract token addresses (limit to top 30 to fit in one URL query)
            addresses = []
            for item in boosted_data:
                # Check chain
                if item.get("chainId") == "solana" or ("/solana/" in item.get("url", "")):
                    addr = item.get("tokenAddress")
                    if addr:
                        addresses.append(addr)
            
            addresses = addresses[:30]
            if not addresses:
                return []
                
            # Fetch pairs data for these tokens
            # API: https://api.dexscreener.com/latest/dex/tokens/addr1,addr2
            tokens_str = ",".join(addresses)
            stats_url = f"https://api.dexscreener.com/latest/dex/tokens/{tokens_str}"
            
            resp = requests.get(stats_url, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            pairs = data.get("pairs", [])

        # Process pairs (common logic for both paths)
        results = []
        for item in pairs:
            chain_id = item.get("chainId")
            if chain_id != "solana":
                continue

            try:
                vol_24h = float(item.get("volume", {}).get("h24", 0))
                
                txns = item.get("txns", {}).get("h24", 0)
                if isinstance(txns, dict):
                    swap_count_24h = txns.get("buys", 0) + txns.get("sells", 0)
                else:
                    swap_count_24h = int(txns)
                
                # unique makers not avail
                unique_makers_24h = 0 
                
                # price change
                price_change_24h = 0
                pc = item.get("priceChange")
                if pc: 
                     price_change_24h = float(pc.get("h24") or 0) 
                
                base_token = item.get("baseToken", {})
                mint = base_token.get("address")
                name = base_token.get("name")
                
                if not mint:
                    continue
                
                # Apply filters
                include = True
                for f in req.filters:
                    val = 0
                    if f.metric == 'volume_24h':
                        val = vol_24h
                    elif f.metric == 'swap_count_24h':
                        val = swap_count_24h
                    # ignore unique_makers check for external
                    
                    if f.condition == 'gt' and not (val > f.value):
                        include = False
                    elif f.condition == 'lt' and not (val < f.value):
                        include = False
                
                if include:
                    results.append({
                        "mint": mint,
                        "name": name,
                        "volume_24h": vol_24h,
                        "swap_count_24h": swap_count_24h,
                        "unique_makers_24h": unique_makers_24h,
                        "price_change_24h": price_change_24h
                    })

            except Exception as e:
                continue

        # Sort
        reverse = (req.sort_direction == 'desc')
        results.sort(key=lambda x: x.get(req.sort_by, 0), reverse=reverse)
        # Deduplicate by mint (pairs might return multiple pools for same token)
        seen = set()
        deduped = []
        for r in results:
            if r['mint'] not in seen:
                seen.add(r['mint'])
                deduped.append(r)
                
        return deduped[:50]

    except Exception as e:
        print(f"External Fetch Error: {e}")
        return []
