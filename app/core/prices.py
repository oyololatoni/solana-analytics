"""
Price Router
============
Endpoints for fetching and updating token prices via Jupiter.
"""
from fastapi import APIRouter
from app.core.db import get_db_connection
from app.core.jupiter import get_token_prices, get_single_price
import logging

logger = logging.getLogger("core.prices")
router = APIRouter(prefix="/prices", tags=["prices"])


@router.get("/refresh")
async def refresh_all_prices():
    """
    Fetch current prices for all tracked tokens from Jupiter,
    update tokens table, record price snapshots, and return results.
    """
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                # Get all active token mints
                await cur.execute("""
                    SELECT address FROM tokens
                    WHERE lifecycle_stage NOT IN ('EXPIRED')
                    OR lifecycle_stage IS NULL
                """)
                rows = await cur.fetchall()
                mints = [r[0] for r in rows]
        
        if not mints:
            return {"status": "ok", "message": "No active tokens", "count": 0}
        
        # Batch fetch from Jupiter
        prices = await get_token_prices(mints)
        
        updated = 0
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                for mint, info in prices.items():
                    price = info.get("price", 0)
                    if price <= 0:
                        continue
                    
                    liquidity = info.get("liquidity") or 0
                    
                    # Update tokens table
                    await cur.execute("""
                        UPDATE tokens SET
                            current_price = %s,
                            current_liquidity_usd = %s,
                            price_updated_at = NOW(),
                            baseline_price = COALESCE(baseline_price, %s),
                            peak_price = GREATEST(COALESCE(peak_price, 0), %s)
                        WHERE address = %s
                    """, (price, liquidity, price, price, mint))
                    
                    # Insert price snapshot for charting
                    await cur.execute("""
                        INSERT INTO price_snapshots (token_mint, price_usd, liquidity_usd)
                        VALUES (%s, %s, %s)
                    """, (mint, price, liquidity))
                    
                    updated += 1
                
                await conn.commit()
        
        return {
            "status": "ok",
            "updated": updated,
            "total_mints": len(mints),
            "prices": {m: p for m, p in prices.items() if p.get("price", 0) > 0}
        }
    
    except Exception as e:
        logger.error(f"Price refresh error: {e}")
        return {"status": "error", "error": str(e)}


@router.get("/{mint}")
async def get_price(mint: str):
    """Get current price and stats for a single token."""
    try:
        # Fetch live price from Jupiter
        live = await get_single_price(mint)
        
        # Get stored price data from DB
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT baseline_price, current_price, peak_price,
                           current_liquidity_usd, price_updated_at
                    FROM tokens WHERE address = %s
                """, (mint,))
                row = await cur.fetchone()
                
                # Get price history (last 48 snapshots for charting)
                await cur.execute("""
                    SELECT price_usd, liquidity_usd, recorded_at
                    FROM price_snapshots
                    WHERE token_mint = %s
                    ORDER BY recorded_at DESC
                    LIMIT 48
                """, (mint,))
                history = await cur.fetchall()
        
        baseline = float(row[0]) if row and row[0] else None
        current = live.get("price", 0)
        peak = float(row[2]) if row and row[2] else 0
        
        # Calculate multipliers
        multiplier = round(current / baseline, 2) if baseline and baseline > 0 else None
        peak_mult = round(peak / baseline, 2) if baseline and baseline > 0 else None
        distance_5x = round((baseline * 5 - current) / current * 100, 1) if current > 0 and baseline else None
        
        return {
            "mint": mint,
            "current_price": current,
            "baseline_price": baseline,
            "peak_price": max(peak, current),
            "liquidity_usd": live.get("liquidity"),
            "multiplier": multiplier,
            "peak_multiplier": peak_mult,
            "distance_to_5x": distance_5x,
            "price_updated_at": str(row[4]) if row and row[4] else None,
            "history": [
                {
                    "price": float(h[0]),
                    "liquidity": float(h[1]) if h[1] else 0,
                    "time": str(h[2])
                }
                for h in reversed(history)
            ] if history else []
        }
    
    except Exception as e:
        logger.error(f"Price lookup error for {mint}: {e}")
        return {"mint": mint, "error": str(e)}
