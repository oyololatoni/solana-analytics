"""
Alerts Router
=============
Endpoints for creating, listing, and checking alerts.
"""
from fastapi import APIRouter
from app.core.db import get_db_connection
import logging

logger = logging.getLogger("api.alerts")
router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("/")
async def list_alerts():
    """List all active alerts."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT id, token_mint, metric, condition, value, cooldown_minutes, created_at
                    FROM alerts
                    WHERE active = TRUE
                    ORDER BY created_at DESC
                """)
                rows = await cur.fetchall()
                return [
                    {
                        "id": r[0],
                        "token_mint": r[1],
                        "metric": r[2],
                        "condition": r[3],
                        "value": float(r[4]) if r[4] else 0,
                        "cooldown_minutes": r[5],
                        "created_at": r[6].isoformat() if r[6] else None,
                    }
                    for r in rows
                ]
    except Exception as e:
        logger.error(f"Error listing alerts: {e}")
        return []


@router.post("/")
async def create_alert(payload: dict):
    """Create a new alert."""
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO alerts (token_mint, metric, condition, value, cooldown_minutes)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    payload["token_mint"],
                    payload["metric"],
                    payload["condition"],
                    payload["value"],
                    payload.get("cooldown_minutes", 60),
                ))
                alert_id = (await cur.fetchone())[0]
                await conn.commit()
                return {"id": alert_id, "status": "created"}
    except Exception as e:
        logger.error(f"Error creating alert: {e}")
        return {"error": str(e)}


@router.post("/check")
async def check_alerts():
    """Check for phase transitions and trigger alerts."""
    try:
        # For now, return a simple check result
        # TODO: Implement actual phase transition detection
        return {"alerts_sent": 0, "message": "No transitions detected"}
    except Exception as e:
        logger.error(f"Error checking alerts: {e}")
        return {"error": str(e), "alerts_sent": 0}
