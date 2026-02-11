from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel
from api.db import get_db_connection
from typing import List, Optional

router = APIRouter(prefix="/alerts", tags=["alerts"])

class AlertCreate(BaseModel):
    token_mint: str
    metric: str
    condition: str
    value: float
    cooldown_minutes: int = 60

class AlertResponse(BaseModel):
    id: str
    token_mint: str
    metric: str
    condition: str
    value: float
    cooldown_minutes: int
    created_at: str

@router.post("/", response_model=AlertResponse)
async def create_alert(alert: AlertCreate):
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Validate metric
            if alert.metric not in ['volume_1h', 'swap_count_1h']:
                 raise HTTPException(status_code=400, detail="Invalid metric. Supported: volume_1h, swap_count_1h")
            
            # Validate condition
            if alert.condition not in ['gt', 'lt']:
                 raise HTTPException(status_code=400, detail="Invalid condition. Supported: gt, lt")

            await cur.execute(
                """
                INSERT INTO alerts (token_mint, metric, condition, value, cooldown_minutes)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, created_at
                """,
                (alert.token_mint, alert.metric, alert.condition, alert.value, alert.cooldown_minutes)
            )
            row = await cur.fetchone()
            if not row:
                raise HTTPException(status_code=500, detail="Failed to create alert")
            
            alert_id, created_at = row
            await conn.commit()
            
            return AlertResponse(
                id=str(alert_id),
                token_mint=alert.token_mint,
                metric=alert.metric,
                condition=alert.condition,
                value=alert.value,
                cooldown_minutes=alert.cooldown_minutes,
                created_at=str(created_at)
            )

@router.get("/", response_model=List[AlertResponse])
async def list_alerts():
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, token_mint, metric, condition, value, cooldown_minutes, created_at
                FROM alerts
                ORDER BY created_at DESC
                """
            )
            rows = await cur.fetchall()
            
            return [
                AlertResponse(
                    id=str(row[0]),
                    token_mint=row[1],
                    metric=row[2],
                    condition=row[3],
                    value=float(row[4]),
                    cooldown_minutes=row[5],
                    created_at=str(row[6])
                )
                for row in rows
            ]
# ---------------------------------------------------------------------------
# Phase Transition Alerts
# ---------------------------------------------------------------------------

async def check_phase_transitions():
    """
    Checks for recent transitions into High-EV phases:
    - POST_DESTRUCTIVE
    - ACCELERATION
    """
    from config import get_token_name
    
    high_ev_phases = {"POST_DESTRUCTIVE", "ACCELERATION"}
    alerts_triggered = []

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            # Get all tokens that have at least 2 history entries
            # We look for tokens where the LATEST entry is High-EV
            # and the PREVIOUS entry was DIFFERENT.
            await cur.execute(
                """
                WITH RankedHistory AS (
                    SELECT 
                        token_mint, phase, ev_score, recorded_at,
                        ROW_NUMBER() OVER (PARTITION BY token_mint ORDER BY recorded_at DESC) as rn
                    FROM token_scores_history
                )
                SELECT 
                    t1.token_mint, 
                    t1.phase as current_phase, 
                    t1.ev_score,
                    t2.phase as prev_phase
                FROM RankedHistory t1
                JOIN RankedHistory t2 ON t1.token_mint = t2.token_mint AND t2.rn = 2
                WHERE t1.rn = 1
                  AND t1.phase IN ('POST_DESTRUCTIVE', 'ACCELERATION')
                  AND t1.phase != t2.phase
                  AND t1.recorded_at > NOW() - INTERVAL '1 hour' -- Only recent
                """
            )
            rows = await cur.fetchall()

            for row in rows:
                mint, current, ev, prev = row
                name = get_token_name(mint)
                
                msg = f"🚀 ALERT: {name} ({mint[:4]}..) entered {current} (EV: {ev}). Prev: {prev}"
                print(msg) # Log to stdout for local dev
                alerts_triggered.append(msg)
                
                # TODO: Slack Webhook Integration
                # if os.environ.get("SLACK_WEBHOOK_URL"):
                #     requests.post(url, json={"text": msg})
    
    return alerts_triggered

@router.post("/check")
async def trigger_phase_check():
    """Manually trigger phase transition check."""
    alerts = await check_phase_transitions()
    return {"status": "ok", "alerts_sent": len(alerts), "messages": alerts}
