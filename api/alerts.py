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
