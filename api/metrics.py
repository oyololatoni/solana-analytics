from fastapi import APIRouter
from api.db import get_db_connection

router = APIRouter(prefix="/metrics", tags=["metrics"])

@router.get("/unique-makers")
async def unique_makers(token: str):
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                  DATE(block_time) AS day,
                  COUNT(DISTINCT wallet) AS value
                FROM events
                WHERE token_mint = %s
                GROUP BY day
                ORDER BY day
                """,
                (token,),
            )
            rows = await cur.fetchall()

    return [{"day": str(row[0]), "value": row[1]} for row in rows]


@router.get("/swaps")
async def swaps_per_day(token: str):
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                  DATE(block_time) AS day,
                  COUNT(*) AS value
                FROM events
                WHERE token_mint = %s
                GROUP BY day
                ORDER BY day
                """,
                (token,),
            )
            rows = await cur.fetchall()

    return [{"day": str(row[0]), "value": row[1]} for row in rows]


@router.get("/volume")
async def volume_per_day(token: str):
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                  DATE(block_time) AS day,
                  SUM(amount) AS value
                FROM events
                WHERE token_mint = %s
                GROUP BY day
                ORDER BY day
                """,
                (token,),
            )
            rows = await cur.fetchall()

    return [{"day": str(row[0]), "value": row[1]} for row in rows]

@router.get("/daily-summary")
async def daily_summary(token: str):
    """
    Returns one row per day with:
    - unique makers
    - swap count
    - total volume
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                  day,
                  COUNT(DISTINCT wallet) AS unique_makers,
                  COUNT(*) AS swaps,
                  SUM(amount) AS volume
                FROM (
                  SELECT
                    DATE(block_time) AS day,
                    wallet,
                    amount
                  FROM events
                  WHERE token_mint = %s
                ) t
                GROUP BY day
                ORDER BY day;
                """,
                (token,),
            )
            rows = await cur.fetchall()

    return [
        {
            "day": str(r[0]),
            "unique_makers": r[1],
            "swaps": r[2],
            "volume": r[3],
        }
        for r in rows
    ]

@router.get("/ingestion-stats")
async def ingestion_stats():
    """
    Returns recent ingestion performance metrics.
    """
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                  source,
                  events_received,
                  swaps_inserted,
                  swaps_ignored,
                  ignored_missing_fields,
                  ignored_no_swap_event,
                  ignored_no_tracked_tokens,
                  ignored_constraint_violation,
                  ignored_exception,
                  created_at
                FROM ingestion_stats
                ORDER BY created_at DESC
                LIMIT 10;
                """
            )
            rows = await cur.fetchall()

    return [
        {
            "source": r[0],
            "events_received": r[1],
            "swaps_inserted": r[2],
            "swaps_ignored": r[3],
            "details": {
                "missing_fields": r[4],
                "no_swap_event": r[5],
                "no_tracked_tokens": r[6],
                "constraint_violations": r[7],
                "exceptions": r[8],
            },
            "timestamp": str(r[9])
        }
        for r in rows
    ]
