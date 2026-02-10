from fastapi import APIRouter
from api.db import get_conn

router = APIRouter(prefix="/metrics", tags=["metrics"])

@router.get("/unique-makers")
def unique_makers(token: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
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
            rows = cur.fetchall()

    return [{"day": str(day), "value": value} for day, value in rows]


@router.get("/swaps")
def swaps_per_day(token: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
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
            rows = cur.fetchall()

    return [{"day": str(day), "value": value} for day, value in rows]


@router.get("/volume")
def volume_per_day(token: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
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
            rows = cur.fetchall()

    return [{"day": str(day), "value": value} for day, value in rows]

@router.get("/daily-summary")
def daily_summary(token: str):
    """
    Returns one row per day with:
    - unique makers
    - swap count
    - total volume
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
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
            rows = cur.fetchall()

    return [
        {
            "day": str(day),
            "unique_makers": unique_makers,
            "swaps": swaps,
            "volume": volume,
        }
        for day, unique_makers, swaps, volume in rows
    ]
@router.get("/ingestion-stats")
def ingestion_stats():
    """
    Returns recent ingestion performance metrics.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
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
            rows = cur.fetchall()

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
