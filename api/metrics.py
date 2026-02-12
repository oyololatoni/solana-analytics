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
def ingestion_stats(limit: int = 24, source: str = "helius"):
    """
    Returns detailed ingestion statistics with breakdown of ignore reasons.
    Useful for debugging why transactions are being rejected.
    
    Args:
        limit: Number of recent records to return (default 24)
        source: Filter by ingestion source (default "helius")
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    created_at,
                    source,
                    events_received,
                    swaps_inserted,
                    swaps_ignored,
                    ignored_missing_fields,
                    ignored_no_swap_event,
                    ignored_no_tracked_tokens,
                    ignored_constraint_violation,
                    ignored_exception
                FROM ingestion_stats
                WHERE source = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (source, limit),
            )
            rows = cur.fetchall()
    
    return [
        {
            "timestamp": row[0].isoformat() if row[0] else None,
            "source": row[1],
            "events_received": row[2],
            "swaps_inserted": row[3],
            "swaps_ignored": row[4],
            "ignore_breakdown": {
                "missing_fields": row[5] or 0,
                "no_swap_event": row[6] or 0,
                "no_tracked_tokens": row[7] or 0,
                "constraint_violation": row[8] or 0,
                "exception": row[9] or 0,
            },
        }
        for row in rows
    ]


@router.get("/ingestion-health")
def ingestion_health():
    """
    Returns health metrics for the ingestion pipeline.
    Alerts on high ignore rates, exceptions, or missing data.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Last hour statistics
            cur.execute(
                """
                SELECT
                    COUNT(*) as records,
                    SUM(events_received) as total_events,
                    SUM(swaps_inserted) as total_inserted,
                    SUM(swaps_ignored) as total_ignored,
                    SUM(ignored_missing_fields) as missing_fields,
                    SUM(ignored_no_swap_event) as no_swap,
                    SUM(ignored_no_tracked_tokens) as no_tracked,
                    SUM(ignored_constraint_violation) as constraint,
                    SUM(ignored_exception) as exceptions,
                    MAX(created_at) as last_ingestion
                FROM ingestion_stats
                WHERE created_at > NOW() - INTERVAL '1 hour'
                """
            )
            
            row = cur.fetchone()
    
    if not row or row[0] == 0:
        return {
            "status": "no_data",
            "message": "No ingestion activity in the last hour",
        }
    
    records, total_events, total_inserted, total_ignored = row[0], row[1] or 0, row[2] or 0, row[3] or 0
    missing_fields, no_swap, no_tracked, constraint, exceptions = row[4] or 0, row[5] or 0, row[6] or 0, row[7] or 0, row[8] or 0
    last_ingestion = row[9]
    
    # Calculate rates
    ignore_rate = (total_ignored / total_events * 100) if total_events > 0 else 0
    exception_rate = (exceptions / total_ignored * 100) if total_ignored > 0 else 0
    
    # Health checks
    alerts = []
    if ignore_rate > 50:
        alerts.append(f"High ignore rate: {ignore_rate:.1f}%")
    if exception_rate > 10:
        alerts.append(f"High exception rate: {exception_rate:.1f}%")
    if exceptions > 100:
        alerts.append(f"Many exceptions in last hour: {exceptions}")
    
    status = "healthy" if len(alerts) == 0 else "warning"
    
    return {
        "status": status,
        "last_ingestion": last_ingestion.isoformat() if last_ingestion else None,
        "last_hour_summary": {
            "events_received": total_events,
            "swaps_inserted": total_inserted,
            "swaps_ignored": total_ignored,
            "ignore_rate_pct": round(ignore_rate, 2),
        },
        "ignore_breakdown": {
            "missing_fields": missing_fields,
            "no_swap_event": no_swap,
            "no_tracked_tokens": no_tracked,
            "constraint_violation": constraint,
            "exception": exceptions,
        },
        "alerts": alerts,
    }
