"""
Metrics Router
==============
Endpoints for ingestion stats and operational metrics.
"""
from fastapi import APIRouter
from app.core.db import get_db_connection
import logging

logger = logging.getLogger("api.metrics")
router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/ingestion-stats")
async def get_ingestion_stats():
    """
    Returns last 10 ingestion stat records for the operations dashboard.
    """
    try:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT 
                        source, events_received, swaps_inserted, swaps_ignored,
                        ignored_missing_fields, ignored_no_swap_event,
                        ignored_no_tracked_tokens, ignored_constraint_violation,
                        ignored_exception, created_at
                    FROM ingestion_stats
                    ORDER BY created_at DESC
                    LIMIT 10
                """)
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
                            "constraint_violation": r[7],
                            "exceptions": r[8],
                        },
                        "timestamp": r[9].isoformat() if r[9] else None,
                    }
                    for r in rows
                ]
    except Exception as e:
        logger.error(f"Error fetching ingestion stats: {e}")
        return []
