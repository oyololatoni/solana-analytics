from contextlib import asynccontextmanager
from psycopg_pool import AsyncConnectionPool
from config import DATABASE_URL
from api import logger

# Global pool instance
pool: AsyncConnectionPool = None

async def init_db():
    global pool
    logger.info("Initializing async connection pool...")
    pool = AsyncConnectionPool(
        conninfo=DATABASE_URL,
        min_size=1,
        max_size=20,
        timeout=10,
        open=False
    )
    await pool.open()
    logger.info("Async pool initialized.")

async def close_db():
    if pool:
        logger.info("Closing async pool...")
        await pool.close()
        logger.info("Async pool closed.")

@asynccontextmanager
async def get_db_connection():
    if not pool:
        raise RuntimeError("Database pool not initialized")
    async with pool.connection() as conn:
        yield conn
