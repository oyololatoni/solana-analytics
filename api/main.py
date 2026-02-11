from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from api import logger
from api.db import init_db, close_db, get_db_connection
from api.metrics import router as metrics_router
from api.webhooks import router as webhooks_router
from api.analytics import router as analytics_router
from api.alerts import router as alerts_router
from api.screener import router as screener_router
from api.routers.features import router as features_router
from config import TRACKED_TOKENS, INGESTION_ENABLED

if not TRACKED_TOKENS:
    logger.error("TRACKED_TOKENS is empty — ingestion would discard everything")
    raise RuntimeError("TRACKED_TOKENS is empty")
    
logger.info(f"[BOOT] INGESTION_ENABLED = {INGESTION_ENABLED}")
logger.info(f"[BOOT] TRACKED_TOKENS = {TRACKED_TOKENS}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()

app = FastAPI(title="Solana Analytics", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/health")
async def health():
    health_status = {"status": "ok", "database": "disconnected"}
    try:
        async with get_db_connection() as conn:
            health_status["database"] = "connected"
    except Exception as e:
        logger.error(f"Health check DB failed: {e}")
        health_status["status"] = "error"
        health_status["error"] = str(e)
    
    return health_status

@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse("static/monitor.html")
    
app.include_router(metrics_router)
app.include_router(webhooks_router)
app.include_router(analytics_router)
app.include_router(alerts_router)
app.include_router(screener_router)
app.include_router(features_router, prefix="/features", tags=["features"])
