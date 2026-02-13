"""
Solana Analytics API
====================
Main application entry point. Mounts all routers and serves static UI.
"""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.api.routers import v2, alerts, metrics
from app.core.db import init_db, close_db
from app.core.prices import router as prices_router
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app.main")

app = FastAPI(title="Solana Analytics API", version="2.1.0")
# ----- Mount Routers -----
# V2 analytics (snapshot-driven) — serves /analytics/*
app.include_router(v2.router)

# Alerts — serves /alerts/*
app.include_router(alerts.router)

# Metrics — serves /metrics/*
app.include_router(metrics.router)

# Prices — serves /prices/*
app.include_router(prices_router)


# ----- Static UI -----
UI_DIR = os.path.join(os.path.dirname(__file__), "ui")

@app.get("/", response_class=FileResponse)
async def serve_root():
    return FileResponse(os.path.join(UI_DIR, "monitor.html"))

@app.get("/details.html", response_class=FileResponse)
async def serve_details():
    return FileResponse(os.path.join(UI_DIR, "details.html"))

@app.get("/charts.html", response_class=FileResponse)
async def serve_charts():
    return FileResponse(os.path.join(UI_DIR, "charts.html"))


# ----- Lifecycle Events -----
@app.on_event("startup")
async def startup():
    await init_db()
    logger.info("Application startup complete.")

@app.on_event("shutdown")
async def shutdown():
    await close_db()
    logger.info("Application shutdown complete.")


# ----- Health Check -----
@app.get("/health")
async def health_check():
    return {"status": "ok", "engine_version": "v2"}
