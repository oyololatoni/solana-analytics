from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from api import logger
from api.db import init_db, close_db, get_db_connection
from api.metrics import router as metrics_router
from api.webhooks import router as webhooks_router
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
def index():
    return """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Solana Analytics</title>
  <style>
    body {
      font-family: Arial, sans-serif;
      padding: 20px;
    }
    table {
      border-collapse: collapse;
      margin-top: 20px;
    }
    th, td {
      border: 1px solid #ccc;
      padding: 8px 12px;
      text-align: right;
    }
    th {
      background: #f4f4f4;
    }
    td:first-child, th:first-child {
      text-align: left;
    }
    button {
      padding: 6px 12px;
      cursor: pointer;
    }
    input {
      padding: 6px;
      width: 420px;
    }
  </style>
</head>
<body>
  <h1>Solana Token Analytics</h1>

  <label>
    Token mint:
    <input id="token" value="9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump" />
  </label>
  <button onclick="loadData()">Load</button>

  <table id="table" style="display:none;">
    <thead>
      <tr>
        <th>Day</th>
        <th>Unique Makers</th>
        <th>Swaps</th>
        <th>Volume</th>
      </tr>
    </thead>
    <tbody></tbody>
  </table>

  <script>
    async function loadData() {
      const token = document.getElementById("token").value;
      const res = await fetch(`/metrics/daily-summary?token=${token}`);
      const data = await res.json();

      const tbody = document.querySelector("#table tbody");
      tbody.innerHTML = "";

      for (const row of data) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${row.day}</td>
          <td>${row.unique_makers}</td>
          <td>${row.swaps}</td>
          <td>${row.volume}</td>
        `;
        tbody.appendChild(tr);
      }

      document.getElementById("table").style.display = "table";
    }
  </script>
</body>
</html>
"""
    
app.include_router(metrics_router)

app.include_router(webhooks_router)

