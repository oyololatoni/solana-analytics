from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import date
import psycopg
from psycopg_pool import ConnectionPool

from config import DATABASE_URL

app = FastAPI(title="Solana Analytics API")
templates = Jinja2Templates(directory="templates")

# Create a global connection pool (THIS is the speed fix)
pool = ConnectionPool(
    DATABASE_URL,
    min_size=1,
    max_size=5,
    timeout=10
)


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics/unique-makers")
def unique_makers(
    token: str = Query(...),
    day: date = Query(...)
):
    sql = """
    SELECT COUNT(DISTINCT wallet)
    FROM events
    WHERE token_mint = %s
      AND event_type = 'swap'
      AND block_time::date = %s;
    """

    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (token, day))
                result = cur.fetchone()[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "token": token,
        "date": str(day),
        "unique_makers": result
    }

