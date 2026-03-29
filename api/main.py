import os
from contextlib import asynccontextmanager
from datetime import datetime

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

load_dotenv()


def build_dsn():
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "db")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "candle_collector")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


pool: ConnectionPool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = ConnectionPool(build_dsn(), min_size=2, max_size=10, kwargs={"row_factory": dict_row})
    pool.wait()
    yield
    pool.close()


app = FastAPI(title="candle-collector", version="0.3.0", lifespan=lifespan)


@app.get("/health")
def health():
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/candles")
def get_candles(
    exchange: str,
    asset_id: str,
    timeframe: str,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(default=1000, le=5000),
):
    conditions = ["exchange = %(exchange)s", "asset_id = %(asset_id)s", "timeframe = %(timeframe)s"]
    params: dict = {"exchange": exchange, "asset_id": asset_id, "timeframe": timeframe, "limit": limit}

    if start is not None:
        conditions.append("timestamp >= %(start)s")
        params["start"] = start
    if end is not None:
        conditions.append("timestamp <= %(end)s")
        params["end"] = end

    where = " AND ".join(conditions)
    query = f"SELECT * FROM candles WHERE {where} ORDER BY timestamp DESC LIMIT %(limit)s"

    with pool.connection() as conn:
        rows = conn.execute(query, params).fetchall()

    # Convert timestamps to ISO strings for JSON serialization
    for row in rows:
        if row.get("timestamp"):
            row["timestamp"] = row["timestamp"].isoformat()
    return rows


@app.get("/candles/exchanges")
def get_exchanges():
    with pool.connection() as conn:
        rows = conn.execute("SELECT DISTINCT exchange FROM collection_status WHERE active = TRUE ORDER BY exchange").fetchall()
    return [row["exchange"] for row in rows]


@app.get("/candles/timeframes")
def get_timeframes():
    with pool.connection() as conn:
        rows = conn.execute("SELECT DISTINCT timeframe FROM collection_status WHERE active = TRUE ORDER BY timeframe").fetchall()
    return [row["timeframe"] for row in rows]


@app.get("/candles/symbols")
def get_symbols(exchange: str | None = None):
    query = "SELECT DISTINCT asset_id, symbol, exchange FROM collection_status WHERE active = TRUE"
    params: dict = {}
    if exchange is not None:
        query += " AND exchange = %(exchange)s"
        params["exchange"] = exchange
    query += " ORDER BY asset_id, exchange"
    with pool.connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return rows
