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
    pool = ConnectionPool(
        build_dsn(), min_size=2, max_size=10,
        kwargs={"row_factory": dict_row, "options": "-c statement_timeout=30000"},
    )
    pool.wait()
    yield
    pool.close()


app = FastAPI(title="candle-collector", version="0.7.0", lifespan=lifespan)


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
        rows = conn.execute(
            "SELECT DISTINCT timeframe FROM collection_status "
            "WHERE active = TRUE AND timeframe <> ALL(%(loop_tfs)s) ORDER BY timeframe",
            {"loop_tfs": list(LOOP_INTERVALS)},
        ).fetchall()
    return [row["timeframe"] for row in rows]


@app.get("/candles/symbols")
def get_symbols(exchange: str | None = None):
    # Exclude the funding/OI loop heartbeat sentinel rows — this is candle metadata.
    query = "SELECT DISTINCT asset_id, symbol, exchange FROM collection_status WHERE active = TRUE AND timeframe <> ALL(%(loop_tfs)s)"
    params: dict = {"loop_tfs": list(LOOP_INTERVALS)}
    if exchange is not None:
        query += " AND exchange = %(exchange)s"
        params["exchange"] = exchange
    query += " ORDER BY asset_id, exchange"
    with pool.connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return rows


# ---------------------------------------------------------------------------
# Funding rate endpoints
# ---------------------------------------------------------------------------


@app.get("/funding")
def get_funding(
    exchange: str,
    asset_id: str,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(default=1000, le=5000),
):
    conditions = ["exchange = %(exchange)s", "asset_id = %(asset_id)s"]
    params: dict = {"exchange": exchange, "asset_id": asset_id, "limit": limit}

    if start is not None:
        conditions.append("timestamp >= %(start)s")
        params["start"] = start
    if end is not None:
        conditions.append("timestamp <= %(end)s")
        params["end"] = end

    where = " AND ".join(conditions)
    query = f"SELECT * FROM funding_rates WHERE {where} ORDER BY timestamp DESC LIMIT %(limit)s"

    with pool.connection() as conn:
        rows = conn.execute(query, params).fetchall()

    for row in rows:
        if row.get("timestamp"):
            row["timestamp"] = row["timestamp"].isoformat()
    return rows


@app.get("/funding/latest")
def get_funding_latest(asset_id: str):
    query = """
        SELECT DISTINCT ON (exchange) exchange, asset_id, symbol, timestamp, rate, predicted_rate, interval_hours
        FROM funding_rates
        WHERE asset_id = %(asset_id)s
        ORDER BY exchange, timestamp DESC
    """
    with pool.connection() as conn:
        rows = conn.execute(query, {"asset_id": asset_id}).fetchall()

    for row in rows:
        if row.get("timestamp"):
            row["timestamp"] = row["timestamp"].isoformat()
    return rows


# ---------------------------------------------------------------------------
# Open interest endpoints
# ---------------------------------------------------------------------------


@app.get("/oi")
def get_oi(
    exchange: str,
    asset_id: str,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(default=1000, le=5000),
):
    conditions = ["exchange = %(exchange)s", "asset_id = %(asset_id)s"]
    params: dict = {"exchange": exchange, "asset_id": asset_id, "limit": limit}

    if start is not None:
        conditions.append("timestamp >= %(start)s")
        params["start"] = start
    if end is not None:
        conditions.append("timestamp <= %(end)s")
        params["end"] = end

    where = " AND ".join(conditions)
    query = f"SELECT * FROM open_interest WHERE {where} ORDER BY timestamp DESC LIMIT %(limit)s"

    with pool.connection() as conn:
        rows = conn.execute(query, params).fetchall()

    for row in rows:
        if row.get("timestamp"):
            row["timestamp"] = row["timestamp"].isoformat()
    return rows


@app.get("/oi/latest")
def get_oi_latest(asset_id: str):
    query = """
        SELECT DISTINCT ON (exchange) exchange, asset_id, symbol, timestamp, open_interest, oi_value_usd
        FROM open_interest
        WHERE asset_id = %(asset_id)s
        ORDER BY exchange, timestamp DESC
    """
    with pool.connection() as conn:
        rows = conn.execute(query, {"asset_id": asset_id}).fetchall()

    for row in rows:
        if row.get("timestamp"):
            row["timestamp"] = row["timestamp"].isoformat()
    return rows


# ---------------------------------------------------------------------------
# Status / health monitoring endpoints
# ---------------------------------------------------------------------------

TIMEFRAME_INTERVALS = {
    "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200,
    "4h": 14400, "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800,
}

# Funding/OI per-loop heartbeat pseudo-timeframes. MIRROR of collector/config.py
# LOOP_INTERVALS — the API is a separate container and cannot import collector/
# (same reason TIMEFRAME_INTERVALS above is duplicated rather than imported). Keep aligned.
# CONTRACT: every candle-facing collection_status query (the three endpoints below)
# excludes these sentinel rows. Loop liveness is surfaced by the monitor, not this API.
LOOP_INTERVALS = {"funding": 3600, "oi": 300}


@app.get("/status")
def get_status(exchange: str | None = None, timeframe: str | None = None):
    # Candle-only: exclude funding/OI loop heartbeat sentinel rows (loop liveness is
    # the monitor's surface, and its staleness threshold differs from this endpoint's).
    conditions = ["active = TRUE", "timeframe <> ALL(%(loop_tfs)s)"]
    params: dict = {"loop_tfs": list(LOOP_INTERVALS)}
    if exchange is not None:
        conditions.append("exchange = %(exchange)s")
        params["exchange"] = exchange
    if timeframe is not None:
        conditions.append("timeframe = %(timeframe)s")
        params["timeframe"] = timeframe

    where = " AND ".join(conditions)
    query = f"""
        SELECT exchange, symbol, timeframe, last_success, error_count, last_error,
               EXTRACT(EPOCH FROM NOW() - last_success) AS seconds_since_success
        FROM collection_status
        WHERE {where}
        ORDER BY exchange, timeframe, symbol
    """

    compression_sql = """
        SELECT ht.hypertable_name,
               s.total_chunks,
               s.compressed_chunks
        FROM timescaledb_information.hypertables ht
        CROSS JOIN LATERAL hypertable_compression_stats(ht.hypertable_name) s
        WHERE ht.hypertable_schema = 'public'
        ORDER BY ht.hypertable_name
    """

    with pool.connection() as conn:
        rows = conn.execute(query, params).fetchall()
        db_size = conn.execute("SELECT pg_database_size(current_database()) AS size_bytes").fetchone()
        try:
            compression_rows = conn.execute(compression_sql).fetchall()
        except Exception:
            compression_rows = []

    # Compute is_stale flag (candle TFs only — loop rows are excluded above).
    for row in rows:
        interval_s = TIMEFRAME_INTERVALS.get(row["timeframe"])
        since = row.get("seconds_since_success")
        row["is_stale"] = interval_s is not None and (since is None or since > 2 * interval_s)
        if row.get("last_success") and hasattr(row["last_success"], "isoformat"):
            row["last_success"] = row["last_success"].isoformat()

    compression = {
        r["hypertable_name"]: {
            "total_chunks": r.get("total_chunks", 0),
            "compressed_chunks": r.get("compressed_chunks", 0),
        }
        for r in compression_rows
    }

    return {
        "collectors": rows,
        "total": len(rows),
        "stale": sum(1 for r in rows if r["is_stale"]),
        "db_size_bytes": db_size["size_bytes"] if db_size else None,
        "compression": compression,
    }


@app.get("/status/gaps")
def get_status_gaps(
    exchange: str,
    timeframe: str,
    asset_id: str | None = None,
    lookback: int = Query(default=10, ge=1, le=1000),
):
    interval_s = TIMEFRAME_INTERVALS.get(timeframe)
    if interval_s is None:
        return {"error": f"Unknown timeframe: {timeframe}", "gaps": []}

    lookback_s = lookback * interval_s

    conditions = [
        "exchange = %(exchange)s",
        "timeframe = %(timeframe)s",
        "timestamp > NOW() - INTERVAL '1 second' * %(lookback_s)s",
    ]
    params: dict = {
        "exchange": exchange,
        "timeframe": timeframe,
        "lookback_s": lookback_s,
        "interval_s": interval_s,
    }

    if asset_id is not None:
        conditions.append("asset_id = %(asset_id)s")
        params["asset_id"] = asset_id

    where = " AND ".join(conditions)
    query = f"""
        WITH consecutive AS (
            SELECT symbol, timestamp,
                   LEAD(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) AS next_ts
            FROM candles
            WHERE {where}
        )
        SELECT symbol, timestamp AS gap_after, next_ts AS gap_before,
               EXTRACT(EPOCH FROM next_ts - timestamp) / %(interval_s)s - 1 AS missing_count
        FROM consecutive
        WHERE next_ts IS NOT NULL
          AND EXTRACT(EPOCH FROM next_ts - timestamp) > %(interval_s)s * 1.5
        ORDER BY symbol, timestamp
    """

    with pool.connection() as conn:
        rows = conn.execute(query, params).fetchall()

    for row in rows:
        for key in ("gap_after", "gap_before"):
            if row.get(key) and hasattr(row[key], "isoformat"):
                row[key] = row[key].isoformat()

    return {"exchange": exchange, "timeframe": timeframe, "gaps": rows, "total": len(rows)}
