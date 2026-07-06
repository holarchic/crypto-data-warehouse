import os
from contextlib import asynccontextmanager
from datetime import datetime
from io import BytesIO

import export
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
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
    query = f"SELECT * FROM {_candle_table(timeframe)} WHERE {where} ORDER BY timestamp DESC LIMIT %(limit)s"

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
        rows = conn.execute(
            "SELECT DISTINCT exchange FROM collection_status "
            "WHERE active = TRUE AND timeframe <> ALL(%(loop_tfs)s) ORDER BY exchange",
            {"loop_tfs": list(LOOP_TIMEFRAMES)},
        ).fetchall()
    return [row["exchange"] for row in rows]


@app.get("/candles/timeframes")
def get_timeframes():
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT timeframe FROM collection_status "
            "WHERE active = TRUE AND timeframe <> ALL(%(loop_tfs)s) ORDER BY timeframe",
            {"loop_tfs": list(LOOP_TIMEFRAMES)},
        ).fetchall()
    return [row["timeframe"] for row in rows]


@app.get("/candles/symbols")
def get_symbols(exchange: str | None = None):
    # Exclude the per-loop heartbeat sentinel rows (LOOP_TIMEFRAMES) — this is candle metadata.
    query = "SELECT DISTINCT asset_id, symbol, exchange FROM collection_status WHERE active = TRUE AND timeframe <> ALL(%(loop_tfs)s)"
    params: dict = {"loop_tfs": list(LOOP_TIMEFRAMES)}
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
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200,
    "4h": 14400, "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800,
}

# Candle-table routing, MIRRORED from collector/exchanges.yaml (config.load_candle_tables); the api/
# container can't import collector/, same as TIMEFRAME_INTERVALS above and LOOP_TIMEFRAMES below.
# Only the NON-default routes live here (default is `candles`); a drift test pins this against config
# so the two mirrors can't diverge. `1m` → candles_1m (a separate hypertable on the ts_1m volume,
# T-015). `/candles` and `/status/gaps` route their FROM clause through _candle_table so a query for
# `1m` reads candles_1m — else the API advertises 1m (from collection_status) while serving 0 rows.
CANDLE_TABLE_BY_TIMEFRAME = {"1m": "candles_1m"}


def _candle_table(timeframe: str) -> str:
    """The candle storage table for a timeframe (default `candles`). Value is a fixed literal from a
    closed map, safe to inline into a query identifier (psycopg can't bind an identifier as a value)."""
    return CANDLE_TABLE_BY_TIMEFRAME.get(timeframe, "candles")

# Per-loop heartbeat pseudo-timeframes — funding, OI, the liquidation-WS, the premium-poll, and the
# HL candle-WS sentinel (the keys of collector/config.py LOOP_INTERVALS). The API is
# a separate container and cannot import collector/, so the
# names are mirrored here (as TIMEFRAME_INTERVALS above is). Only the NAMES are needed —
# the staleness intervals are the monitor's concern. CONTRACT: every candle-facing
# collection_status query in this module excludes these sentinel rows; loop liveness is
# surfaced by the monitor, not this API.
LOOP_TIMEFRAMES = ("funding", "oi", "liq_ws", "premium", "candle_ws")


@app.get("/status")
def get_status(exchange: str | None = None, timeframe: str | None = None):
    # Candle-only: exclude the per-loop heartbeat sentinel rows (LOOP_TIMEFRAMES) — loop
    # liveness is the monitor's surface, and its staleness threshold differs from this endpoint's.
    conditions = ["active = TRUE", "timeframe <> ALL(%(loop_tfs)s)"]
    params: dict = {"loop_tfs": list(LOOP_TIMEFRAMES)}
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
            FROM {_candle_table(timeframe)}
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


@app.get("/export/parquet")
def export_parquet(
    table: str,
    start: datetime,
    end: datetime,
    asset_id: str | None = None,
):
    # Forward-feed parquet export for the backtester's T-031 ingest (T-011.1). Thin wrapper: validate →
    # parameterized range query → api/export.py owns the byte-contract (schema + NULL→NaN). `table`,
    # its columns, and `ts_col` come from the FEED_SPECS literal (never user input), so they are safe
    # to interpolate — same trust model as `_candle_table`; only values are parameterized.
    spec = export.FEED_SPECS.get(table)
    if spec is None:
        raise HTTPException(status_code=400, detail=f"unknown table {table!r}; valid: {sorted(export.FEED_SPECS)}")
    # TIMESTAMPTZ boundaries: a naive datetime would be read against the session tz and silently shift
    # the window (a byte-contract hazard) — require tz-aware UTC.
    if start.tzinfo is None or end.tzinfo is None:
        raise HTTPException(status_code=400, detail="start and end must be timezone-aware (UTC)")
    if start >= end:
        raise HTTPException(status_code=400, detail="start must be < end")

    cols = ", ".join(export.select_columns(table))
    # Half-open [start, end): `< end` (NOT `<= end`) so consecutive windows never double-count the
    # seam row — the backtester's row-content hash is duplicate-sensitive.
    conditions = [f"{spec.ts_col} >= %(start)s", f"{spec.ts_col} < %(end)s"]
    params: dict = {"start": start, "end": end, "limit": export.EXPORT_MAX_ROWS + 1}
    if spec.venue_col is not None:
        conditions.append(f"{spec.venue_col} = %(venue)s")
        params["venue"] = spec.venue
    if asset_id is not None:
        conditions.append("asset_id = %(asset_id)s")
        params["asset_id"] = asset_id
    where = " AND ".join(conditions)
    query = f"SELECT {cols} FROM {spec.db_table} WHERE {where} ORDER BY {spec.ts_col} LIMIT %(limit)s"

    with pool.connection() as conn:
        rows = conn.execute(query, params).fetchall()

    # Reject (don't truncate) an over-cap window — a truncated parquet is a broken feed.
    if len(rows) > export.EXPORT_MAX_ROWS:
        raise HTTPException(
            status_code=413,
            detail=f"window too large (> {export.EXPORT_MAX_ROWS} rows) — narrow the range or use the bulk export script",
        )
    # Zero-row window → 204, NOT an empty parquet: the backtester's structural hash rejects an empty feed
    # ("empty feed (no rows)"), so cc must not emit a hashable-but-empty file. 204 = "no data, skip".
    if not rows:
        return Response(status_code=204)

    data = export.rows_to_parquet(table, rows)
    filename = f"{table}_{start.date()}_{end.date()}.parquet"
    return StreamingResponse(
        BytesIO(data),
        media_type="application/vnd.apache.parquet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
