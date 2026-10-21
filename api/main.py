import os
import tomllib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import config
import export
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

load_dotenv()

# pyproject.toml holds the one release version. It sits one directory above this file both in the
# repo and in the image (/app/main.py, /pyproject.toml). A missing file stops the import, so the API
# never reports a wrong version.
RELEASE_VERSION = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())["project"]["version"]


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


app = FastAPI(title="candle-collector", version=RELEASE_VERSION, lifespan=lifespan)


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


@app.get("/liquidations")
def get_liquidations(
    venue: str | None = None,
    asset_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(default=1000, ge=1, le=5000),
):
    # The liquidations read/monitoring endpoint (peer of /oi, /funding). All filters optional — a
    # coin-agnostic recent-events view is the useful monitoring default ("is the HL tape landing?"). The
    # no-filter path is a bounded recent read: TimescaleDB serves ORDER BY timestamp DESC LIMIT from the
    # newest time-chunk(s), and the pool's statement_timeout caps it. `liquidations` keys on `venue` (not
    # `exchange`). The bulk backtester path is /export/parquet?table=hl_liquidations.
    conditions = []
    params: dict = {"limit": limit}
    if venue is not None:
        conditions.append("venue = %(venue)s")
        params["venue"] = venue
    if asset_id is not None:
        conditions.append("asset_id = %(asset_id)s")
        params["asset_id"] = asset_id
    if start is not None:
        conditions.append("timestamp >= %(start)s")
        params["start"] = start
    if end is not None:
        conditions.append("timestamp <= %(end)s")
        params["end"] = end
    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    query = f"SELECT * FROM liquidations{where} ORDER BY timestamp DESC LIMIT %(limit)s"

    with pool.connection() as conn:
        rows = conn.execute(query, params).fetchall()

    for row in rows:
        if row.get("timestamp"):
            row["timestamp"] = row["timestamp"].isoformat()
    return rows


# ---------------------------------------------------------------------------
# Status / health monitoring endpoints
# ---------------------------------------------------------------------------

# Feed metadata comes from the collector's own config module: the api image copies
# collector/config.py and exchanges.yaml beside this file (api/Dockerfile), so routing, intervals and
# loop names have one source. Read at import, never per request.
#
# Timeframes the collectors don't write but whose stored candles this API still serves and
# gap-checks. They live in `candles`, the default table.
RETIRED_TIMEFRAME_INTERVALS = {"1d": 86400, "3d": 259200, "1w": 604800}
TIMEFRAME_INTERVALS = config.load_timeframe_intervals() | RETIRED_TIMEFRAME_INTERVALS
_CANDLE_TABLES = config.load_candle_tables()


def _candle_table(timeframe: str) -> str:
    """The candle storage table for a timeframe (default `candles`). config validates every value
    against its CANDLE_TABLES allowlist, so it is safe to inline as a query identifier (psycopg can't
    bind an identifier as a value). `/candles` and `/status/gaps` route through it, so `1m` reads
    candles_1m."""
    return _CANDLE_TABLES.get(timeframe, "candles")


# The per-loop heartbeat pseudo-timeframes (funding, OI, the WS feeds, premium, the HL 1m repair).
# CONTRACT: every candle-facing collection_status query in this module excludes these sentinel rows;
# loop liveness is the monitor's surface, not this API's.
LOOP_TIMEFRAMES = tuple(config.LOOP_INTERVALS)


def _status_is_stale(seconds_since_success: float | None, interval_s: int | None) -> bool:
    """/status's per-pair freshness: no success yet, or none within two intervals (120 s at 1m). An
    unknown timeframe is never stale here. This is not the monitor's paging threshold, which waits
    `interval + max(interval, 1800)` (collector/monitor.py compute_staleness)."""
    if interval_s is None:
        return False
    return seconds_since_success is None or seconds_since_success > 2 * interval_s


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
               s.number_compressed_chunks AS compressed_chunks
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
        row["is_stale"] = _status_is_stale(row.get("seconds_since_success"), TIMEFRAME_INTERVALS.get(row["timeframe"]))
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


@app.get(
    "/export/parquet",
    response_class=Response,  # raw parquet bytes, not the default JSONResponse
    responses={
        200: {"content": {"application/vnd.apache.parquet": {}}, "description": "Parquet feed window"},
        204: {"description": "No rows in the window — skip (the backtester hash rejects an empty feed)"},
        400: {"description": "Unknown table, or naive/reversed bounds"},
        413: {"description": "Window exceeds EXPORT_MAX_ROWS — narrow it or use the bulk export script"},
    },
)
def export_parquet(
    table: str,
    start: datetime,
    end: datetime,
    asset_id: str | None = None,
):
    # Forward-feed parquet export for the backtester's forward ingest (T-011). Thin wrapper: validate →
    # api/export.py's feed_query (parameterized, half-open window) → rows_to_parquet, which owns the
    # byte-contract (schema + NULL policy).
    spec = export.FEED_SPECS.get(table)
    if spec is None:
        raise HTTPException(status_code=400, detail=f"unknown table {table!r}; valid: {sorted(export.FEED_SPECS)}")
    # TIMESTAMPTZ boundaries: a naive datetime would be read against the session tz and silently shift
    # the window (a byte-contract hazard) — require tz-aware, then normalize to UTC so the query and the
    # filename are unambiguously UTC (an accepted +offset input maps to the same instant but must not
    # stamp a caller-local date).
    if start.tzinfo is None or end.tzinfo is None:
        raise HTTPException(status_code=400, detail="start and end must be timezone-aware (UTC)")
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    if start >= end:
        raise HTTPException(status_code=400, detail="start must be < end")

    query, params = export.feed_query(table, start, end, asset_id)

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

    # The full parquet is already materialized (bounded by EXPORT_MAX_ROWS), so return it as one
    # Response body — no chunked streaming, no extra BytesIO copy.
    data = export.rows_to_parquet(table, rows)
    filename = f"{table}_{start.date()}_{end.date()}.parquet"
    return Response(
        content=data,
        media_type="application/vnd.apache.parquet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
