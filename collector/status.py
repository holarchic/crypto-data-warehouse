"""collection_status table helpers — upsert on every fetch cycle."""

import logging

log = logging.getLogger(__name__)

UPSERT_SUCCESS_SQL = """
    INSERT INTO collection_status (exchange, market_type, symbol, timeframe, active, asset_id, last_timestamp, last_success, error_count)
    VALUES (%(exchange)s, %(market_type)s, %(symbol)s, %(timeframe)s, TRUE, %(asset_id)s, to_timestamp(%(last_timestamp)s), NOW(), 0)
    ON CONFLICT (exchange, market_type, symbol, timeframe) DO UPDATE SET
        active = TRUE,
        asset_id = COALESCE(%(asset_id)s, collection_status.asset_id),
        last_timestamp = GREATEST(collection_status.last_timestamp, to_timestamp(%(last_timestamp)s)),
        last_success = NOW(),
        error_count = 0,
        last_error = NULL
"""

UPSERT_ERROR_SQL = """
    INSERT INTO collection_status (exchange, market_type, symbol, timeframe, active, error_count, last_error)
    VALUES (%(exchange)s, %(market_type)s, %(symbol)s, %(timeframe)s, TRUE, 1, %(last_error)s)
    ON CONFLICT (exchange, market_type, symbol, timeframe) DO UPDATE SET
        error_count = collection_status.error_count + 1,
        last_error = %(last_error)s
"""

UPSERT_DISCOVERY_SQL = """
    INSERT INTO collection_status (exchange, market_type, symbol, timeframe, active, asset_id, native_symbol, last_24h_volume_usd, discovered_at)
    VALUES (%(exchange)s, %(market_type)s, %(symbol)s, %(timeframe)s, TRUE, %(asset_id)s, %(native_symbol)s, %(volume)s, NOW())
    ON CONFLICT (exchange, market_type, symbol, timeframe) DO UPDATE SET
        active = TRUE,
        asset_id = %(asset_id)s,
        native_symbol = %(native_symbol)s,
        last_24h_volume_usd = %(volume)s,
        deactivated_at = NULL
"""

DEACTIVATE_STALE_SQL = """
    UPDATE collection_status
    SET active = FALSE, deactivated_at = NOW()
    WHERE exchange = %(exchange)s AND timeframe = %(timeframe)s AND active = TRUE
      AND symbol != ALL(%(active_symbols)s)
"""

QUERY_ACTIVE_PAIRS_SQL = """
    SELECT symbol, asset_id, native_symbol, market_type, last_timestamp
    FROM collection_status
    WHERE exchange = %(exchange)s AND timeframe = %(timeframe)s AND active = TRUE
    ORDER BY symbol
"""


def upsert_status_success(conn, exchange: str, market_type: str, symbol: str, timeframe: str, last_timestamp, asset_id: str | None = None):
    """Record a successful fetch cycle."""
    params = {
        "exchange": exchange,
        "market_type": market_type,
        "symbol": symbol,
        "timeframe": timeframe,
        "last_timestamp": last_timestamp,
        "asset_id": asset_id,
    }
    with conn.cursor() as cur:
        cur.execute(UPSERT_SUCCESS_SQL, params)
    conn.commit()


def upsert_status_error(conn, exchange: str, market_type: str, symbol: str, timeframe: str, error_msg: str):
    """Record a failed fetch cycle."""
    params = {
        "exchange": exchange,
        "market_type": market_type,
        "symbol": symbol,
        "timeframe": timeframe,
        "last_error": error_msg[:500],
    }
    with conn.cursor() as cur:
        cur.execute(UPSERT_ERROR_SQL, params)
    conn.commit()


def upsert_discovery_pair(conn, exchange: str, market_type: str, symbol: str, timeframe: str, asset_id: str, native_symbol: str, volume: float):
    """Insert or reactivate a discovered pair for collection."""
    params = {
        "exchange": exchange,
        "market_type": market_type,
        "symbol": symbol,
        "timeframe": timeframe,
        "asset_id": asset_id,
        "native_symbol": native_symbol,
        "volume": volume,
    }
    with conn.cursor() as cur:
        cur.execute(UPSERT_DISCOVERY_SQL, params)


def deactivate_stale_pairs(conn, exchange: str, timeframe: str, active_symbols: list[str]) -> int:
    """Deactivate pairs that are no longer qualifying on this exchange/timeframe."""
    with conn.cursor() as cur:
        cur.execute(DEACTIVATE_STALE_SQL, {"exchange": exchange, "timeframe": timeframe, "active_symbols": active_symbols})
        return cur.rowcount


def query_active_pairs(conn, exchange: str, timeframe: str) -> list[dict]:
    """Return active pairs for this exchange/timeframe from collection_status."""
    with conn.cursor() as cur:
        cur.execute(QUERY_ACTIVE_PAIRS_SQL, {"exchange": exchange, "timeframe": timeframe})
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
