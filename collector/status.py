"""collection_status table helpers — upsert on every fetch cycle."""

import logging

log = logging.getLogger(__name__)

UPSERT_SUCCESS_SQL = """
    INSERT INTO collection_status (exchange, market_type, symbol, timeframe, active, last_timestamp, last_success, error_count)
    VALUES (%(exchange)s, %(market_type)s, %(symbol)s, %(timeframe)s, TRUE, %(last_timestamp)s, NOW(), 0)
    ON CONFLICT (exchange, market_type, symbol, timeframe) DO UPDATE SET
        active = TRUE,
        last_timestamp = GREATEST(collection_status.last_timestamp, %(last_timestamp)s),
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


def upsert_status_success(conn, exchange: str, market_type: str, symbol: str, timeframe: str, last_timestamp):
    """Record a successful fetch cycle."""
    params = {
        "exchange": exchange,
        "market_type": market_type,
        "symbol": symbol,
        "timeframe": timeframe,
        "last_timestamp": last_timestamp,
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
