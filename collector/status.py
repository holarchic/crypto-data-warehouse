"""collection_status table helpers — upsert on every fetch cycle."""

import logging

log = logging.getLogger(__name__)

# Sentinel symbol for the per-loop liveness heartbeat rows (funding/OI poll loops and
# the streaming WS loop). One row per (exchange, loop), keyed (exchange, market_type,
# "*", loop-pseudo-tf). Never a real ccxt symbol, so it can't collide with a candle pair.
LOOP_SENTINEL_SYMBOL = "*"

# Discovery owns (re)activation: the conflict update never touches `active`. A collector cycle runs
# on a pair list fetched before discovery may have deactivated a pair; resurrecting it here would keep
# the pair collected past its drop AND feed discovery's hysteresis a false "still active".
UPSERT_SUCCESS_SQL = """
    INSERT INTO collection_status (exchange, market_type, symbol, timeframe, active, asset_id, last_timestamp, last_success, error_count)
    VALUES (%(exchange)s, %(market_type)s, %(symbol)s, %(timeframe)s, TRUE, %(asset_id)s, to_timestamp(%(last_timestamp)s), NOW(), 0)
    ON CONFLICT (exchange, market_type, symbol, timeframe) DO UPDATE SET
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
        deactivated_at = NULL,
        -- Reset last_success on reactivation: the stored timestamp is from the last
        -- time the pair was being polled (possibly days/weeks ago), which would
        -- trip the monitor's staleness alert the moment the pair is re-added. The
        -- next collector cycle will update last_success with a real value.
        last_success = CASE
            WHEN collection_status.active = FALSE THEN NOW()
            ELSE collection_status.last_success
        END
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

# Loop-heartbeat sentinel rows carry no asset_id, so the NULL filter keeps them out.
QUERY_ACTIVE_ASSET_IDS_SQL = """
    SELECT DISTINCT asset_id FROM collection_status
    WHERE active = TRUE AND asset_id IS NOT NULL
"""

UPSERT_LOOP_HEARTBEAT_SQL = """
    INSERT INTO collection_status (exchange, market_type, symbol, timeframe, active, last_success, error_count)
    VALUES (%(exchange)s, %(market_type)s, %(symbol)s, %(loop)s, TRUE, NOW(), 0)
    ON CONFLICT (exchange, market_type, symbol, timeframe) DO UPDATE SET
        active = TRUE,
        last_success = NOW(),
        error_count = 0,
        last_error = NULL
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


def upsert_loop_heartbeat(conn, exchange: str, market_type: str, loop: str):
    """Record a per-loop liveness heartbeat for a poll cycle or a WS connection timer.

    `loop` is a pseudo-timeframe (config.FUNDING_LOOP / OI_LOOP / LIQUIDATION_WS) stored
    in the `timeframe` column. The monitor's staleness check flags this sentinel row when
    it goes stale — the signal a silently-dead loop would otherwise never produce.
    """
    params = {
        "exchange": exchange,
        "market_type": market_type,
        "symbol": LOOP_SENTINEL_SYMBOL,
        "loop": loop,
    }
    with conn.cursor() as cur:
        cur.execute(UPSERT_LOOP_HEARTBEAT_SQL, params)
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


def query_active_asset_ids(conn) -> set[str]:
    """Assets with an active pair on any exchange — discovery's hysteresis input."""
    with conn.cursor() as cur:
        cur.execute(QUERY_ACTIVE_ASSET_IDS_SQL)
        return {asset_id for (asset_id,) in cur.fetchall()}


def query_active_pairs(conn, exchange: str, timeframe: str) -> list[dict]:
    """Return active pairs for this exchange/timeframe from collection_status."""
    with conn.cursor() as cur:
        cur.execute(QUERY_ACTIVE_PAIRS_SQL, {"exchange": exchange, "timeframe": timeframe})
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
