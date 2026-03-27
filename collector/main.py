import logging
import os
import time

import ccxt
import psycopg
from config import ExchangeConfig, load_config
from dotenv import load_dotenv

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SafeHyperliquid — guard against ccxt bug in load_markets()
# ---------------------------------------------------------------------------


class SafeHyperliquid(ccxt.hyperliquid):
    """Guard against ccxt bug: None base symbol in HL spot tokens crashes load_markets."""

    def fetch_spot_markets(self, params=None):
        params = params if params is not None else {}
        try:
            return super().fetch_spot_markets(params)
        except TypeError:
            log.warning("fetch_spot_markets TypeError (ccxt bug) — skipping spot markets")
            return []


# ---------------------------------------------------------------------------
# Exchange factory
# ---------------------------------------------------------------------------


def create_exchange(config: ExchangeConfig):
    """Create a ccxt exchange instance from config. SafeHyperliquid for HL, generic for others."""
    opts = {"timeout": 30000, **config.options}

    if config.ccxt_class == "hyperliquid":
        exchange = SafeHyperliquid(opts)
    else:
        cls = getattr(ccxt, config.ccxt_class, None)
        if cls is None:
            raise ValueError(f"ccxt has no class '{config.ccxt_class}'")
        exchange = cls(opts)

    exchange.load_markets()
    return exchange


def resolve_market_info(exchange, config: ExchangeConfig):
    """Extract asset_id, native_symbol, market_type from the exchange's BTC market."""
    market = exchange.market(config.btc_symbol)
    asset_id = market["base"]
    native_symbol = market["id"]
    mtype = config.market_type
    log.info("Resolved %s — asset_id=%s native_symbol=%s market_type=%s", config.name, asset_id, native_symbol, mtype)
    return {"asset_id": asset_id, "native_symbol": native_symbol, "market_type": mtype}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_dsn():
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "db")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "candle_collector")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def fetch_candles(exchange, symbol, market_info, exchange_name, timeframe, interval_s, limit=10):
    raw = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    now_ms = int(time.time() * 1000)
    candles = []
    for ts_ms, o, h, lo, c, vol in raw:
        # Drop incomplete candle (still forming)
        if ts_ms + interval_s * 1000 > now_ms:
            continue
        candles.append({
            "exchange": exchange_name,
            "market_type": market_info["market_type"],
            "asset_id": market_info["asset_id"],
            "symbol": symbol,
            "native_symbol": market_info["native_symbol"],
            "timeframe": timeframe,
            "timestamp": ts_ms / 1000,  # psycopg handles epoch -> timestamptz
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "volume": vol,
            "quote_volume": None,
            "trades": None,
        })
    return candles


INSERT_SQL = """
    INSERT INTO candles (exchange, market_type, asset_id, symbol, native_symbol,
                         timeframe, timestamp, open, high, low, close,
                         volume, quote_volume, trades)
    VALUES (%(exchange)s, %(market_type)s, %(asset_id)s, %(symbol)s, %(native_symbol)s,
            %(timeframe)s, to_timestamp(%(timestamp)s), %(open)s, %(high)s, %(low)s, %(close)s,
            %(volume)s, %(quote_volume)s, %(trades)s)
    ON CONFLICT DO NOTHING
"""


def insert_candles(conn, candles):
    if not candles:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for row in candles:
            cur.execute(INSERT_SQL, row)
            inserted += cur.rowcount
    conn.commit()
    return inserted


def sleep_until_next_boundary(interval_s, buffer_s):
    now = time.time()
    next_boundary = (now // interval_s + 1) * interval_s + buffer_s
    sleep_s = next_boundary - now
    log.info("Sleeping %.0fs until next %ss boundary (+%ss buffer)", sleep_s, interval_s, buffer_s)
    time.sleep(sleep_s)


def connect_db(dsn, max_retries=10, retry_delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg.connect(dsn)
            log.info("Connected to PostgreSQL")
            return conn
        except psycopg.OperationalError:
            log.warning("DB connection attempt %d/%d failed, retrying in %ds...", attempt, max_retries, retry_delay)
            time.sleep(retry_delay)
    raise RuntimeError(f"Could not connect to PostgreSQL after {max_retries} attempts")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    config = load_config()
    log.info("Starting collector for exchange=%s symbol=%s timeframes=%s", config.name, config.btc_symbol, [tf.name for tf in config.timeframes])

    exchange = create_exchange(config)
    market_info = resolve_market_info(exchange, config)
    dsn = build_dsn()
    conn = connect_db(dsn)

    # Use first timeframe for single-threaded mode (v0.1 compat)
    # Multi-timeframe threading added in Chunk 3
    tf = config.timeframes[0] if config.timeframes else None
    if tf is None:
        raise RuntimeError(f"No timeframes configured for {config.name}")

    timeframe = tf.name
    interval_s = tf.interval_s
    buffer_s = tf.buffer_s

    # Seed: fetch last 100 closed candles
    log.info("Seeding %s candles...", timeframe)
    candles = fetch_candles(exchange, config.btc_symbol, market_info, config.name, timeframe, interval_s, limit=100)
    inserted = insert_candles(conn, candles)
    log.info("Seeded %d candles (%d fetched, %d already existed)", inserted, len(candles), len(candles) - inserted)

    # Poll loop
    while True:
        try:
            sleep_until_next_boundary(interval_s, buffer_s)
            candles = fetch_candles(exchange, config.btc_symbol, market_info, config.name, timeframe, interval_s, limit=10)
            inserted = insert_candles(conn, candles)
            log.info("Poll [%s]: fetched=%d inserted=%d", timeframe, len(candles), inserted)
        except KeyboardInterrupt:
            log.info("Shutting down")
            break
        except Exception:
            log.exception("Error in poll cycle, sleeping 30s before retry")
            time.sleep(30)


if __name__ == "__main__":
    run()
