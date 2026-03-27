import logging
import os
import threading
import time

import ccxt
import psycopg
from backoff import RETRIABLE_EXCEPTIONS, backoff_sleep
from config import ExchangeConfig, TimeframeConfig, load_config
from dotenv import load_dotenv
from status import upsert_status_error, upsert_status_success

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


def fetch_candles(exchange, symbol, market_info, exchange_name, timeframe, interval_s, api_lock, limit=10):
    with api_lock:
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
# Per-timeframe collection loop (runs in its own thread)
# ---------------------------------------------------------------------------


def collect_loop(exchange, config: ExchangeConfig, market_info: dict, tf: TimeframeConfig, dsn: str, api_lock: threading.Lock):
    """Seed + poll loop for a single timeframe. Own DB connection. Never exits."""
    thread_name = f"{config.name}/{tf.name}"
    conn = connect_db(dsn)

    # Seed: fetch last 100 closed candles
    log.info("[%s] Seeding candles...", thread_name)
    try:
        candles = fetch_candles(exchange, config.btc_symbol, market_info, config.name, tf.name, tf.interval_s, api_lock, limit=100)
        inserted = insert_candles(conn, candles)
        log.info("[%s] Seeded %d candles (%d fetched, %d already existed)", thread_name, inserted, len(candles), len(candles) - inserted)
    except Exception:
        log.exception("[%s] Seed failed, will retry in poll loop", thread_name)

    # Poll loop — never exits
    error_count = 0
    while True:
        try:
            sleep_until_next_boundary(tf.interval_s, tf.buffer_s)
            candles = fetch_candles(exchange, config.btc_symbol, market_info, config.name, tf.name, tf.interval_s, api_lock, limit=10)
            inserted = insert_candles(conn, candles)
            log.info("[%s] Poll: fetched=%d inserted=%d", thread_name, len(candles), inserted)
            error_count = 0
            last_ts = max((c["timestamp"] for c in candles), default=None) if candles else None
            upsert_status_success(conn, config.name, market_info["market_type"], config.btc_symbol, tf.name, last_ts)
        except RETRIABLE_EXCEPTIONS as exc:
            error_count += 1
            log.exception("[%s] Retriable error (error_count=%d)", thread_name, error_count)
            if conn.closed:
                try:
                    conn = connect_db(dsn)
                except RuntimeError:
                    log.error("[%s] DB reconnect failed, will retry next cycle", thread_name)
            if not conn.closed:
                upsert_status_error(conn, config.name, market_info["market_type"], config.btc_symbol, tf.name, str(exc))
            backoff_sleep(error_count)
        except Exception as exc:
            error_count += 1
            log.exception("[%s] Unexpected error (error_count=%d)", thread_name, error_count)
            if not conn.closed:
                upsert_status_error(conn, config.name, market_info["market_type"], config.btc_symbol, tf.name, str(exc))
            backoff_sleep(error_count)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    config = load_config()
    if not config.timeframes:
        raise RuntimeError(f"No timeframes configured for {config.name}")

    log.info("Starting collector for exchange=%s symbol=%s timeframes=%s", config.name, config.btc_symbol, [tf.name for tf in config.timeframes])

    exchange = create_exchange(config)
    market_info = resolve_market_info(exchange, config)
    dsn = build_dsn()

    # Shared lock — serializes API calls when timeframe boundaries align
    api_lock = threading.Lock()

    # One daemon thread per timeframe
    threads = []
    for tf in config.timeframes:
        t = threading.Thread(
            target=collect_loop,
            args=(exchange, config, market_info, tf, dsn, api_lock),
            name=f"{config.name}-{tf.name}",
            daemon=True,
        )
        t.start()
        threads.append(t)
        log.info("Started thread %s", t.name)

    # Main thread waits — daemon threads die with the process
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    run()
