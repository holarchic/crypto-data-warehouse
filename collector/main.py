import logging
import os
import threading
import time

import ccxt
import psycopg
from backoff import RETRIABLE_EXCEPTIONS
from config import ExchangeConfig, TimeframeConfig, load_config
from dotenv import load_dotenv
from status import query_active_pairs, upsert_status_error, upsert_status_success

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


def fetch_candles(exchange, symbol, market_info, exchange_name, timeframe, interval_s, api_lock, rate_limit_s, limit=10):
    with api_lock:
        time.sleep(rate_limit_s)
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


def _btc_fallback_pairs(config: ExchangeConfig, exchange) -> list[dict]:
    """Build a single-pair list from config btc_symbol for backward compat (no discovery data)."""
    market = exchange.markets.get(config.btc_symbol, {})
    return [{
        "symbol": config.btc_symbol,
        "asset_id": market.get("base", "BTC"),
        "native_symbol": market.get("id", ""),
        "market_type": config.market_type,
        "last_timestamp": None,
    }]


def _collect_pair(exchange, pair, config, tf, api_lock, conn, thread_name):
    """Fetch and insert candles for a single pair. Returns True on success."""
    market_info = {
        "asset_id": pair["asset_id"] or "BTC",
        "native_symbol": pair["native_symbol"] or "",
        "market_type": pair["market_type"] or config.market_type,
    }
    # Seed (100) on first encounter, poll (10) thereafter
    limit = 100 if pair["last_timestamp"] is None else 10
    rate_limit_s = config.rate_limit_ms / 1000
    candles = fetch_candles(
        exchange, pair["symbol"], market_info, config.name, tf.name, tf.interval_s, api_lock, rate_limit_s, limit=limit,
    )
    inserted = insert_candles(conn, candles)
    last_ts = max((c["timestamp"] for c in candles), default=None) if candles else None
    upsert_status_success(conn, config.name, market_info["market_type"], pair["symbol"], tf.name, last_ts)

    label = "Seed" if limit == 100 else "Poll"
    log.info("[%s] %s %s: fetched=%d inserted=%d", thread_name, label, pair["asset_id"], len(candles), inserted)
    return True


def collect_loop(exchange, config: ExchangeConfig, tf: TimeframeConfig, dsn: str, api_lock: threading.Lock):
    """Multi-pair poll loop for a single timeframe. Own DB connection. Never exits."""
    thread_name = f"{config.name}/{tf.name}"
    conn = connect_db(dsn)

    # Poll loop — never exits
    first_cycle = True
    while True:
        if not first_cycle:
            sleep_until_next_boundary(tf.interval_s, tf.buffer_s)
        first_cycle = False

        # Reconnect if needed
        if conn.closed:
            try:
                conn = connect_db(dsn)
            except RuntimeError:
                log.error("[%s] DB reconnect failed, will retry next cycle", thread_name)
                time.sleep(30)
                continue

        # Get active pairs from discovery, fall back to BTC from config
        try:
            pairs = query_active_pairs(conn, config.name, tf.name)
        except Exception:
            log.exception("[%s] Failed to query active pairs", thread_name)
            conn.rollback()
            pairs = []
        if not pairs:
            pairs = _btc_fallback_pairs(config, exchange)

        log.info("[%s] Cycle: %d active pairs", thread_name, len(pairs))

        # Iterate over each pair — errors are isolated per-pair
        for pair in pairs:
            try:
                _collect_pair(exchange, pair, config, tf, api_lock, conn, thread_name)
            except RETRIABLE_EXCEPTIONS as exc:
                log.warning("[%s] Retriable error on %s: %s", thread_name, pair["symbol"], exc)
                if conn.closed:
                    try:
                        conn = connect_db(dsn)
                    except RuntimeError:
                        log.error("[%s] DB reconnect failed mid-cycle", thread_name)
                        break
                if not conn.closed:
                    conn.rollback()
                    upsert_status_error(conn, config.name, pair.get("market_type", config.market_type), pair["symbol"], tf.name, str(exc))
            except Exception as exc:
                log.warning("[%s] Error on %s: %s", thread_name, pair["symbol"], exc)
                if not conn.closed:
                    conn.rollback()
                    upsert_status_error(conn, config.name, pair.get("market_type", config.market_type), pair["symbol"], tf.name, str(exc))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    config = load_config()
    if not config.timeframes:
        raise RuntimeError(f"No timeframes configured for {config.name}")

    log.info("Starting collector for exchange=%s timeframes=%s", config.name, [tf.name for tf in config.timeframes])

    exchange = create_exchange(config)
    dsn = build_dsn()

    # Shared lock — serializes API calls to respect exchange rate limits
    api_lock = threading.Lock()

    # One daemon thread per timeframe
    threads = []
    for tf in config.timeframes:
        t = threading.Thread(
            target=collect_loop,
            args=(exchange, config, tf, dsn, api_lock),
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
