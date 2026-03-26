import logging
import os
import time

import ccxt
import psycopg
from dotenv import load_dotenv

log = logging.getLogger(__name__)

SYMBOL = "BTC/USDC:USDC"
TIMEFRAME = "5m"
INTERVAL_S = 300
BUFFER_S = 30

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
# Helpers
# ---------------------------------------------------------------------------


def build_dsn():
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "db")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "candle_collector")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def init_exchange():
    exchange = SafeHyperliquid({"timeout": 30000})
    exchange.load_markets()
    market = exchange.market(SYMBOL)
    asset_id = market["base"]
    native_symbol = market["id"]
    log.info("Initialized HyperLiquid — asset_id=%s native_symbol=%s", asset_id, native_symbol)
    mtype = "perp" if market.get("swap") else market.get("type", "spot")
    return exchange, {"asset_id": asset_id, "native_symbol": native_symbol, "market_type": mtype}


def fetch_candles(exchange, symbol, market_info, limit=10):
    raw = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=limit)
    now_ms = int(time.time() * 1000)
    candles = []
    for ts_ms, o, h, lo, c, vol in raw:
        # Drop incomplete candle (still forming)
        if ts_ms + INTERVAL_S * 1000 > now_ms:
            continue
        candles.append({
            "exchange": "hyperliquid",
            "market_type": market_info["market_type"],
            "asset_id": market_info["asset_id"],
            "symbol": symbol,
            "native_symbol": market_info["native_symbol"],
            "timeframe": TIMEFRAME,
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


def sleep_until_next_boundary(interval_s=INTERVAL_S, buffer_s=BUFFER_S):
    now = time.time()
    next_boundary = (now // interval_s + 1) * interval_s + buffer_s
    sleep_s = next_boundary - now
    log.info("Sleeping %.0fs until next %sm boundary (+%ss buffer)", sleep_s, interval_s // 60, buffer_s)
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

    exchange, market_info = init_exchange()
    dsn = build_dsn()
    conn = connect_db(dsn)

    # Seed: fetch last 100 closed candles
    log.info("Seeding candles...")
    candles = fetch_candles(exchange, SYMBOL, market_info, limit=100)
    inserted = insert_candles(conn, candles)
    log.info("Seeded %d candles (%d fetched, %d already existed)", inserted, len(candles), len(candles) - inserted)

    # Poll loop
    while True:
        try:
            sleep_until_next_boundary()
            candles = fetch_candles(exchange, SYMBOL, market_info, limit=10)
            inserted = insert_candles(conn, candles)
            log.info("Poll: fetched=%d inserted=%d", len(candles), inserted)
        except KeyboardInterrupt:
            log.info("Shutting down")
            break
        except Exception:
            log.exception("Error in poll cycle, sleeping 30s before retry")
            time.sleep(30)


if __name__ == "__main__":
    run()
