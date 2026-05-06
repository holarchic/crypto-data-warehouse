import logging
import threading
import time

from backoff import RETRIABLE_EXCEPTIONS
from config import ExchangeConfig, TimeframeConfig, load_config
from dotenv import load_dotenv
from funding import funding_loop
from oi import oi_loop
from rate_limiter import (
    RateLimiter,
    WeightRateLimiter,
    binance_kline_weight,
    paced_call,
)
from status import query_active_pairs, upsert_status_error, upsert_status_success

from db import build_dsn, connect_db, create_exchange, maybe_reload_markets

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def fetch_candles(exchange, symbol, market_info, exchange_name, timeframe, interval_s, rate_limiter, since_ms=None, limit=1000):
    raw = paced_call(
        rate_limiter, exchange, binance_kline_weight(limit),
        exchange.fetch_ohlcv, symbol, timeframe, since=since_ms, limit=limit,
    )
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


def _collect_pair(exchange, pair, config, tf, rate_limiter, conn, thread_name):
    """Fetch and insert candles for a single pair. Returns True on success.

    Gap-aware: fetches from last_timestamp forward (`since`), so a pair that
    missed N cycles backfills N candles on its next successful run. The fixed
    limit=10 approach lost data permanently after 10 consecutive failures.
    """
    market_info = {
        "asset_id": pair["asset_id"] or "BTC",
        "native_symbol": pair["native_symbol"] or "",
        "market_type": pair["market_type"] or config.market_type,
    }
    stored_last = pair["last_timestamp"]
    if stored_last is None:
        # Seed: fetch max history the exchange will return
        since_ms = None
        label = "Seed"
    else:
        # Poll: fetch from just after last stored candle. 1ms nudge avoids the
        # ambiguous "since == last candle open" edge case across exchanges.
        # last_timestamp is a timestamptz → datetime, convert to epoch ms.
        since_ms = int(stored_last.timestamp() * 1000) + 1
        label = "Poll"
    candles = fetch_candles(
        exchange, pair["symbol"], market_info, config.name, tf.name, tf.interval_s, rate_limiter,
        since_ms=since_ms, limit=1000,
    )
    inserted = insert_candles(conn, candles)
    last_ts = max((c["timestamp"] for c in candles), default=None) if candles else None
    upsert_status_success(conn, config.name, market_info["market_type"], pair["symbol"], tf.name, last_ts, asset_id=market_info["asset_id"])

    log.info("[%s] %s %s: fetched=%d inserted=%d", thread_name, label, pair["asset_id"], len(candles), inserted)
    return True


# Defer first cycle to the next boundary only for mid-interval TFs (30m-12h). These
# have many pairs and cluster-start on shared boundaries, so running them all at once
# on restart saturates the exchange's rate-limit budget the hardest. Short TFs (< 30m)
# run immediately so candle data starts flowing. Very long TFs (>= 1d) also run
# immediately because their "next boundary" computed from the UNIX epoch can be days
# away (1w boundaries are Thursdays — a post-restart defer could leave 1w pairs stale
# for a week).
_DEFER_FIRST_CYCLE_MIN_S = 1800   # 30m
_DEFER_FIRST_CYCLE_MAX_S = 86400  # 1d (exclusive)


def _defer_first_cycle(interval_s: int) -> bool:
    return _DEFER_FIRST_CYCLE_MIN_S <= interval_s < _DEFER_FIRST_CYCLE_MAX_S


def collect_loop(exchange, config: ExchangeConfig, tf: TimeframeConfig, dsn: str, rate_limiter: RateLimiter):
    """Multi-pair poll loop for a single timeframe. Own DB connection. Never exits."""
    thread_name = f"{config.name}/{tf.name}"
    conn = connect_db(dsn)

    # Poll loop — never exits
    first_cycle = not _defer_first_cycle(tf.interval_s)
    while True:
        if not first_cycle:
            sleep_until_next_boundary(tf.interval_s, tf.buffer_s)
        first_cycle = False

        try:
            maybe_reload_markets(exchange)
        except Exception:
            log.exception("[%s] Markets reload failed — continuing with stale cache", thread_name)

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
                _collect_pair(exchange, pair, config, tf, rate_limiter, conn, thread_name)
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

    # Shared rate limiter — paces API calls to respect the exchange rate limit without
    # serializing the actual network round-trip across threads. Binance uses a
    # weight-budget limiter synced from response headers; others use fixed interval.
    if config.rate_limit_weight_budget is not None:
        rate_limiter = WeightRateLimiter(
            budget=config.rate_limit_weight_budget,
            window_s=config.rate_limit_window_s,
            safety=config.rate_limit_safety,
        )
        log.info(
            "Using WeightRateLimiter: budget=%d/%ds safety=%d",
            config.rate_limit_weight_budget, config.rate_limit_window_s, config.rate_limit_safety,
        )
    else:
        rate_limiter = RateLimiter(config.rate_limit_ms / 1000)
        log.info("Using RateLimiter: interval=%dms", config.rate_limit_ms)

    # One daemon thread per timeframe
    threads = []
    for tf in config.timeframes:
        t = threading.Thread(
            target=collect_loop,
            args=(exchange, config, tf, dsn, rate_limiter),
            name=f"{config.name}-{tf.name}",
            daemon=True,
        )
        t.start()
        threads.append(t)
        log.info("Started thread %s", t.name)

    # Funding rate thread
    if config.has_funding:
        t = threading.Thread(
            target=funding_loop,
            args=(exchange, config, dsn, rate_limiter),
            name=f"{config.name}-funding",
            daemon=True,
        )
        t.start()
        threads.append(t)
        log.info("Started thread %s", t.name)

    # Open interest thread
    if config.has_oi:
        t = threading.Thread(
            target=oi_loop,
            args=(exchange, config, dsn, rate_limiter),
            name=f"{config.name}-oi",
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
