import logging
import threading
import time

from candles import candle_loop
from config import load_config
from dotenv import load_dotenv
from funding import funding_loop
from hl_candles import build_hl_candle_spec
from liquidations import build_liq_spec
from oi import oi_loop
from premium_index import premium_index_loop
from rate_limiter import build_rate_limiter
from ws import ws_loop

from db import build_dsn, create_exchange

log = logging.getLogger(__name__)


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
    # serializing the actual network round-trip across threads. Binance uses a weight-budget
    # limiter (synced from response headers, with the dense-OI priority reserve); others use a
    # fixed interval. The factory owns the choice + the priority_reserve wiring.
    rate_limiter = build_rate_limiter(config)

    # One daemon thread per timeframe
    for tf in config.timeframes:
        t = threading.Thread(
            target=candle_loop,
            args=(exchange, config, tf, dsn, rate_limiter),
            name=f"{config.name}-{tf.name}",
            daemon=True,
        )
        t.start()
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
        log.info("Started thread %s", t.name)

    # Liquidation WS feed thread (T-009.2) — the venue's spec injected into the generic engine.
    # `build_liq_spec` raises on a flag set for a venue with no builder (fail fast, not a silent skip).
    if config.has_liquidation_ws:
        t = threading.Thread(
            target=ws_loop,
            args=(build_liq_spec(config.name), config, dsn),
            name=f"{config.name}-liq",
            daemon=True,
        )
        t.start()
        log.info("Started thread %s", t.name)

    # Premium-index poll thread (T-010) — Binance premiumIndexKlines 5m → premium_index.
    if config.has_premium_index:
        t = threading.Thread(
            target=premium_index_loop,
            args=(exchange, config, dsn, rate_limiter),
            name=f"{config.name}-premium",
            daemon=True,
        )
        t.start()
        log.info("Started thread %s", t.name)

    # HL 1m candle feed (T-015.1) — a WS-only candle stream (real-time, zero REST; drop gaps and unclosed
    # bars are repaired daily by hl_candles_repair, never a live sweep — a live backfill was measured to starve HL's
    # tight limiter). STREAMED, not polled: 1m is deliberately NOT in supported_timeframes, so no
    # per-symbol REST 1m collect_loop launches above (it would starve the unbackfillable dense OI).
    if config.has_candle_ws:
        # One connection for the whole HL universe (well inside HL's per-IP sub ceiling). The spec
        # resolves each coin's HL WS name from ccxt markets, so `exchange` is passed in.
        t = threading.Thread(
            target=ws_loop,
            args=(build_hl_candle_spec(exchange), config, dsn),
            name=f"{config.name}-candle-ws",
            daemon=True,
        )
        t.start()
        log.info("Started thread %s", t.name)

    # Main thread waits — daemon threads die with the process
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    run()
