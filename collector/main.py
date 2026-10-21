import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from candles import candle_loop
from config import ExchangeConfig, load_config
from dotenv import load_dotenv
from funding import funding_loop
from hl_candles import build_hl_candle_spec
from liquidations import build_liq_spec
from oi import oi_loop
from premium_index import premium_index_loop
from rate_limiter import RateLimiter, build_rate_limiter
from ws import ws_loop

from db import build_dsn, create_exchange

log = logging.getLogger(__name__)

# How often the root checks its feed threads: a dead feed is noticed within this many seconds.
SUPERVISE_POLL_S = 10


@dataclass(frozen=True)
class Feed:
    """One collector thread: its name and the loop it runs."""

    name: str
    target: Callable
    args: tuple


def build_feeds(config: ExchangeConfig, exchange, dsn: str, rate_limiter: RateLimiter) -> list[Feed]:
    """Every feed the venue's config enables, in launch order."""
    feeds = [Feed(f"{config.name}-{tf.name}", candle_loop, (exchange, config, tf, dsn, rate_limiter))
             for tf in config.timeframes]
    if config.has_funding:
        feeds.append(Feed(f"{config.name}-funding", funding_loop, (exchange, config, dsn, rate_limiter)))
    if config.has_oi:
        feeds.append(Feed(f"{config.name}-oi", oi_loop, (exchange, config, dsn, rate_limiter)))
    # The venue's liquidation spec injected into the generic WS engine. `build_liq_spec` raises on a
    # flag set for a venue with no builder (fail fast, not a silent skip).
    if config.has_liquidation_ws:
        feeds.append(Feed(f"{config.name}-liq", ws_loop, (build_liq_spec(config.name), config, dsn)))
    if config.has_premium_index:
        feeds.append(Feed(f"{config.name}-premium", premium_index_loop, (exchange, config, dsn, rate_limiter)))
    # HL 1m is streamed, not polled: one WS connection for the whole HL universe, zero REST (drop gaps
    # and unclosed bars are repaired daily by hl_candles_repair; a live backfill was measured to starve
    # HL's tight limiter). 1m is deliberately NOT in supported_timeframes, so no REST 1m candle feed
    # exists to starve the unbackfillable dense OI. The spec resolves each coin's HL WS name from ccxt
    # markets, so it takes `exchange`.
    if config.has_candle_ws:
        feeds.append(Feed(f"{config.name}-candle-ws", ws_loop, (build_hl_candle_spec(exchange), config, dsn)))
    return feeds


def wait_for_dead_feed(threads: list[threading.Thread], poll_s: float) -> str:
    """Block until a feed thread has stopped, by an exception or a return, and return its name."""
    while True:
        for thread in threads:
            if not thread.is_alive():
                return thread.name
        time.sleep(poll_s)


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

    threads = []
    for feed in build_feeds(config, exchange, dsn, rate_limiter):
        thread = threading.Thread(target=feed.target, args=feed.args, name=feed.name, daemon=True)
        thread.start()
        threads.append(thread)
        log.info("Started thread %s", thread.name)

    try:
        dead = wait_for_dead_feed(threads, SUPERVISE_POLL_S)
    except KeyboardInterrupt:
        log.info("Shutting down")
        return
    # Docker restarts processes, not threads, so a dead feed takes the collector down and
    # `restart: always` brings every feed back with fresh state. os._exit, not sys.exit: interpreter
    # shutdown joins the dense-OI pool's executor workers, and one mid-request would hold the exit open.
    log.critical("Feed thread %s stopped; exiting so Docker restarts the collector", dead)
    logging.shutdown()
    os._exit(1)


if __name__ == "__main__":
    run()
