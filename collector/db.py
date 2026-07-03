"""Shared database and exchange utilities — extracted to avoid circular imports."""

import logging
import os
import threading
import time

import ccxt
import psycopg
from config import ExchangeConfig

log = logging.getLogger(__name__)

MARKETS_RELOAD_INTERVAL_S = int(os.environ.get("MARKETS_RELOAD_INTERVAL_S", 86400))
_MARKETS_RELOAD_LOCK = threading.Lock()


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

    # Close the just-constructed session if load_markets fails — this is the only layer that
    # owns the exchange at that point, so a failure here must not leak it (a retrying caller,
    # e.g. the dense pool's per-cycle init, would otherwise leak one session per attempt).
    try:
        exchange.load_markets()
    except Exception:
        try:
            exchange.close()
        except Exception:
            pass
        raise
    exchange._cc_markets_loaded_at = time.time()
    return exchange


def maybe_reload_markets(exchange, interval_s: int = MARKETS_RELOAD_INTERVAL_S):
    """Refresh ccxt's in-memory markets dict if it's older than `interval_s`.

    Daily discovery adds newly-listed pairs to collection_status, but the
    collector's `exchange.markets` is only populated at container start. Without
    this, newly-discovered symbols return "does not have market symbol" until
    the next container restart — which can be weeks. The lock keeps the reload
    serialized across the per-timeframe threads that share one exchange.
    """
    with _MARKETS_RELOAD_LOCK:
        last = getattr(exchange, "_cc_markets_loaded_at", 0)
        age = time.time() - last
        if age <= interval_s:
            return False
        exchange.load_markets(reload=True)
        exchange._cc_markets_loaded_at = time.time()
        log.info("Reloaded markets (age was %.0fs, %d symbols)", age, len(exchange.markets))
        return True


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def build_dsn():
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "db")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "candle_collector")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def connect_db(dsn, max_retries=10, retry_delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg.connect(dsn, options="-c statement_timeout=60000")
            log.info("Connected to PostgreSQL")
            return conn
        except psycopg.OperationalError:
            log.warning("DB connection attempt %d/%d failed, retrying in %ds...", attempt, max_retries, retry_delay)
            time.sleep(retry_delay)
    raise RuntimeError(f"Could not connect to PostgreSQL after {max_retries} attempts")
