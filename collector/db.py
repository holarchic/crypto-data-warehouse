"""Shared database and exchange utilities — extracted to avoid circular imports."""

import logging
import os
import time

import ccxt
import psycopg
from config import ExchangeConfig

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
