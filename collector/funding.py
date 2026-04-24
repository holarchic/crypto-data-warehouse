"""Funding rate collection — one thread per exchange, polls every hour."""

import logging
import time

from backoff import RETRIABLE_EXCEPTIONS
from config import ExchangeConfig
from rate_limiter import (
    BINANCE_FUNDING_BULK_WEIGHT,
    BINANCE_FUNDING_HISTORY_WEIGHT,
    RateLimiter,
    paced_call,
)
from status import query_active_pairs

from db import connect_db

log = logging.getLogger(__name__)

INSERT_FUNDING_SQL = """
    INSERT INTO funding_rates (exchange, asset_id, symbol, timestamp, rate, predicted_rate, interval_hours)
    VALUES (%(exchange)s, %(asset_id)s, %(symbol)s, to_timestamp(%(timestamp)s),
            %(rate)s, %(predicted_rate)s, %(interval_hours)s)
    ON CONFLICT DO NOTHING
"""

FUNDING_INTERVAL_HOURS = {
    "hyperliquid": 1,
}
DEFAULT_FUNDING_INTERVAL = 8


def _get_active_pairs(conn, exchange_name):
    """Get active pairs from collection_status (5m timeframe = finest granularity)."""
    return query_active_pairs(conn, exchange_name, "5m")


def _parse_funding_record(record, exchange_name, pair, from_bulk=False):
    """Parse a ccxt funding rate record into an insert dict.

    Bulk endpoint: fundingTimestamp = NEXT settlement (future).
      → last settlement = fundingTimestamp - interval_hours * 3600s
    History endpoint: timestamp = actual settlement time, fundingTimestamp = None.
    """
    interval_hours = FUNDING_INTERVAL_HOURS.get(exchange_name, DEFAULT_FUNDING_INTERVAL)

    if from_bulk:
        fts = record.get("fundingTimestamp")
        if fts is None:
            return None
        ts_ms = fts - interval_hours * 3600 * 1000
    else:
        ts_ms = record.get("timestamp")
        if ts_ms is None:
            return None

    return {
        "exchange": exchange_name,
        "asset_id": pair["asset_id"],
        "symbol": pair["symbol"],
        "timestamp": ts_ms / 1000,
        "rate": record.get("fundingRate"),
        "predicted_rate": record.get("nextFundingRate"),
        "interval_hours": interval_hours,
    }


def _insert_funding(conn, rows):
    """Insert funding rate rows. Returns count of inserted rows."""
    if not rows:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(INSERT_FUNDING_SQL, row)
            inserted += cur.rowcount
    conn.commit()
    return inserted


def _seed_funding(exchange, config, conn, pairs, rate_limiter):
    """Seed historical funding rates — fetch_funding_rate_history(limit=100) per pair."""
    if config.ccxt_class == "dydx":
        log.info("[%s/funding] Skipping seed for dYdX (expensive for limited value)", config.name)
        return

    total_inserted = 0

    for pair in pairs:
        try:
            history = paced_call(
                rate_limiter, exchange, BINANCE_FUNDING_HISTORY_WEIGHT,
                exchange.fetch_funding_rate_history, pair["symbol"], limit=100,
            )
            rows = []
            for record in history:
                row = _parse_funding_record(record, config.name, pair)
                if row:
                    rows.append(row)
            inserted = _insert_funding(conn, rows)
            total_inserted += inserted
        except RETRIABLE_EXCEPTIONS as exc:
            log.warning("[%s/funding] Seed retriable error on %s: %s", config.name, pair["symbol"], exc)
            if conn.closed:
                return
            conn.rollback()
        except Exception as exc:
            log.warning("[%s/funding] Seed error on %s: %s", config.name, pair["symbol"], exc)
            if not conn.closed:
                conn.rollback()

    log.info("[%s/funding] Seed complete: %d rows inserted across %d pairs", config.name, total_inserted, len(pairs))


def _poll_bulk(exchange, config, conn, pairs, rate_limiter):
    """Bulk fetch_funding_rates() — 1 API call per exchange."""
    try:
        all_rates = paced_call(
            rate_limiter, exchange, BINANCE_FUNDING_BULK_WEIGHT,
            exchange.fetch_funding_rates,
        )
    except RETRIABLE_EXCEPTIONS as exc:
        log.warning("[%s/funding] Bulk fetch retriable error: %s", config.name, exc)
        if not conn.closed:
            conn.rollback()
        return 0
    except Exception as exc:
        log.warning("[%s/funding] Bulk fetch error: %s", config.name, exc)
        if not conn.closed:
            conn.rollback()
        return 0

    # Build symbol lookup from active pairs
    active_symbols = {p["symbol"]: p for p in pairs}
    rows = []
    for symbol, record in all_rates.items():
        if symbol not in active_symbols:
            continue
        pair = active_symbols[symbol]
        row = _parse_funding_record(record, config.name, pair, from_bulk=True)
        if row:
            rows.append(row)

    inserted = _insert_funding(conn, rows)
    return inserted


def _poll_dydx(exchange, config, conn, pairs, rate_limiter):
    """dYdX fallback: per-symbol fetch_funding_rate_history(limit=1)."""
    total_inserted = 0

    for pair in pairs:
        try:
            history = paced_call(
                rate_limiter, exchange, BINANCE_FUNDING_HISTORY_WEIGHT,
                exchange.fetch_funding_rate_history, pair["symbol"], limit=1,
            )
            rows = []
            for record in history:
                row = _parse_funding_record(record, config.name, pair)
                if row:
                    rows.append(row)
            total_inserted += _insert_funding(conn, rows)
        except RETRIABLE_EXCEPTIONS as exc:
            log.warning("[%s/funding] dYdX retriable error on %s: %s", config.name, pair["symbol"], exc)
            if conn.closed:
                return total_inserted
            conn.rollback()
        except Exception as exc:
            log.warning("[%s/funding] dYdX error on %s: %s", config.name, pair["symbol"], exc)
            if not conn.closed:
                conn.rollback()

    return total_inserted


def _sleep_until_next_hour():
    """Sleep until the next hour boundary + 60s buffer."""
    now = time.time()
    next_hour = (now // 3600 + 1) * 3600 + 60
    sleep_s = next_hour - now
    log.info("[funding] Sleeping %.0fs until next hour boundary", sleep_s)
    time.sleep(sleep_s)


def funding_loop(exchange, config: ExchangeConfig, dsn: str, rate_limiter: RateLimiter):
    """Funding rate collection loop. Own DB connection. Runs as daemon thread."""
    thread_name = f"{config.name}/funding"
    conn = connect_db(dsn)
    seeded = False

    while True:
        # Reconnect if needed
        if conn.closed:
            try:
                conn = connect_db(dsn)
            except RuntimeError:
                log.error("[%s] DB reconnect failed, will retry next cycle", thread_name)
                time.sleep(30)
                continue

        # Get active pairs
        try:
            pairs = _get_active_pairs(conn, config.name)
        except Exception:
            log.exception("[%s] Failed to query active pairs", thread_name)
            conn.rollback()
            _sleep_until_next_hour()
            continue

        if not pairs:
            log.info("[%s] No active pairs, sleeping", thread_name)
            _sleep_until_next_hour()
            continue

        # Seed on first cycle
        if not seeded:
            log.info("[%s] Seeding funding history for %d pairs", thread_name, len(pairs))
            _seed_funding(exchange, config, conn, pairs, rate_limiter)
            seeded = True

        # Poll
        if config.ccxt_class == "dydx":
            inserted = _poll_dydx(exchange, config, conn, pairs, rate_limiter)
        else:
            inserted = _poll_bulk(exchange, config, conn, pairs, rate_limiter)

        log.info("[%s] Poll: %d funding rates inserted (%d pairs)", thread_name, inserted, len(pairs))

        _sleep_until_next_hour()
