"""Open interest collection — one thread per exchange, snapshots every 5 minutes."""

import json
import logging
import threading
import time
import urllib.request

from backoff import RETRIABLE_EXCEPTIONS
from config import ExchangeConfig
from main import connect_db
from status import query_active_pairs

log = logging.getLogger(__name__)

INSERT_OI_SQL = """
    INSERT INTO open_interest (exchange, asset_id, symbol, timestamp, open_interest, oi_value_usd)
    VALUES (%(exchange)s, %(asset_id)s, %(symbol)s, to_timestamp(%(timestamp)s),
            %(open_interest)s, %(oi_value_usd)s)
    ON CONFLICT DO NOTHING
"""

OI_POLL_INTERVAL_S = 300  # 5 minutes
OI_POLL_BUFFER_S = 30


def _get_active_pairs(conn, exchange_name):
    """Get active pairs from collection_status (5m timeframe = finest granularity)."""
    return query_active_pairs(conn, exchange_name, "5m")


def _insert_oi(conn, rows):
    """Insert OI rows. Returns count of inserted rows."""
    if not rows:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(INSERT_OI_SQL, row)
            inserted += cur.rowcount
    conn.commit()
    return inserted


def _sleep_until_next_5m():
    """Sleep until next 5-minute boundary + buffer."""
    now = time.time()
    next_boundary = (now // OI_POLL_INTERVAL_S + 1) * OI_POLL_INTERVAL_S + OI_POLL_BUFFER_S
    sleep_s = next_boundary - now
    log.info("[oi] Sleeping %.0fs until next 5m boundary", sleep_s)
    time.sleep(sleep_s)


# ---------------------------------------------------------------------------
# Strategy A — ccxt per-symbol (Binance, Bybit, OKX)
# ---------------------------------------------------------------------------


def _fetch_price_map(exchange, api_lock, rate_limit_s):
    """Bulk fetch_tickers() → {symbol: last_price}. One API call."""
    try:
        with api_lock:
            time.sleep(rate_limit_s)
            tickers = exchange.fetch_tickers()
        return {sym: t.get("last") or t.get("close") for sym, t in tickers.items() if t.get("last") or t.get("close")}
    except Exception as exc:
        log.warning("[oi] fetch_tickers failed (oi_value_usd will be None): %s", exc)
        return {}


def _oi_loop_ccxt(exchange, config: ExchangeConfig, dsn: str, api_lock: threading.Lock):
    """Per-symbol fetch_open_interest() for ccxt exchanges."""
    thread_name = f"{config.name}/oi"
    conn = connect_db(dsn)
    rate_limit_s = config.rate_limit_ms / 1000

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
            _sleep_until_next_5m()
            continue

        if not pairs:
            log.info("[%s] No active pairs, sleeping", thread_name)
            _sleep_until_next_5m()
            continue

        # Bulk price map for computing oi_value_usd when ccxt doesn't return it
        price_map = _fetch_price_map(exchange, api_lock, rate_limit_s)

        rows = []
        for pair in pairs:
            try:
                with api_lock:
                    time.sleep(rate_limit_s)
                    oi_data = exchange.fetch_open_interest(pair["symbol"])

                if oi_data is None:
                    continue

                ts = oi_data.get("timestamp")
                if ts is not None:
                    ts = ts / 1000
                else:
                    ts = time.time()

                oi_amount = oi_data.get("openInterestAmount")
                oi_value = oi_data.get("openInterestValue")

                # Compute oi_value_usd from price map if ccxt didn't return it
                if oi_value is None and oi_amount is not None:
                    price = price_map.get(pair["symbol"])
                    if price is not None:
                        oi_value = oi_amount * price

                rows.append({
                    "exchange": config.name,
                    "asset_id": pair["asset_id"],
                    "symbol": pair["symbol"],
                    "timestamp": ts,
                    "open_interest": oi_amount,
                    "oi_value_usd": oi_value,
                })
            except RETRIABLE_EXCEPTIONS as exc:
                log.warning("[%s] Retriable error on %s: %s", thread_name, pair["symbol"], exc)
                if conn.closed:
                    break
                conn.rollback()
            except Exception as exc:
                log.warning("[%s] Error on %s: %s", thread_name, pair["symbol"], exc)
                if not conn.closed:
                    conn.rollback()

        inserted = _insert_oi(conn, rows)
        log.info("[%s] Poll: %d OI snapshots inserted (%d pairs)", thread_name, inserted, len(pairs))

        _sleep_until_next_5m()


# ---------------------------------------------------------------------------
# Strategy B — HyperLiquid native bulk (1 API call → all pairs)
# ---------------------------------------------------------------------------

HL_INFO_URL = "https://api.hyperliquid.xyz/info"


def _fetch_hl_meta_and_ctxs():
    """POST metaAndAssetCtxs to HyperLiquid info API. Returns (universe, asset_ctxs)."""
    payload = json.dumps({"type": "metaAndAssetCtxs"}).encode()
    req = urllib.request.Request(HL_INFO_URL, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    # Response: [{universe: [...]}, [{openInterest, markPx, ...}, ...]]
    universe = data[0]["universe"]
    asset_ctxs = data[1]
    return universe, asset_ctxs


def _oi_loop_hyperliquid(config: ExchangeConfig, dsn: str):
    """Native bulk OI for HyperLiquid — 1 API call → all pairs."""
    thread_name = f"{config.name}/oi"
    conn = connect_db(dsn)

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
            _sleep_until_next_5m()
            continue

        if not pairs:
            log.info("[%s] No active pairs, sleeping", thread_name)
            _sleep_until_next_5m()
            continue

        # Build asset_id → pair lookup
        asset_to_pair = {p["asset_id"]: p for p in pairs}

        try:
            universe, asset_ctxs = _fetch_hl_meta_and_ctxs()
            now_ts = time.time()
            rows = []

            for meta, ctx in zip(universe, asset_ctxs):
                asset_name = meta.get("name", "")
                if asset_name not in asset_to_pair:
                    continue

                pair = asset_to_pair[asset_name]
                oi_str = ctx.get("openInterest", "0")
                mark_str = ctx.get("markPx", "0")
                oi_val = float(oi_str)
                mark_val = float(mark_str)

                rows.append({
                    "exchange": config.name,
                    "asset_id": pair["asset_id"],
                    "symbol": pair["symbol"],
                    "timestamp": now_ts,
                    "open_interest": oi_val,
                    "oi_value_usd": oi_val * mark_val,
                })

            inserted = _insert_oi(conn, rows)
            log.info("[%s] Poll: %d OI snapshots inserted (%d active pairs)", thread_name, inserted, len(pairs))

        except Exception as exc:
            log.warning("[%s] Error fetching HL OI: %s", thread_name, exc)
            if not conn.closed:
                conn.rollback()

        _sleep_until_next_5m()


# ---------------------------------------------------------------------------
# Entry point — dispatches to the right strategy
# ---------------------------------------------------------------------------


def oi_loop(exchange, config: ExchangeConfig, dsn: str, api_lock: threading.Lock):
    """OI collection loop. Dispatches to native HL bulk or ccxt per-symbol."""
    if config.ccxt_class == "hyperliquid":
        _oi_loop_hyperliquid(config, dsn)
    else:
        _oi_loop_ccxt(exchange, config, dsn, api_lock)
