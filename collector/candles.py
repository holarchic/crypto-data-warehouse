"""The REST candle feed: one `candle_loop` thread per timeframe, polling every active pair.

Each poll fetches from the pair's stored cursor forward (`since`), so a pair that missed cycles
backfills on its next successful ones, and writes the bars and the cursor in one transaction.

`poll_limit` sizes the `fetch_ohlcv` `limit` to the actual gap since the last stored
candle instead of a flat 1000. On Binance the kline weight scales with `limit`
(`binance_kline_weight`: <100→1, <500→2, ≤1000→5), so a steady-state poll that needs
one new candle should cost weight 1, not 5 — the flat-1000 poll is what bloated the
mass-settlement candle burst ~5× and starved the dense-OI weight lane (T-007.4).
Seed (no stored candle) still fetches `CANDLE_SEED_LIMIT` for maximum history, and only
weight-bound venues (`rate_limit_weight_budget` set) size the limit; the weight only binds on
Binance, and there's no reason to change live fetch semantics on venues that aren't
weight-limited.
"""

import logging
import time

from backoff import RETRIABLE_EXCEPTIONS
from config import CANDLE_TABLES, ExchangeConfig, TimeframeConfig
from rate_limiter import RateLimiter, binance_kline_weight, paced_call
from slots import sleep_until_next_slot
from status import query_active_pairs, upsert_status_error, upsert_status_success

from db import connect_db, maybe_reload_markets, safe_rollback

log = logging.getLogger(__name__)

CANDLE_LIMIT_MARGIN = 5   # extra candles past the computed gap, covering a boundary-timing
#                           race (a candle closing between `now` and the fetch) so we never
#                           under-fetch — an unfetched closed candle would be a data gap.
CANDLE_LIMIT_MIN = 2      # floor: guards clock skew (now < since ⇒ negative gap) so `limit`
#                           is never 0/negative. Recovery for real gaps comes from the gap
#                           term, not this floor.
CANDLE_SEED_LIMIT = 1000  # Seed / large-gap ceiling: max history the exchange returns per call.


def poll_limit(since_ms: int, now_ms: int, interval_s: int) -> int:
    """Candles to request on a Poll fetch: the closed-candle gap since `since_ms` plus a
    margin, floored at `CANDLE_LIMIT_MIN` and capped at `CANDLE_SEED_LIMIT`. Caught up
    (gap ≈ 1) → a small limit (weight 1 on Binance); a pair down K cycles → ~K + margin
    (capped 1000) so gap recovery is preserved exactly as the flat-1000 ceiling gave."""
    gap_candles = (now_ms - since_ms) // (interval_s * 1000)
    return max(CANDLE_LIMIT_MIN, min(CANDLE_SEED_LIMIT, gap_candles + CANDLE_LIMIT_MARGIN))


def fetch_candles(exchange, symbol, market_info, exchange_name, timeframe, interval_s, rate_limiter, since_ms=None, limit=1000,
                  until_ms=None):
    params = {"until": until_ms} if until_ms is not None else {}
    raw = paced_call(
        rate_limiter, exchange, binance_kline_weight(limit),
        exchange.fetch_ohlcv, symbol, timeframe, since=since_ms, limit=limit, params=params,
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
            "timestamp": ts_ms / 1000,  # epoch seconds; the INSERT's to_timestamp() makes the timestamptz
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "volume": vol,
            "quote_volume": None,
            "trades": None,
        })
    return candles


# One INSERT per candle table (candles, candles_1m). The table name is substituted at import from
# the closed CANDLE_TABLES allowlist — NOT interpolated from any runtime/external value — so there
# is no injection surface (psycopg can't bind an identifier as a value; a literal-template over a
# fixed allowlist is the safe idiom). The column list has a single source (this template).
_INSERT_TEMPLATE = """
    INSERT INTO {table} (exchange, market_type, asset_id, symbol, native_symbol,
                         timeframe, timestamp, open, high, low, close,
                         volume, quote_volume, trades)
    VALUES (%(exchange)s, %(market_type)s, %(asset_id)s, %(symbol)s, %(native_symbol)s,
            %(timeframe)s, to_timestamp(%(timestamp)s), %(open)s, %(high)s, %(low)s, %(close)s,
            %(volume)s, %(quote_volume)s, %(trades)s)
    ON CONFLICT DO NOTHING
"""
INSERT_SQL = {table: _INSERT_TEMPLATE.format(table=table) for table in CANDLE_TABLES}


def insert_candles(conn, candles, table="candles"):
    """Insert inside the caller's transaction; returns the inserted count."""
    if not candles:
        return 0
    sql = INSERT_SQL[table]  # KeyError on an unknown table = fail fast (config-validated at load)
    inserted = 0
    with conn.cursor() as cur:
        for row in candles:
            cur.execute(sql, row)
            inserted += cur.rowcount
    return inserted


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


def _collect_pair(exchange, pair, config, tf, rate_limiter, conn, thread_name, scan_from):
    """Fetch and insert candles for a single pair. Returns True on success.

    Gap-aware: fetches from last_timestamp forward (`since`), so a pair that
    missed N cycles backfills N candles over its next successful runs (`poll_max_bars` a poll on a
    capped venue). The fixed
    limit=10 approach lost data permanently after 10 consecutive failures.

    `scan_from` ({symbol: ms}, owned by the calling loop) carries a window-capped venue past empty
    windows: when a capped window behind now returns no bars (a cursor older than the venue's
    history, or a bar-less stretch), the next poll starts after it instead of asking again, so the
    cursor reaches the next bar without one uncapped, heavy request.
    """
    market_info = {
        "asset_id": pair["asset_id"] or "BTC",
        "native_symbol": pair["native_symbol"] or "",
        "market_type": pair["market_type"] or config.market_type,
    }
    stored_last = pair["last_timestamp"]
    if stored_last is None:
        # Seed: fetch max history the exchange will return. A window-capped venue walks the same
        # depth forward one capped window a poll instead of one heavy tail request.
        since_ms = None
        if config.poll_max_bars is not None:
            since_ms = int(time.time() * 1000) - CANDLE_SEED_LIMIT * tf.interval_s * 1000
        limit = CANDLE_SEED_LIMIT
        label = "Seed"
    else:
        # Poll: fetch from just after last stored candle. 1ms nudge avoids the
        # ambiguous "since == last candle open" edge case across exchanges.
        # last_timestamp is a timestamptz → datetime, convert to epoch ms.
        since_ms = int(stored_last.timestamp() * 1000) + 1
        # Weight diet (T-007.4): on weight-bound venues (Binance) size the kline `limit` to
        # the actual gap so a caught-up poll costs weight 1 not 5 — this is what un-bloated the
        # mass-settlement candle burst that was starving the dense-OI lane. Only weight-bound
        # venues (`rate_limit_weight_budget` set) gate here; others keep the flat max fetch,
        # since the weight only binds on Binance and there's no reason to change their semantics.
        if config.rate_limit_weight_budget is not None:
            limit = poll_limit(since_ms, int(time.time() * 1000), tf.interval_s)
        else:
            limit = CANDLE_SEED_LIMIT
        label = "Poll"
    until_ms = None
    if since_ms is not None and config.poll_max_bars is not None:
        since_ms = max(since_ms, scan_from.get(pair["symbol"], 0))
        # HL answers every candle whose bucket contains a time in [since, until], both ends
        # included, so the window ends one ms before the bucket that would be the (cap + 1)th.
        bar_ms = tf.interval_s * 1000
        until_ms = (since_ms // bar_ms + config.poll_max_bars) * bar_ms - 1
    candles = fetch_candles(
        exchange, pair["symbol"], market_info, config.name, tf.name, tf.interval_s, rate_limiter,
        since_ms=since_ms, limit=limit, until_ms=until_ms,
    )
    if until_ms is not None:
        if not candles and until_ms < time.time() * 1000:
            scan_from[pair["symbol"]] = until_ms + 1
        else:
            scan_from.pop(pair["symbol"], None)
    # Bars and cursor commit together: a failed cursor write rolls the bars back with it, and the
    # next poll re-fetches the same window.
    inserted = insert_candles(conn, candles, tf.table)
    last_ts = max((c["timestamp"] for c in candles), default=None) if candles else None
    upsert_status_success(conn, config.name, market_info["market_type"], pair["symbol"], tf.name, last_ts, asset_id=market_info["asset_id"])
    conn.commit()

    log.info("[%s] %s %s: fetched=%d inserted=%d", thread_name, label, pair["asset_id"], len(candles), inserted)
    return True


# Defer first cycle to the next slot only for mid-interval TFs (30m-12h). These
# have many pairs and cluster-start on shared boundaries, so running them all at once
# on restart saturates the exchange's rate-limit budget the hardest. Short TFs (< 30m)
# run immediately so candle data starts flowing. Very long TFs (>= 1d) also run
# immediately because their next slot, computed from the UNIX epoch, can be days
# away (1w boundaries are Thursdays — a post-restart defer could leave 1w pairs stale
# for a week).
_DEFER_FIRST_CYCLE_MIN_S = 1800   # 30m
_DEFER_FIRST_CYCLE_MAX_S = 86400  # 1d (exclusive)


def _defer_first_cycle(interval_s: int) -> bool:
    return _DEFER_FIRST_CYCLE_MIN_S <= interval_s < _DEFER_FIRST_CYCLE_MAX_S


def _record_pair_error(conn, config: ExchangeConfig, pair, tf: TimeframeConfig, exc) -> None:
    """Best-effort per-pair error-status write. MUST NOT raise out of the collect handler — a db
    restart mid-write would otherwise propagate and kill the collection thread (T-014). The
    rollback is unconditionally safe; the status write is gated on a live conn AND wrapped, so a
    restart between the check and the write can't escape either (the check alone is not protection)."""
    safe_rollback(conn)
    if conn.closed:
        return
    try:
        upsert_status_error(conn, config.name, pair.get("market_type", config.market_type),
                            pair["symbol"], tf.name, str(exc))
        conn.commit()
    except Exception:
        safe_rollback(conn)


def candle_loop(exchange, config: ExchangeConfig, tf: TimeframeConfig, dsn: str, rate_limiter: RateLimiter):
    """Multi-pair poll loop for a single timeframe. Own DB connection. Never exits."""
    thread_name = f"{config.name}/{tf.name}"
    conn = connect_db(dsn)

    # Poll loop — never exits
    first_cycle = not _defer_first_cycle(tf.interval_s)
    started = None
    scan_from: dict[str, int] = {}  # per-pair position past empty capped windows (see _collect_pair)
    while True:
        if not first_cycle:
            sleep_until_next_slot(tf.interval_s, tf.buffer_s, started, thread_name)
        first_cycle = False
        started = time.time()

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
            safe_rollback(conn)
            pairs = []
        if not pairs:
            pairs = _btc_fallback_pairs(config, exchange)
        active_symbols = {p["symbol"] for p in pairs}
        for symbol in [s for s in scan_from if s not in active_symbols]:
            del scan_from[symbol]

        log.info("[%s] Cycle: %d active pairs", thread_name, len(pairs))

        # Iterate over each pair — errors are isolated per-pair
        for pair in pairs:
            try:
                _collect_pair(exchange, pair, config, tf, rate_limiter, conn, thread_name, scan_from)
            except RETRIABLE_EXCEPTIONS as exc:
                log.warning("[%s] Retriable error on %s: %s", thread_name, pair["symbol"], exc)
                if conn.closed:
                    try:
                        conn = connect_db(dsn)
                    except RuntimeError:
                        log.error("[%s] DB reconnect failed mid-cycle", thread_name)
                        break
                _record_pair_error(conn, config, pair, tf, exc)
            except Exception as exc:
                log.warning("[%s] Error on %s: %s", thread_name, pair["symbol"], exc)
                _record_pair_error(conn, config, pair, tf, exc)
