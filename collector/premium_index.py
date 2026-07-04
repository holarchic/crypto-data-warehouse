"""Binance premium-index term-structure poll (T-010).

The premium index is the perp-mark-vs-spot-index spread that drives funding — a
conditioning input for downstream backtests. cc captures it live as **5-min OHLC
klines** (`premiumIndexKlines`, via ccxt `fetch_premium_index_ohlcv`) into the `premium_index`
table. 5m is a strict superset of the backtester's 1h seed (downsamples to 1h for the T-011
export) and resolves the pre-settlement premium ramp a 1h bar blurs.

Binance-only: HL premium lives in `hl_asset_ctxs` (T-008); there is no bybit-premium seed.

Shape mirrors the candle collect loop — boundary-aligned poll, gap-aware `since` cursor, the
T-007.4 weight diet (`poll_limit` keeps a caught-up poll at kline weight 1, not 5), and the
T-014 error discipline (`safe_rollback` on every error path). It stays **non-priority** on the
shared Binance `WeightRateLimiter` (it yields to candles + the dense-OI reserve — premium is
neither the primary product nor unbackfillable). First run is **forward-only** (a few recent
bars, not the 1000-bar candle seed) so the empty-table rollout carries no bootstrap weight spike;
deep history is backfillable via the live REST endpoint (T-011).
"""

import logging
import time

from backoff import RETRIABLE_EXCEPTIONS
from candles import poll_limit
from config import PREMIUM_LOOP, ExchangeConfig
from rate_limiter import RateLimiter, binance_kline_weight, paced_call
from status import query_active_pairs, upsert_loop_heartbeat

from db import connect_db, maybe_reload_markets, safe_rollback

log = logging.getLogger(__name__)

# Cadence is FIXED here, not a runtime config knob: the ccxt timeframe string, the
# incomplete-bar guard, and the `premium` monitor staleness floor (config.LOOP_INTERVALS)
# are coupled — a config-only change would silently break monitoring (codex plan-review).
# Changing the cadence is a small, one-place code edit that moves all three together.
PREMIUM_TIMEFRAME = "5m"
PREMIUM_INTERVAL_S = 300
PREMIUM_POLL_BUFFER_S = 15   # fetch just after the 5m bar closes (server-side settle margin)
# Forward-only first fetch: with no cursor, pull only the latest few bars (since=None → ccxt
# returns the most-recent `limit`), NOT the 1000-bar candle seed — deep history is backfillable
# via T-011, so the empty-table rollout must not seed ~3.5 days × ~500 symbols and spike weight.
PREMIUM_SEED_LIMIT = 3

INSERT_PREMIUM_SQL = """
    INSERT INTO premium_index (exchange, asset_id, symbol, native_symbol, timestamp,
            premium_open, premium_high, premium_low, premium_close)
    VALUES (%(exchange)s, %(asset_id)s, %(symbol)s, %(native_symbol)s, to_timestamp(%(timestamp)s),
            %(premium_open)s, %(premium_high)s, %(premium_low)s, %(premium_close)s)
    ON CONFLICT DO NOTHING
"""

# Per-symbol restart cursor: the last stored premium-bar open time. Seeded once from the DB so a
# restart backfills the gap; advanced in-memory as bars land. (Premium is not in collection_status
# — it writes only the loop heartbeat sentinel, like funding/oi — so the cursor can't be read from
# there; it lives in this table.)
SEED_CURSOR_SQL = """
    SELECT symbol, EXTRACT(EPOCH FROM MAX(timestamp)) AS ts
    FROM premium_index
    WHERE exchange = %(exchange)s
    GROUP BY symbol
"""


def _build_premium_rows(raw, pair: dict, exchange_name: str, now_ms: int) -> list[dict]:
    """ccxt `fetch_premium_index_ohlcv` rows → premium_index row dicts. Each raw row is
    `[open_ms, open, high, low, close, volume]` with OHLC = premium-index values; the 6th
    (volume) is a ccxt-dummy and is not persisted. Drops the still-forming last bar (not yet
    closed) — same guard as `fetch_candles`, keyed off `PREMIUM_INTERVAL_S` (not a literal 5m)."""
    rows = []
    for ts_ms, o, h, low, c, _vol in raw:
        if ts_ms + PREMIUM_INTERVAL_S * 1000 > now_ms:
            continue  # bar still forming — skip until it closes
        rows.append({
            "exchange": exchange_name,
            "asset_id": pair["asset_id"],
            "symbol": pair["symbol"],
            "native_symbol": pair["native_symbol"],
            "timestamp": ts_ms / 1000,
            "premium_open": o,
            "premium_high": h,
            "premium_low": low,
            "premium_close": c,
        })
    return rows


def _insert_premium(conn, rows: list[dict]) -> int:
    """Insert premium rows in one transaction; returns inserted count. Owns the commit."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        inserted = 0
        for row in rows:
            cur.execute(INSERT_PREMIUM_SQL, row)
            inserted += cur.rowcount
    conn.commit()
    return inserted


def _seed_cursors(conn, exchange_name: str) -> dict[str, float]:
    """{symbol: last_premium_bar_ts_seconds} from the DB, so a restart resumes from the gap.
    Empty on a fresh table (each symbol then starts forward-only)."""
    with conn.cursor() as cur:
        cur.execute(SEED_CURSOR_SQL, {"exchange": exchange_name})
        return {sym: float(ts) for sym, ts in cur.fetchall() if ts is not None}


def _sleep_until_next_boundary(interval_s: int, buffer_s: int) -> None:
    """Sleep to the next `interval_s` boundary + buffer, so each poll fires just after a bar
    closes. Local copy of `main.sleep_until_next_boundary` (importing from `main` is circular —
    `main` imports this loop); `oi.py` sets the per-module-helper precedent. The 3-copy
    duplication with `main`/`oi` is a known future shared-util extraction, out of scope here."""
    now = time.time()
    next_boundary = (now // interval_s + 1) * interval_s + buffer_s
    sleep_s = next_boundary - now
    log.info("[premium] Sleeping %.0fs until next %ds boundary", sleep_s, interval_s)
    time.sleep(sleep_s)


def _heartbeat(conn, config: ExchangeConfig, thread_name: str) -> None:
    """Per-loop liveness heartbeat, guarded so a write hiccup can't kill the loop (it
    reconnects next cycle via the top-of-cycle `conn.closed` guard). Liveness only — a loop
    that runs but whose API calls persistently fail keeps a fresh heartbeat (same as funding/oi)."""
    try:
        upsert_loop_heartbeat(conn, config.name, config.market_type, PREMIUM_LOOP)
    except Exception:
        log.exception("[%s] Heartbeat upsert failed", thread_name)
        if not conn.closed:
            safe_rollback(conn)


def _collect_premium_pair(exchange, pair: dict, config: ExchangeConfig, rate_limiter,
                          conn, cursors: dict, now_ms: int) -> tuple[int, int]:
    """Fetch + insert one symbol's premium bars. Returns (closed_bars, inserted). Advances the
    in-memory cursor to the newest closed bar ONLY after a successful commit (a fetch/insert
    failure leaves the cursor unadvanced → retried next cycle, no gap). Non-priority on the
    shared limiter (yields to candles + dense OI)."""
    symbol = pair["symbol"]
    cursor = cursors.get(symbol)
    if cursor is None:
        since_ms = None            # forward-only first fetch: latest PREMIUM_SEED_LIMIT bars
        limit = PREMIUM_SEED_LIMIT
    else:
        since_ms = int(cursor * 1000) + 1
        limit = poll_limit(since_ms, now_ms, PREMIUM_INTERVAL_S)  # weight diet (T-007.4)
    raw = paced_call(
        rate_limiter, exchange, binance_kline_weight(limit),
        exchange.fetch_premium_index_ohlcv, symbol, PREMIUM_TIMEFRAME, since_ms, limit,
    )
    rows = _build_premium_rows(raw, pair, config.name, now_ms)
    inserted = _insert_premium(conn, rows)
    if rows:  # advance past every closed bar seen (even dedup'd ones), post-commit
        cursors[symbol] = max(r["timestamp"] for r in rows)
    return len(rows), inserted


def premium_index_loop(exchange, config: ExchangeConfig, dsn: str, rate_limiter: RateLimiter):
    """Boundary-scheduled 5-min premium-index poll for Binance. Own DB connection; never exits."""
    thread_name = f"{config.name}/premium"
    conn = connect_db(dsn)
    cursors: dict[str, float] = {}
    seeded = False

    while True:
        _sleep_until_next_boundary(PREMIUM_INTERVAL_S, PREMIUM_POLL_BUFFER_S)

        try:
            maybe_reload_markets(exchange)
        except Exception:
            log.exception("[%s] Markets reload failed — continuing with stale cache", thread_name)

        if conn.closed:
            try:
                conn = connect_db(dsn)
            except RuntimeError:
                log.error("[%s] DB reconnect failed, will retry next cycle", thread_name)
                continue

        try:
            pairs = query_active_pairs(conn, config.name, "5m")
        except Exception:
            log.exception("[%s] Failed to query active pairs", thread_name)
            safe_rollback(conn)
            continue

        if not pairs:
            log.info("[%s] No active pairs, sleeping", thread_name)
            _heartbeat(conn, config, thread_name)
            continue

        if not seeded:
            try:
                cursors = _seed_cursors(conn, config.name)
                seeded = True  # only after success — a failed seed retries next cycle so a
                # DB-backed restart resume is never silently downgraded to forward-only
                log.info("[%s] Seeded premium cursor for %d symbols", thread_name, len(cursors))
            except Exception:
                log.exception("[%s] Cursor seed failed, retrying next cycle", thread_name)
                if not conn.closed:
                    safe_rollback(conn)

        # Prune the cursor map to the active set (bounded under listing churn).
        active_syms = {p["symbol"] for p in pairs}
        for sym in [s for s in cursors if s not in active_syms]:
            del cursors[sym]

        now_ms = int(time.time() * 1000)
        fetched = inserted = errors = 0
        for pair in pairs:
            try:
                f, i = _collect_premium_pair(exchange, pair, config, rate_limiter, conn, cursors, now_ms)
                fetched += f
                inserted += i
            except RETRIABLE_EXCEPTIONS as exc:
                errors += 1
                log.warning("[%s] Retriable error on %s: %s", thread_name, pair["symbol"], exc)
                if conn.closed:
                    try:
                        conn = connect_db(dsn)
                    except RuntimeError:
                        log.error("[%s] DB reconnect failed mid-cycle", thread_name)
                        break
                safe_rollback(conn)  # clear any aborted txn (no per-symbol status row to write)
            except Exception as exc:
                errors += 1
                log.warning("[%s] Error on %s: %s", thread_name, pair["symbol"], exc)
                safe_rollback(conn)

        log.info("[%s] Poll: fetched=%d inserted=%d (%d pairs, %d errors)",
                 thread_name, fetched, inserted, len(pairs), errors)
        _heartbeat(conn, config, thread_name)
