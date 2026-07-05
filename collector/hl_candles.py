"""HyperLiquid 1-minute candle WS feed + REST gap-backfill (T-015.1) → `candles_1m`.

HL's ~1 req/s shared info budget is reserved for the unbackfillable dense 1-min OI, so a per-symbol
REST 1m loop is off the table. Instead this module runs HL's native WS `candle` subscription as the
real-time primary (zero REST) and a low-priority REST backfill (this file's second half) that repairs
only what the WS drops. Both write `candles_1m` via an UPSERT (below), never the generic
`ON CONFLICT DO NOTHING` insert — HL streams the *forming* bar continuously with no closed flag, so the
row must converge to the bar's closed value (last-write-wins).

**asset_id resolution.** The WS candle frame carries `s` = the coin name, which for HL *is* the cc
`asset_id` (HL's `native_symbol` is a numeric asset index like 28/210 — the opposite of the
liquidations feed, which keys on `native_symbol`). `topics_for_pairs` rebuilds a shared
`asset_id → {symbol, native_symbol, market_type}` map that `parse_message` reads; both run in the one
`ws_loop` thread, so the map needs no lock (the liquidations pattern).
"""

import json
import logging
import time

from backoff import RETRIABLE_EXCEPTIONS
from config import CANDLE_BACKFILL, CANDLE_WS, ExchangeConfig
from rate_limiter import RateLimiter
from status import query_active_pairs, upsert_loop_heartbeat
from ws import WSFeedSpec

from db import connect_db, maybe_reload_markets, safe_rollback

log = logging.getLogger(__name__)

# --- Endpoint + engine tunables --------------------------------------------
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
HL_PING_INTERVAL_S = 30.0   # HL closes an idle socket at 60s (timeouts doc) → ping at 2× margin

# HL publishes 1000 subscriptions / 10 connections / 30 new-conns/min PER IP. At ~161 subs on one
# connection there is a wide margin; warn as the count nears the ceiling so a future universe-growth
# breach is visible. Note the 1000 is IP-scoped, so more *connections* would not raise it — a real
# breach (far off) means fewer subs or a second IP, not just sharding.
HL_SUB_CEILING = 1000
HL_SUB_WARN = 900

INSERT_CANDLES_1M_SQL = """
    INSERT INTO candles_1m (exchange, market_type, asset_id, symbol, native_symbol,
                            timeframe, timestamp, open, high, low, close,
                            volume, quote_volume, trades)
    VALUES (%(exchange)s, %(market_type)s, %(asset_id)s, %(symbol)s, %(native_symbol)s,
            %(timeframe)s, to_timestamp(%(timestamp)s), %(open)s, %(high)s, %(low)s, %(close)s,
            %(volume)s, %(quote_volume)s, %(trades)s)
    ON CONFLICT (exchange, market_type, symbol, timeframe, timestamp) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
        volume = EXCLUDED.volume, quote_volume = EXCLUDED.quote_volume, trades = EXCLUDED.trades
"""


# ---------------------------------------------------------------------------
# Parse helpers — parse-or-drop: bad *identity* (coin/time) drops the frame; bad *magnitude*
# (o/h/l/c/v) lands the row with that column NULL.
# ---------------------------------------------------------------------------


def _num(x):
    """Magnitude field → float or None. Never raises; a NULL OHLCV still lands a row."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _int(x):
    """Trade-count field → int or None (never raises)."""
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _ts_seconds(ms):
    """Bar-open ms → epoch seconds, or None if non-numeric (identity field → drop)."""
    try:
        return int(ms) / 1000.0
    except (TypeError, ValueError):
        return None


def _rebuild_map(asset_map, pairs):
    """Rebuild the shared asset_id→{symbol,native_symbol,market_type} map IN PLACE (clear+update, not
    rebind, so the closures keep the same dict object). Single-threaded per ws_loop → no lock."""
    asset_map.clear()
    asset_map.update({
        p["asset_id"]: {
            "symbol": p["symbol"],
            "native_symbol": p.get("native_symbol") or "",
            "market_type": p.get("market_type") or "perp",
        }
        for p in pairs if p.get("asset_id")
    })


def _parse_hl_candle(raw, asset_map):
    """HL WS frame → [candles_1m row]. Only `{"channel":"candle","data":{…}}` frames yield a row;
    every control frame (`subscriptionResponse` at subscribe, `pong` after a ping, any other channel)
    has no `candle` channel → `[]`. An unknown coin (no map entry) or a bad bar-open time → dropped."""
    msg = json.loads(raw)
    if msg.get("channel") != "candle":
        return []  # subscriptionResponse / pong / other channel — no candle
    d = msg.get("data")
    if not isinstance(d, dict):
        return []
    info = asset_map.get(d.get("s"))  # s = coin name = cc asset_id
    ts = _ts_seconds(d.get("t"))      # t = bar-open time (ms)
    if info is None or ts is None:
        return []  # untracked coin / bad time → unplaceable, drop
    return [{
        "exchange": "hyperliquid",
        "market_type": info["market_type"],
        "asset_id": d["s"],
        "symbol": info["symbol"],
        "native_symbol": info["native_symbol"],
        "timeframe": "1m",
        "timestamp": ts,
        "open": _num(d.get("o")),
        "high": _num(d.get("h")),
        "low": _num(d.get("l")),
        "close": _num(d.get("c")),
        "volume": _num(d.get("v")),
        "quote_volume": None,        # HL candle carries base volume only
        "trades": _int(d.get("n")),
    }]


def _insert_candles_1m(conn, rows):
    """Batch-UPSERT candle rows in one transaction; returns rowcount. Raises on failure so the engine's
    `_flush` rolls back + retains the buffer. The UPSERT (not DO NOTHING) is what lets a forming bar's
    later update converge the stored row to the bar's closed value."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        affected = 0
        for row in rows:
            cur.execute(INSERT_CANDLES_1M_SQL, row)
            affected += cur.rowcount
    conn.commit()
    return affected


# ---------------------------------------------------------------------------
# Subscribe framing + ceiling guard
# ---------------------------------------------------------------------------


def _sub_frame(method, coin):
    return json.dumps({"method": method, "subscription": {"type": "candle", "coin": coin, "interval": "1m"}})


def _check_sub_ceiling(coins, thread_name="hyperliquid/candle"):
    """Warn as the subscription count nears HL's per-IP 1000-sub ceiling. Returns the count (tests)."""
    n = len(coins)
    if n >= HL_SUB_WARN:
        log.warning(
            "[%s] Candle subscriptions %d/%d approaching the per-IP ceiling — fewer subs or a second IP"
            " needed soon (more connections do not raise an IP-scoped cap)", thread_name, n, HL_SUB_CEILING,
        )
    return n


# ---------------------------------------------------------------------------
# Spec builder
# ---------------------------------------------------------------------------


def build_hl_candle_spec() -> WSFeedSpec:
    """HL per-coin `candle` feed on the generic `ws.py` engine. `topics_for_pairs` returns coin *ids*
    (the engine diffs the id set across resubs) and rebuilds the shared asset map; the frame builders
    wrap each id in HL's candle-subscribe JSON; `parse_message` resolves the id back to the row."""
    asset_map: dict[str, dict] = {}

    def topics_for_pairs(pairs):
        _rebuild_map(asset_map, pairs)
        coins = [p["asset_id"] for p in pairs if p.get("asset_id")]
        _check_sub_ceiling(coins)
        return coins

    return WSFeedSpec(
        name="candle",
        loop_tf=CANDLE_WS,
        url=HL_WS_URL,
        topics_for_pairs=topics_for_pairs,
        subscribe_frames=lambda coins: [_sub_frame("subscribe", c) for c in coins],
        unsubscribe_frames=lambda coins: [_sub_frame("unsubscribe", c) for c in coins],
        parse_message=lambda raw: _parse_hl_candle(raw, asset_map),
        insert_batch=_insert_candles_1m,
        ping_frame=json.dumps({"method": "ping"}),
        ping_interval_s=HL_PING_INTERVAL_S,
    )


# ===========================================================================
# REST gap-backfill loop (durability) — repairs only what the WS drops.
#
# The WS candle feed is the real-time primary; a socket drop replays no history, so this low-priority
# sweep recovers gaps. Two repair modes, both universe-driven (the 5m active set — HL 1m mirrors the
# 5m discovery universe) and both UPSERTing via `_insert_candles_1m`:
#   (a) Gap repair — a WS drop that spans ≥1 whole minute leaves a *missing*-bar gap (and, after a
#       reconnect that resumes current bars, a MID-window gap). LEAD detection finds the gap; the fetch
#       starts at the bar BEFORE it (`gap_after`) so the stale boundary bar is overwritten too. A coin
#       with no recent rows is seeded (since=None). A coin merely behind the tape catches up from
#       last_ts+1.
#   (b) Trailing repair — a drop that reconnects sub-minute leaves bar T stale while T+1 exists → NO
#       gap for (a) to see. So each sweep also re-fetches the last few closed bars for a rotating
#       1/CYCLE share of the universe; over CYCLE sweeps every coin's recent bars are reconciled.
# Bounded per sweep by fetches AND bars (candleSnapshot weight scales with returned items) so a large
# backlog fills across sweeps rather than starving HL's shared IP budget in one burst.
# ===========================================================================

BACKFILL_SWEEP_INTERVAL_S = 300   # the ~0.05 req/s trailing cost + the 50-min full-rotation assume this
BACKFILL_SWEEP_BUFFER_S = 20      # fetch a little after the boundary so the just-closed bar is final
BACKFILL_LOOKBACK_S = 7200        # gap-scan window (2h) — a fresh WS-drop gap is visible immediately
BACKFILL_MAX_FETCHES = 40         # coins fetched per sweep (a big backlog spreads across sweeps)
BACKFILL_MAX_BARS = 5000          # ...and the total bars — the real weight bound
BACKFILL_TRAILING_BARS = 2        # recent closed bars re-fetched per trailing coin
BACKFILL_TRAILING_CYCLE = 10      # rotate the trailing share over this many sweeps (full cover ≈ 50min)
_INTERVAL_S = 60                  # 1m

_NO_REPAIR = object()             # sentinel: this coin needs no gap repair this sweep

SCAN_CANDLES_1M_SQL = """
    WITH win AS (
        SELECT symbol, timestamp,
               LEAD(timestamp) OVER (PARTITION BY symbol ORDER BY timestamp) AS next_ts
        FROM candles_1m
        WHERE exchange = 'hyperliquid' AND timeframe = '1m'
          AND timestamp > NOW() - INTERVAL '1 second' * %(lookback_s)s
    ),
    gaps AS (
        SELECT symbol, MIN(timestamp) AS earliest_gap_after
        FROM win
        WHERE next_ts IS NOT NULL
          AND EXTRACT(EPOCH FROM next_ts - timestamp) > %(interval_s)s * 1.5
        GROUP BY symbol
    )
    SELECT w.symbol, MAX(w.timestamp) AS last_ts, MIN(g.earliest_gap_after) AS earliest_gap_after
    FROM win w LEFT JOIN gaps g USING (symbol)
    GROUP BY w.symbol
"""


def _scan_candles_1m(conn, lookback_s=BACKFILL_LOOKBACK_S, interval_s=_INTERVAL_S):
    """Per-symbol {last_ts, earliest_gap_after} for HL 1m over the recent window. Returns a dict keyed
    by symbol; a symbol absent from the dict has no rows in the window (→ a zero-row seed candidate)."""
    with conn.cursor() as cur:
        cur.execute(SCAN_CANDLES_1M_SQL, {"lookback_s": lookback_s, "interval_s": interval_s})
        return {row[0]: {"last_ts": row[1], "earliest_gap_after": row[2]} for row in cur.fetchall()}


def _gap_since_ms(scan_row, now_ms, interval_s=_INTERVAL_S):
    """The `since` (epoch ms) to repair one coin that HAS recent rows, or `_NO_REPAIR` if none needed:
    - a mid-window gap → `gap_after` (the bar before the gap) so the fetch overwrites the stale
      boundary bar and fills the hole;
    - else behind the tape by >1.5 bars → `last_ts + 1 bar` (trailing catch-up);
    - else → `_NO_REPAIR`."""
    gap_after = scan_row.get("earliest_gap_after")
    if gap_after is not None:
        return int(gap_after.timestamp() * 1000)
    last_ts = scan_row.get("last_ts")
    if last_ts is not None:
        last_ms = int(last_ts.timestamp() * 1000)
        if now_ms - last_ms > interval_s * 1000 * 1.5:
            return last_ms + interval_s * 1000
    return _NO_REPAIR


def _repair_candidates(universe, scan, now_ms, interval_s=_INTERVAL_S):
    """Merge the active universe with the scan into `(pair, since_ms)` gap-repair candidates. A coin
    with no recent rows → `since=None` (seed the latest window; older segments close over sweeps). A
    coin with rows → `_gap_since_ms` (skipped if it needs no repair)."""
    candidates = []
    for pair in universe:
        row = scan.get(pair["symbol"])
        if row is None:
            candidates.append((pair, None))            # zero-row → seed
            continue
        since_ms = _gap_since_ms(row, now_ms, interval_s)
        if since_ms is not _NO_REPAIR:
            candidates.append((pair, since_ms))
    return candidates


def _trailing_share(universe, sweep_index, cycle=BACKFILL_TRAILING_CYCLE):
    """The rotating 1/`cycle` slice of the universe to trailing-repair this sweep. Over `cycle`
    consecutive sweeps every coin is covered exactly once (`i % cycle == sweep_index % cycle`)."""
    r = sweep_index % cycle
    return [pair for i, pair in enumerate(universe) if i % cycle == r]


def _market_info(pair, market_type):
    return {"asset_id": pair["asset_id"], "native_symbol": pair.get("native_symbol") or "",
            "market_type": pair.get("market_type") or market_type}


def _sleep_until_next_boundary(interval_s, buffer_s):
    now = time.time()
    time.sleep((now // interval_s + 1) * interval_s + buffer_s - now)


def _heartbeat(conn, config, thread_name):
    """Loop liveness sentinel, guarded so a write hiccup can't kill the loop (mirrors funding/oi)."""
    try:
        upsert_loop_heartbeat(conn, config.name, config.market_type, CANDLE_BACKFILL)
    except Exception:
        log.exception("[%s] Heartbeat upsert failed", thread_name)
        safe_rollback(conn)


def _run_backfill_sweep(exchange, config, conn, rate_limiter, sweep_index, thread_name):
    """One sweep: scan → gap candidates + rotating trailing share → bounded fetch/UPSERT. Gap repair
    takes precedence over trailing for the same coin. Returns (repaired_bars, remaining_candidates)."""
    from main import fetch_candles  # deferred: main imports this module at top (chunk 4 wiring)

    universe = query_active_pairs(conn, config.name, "5m")
    if not universe:
        _heartbeat(conn, config, thread_name)
        return 0, 0

    now_ms = int(time.time() * 1000)
    scan = _scan_candles_1m(conn)
    gap_cands = _repair_candidates(universe, scan, now_ms)
    gap_symbols = {pair["symbol"] for pair, _ in gap_cands}
    # Trailing coins the gap pass already covers need no separate trailing fetch.
    trailing = [(pair, now_ms - (BACKFILL_TRAILING_BARS + 1) * _INTERVAL_S * 1000)
                for pair in _trailing_share(universe, sweep_index) if pair["symbol"] not in gap_symbols]
    queue = gap_cands + trailing   # gap repair first

    repaired = fetches = 0
    for pair, since_ms in queue:
        if fetches >= BACKFILL_MAX_FETCHES or repaired >= BACKFILL_MAX_BARS:
            break
        limit = BACKFILL_TRAILING_BARS + 1 if since_ms and since_ms >= now_ms - (BACKFILL_TRAILING_BARS + 2) * _INTERVAL_S * 1000 else 1000
        try:
            rows = fetch_candles(exchange, pair["symbol"], _market_info(pair, config.market_type),
                                 config.name, "1m", _INTERVAL_S, rate_limiter, since_ms=since_ms, limit=limit)
            repaired += _insert_candles_1m(conn, rows)
            fetches += 1
        except RETRIABLE_EXCEPTIONS as exc:
            log.warning("[%s] Backfill retriable on %s: %s", thread_name, pair["symbol"], exc)
            safe_rollback(conn)
        except Exception as exc:
            log.warning("[%s] Backfill error on %s: %s", thread_name, pair["symbol"], exc)
            safe_rollback(conn)

    remaining = max(0, len(queue) - fetches)
    _heartbeat(conn, config, thread_name)
    log.info("[%s] Sweep %d: %d coins fetched, %d bars repaired, %d candidates deferred",
             thread_name, sweep_index, fetches, repaired, remaining)
    return repaired, remaining


def hl_candle_backfill_loop(exchange, config: ExchangeConfig, dsn: str, rate_limiter: RateLimiter,
                            *, max_sweeps: int | None = None):
    """HL 1m gap-backfill thread target (daemon). Own DB conn; a low-priority 300s sweep that RESTs
    only real gaps + a tiny rotating trailing share. `max_sweeps` bounds the loop for tests (None =
    forever), mirroring `ws_loop`'s `max_sessions`."""
    thread_name = f"{config.name}/{CANDLE_BACKFILL}"
    conn = connect_db(dsn)
    sweep = 0
    while max_sweeps is None or sweep < max_sweeps:
        _sleep_until_next_boundary(BACKFILL_SWEEP_INTERVAL_S, BACKFILL_SWEEP_BUFFER_S)

        if conn.closed:
            try:
                conn = connect_db(dsn)
            except RuntimeError:
                log.error("[%s] DB reconnect failed, will retry next sweep", thread_name)
                time.sleep(30)
                continue
        try:
            maybe_reload_markets(exchange)
        except Exception:
            log.exception("[%s] Markets reload failed — continuing with stale cache", thread_name)

        try:
            _run_backfill_sweep(exchange, config, conn, rate_limiter, sweep, thread_name)
        except Exception:
            log.exception("[%s] Sweep failed", thread_name)
            safe_rollback(conn)
        sweep += 1
