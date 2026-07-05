"""HyperLiquid 1-minute candle WS feed + REST gap-backfill (T-015.1) → `candles_1m`.

HL's ~1 req/s shared info budget is reserved for the unbackfillable dense 1-min OI, so a per-symbol
REST 1m loop is off the table. Instead this module runs HL's native WS `candle` subscription as the
real-time primary (zero REST) and a low-priority REST backfill (this file's second half) that repairs
only what the WS drops. Both write `candles_1m` via an UPSERT (below), never the generic
`ON CONFLICT DO NOTHING` insert — HL streams the *forming* bar continuously with no closed flag, so the
row must converge to the bar's closed value (last-write-wins).

**Coin-name mapping (load-bearing).** The WS `coin`/`s` field is HL's authoritative meta *name*, which
is NOT always the cc `asset_id`: ccxt uppercases the base, so HL's 1000×-token names (`kPEPE`, `kBONK`,
`kSHIB`, …) become `KPEPE`/`KBONK`/`KSHIB` as cc's `asset_id`. Subscribing with the wrong-cased name
makes HL **drop the whole connection** (verified 2026-07-05 — one bad coin killed all 161 subs). So the
WS name is resolved from ccxt `market['info']['name']` (authoritative — `KAITO`/`KAS` stay uppercase,
only the 1000× tokens lowercase the `k`). `topics_for_pairs` rebuilds a shared `hl_name → {asset_id,
symbol, native_symbol, market_type}` map (keyed on the HL name so `parse_message` can route the `s`
field) and subscribes with the HL names; the stored row keeps cc's canonical uppercase `asset_id`. Both
run in the one `ws_loop` thread, so the map needs no lock (the liquidations pattern). HL's
`native_symbol` is a numeric asset index (28/210) — unused here, unlike the liquidations feed.
"""

import json
import logging
import time

from backoff import RETRIABLE_EXCEPTIONS
from candles import CANDLE_SEED_LIMIT
from config import CANDLE_BACKFILL, CANDLE_WS, ExchangeConfig
from rate_limiter import RateLimiter
from status import query_active_pairs, upsert_loop_heartbeat
from ws import WSFeedSpec

from db import connect_db, maybe_reload_markets, safe_rollback

log = logging.getLogger(__name__)

# --- Endpoint + engine tunables --------------------------------------------
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
HL_PING_INTERVAL_S = 30.0   # HL closes an idle socket at 60s (timeouts doc) → ping at 2× margin

# HL documents 1000 subs / 10 conns / 30 new-conns/min per IP; ~161 candle subs on one connection is
# well inside (verified 2026-07-05: all 161 held on a single connection once coin names were correct —
# see the name-mapping note below). The ceiling-guard warns as the count nears the documented limit.
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


def _hl_name(pair, name_by_asset):
    """The HL WS coin name for a pair: ccxt's authoritative `info.name` (falls back to the cc asset_id
    when absent — correct for every coin whose HL name already equals its uppercase base)."""
    return name_by_asset.get(pair["asset_id"], pair["asset_id"])


def _rebuild_map(asset_map, pairs, name_by_asset):
    """Rebuild the shared HL-name→{asset_id,symbol,native_symbol,market_type} map IN PLACE (clear+update,
    not rebind, so the closures keep the same dict object). Keyed on the HL WS name (what `parse_message`
    sees in `s`); the value keeps cc's canonical `asset_id`. Single-threaded per ws_loop → no lock."""
    asset_map.clear()
    asset_map.update({
        _hl_name(p, name_by_asset): {
            "asset_id": p["asset_id"],
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
    info = asset_map.get(d.get("s"))  # s = HL coin name → map → cc canonical asset_id
    ts = _ts_seconds(d.get("t"))      # t = bar-open time (ms)
    if info is None or ts is None:
        return []  # untracked coin / bad time → unplaceable, drop
    return [{
        "exchange": "hyperliquid",
        "market_type": info["market_type"],
        "asset_id": info["asset_id"],
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
    """Warn as the subscription count nears HL's documented per-IP ceiling. Returns the count (tests)."""
    n = len(coins)
    if n >= HL_SUB_WARN:
        log.warning("[%s] %d candle subs nearing the ~%d ceiling — shard across connections soon",
                    thread_name, n, HL_SUB_CEILING)
    return n


# ---------------------------------------------------------------------------
# Spec builder
# ---------------------------------------------------------------------------


def hl_coin_names(exchange) -> dict[str, str]:
    """Map cc `asset_id` (ccxt's uppercase base) → HL's authoritative WS coin name (`info.name`). Differs
    only for 1000× tokens (`KPEPE`→`kPEPE`, …). Rebuilt each resubscribe so a new listing's name is
    picked up as ccxt reloads markets — a wrong name would make HL drop the whole connection."""
    names = {}
    for m in exchange.markets.values():
        info = m.get("info")
        if m.get("swap") and isinstance(info, dict) and info.get("name") and m.get("base"):
            names[m["base"]] = info["name"]
    return names


def build_hl_candle_spec(exchange) -> WSFeedSpec:
    """HL `candle` feed on the generic `ws.py` engine — one connection for the whole ~161-coin universe
    (well inside HL's per-IP sub ceiling). `topics_for_pairs` resolves each pair to its HL WS coin name
    (via ccxt `info.name`), rebuilds the shared `hl_name → {asset_id,…}` map, and returns the HL names
    (the engine diffs that set across resubs); the frame builders wrap each in HL's candle-subscribe
    JSON; `parse_message` routes the frame's `s` back through the map to the canonical row."""
    asset_map: dict[str, dict] = {}

    def topics_for_pairs(pairs):
        _rebuild_map(asset_map, pairs, hl_coin_names(exchange))
        coins = list(asset_map)   # the HL WS names
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
        cols = [d[0] for d in cur.description]           # name fields, don't index by position
        rows = (dict(zip(cols, row)) for row in cur.fetchall())
        return {r["symbol"]: {"last_ts": r["last_ts"], "earliest_gap_after": r["earliest_gap_after"]}
                for r in rows}


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


def _fetch_limit(since_ms, now_ms):
    """Bars to request for a candidate: a recent `since` (a trailing coin or a small fresh gap) needs
    only a few bars; a seed (`since=None`) or an old gap needs the full window (the per-sweep bar cap
    then clamps this, so a huge backlog still spreads across sweeps)."""
    if since_ms and since_ms >= now_ms - (BACKFILL_TRAILING_BARS + 2) * _INTERVAL_S * 1000:
        return BACKFILL_TRAILING_BARS + 1
    return CANDLE_SEED_LIMIT


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


def _run_backfill_sweep(exchange, config, conn, rate_limiter, sweep_index, thread_name, fetch_candles):
    """One sweep: scan → gap candidates + rotating trailing share → bounded fetch/UPSERT. Gap repair
    takes precedence over trailing for the same coin. `fetch_candles` is injected (not imported from
    `main`) so this feature module has no reverse dependency on the application root. Returns
    (repaired_bars, remaining_candidates)."""
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

    # Bound by ATTEMPTS (not successes — a failing sweep must not fetch unbounded) and by BARS. The
    # per-fetch `limit` is clamped to the remaining bar budget so the sweep can never overshoot
    # BACKFILL_MAX_BARS by a full request — the OI-protection contract is strict, not approximate.
    repaired = attempts = 0
    for pair, since_ms in queue:
        remaining_bars = BACKFILL_MAX_BARS - repaired
        if attempts >= BACKFILL_MAX_FETCHES or remaining_bars <= 0:
            break
        attempts += 1
        limit = min(_fetch_limit(since_ms, now_ms), remaining_bars)
        try:
            rows = fetch_candles(exchange, pair["symbol"], _market_info(pair, config.market_type),
                                 config.name, "1m", _INTERVAL_S, rate_limiter, since_ms=since_ms, limit=limit)
            repaired += _insert_candles_1m(conn, rows)
        except RETRIABLE_EXCEPTIONS as exc:
            log.warning("[%s] Backfill retriable on %s: %s", thread_name, pair["symbol"], exc)
            safe_rollback(conn)
        except Exception as exc:
            log.warning("[%s] Backfill error on %s: %s", thread_name, pair["symbol"], exc)
            safe_rollback(conn)

    remaining = max(0, len(queue) - attempts)
    _heartbeat(conn, config, thread_name)
    log.info("[%s] Sweep %d: %d coins attempted, %d bars repaired, %d candidates deferred",
             thread_name, sweep_index, attempts, repaired, remaining)
    return repaired, remaining


def hl_candle_backfill_loop(exchange, config: ExchangeConfig, dsn: str, rate_limiter: RateLimiter,
                            fetch_candles, *, max_sweeps: int | None = None):
    """HL 1m gap-backfill thread target (daemon). Own DB conn; a low-priority 300s sweep that RESTs
    only real gaps + a tiny rotating trailing share. `fetch_candles` is injected by `main.run()` (the
    app root wires the concrete fetcher into this feature — one-way dependency flow). `max_sweeps`
    bounds the loop for tests (None = forever), mirroring `ws_loop`'s `max_sessions`."""
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
            _run_backfill_sweep(exchange, config, conn, rate_limiter, sweep, thread_name, fetch_candles)
        except Exception:
            log.exception("[%s] Sweep failed", thread_name)
            safe_rollback(conn)
        sweep += 1
