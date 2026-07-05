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

from config import CANDLE_WS
from ws import WSFeedSpec

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
