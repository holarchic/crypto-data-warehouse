"""HyperLiquid 1-minute candle WS feed (T-015.1) → `candles_1m`.

HL's ~1 req/s shared info budget is reserved for the unbackfillable dense 1-min OI, so any live REST
1m path is off the table — a live REST backfill was deploy-measured (2026-07-05) to starve HL's tight
single limiter (5m…1w per-bar coverage fell 161→33, HL having no priority-reserve lane). So this module
is **WS-only**: HL's native WS `candle` subscription is the real-time source (zero REST). A rare WS-drop
gap is repaired out-of-band via the T-011 export/backfill path, not a live-contending sweep. The insert
UPSERTs (below), never the generic `ON CONFLICT DO NOTHING` — HL streams the *forming* bar continuously
with no closed flag, so the row must converge to the bar's closed value (last-write-wins).

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

from config import CANDLE_WS
from ws import WSFeedSpec

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
