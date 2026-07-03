"""Binance + Bybit liquidation WS feed specs (Grade C forward sidecar).

Two `WSFeedSpec` records injected into `ws.ws_loop` — the generic engine owns the
connection/reconnect/batch/heartbeat machinery; this module owns only the venue data:
routed URLs, topic derivation, frame parsing, side normalization, and the batch insert.

**asset_id resolution.** `WSFeedSpec.parse_message` receives only the raw frame, but a
`liquidations` row needs the cc `asset_id` while the frame carries the venue-*native*
symbol (`BTCUSDT`). Each builder closes over a shared mutable `native_to_asset` map that
`topics_for_pairs` rebuilds (engine-called at subscribe + 60 s resub) and `parse_message`
reads. Both run in the ONE `ws_loop` thread for that feed (`_run_session` calls them
sequentially), so the map needs no lock. Binance's all-market stream is filtered to cc's
tracked pairs — an untracked symbol has no map entry, so its event is skipped rather than
stored with a guessed id.
"""

import json
import logging

from config import LIQUIDATION_WS
from ws import WSFeedSpec

log = logging.getLogger(__name__)

# --- Endpoints -------------------------------------------------------------
# Binance USDM WS uses ROUTED base paths; `!forceOrder@arr` requires the `/market` route —
# an unrouted `/ws` connection receives only Public data (no liquidations). Verified against
# primary docs 2026-07.
BINANCE_LIQ_URL = "wss://fstream.binance.com/market/ws/!forceOrder@arr"
BYBIT_LIQ_URL = "wss://stream.bybit.com/v5/public/linear"

BINANCE_PROACTIVE_RECONNECT_S = 82800.0  # 23 h < Binance's 24 h server-side drop
BYBIT_PING_INTERVAL_S = 20.0             # Bybit closes an idle socket (~30 s) without a ping

# Bybit linear has NO args-per-request cap (the ≤10 is Spot-only); the ceiling is a
# 21 000-char args total *per connection*. Warn as the cumulative subscribed-topic chars
# approach it, so a future universe-growth breach is visible before subscribes silently fail
# (sharding — multiple ws_loop threads over pair subsets — is deferred until then). Frames are
# chunked for clean resubscribe diffs; the shard trigger is the char total, not the frame count.
BYBIT_ARGS_CHAR_CEILING = 21000
BYBIT_ARGS_CHAR_WARN = 18000
BYBIT_SUBSCRIBE_CHUNK = 100              # topics per subscribe frame (well under the char ceiling)

# Side normalization → `liquidated_side ∈ {long, short}`. The venues' conventions are OPPOSITE:
# Binance `o.S` is the ORDER side (a forced SELL closes/liquidates a LONG); Bybit `S` is the
# POSITION side liquidated (Buy = a long was liquidated).
_BINANCE_LIQ_SIDE = {"SELL": "long", "BUY": "short"}
_BYBIT_LIQ_SIDE = {"Buy": "long", "Sell": "short"}

INSERT_LIQ_SQL = """
    INSERT INTO liquidations (timestamp, venue, grade, asset_id, liquidated_side, px, sz)
    VALUES (to_timestamp(%(timestamp)s), %(venue)s, 'c', %(asset_id)s, %(liquidated_side)s,
            %(px)s, %(sz)s)
    ON CONFLICT (venue, asset_id, timestamp, oid, tid) DO NOTHING
"""


# ---------------------------------------------------------------------------
# Parse helpers — parse-or-drop split: bad *identity* (symbol/side/time) drops the event;
# bad *magnitude* (px/sz) lands the row with that column NULL (side+time is the C-grade signal).
# ---------------------------------------------------------------------------


def _num(x):
    """Magnitude field → float or None. Never raises; a NULL px/sz still lands a row."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _ts_seconds(ms):
    """Venue ms timestamp → epoch seconds, or None if non-numeric (identity field → drop)."""
    try:
        return int(ms) / 1000.0
    except (TypeError, ValueError):
        return None


def _row(venue, asset_id, liquidated_side, px, sz, ts):
    return {"venue": venue, "asset_id": asset_id, "liquidated_side": liquidated_side,
            "px": px, "sz": sz, "timestamp": ts}


def _rebuild_map(native_to_asset, pairs):
    """Rebuild the shared native_symbol→asset_id map IN PLACE (clear+update, not rebind, so the
    closures keep the same dict object). Single-threaded per ws_loop → no lock needed."""
    native_to_asset.clear()
    native_to_asset.update(
        {p["native_symbol"]: p["asset_id"] for p in pairs if p.get("native_symbol")}
    )


def _insert_liquidations(conn, events):
    """Batch-insert Grade-C rows in one transaction; returns inserted count. Raises on failure
    so the engine's `_flush` rolls back + retains the buffer. `ON CONFLICT` is inert for C rows
    (NULL oid/tid + Postgres NULLS DISTINCT never conflict — every event lands, incl. a
    reconnect-replay dup, the accepted sidecar cost), kept for uniformity."""
    if not events:
        return 0
    with conn.cursor() as cur:
        inserted = 0
        for e in events:
            cur.execute(INSERT_LIQ_SQL, e)
            inserted += cur.rowcount
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _parse_binance_liq(raw, native_to_asset):
    """Binance `!forceOrder@arr` frame → [event] (one order per frame; the all-market stream
    pushes ≤1/symbol/1000 ms). `[]` for a non-forceOrder frame or an unplaceable event."""
    o = json.loads(raw).get("o")
    if not isinstance(o, dict):
        return []  # not a forceOrder payload
    asset_id = native_to_asset.get(o.get("s"))
    liquidated_side = _BINANCE_LIQ_SIDE.get(o.get("S"))
    ts = _ts_seconds(o.get("T"))
    if asset_id is None or liquidated_side is None or ts is None:
        return []  # untracked symbol / unknown side / bad time → unplaceable, drop
    px = _num(o.get("ap"))
    if px is None:
        px = _num(o.get("p"))
    sz = _num(o.get("z"))
    if sz is None:
        sz = _num(o.get("q"))
    return [_row("binance", asset_id, liquidated_side, px, sz, ts)]


def _parse_bybit_liq(raw, native_to_asset):
    """Bybit `allLiquidation.{sym}` frame → [events] (`data[]` unrolled). ANY frame without a
    `data` array (subscribe ack, `op:pong`, any control shape) → `[]` — the absence of `data`
    is the signal, so we don't enumerate control-frame variants."""
    data = json.loads(raw).get("data")
    if not isinstance(data, list):
        return []  # control / pong / ack — no events
    events = []
    for d in data:
        if not isinstance(d, dict):
            continue
        asset_id = native_to_asset.get(d.get("s"))
        liquidated_side = _BYBIT_LIQ_SIDE.get(d.get("S"))
        ts = _ts_seconds(d.get("T"))
        if asset_id is None or liquidated_side is None or ts is None:
            continue  # unplaceable identity → skip this event, keep the rest
        events.append(_row("bybit", asset_id, liquidated_side, _num(d.get("p")), _num(d.get("v")), ts))
    return events


# ---------------------------------------------------------------------------
# Bybit subscribe framing + ceiling guard
# ---------------------------------------------------------------------------


def _bybit_op_frames(op, topics):
    """Chunk topics into `{"op": op, "args": [...]}` frames (≤`BYBIT_SUBSCRIBE_CHUNK` each)."""
    return [
        json.dumps({"op": op, "args": topics[i:i + BYBIT_SUBSCRIBE_CHUNK]})
        for i in range(0, len(topics), BYBIT_SUBSCRIBE_CHUNK)
    ]


def _bybit_subscribe_frames(topics):
    return _bybit_op_frames("subscribe", topics)


def _bybit_unsubscribe_frames(topics):
    return _bybit_op_frames("unsubscribe", topics)


def _check_bybit_ceiling(topics, thread_name="bybit/liq"):
    """Warn as the cumulative subscribed-topic char total approaches the 21 000-char
    per-connection ceiling — the breach is visible before subscribes silently fail (sharding
    deferred until this fires). Returns the char total (for tests)."""
    total = sum(len(t) for t in topics)
    if total >= BYBIT_ARGS_CHAR_WARN:
        log.warning(
            "[%s] Subscribed-topic chars %d/%d approaching the per-connection ceiling (%d topics)"
            " — sharding needed soon", thread_name, total, BYBIT_ARGS_CHAR_CEILING, len(topics),
        )
    return total


# ---------------------------------------------------------------------------
# Spec builders
# ---------------------------------------------------------------------------


def build_binance_liq_spec() -> WSFeedSpec:
    """Binance all-market liquidation stream (URL-routed `/market`, no per-symbol topics).
    `topics_for_pairs` returns `[]` but STILL rebuilds the shared map so `parse_message`
    resolves asset_id."""
    native_to_asset: dict[str, str] = {}

    def topics_for_pairs(pairs):
        _rebuild_map(native_to_asset, pairs)
        return []  # all-market stream lives in the URL

    return WSFeedSpec(
        name="liq",
        loop_tf=LIQUIDATION_WS,
        url=BINANCE_LIQ_URL,
        topics_for_pairs=topics_for_pairs,
        subscribe_frames=lambda topics: [],
        unsubscribe_frames=lambda topics: [],
        parse_message=lambda raw: _parse_binance_liq(raw, native_to_asset),
        insert_batch=_insert_liquidations,
        proactive_reconnect_s=BINANCE_PROACTIVE_RECONNECT_S,
    )


def build_bybit_liq_spec() -> WSFeedSpec:
    """Bybit per-symbol `allLiquidation.{native}` feed. `topics_for_pairs` rebuilds the shared
    map, checks the char ceiling, and returns the topic list; subscribes are batched, ping 20 s."""
    native_to_asset: dict[str, str] = {}

    def topics_for_pairs(pairs):
        _rebuild_map(native_to_asset, pairs)
        topics = [f"allLiquidation.{p['native_symbol']}" for p in pairs if p.get("native_symbol")]
        _check_bybit_ceiling(topics)
        return topics

    return WSFeedSpec(
        name="liq",
        loop_tf=LIQUIDATION_WS,
        url=BYBIT_LIQ_URL,
        topics_for_pairs=topics_for_pairs,
        subscribe_frames=_bybit_subscribe_frames,
        unsubscribe_frames=_bybit_unsubscribe_frames,
        parse_message=lambda raw: _parse_bybit_liq(raw, native_to_asset),
        insert_batch=_insert_liquidations,
        ping_frame=json.dumps({"op": "ping"}),
        ping_interval_s=BYBIT_PING_INTERVAL_S,
    )


# Venue → spec builder, for `main.run()` to launch the right feed behind `has_liquidation_ws`.
LIQ_SPEC_BUILDERS = {"binance": build_binance_liq_spec, "bybit": build_bybit_liq_spec}
