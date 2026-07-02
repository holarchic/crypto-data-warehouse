"""Open interest collection — one thread per exchange.

Cadence is per-strategy: HyperLiquid polls every 1 minute (bulk metaAndAssetCtxs,
which also feeds the hl_asset_ctxs context bundle); ccxt exchanges every 5 minutes.
"""

import json
import logging
import math
import time
import urllib.request

from backoff import RETRIABLE_EXCEPTIONS
from config import OI_LOOP, ExchangeConfig
from rate_limiter import (
    BINANCE_OI_WEIGHT,
    BINANCE_TICKERS_BULK_WEIGHT,
    RateLimiter,
    paced_call,
)
from status import query_active_pairs, upsert_loop_heartbeat

from db import connect_db

log = logging.getLogger(__name__)

INSERT_OI_SQL = """
    INSERT INTO open_interest (exchange, asset_id, symbol, timestamp, open_interest, oi_value_usd)
    VALUES (%(exchange)s, %(asset_id)s, %(symbol)s, to_timestamp(%(timestamp)s),
            %(open_interest)s, %(oi_value_usd)s)
    ON CONFLICT DO NOTHING
"""

# HyperLiquid asset_ctxs bundle: the full metaAndAssetCtxs per-asset context. Column
# order mirrors the backtester Grade-B seed (hl-asset-ctxs.parquet) 1:1.
INSERT_HL_CTX_SQL = """
    INSERT INTO hl_asset_ctxs (asset_id, symbol, timestamp, funding, open_interest,
            prev_day_px, day_ntl_vlm, premium, oracle_px, mark_px, mid_px,
            impact_bid_px, impact_ask_px)
    VALUES (%(asset_id)s, %(symbol)s, to_timestamp(%(timestamp)s), %(funding)s,
            %(open_interest)s, %(prev_day_px)s, %(day_ntl_vlm)s, %(premium)s,
            %(oracle_px)s, %(mark_px)s, %(mid_px)s, %(impact_bid_px)s, %(impact_ask_px)s)
    ON CONFLICT DO NOTHING
"""

OI_POLL_INTERVAL_S = 300  # 5 minutes (ccxt exchanges)
OI_POLL_BUFFER_S = 30
HL_OI_POLL_INTERVAL_S = 60  # HyperLiquid: 1-min (T-007.1, matches the Grade-B seed cadence)


class _CtxParseError(ValueError):
    """A present HL ctx field was non-numeric / NaN — the asset's snapshot is
    untrustworthy, so the caller drops that asset for the cycle (parse-or-drop)."""


def _to_float(x, field=None):
    """Parse a HL ctx field, distinguishing absent from malformed (parse, don't validate):

    - **Legitimately absent** (None / "") → None. HL omits premium/midPx/impactPxs on
      illiquid assets; that is normal — stored as SQL NULL (the parquet export converts
      NULL→NaN to match the seed).
    - **Present but non-numeric / NaN** → raise `_CtxParseError`. This is HL schema drift
      or a corrupt payload; the caller (`_build_hl_snapshots`) drops that asset's rows so
      garbage never lands as a normal snapshot, and cannot be confused with an omitted
      field. Matches the seed producer (fetch_hl_asset_ctxs.py) skipping malformed rows.

    0.0 is preserved — zero OI/funding is valid, so no `or`-coalescing. `bool` and
    non-finite (NaN/±inf) values are rejected: a bool is not a real HL numeric, and
    inf/NaN are corrupt magnitudes.
    """
    if x is None or x == "":
        return None
    if isinstance(x, bool):  # bool is an int subclass — float(True)==1.0 would sneak through
        raise _CtxParseError(f"{field or '?'}={x!r} is bool, not a number")
    try:
        v = float(x)
    except (TypeError, ValueError):
        raise _CtxParseError(f"{field or '?'}={x!r} not numeric") from None
    if not math.isfinite(v):  # NaN or ±inf
        raise _CtxParseError(f"{field or '?'} not finite ({v})")
    return v


def _get_active_pairs(conn, exchange_name):
    """Get active pairs from collection_status (5m timeframe = finest granularity)."""
    return query_active_pairs(conn, exchange_name, "5m")


def _exec_rows(cur, sql, rows):
    """Execute `sql` for each row on an open cursor. Returns inserted count. Does not
    commit — the caller owns the transaction boundary."""
    inserted = 0
    for row in rows:
        cur.execute(sql, row)
        inserted += cur.rowcount
    return inserted


def _insert_oi(conn, rows):
    """Insert OI rows in one transaction. Returns count of inserted rows."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        inserted = _exec_rows(cur, INSERT_OI_SQL, rows)
    conn.commit()
    return inserted


def _insert_hl_snapshot(conn, oi_rows, ctx_rows):
    """Atomic HL dual-write: OI + asset_ctxs under ONE transaction (single commit), so
    a failure on either insert rolls back BOTH and the two tables never diverge for a
    cycle. Owns the transaction boundary. Returns (oi_inserted, ctx_inserted)."""
    with conn.cursor() as cur:
        oi_n = _exec_rows(cur, INSERT_OI_SQL, oi_rows)
        ctx_n = _exec_rows(cur, INSERT_HL_CTX_SQL, ctx_rows)
    conn.commit()
    return oi_n, ctx_n


def _sleep_until_next(interval_s, buffer_s):
    """Sleep until the next `interval_s` boundary + buffer."""
    now = time.time()
    next_boundary = (now // interval_s + 1) * interval_s + buffer_s
    sleep_s = next_boundary - now
    log.info("[oi] Sleeping %.0fs until next %ds boundary", sleep_s, interval_s)
    time.sleep(sleep_s)


def _sleep_until_next_5m():
    """Sleep until next 5-minute boundary + buffer (ccxt OI path)."""
    _sleep_until_next(OI_POLL_INTERVAL_S, OI_POLL_BUFFER_S)


def _heartbeat(conn, config: ExchangeConfig, thread_name: str):
    """Write the per-loop liveness heartbeat, guarded so a write hiccup can't kill
    the loop (it reconnects next cycle via the `if conn.closed` guard).

    Marks loop *liveness*, not data-flow success: it fires once per completed cycle
    INCLUDING cycles where the poll caught an API/per-pair error (the loop is alive).
    Only the reconnect-fail and pairs-query-fail paths skip it — they `continue` first.
    """
    try:
        upsert_loop_heartbeat(conn, config.name, config.market_type, OI_LOOP)
    except Exception:
        log.exception("[%s] Heartbeat upsert failed", thread_name)
        if not conn.closed:
            conn.rollback()


# ---------------------------------------------------------------------------
# Strategy A — ccxt per-symbol (Binance, Bybit, OKX)
# ---------------------------------------------------------------------------


def _fetch_price_map(exchange, rate_limiter):
    """Bulk fetch_tickers() → {symbol: last_price}. One API call."""
    try:
        tickers = paced_call(
            rate_limiter, exchange, BINANCE_TICKERS_BULK_WEIGHT,
            exchange.fetch_tickers,
        )
        return {sym: t.get("last") or t.get("close") for sym, t in tickers.items() if t.get("last") or t.get("close")}
    except Exception as exc:
        log.warning("[oi] fetch_tickers failed (oi_value_usd will be None): %s", exc)
        return {}


def _oi_loop_ccxt(exchange, config: ExchangeConfig, dsn: str, rate_limiter: RateLimiter):
    """Per-symbol fetch_open_interest() for ccxt exchanges."""
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
            _heartbeat(conn, config, thread_name)
            _sleep_until_next_5m()
            continue

        # Bulk price map for computing oi_value_usd when ccxt doesn't return it
        price_map = _fetch_price_map(exchange, rate_limiter)

        rows = []
        for pair in pairs:
            try:
                oi_data = paced_call(
                    rate_limiter, exchange, BINANCE_OI_WEIGHT,
                    exchange.fetch_open_interest, pair["symbol"],
                )

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

        _heartbeat(conn, config, thread_name)
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


def _parse_hl_ctx(ctx: dict) -> dict:
    """Parse one HL asset ctx into the 10 seed floats (parse-or-drop contract):
    legitimately-absent fields → None (SQL NULL); a present-but-malformed field →
    `_CtxParseError` (the caller drops the asset). `impactPxs` is a `[bid, ask]` list,
    but may be null or short on illiquid assets → None-guarded; a present non-list
    `impactPxs` is malformed → `_CtxParseError`.
    """
    ip = ctx.get("impactPxs")
    if ip is None:
        ip = []
    elif not isinstance(ip, list):
        raise _CtxParseError(f"impactPxs not a list ({type(ip).__name__})")
    return {
        "funding": _to_float(ctx.get("funding"), "funding"),
        "open_interest": _to_float(ctx.get("openInterest"), "openInterest"),
        "prev_day_px": _to_float(ctx.get("prevDayPx"), "prevDayPx"),
        "day_ntl_vlm": _to_float(ctx.get("dayNtlVlm"), "dayNtlVlm"),
        "premium": _to_float(ctx.get("premium"), "premium"),
        "oracle_px": _to_float(ctx.get("oraclePx"), "oraclePx"),
        "mark_px": _to_float(ctx.get("markPx"), "markPx"),
        "mid_px": _to_float(ctx.get("midPx"), "midPx"),
        "impact_bid_px": _to_float(ip[0], "impactPxs[0]") if len(ip) > 0 else None,
        "impact_ask_px": _to_float(ip[1], "impactPxs[1]") if len(ip) > 1 else None,
    }


def _build_hl_snapshots(universe, asset_ctxs, asset_to_pair, exchange, now_ts):
    """Build (oi_rows, ctx_rows) for the active HL assets from one metaAndAssetCtxs
    response. Owns the boundary contract: `universe[i]` describes `asset_ctxs[i]`
    (positional; HL exposes no row-level join key), so a length mismatch is response-
    shape drift and raises ValueError (the caller aborts the cycle rather than
    zip-truncating the tail). Only assets present in `asset_to_pair` are emitted.
    """
    if len(universe) != len(asset_ctxs):
        raise ValueError(
            f"HL response length mismatch (universe={len(universe)} ctxs={len(asset_ctxs)})"
        )

    oi_rows = []
    ctx_rows = []
    dropped = []
    for meta, ctx in zip(universe, asset_ctxs):
        pair = asset_to_pair.get(meta.get("name", ""))
        if pair is None:
            continue

        # Parse-or-drop: a malformed present field means the whole snapshot is
        # untrustworthy → skip this asset (don't land garbage as a normal row).
        try:
            bundle = _parse_hl_ctx(ctx)
        except _CtxParseError as exc:
            dropped.append(f"{meta.get('name', '?')}({exc})")
            continue

        oi_val = bundle["open_interest"]
        mark_val = bundle["mark_px"]
        oi_value_usd = oi_val * mark_val if oi_val is not None and mark_val is not None else None

        oi_rows.append({
            "exchange": exchange,
            "asset_id": pair["asset_id"],
            "symbol": pair["symbol"],
            "timestamp": now_ts,
            "open_interest": oi_val,
            "oi_value_usd": oi_value_usd,
        })
        ctx_rows.append({
            "asset_id": pair["asset_id"],
            "symbol": pair["symbol"],
            "timestamp": now_ts,
            **bundle,
        })

    if dropped:  # one log at the cycle boundary, not per-field in the hot loop
        log.warning("[oi] Dropped %d HL asset(s) with malformed ctx: %s", len(dropped), dropped[:10])
    return oi_rows, ctx_rows


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
            _sleep_until_next(HL_OI_POLL_INTERVAL_S, OI_POLL_BUFFER_S)
            continue

        if not pairs:
            log.info("[%s] No active pairs, sleeping", thread_name)
            _heartbeat(conn, config, thread_name)
            _sleep_until_next(HL_OI_POLL_INTERVAL_S, OI_POLL_BUFFER_S)
            continue

        # Build asset_id → pair lookup
        asset_to_pair = {p["asset_id"]: p for p in pairs}

        try:
            universe, asset_ctxs = _fetch_hl_meta_and_ctxs()
            now_ts = time.time()
            oi_rows, ctx_rows = _build_hl_snapshots(
                universe, asset_ctxs, asset_to_pair, config.name, now_ts,
            )
            oi_inserted, ctx_inserted = _insert_hl_snapshot(conn, oi_rows, ctx_rows)
            log.info(
                "[%s] Poll: %d OI + %d ctx rows inserted (%d active pairs)",
                thread_name, oi_inserted, ctx_inserted, len(pairs),
            )
        except Exception as exc:
            # Includes _build_hl_snapshots' ValueError on response-shape drift — abort
            # the cycle with nothing committed. Heartbeat (liveness) still fires below.
            log.warning("[%s] Error in HL OI cycle: %s", thread_name, exc)
            if not conn.closed:
                conn.rollback()

        _heartbeat(conn, config, thread_name)
        _sleep_until_next(HL_OI_POLL_INTERVAL_S, OI_POLL_BUFFER_S)


# ---------------------------------------------------------------------------
# Entry point — dispatches to the right strategy
# ---------------------------------------------------------------------------


def oi_loop(exchange, config: ExchangeConfig, dsn: str, rate_limiter: RateLimiter):
    """OI collection loop. Dispatches to native HL bulk or ccxt per-symbol."""
    if config.ccxt_class == "hyperliquid":
        _oi_loop_hyperliquid(config, dsn)
    else:
        _oi_loop_ccxt(exchange, config, dsn, rate_limiter)
