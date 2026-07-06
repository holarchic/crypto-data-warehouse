"""Open interest collection — one thread per exchange, three strategies.

- **HyperLiquid** (`_oi_loop_hyperliquid`): one bulk `metaAndAssetCtxs` POST/cycle at
  `oi_poll_interval_s` (1-min), dual-writing OI + the `hl_asset_ctxs` context bundle.
- **Bybit/OKX** (`_oi_loop_bulk`): one bulk call/cycle (Bybit `fetch_tickers` → OI+USD;
  OKX `fetch_open_interests` + a price map for USD), flat `oi_poll_interval_s` (30s).
- **Binance** (`_oi_loop_binance`): the only per-symbol-expensive venue — a 5-min
  baseline via `openInterestHist` (separate weight bucket) plus sub-5-min dense
  `openInterest` for symbols within ±`oi_settlement_window_s` of their own settlement.

All cadences come from `ExchangeConfig` (see `exchanges.yaml`), not module constants.
"""

import json
import logging
import math
import queue
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

from backoff import RETRIABLE_EXCEPTIONS
from config import OI_LOOP, ExchangeConfig
from rate_limiter import (
    BINANCE_FUNDING_BULK_WEIGHT,
    BINANCE_OI_HIST_SPACING_S,
    BINANCE_OI_HIST_WEIGHT,
    BINANCE_OI_WEIGHT,
    BINANCE_TICKERS_BULK_WEIGHT,
    RateLimiter,
    paced_call,
)
from status import query_active_pairs, upsert_loop_heartbeat

from db import connect_db, create_exchange, maybe_reload_markets, safe_rollback

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

# Binance baseline cursor seed: the last *baseline* (5-min-aligned) OI timestamp per
# symbol. open_interest mixes dense (arbitrary `now`) rows with 5-min baseline rows, so
# raw MAX(timestamp) would seed from a newer dense ts and permanently skip missing 5-min
# points on restart. Baseline rows land on exact 5-min marks (epoch % 300 == 0, zero
# sub-second); dense rows carry sub-second fractions, so ::numeric %% 300 = 0 isolates
# baseline rows only. (%% is a literal % in a psycopg param string.)
SEED_BASELINE_CURSOR_SQL = """
    SELECT symbol, EXTRACT(EPOCH FROM MAX(timestamp)) AS ts
    FROM open_interest
    WHERE exchange = %(exchange)s
      AND EXTRACT(EPOCH FROM timestamp)::numeric %% 300 = 0
    GROUP BY symbol
"""

OI_POLL_BUFFER_S = 30
BASELINE_HIST_LIMIT = 500  # openInterestHist max page; since-based, so paged catch-up


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
    """Sleep until the next `interval_s` boundary + buffer. The buffer is clamped to half
    the interval so it can never push the target a full interval past the boundary — at a
    sub-minute cadence (Bybit/OKX 30s) a raw 30s buffer would land on the *next* boundary
    and silently double the poll period to ~60s."""
    buffer_s = min(buffer_s, interval_s // 2)
    now = time.time()
    next_boundary = (now // interval_s + 1) * interval_s + buffer_s
    sleep_s = next_boundary - now
    log.info("[oi] Sleeping %.0fs until next %ds boundary", sleep_s, interval_s)
    time.sleep(sleep_s)


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
            safe_rollback(conn)


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------


def _fetch_price_map(exchange, rate_limiter):
    """Bulk fetch_tickers() → {symbol: last_price}. One API call. Used by the OKX bulk
    path (its `/public/open-interest` omits USD) to compute oi_value_usd."""
    try:
        tickers = paced_call(
            rate_limiter, exchange, BINANCE_TICKERS_BULK_WEIGHT,
            exchange.fetch_tickers,
        )
        return {sym: t.get("last") or t.get("close") for sym, t in tickers.items() if t.get("last") or t.get("close")}
    except Exception as exc:
        log.warning("[oi] fetch_tickers failed (oi_value_usd will be None): %s", exc)
        return {}


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
        # Parse-or-drop: any malformed present datum (bad field, wrong-container
        # impactPxs, or a non-dict meta/ctx) means the snapshot is untrustworthy →
        # skip this asset. Row-container drift is drained here per-asset too, so it
        # can't escape as a raw exception that aborts the whole cycle.
        try:
            if not isinstance(meta, dict) or not isinstance(ctx, dict):
                raise _CtxParseError(
                    f"non-dict meta/ctx ({type(meta).__name__}/{type(ctx).__name__})"
                )
            # `meta["name"]` is HL's native coin name (`kPEPE` for 1000× tokens); the active-pair map is
            # keyed on cc's ccxt-canonical asset_id (`KPEPE`). HL names are uppercase but for the leading
            # `k`, so `.upper()` bridges name→asset_id without adding ccxt to this raw-POST loop. Without
            # it every 1000× token silently missed the lookup and was dropped from both tables.
            pair = asset_to_pair.get(meta.get("name", "").upper())
            if pair is None:
                continue  # not an active pair — not malformed, just not collected
            bundle = _parse_hl_ctx(ctx)
        except _CtxParseError as exc:
            name = meta.get("name", "?") if isinstance(meta, dict) else "?"
            dropped.append(f"{name}({exc})")
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
            # HL-native name (= the backtester seed `coin`, e.g. `kPEPE`) so the T-011 export is a
            # passthrough. Differs from `open_interest.asset_id` (ccxt-canonical `KPEPE`) only for 1000×
            # tokens; `hl_asset_ctxs` is HL-only + seed-mirroring, `open_interest` is cross-venue.
            "asset_id": meta["name"],
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
            safe_rollback(conn)
            _sleep_until_next(config.oi_poll_interval_s, OI_POLL_BUFFER_S)
            continue

        if not pairs:
            log.info("[%s] No active pairs, sleeping", thread_name)
            _heartbeat(conn, config, thread_name)
            _sleep_until_next(config.oi_poll_interval_s, OI_POLL_BUFFER_S)
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
                safe_rollback(conn)

        _heartbeat(conn, config, thread_name)
        _sleep_until_next(config.oi_poll_interval_s, OI_POLL_BUFFER_S)


# ---------------------------------------------------------------------------
# Strategy C — bulk (Bybit, OKX): one bulk call/cycle, flat cadence
# ---------------------------------------------------------------------------


def _parse_bulk_oi_bybit(tickers, active, exchange_name, now_ts):
    """Bybit `fetch_tickers` → OI rows. OI is NOT on the ccxt unified ticker — it lives
    in `ticker["info"]` (`openInterest` contracts + `openInterestValue` USD). Parse-or-
    drop: an absent field → None (NULL); a present-but-malformed one drops that symbol."""
    rows, dropped = [], []
    for sym, pair in active.items():
        t = tickers.get(sym)
        if t is None:
            continue
        info = t.get("info") or {}
        try:
            oi_amt = _to_float(info.get("openInterest"), "openInterest")
            oi_usd = _to_float(info.get("openInterestValue"), "openInterestValue")
        except _CtxParseError as exc:
            dropped.append(f"{sym}({exc})")
            continue
        rows.append({
            "exchange": exchange_name, "asset_id": pair["asset_id"], "symbol": sym,
            "timestamp": now_ts, "open_interest": oi_amt, "oi_value_usd": oi_usd,
        })
    return rows, dropped


def _parse_bulk_oi_okx(ois, price_map, active, exchange_name, now_ts):
    """OKX `fetch_open_interests` → OI rows. The bulk `/public/open-interest` returns
    `oi`+`oiCcy` but NO `oiUsd`, so USD is computed from the price map (openInterestAmount
    × last), preserving the prior per-symbol path's semantics. Values are already
    ccxt-parsed numbers, so no `_to_float`."""
    rows = []
    for sym, pair in active.items():
        oi_struct = ois.get(sym)
        if oi_struct is None:
            continue
        oi_amt = oi_struct.get("openInterestAmount")
        oi_usd = oi_struct.get("openInterestValue")
        if oi_usd is None and oi_amt is not None:
            price = price_map.get(sym)
            if price is not None:
                oi_usd = oi_amt * price
        rows.append({
            "exchange": exchange_name, "asset_id": pair["asset_id"], "symbol": sym,
            "timestamp": now_ts, "open_interest": oi_amt, "oi_value_usd": oi_usd,
        })
    return rows


def _oi_loop_bulk(exchange, config: ExchangeConfig, dsn: str, rate_limiter: RateLimiter):
    """Bulk OI for Bybit/OKX — one bulk call/cycle, flat `oi_poll_interval_s`."""
    thread_name = f"{config.name}/oi"
    conn = connect_db(dsn)

    while True:
        if conn.closed:
            try:
                conn = connect_db(dsn)
            except RuntimeError:
                log.error("[%s] DB reconnect failed, will retry next cycle", thread_name)
                time.sleep(30)
                continue

        try:
            pairs = _get_active_pairs(conn, config.name)
        except Exception:
            log.exception("[%s] Failed to query active pairs", thread_name)
            safe_rollback(conn)
            _sleep_until_next(config.oi_poll_interval_s, OI_POLL_BUFFER_S)
            continue

        if not pairs:
            log.info("[%s] No active pairs, sleeping", thread_name)
            _heartbeat(conn, config, thread_name)
            _sleep_until_next(config.oi_poll_interval_s, OI_POLL_BUFFER_S)
            continue

        active = {p["symbol"]: p for p in pairs}
        now_ts = time.time()
        dropped = []
        try:
            if config.name == "bybit":
                tickers = paced_call(
                    rate_limiter, exchange, BINANCE_TICKERS_BULK_WEIGHT, exchange.fetch_tickers,
                )
                rows, dropped = _parse_bulk_oi_bybit(tickers, active, config.name, now_ts)
            else:  # okx
                ois = paced_call(
                    rate_limiter, exchange, BINANCE_OI_WEIGHT, exchange.fetch_open_interests,
                )
                price_map = _fetch_price_map(exchange, rate_limiter)
                rows = _parse_bulk_oi_okx(ois, price_map, active, config.name, now_ts)
            inserted = _insert_oi(conn, rows)
            if dropped:
                log.warning("[%s] Dropped %d symbol(s) with malformed OI: %s",
                            thread_name, len(dropped), dropped[:10])
            log.info("[%s] Poll: %d OI snapshots inserted (%d pairs)", thread_name, inserted, len(pairs))
        except Exception as exc:
            log.warning("[%s] Error in bulk OI cycle: %s", thread_name, exc)
            if not conn.closed:
                safe_rollback(conn)

        _heartbeat(conn, config, thread_name)
        _sleep_until_next(config.oi_poll_interval_s, OI_POLL_BUFFER_S)


# ---------------------------------------------------------------------------
# Strategy D — Binance two-source (baseline off-pool + dense main-pool)
# ---------------------------------------------------------------------------


def _seed_baseline_cursor(conn, exchange_name):
    """Seed {symbol: last_baseline_ts_seconds} from the DB so a restart backfills the
    gap. Isolates 5-min-aligned baseline rows from dense rows (see SEED_BASELINE_CURSOR_SQL).
    Returns {} on empty DB (each symbol then starts from openInterestHist's default window)."""
    with conn.cursor() as cur:
        cur.execute(SEED_BASELINE_CURSOR_SQL, {"exchange": exchange_name})
        return {sym: float(ts) for sym, ts in cur.fetchall() if ts is not None}


def _prune_binance_state(active_keys, *state_dicts):
    """Drop entries for symbols no longer active from each cross-tick state dict, so the
    maps stay bounded to the active set under listing churn (not symbols-ever-seen)."""
    live = set(active_keys)
    for d in state_dicts:
        for sym in [s for s in d if s not in live]:
            del d[sym]


def _baseline_batch_size(n_pairs, tick_s, interval_s):
    """Symbols per tick so a full baseline sweep completes within `interval_s`:
    ceil(n · tick / interval), at least 1. (Gap-freedom comes from `since`, not this rate.)"""
    ticks_per_interval = max(1, interval_s // tick_s)
    return max(1, math.ceil(n_pairs / ticks_per_interval))


def _binance_dense_set(funding_rates, active, now, window_s, prev_next_ft, settlement_at):
    """Partition active symbols into the dense (near-settlement) set, mutating the
    cross-tick state dicts. A symbol is dense if it is within `window_s` BEFORE its next
    settlement (`0 ≤ nextFundingTime − now ≤ window`) OR within `window_s` AFTER a
    settlement just detected by `nextFundingTime` jumping vs the prior tick (the funding
    interval isn't in the bulk premiumIndex, so change-detection stands in for it).
    Restart blind spot: `prev_next_ft` empty ⇒ no post-window until the first jump."""
    dense = []
    for sym, pair in active.items():
        fr = funding_rates.get(sym)
        if fr is None:
            continue
        nft_ms = fr.get("fundingTimestamp")
        if nft_ms is None:
            continue
        prev = prev_next_ft.get(sym)
        if prev is not None and nft_ms != prev:
            settlement_at[sym] = now  # nextFundingTime rolled ⇒ a settlement just occurred
        prev_next_ft[sym] = nft_ms
        time_to = nft_ms / 1000 - now
        pre = 0 <= time_to <= window_s
        sa = settlement_at.get(sym)
        post = sa is not None and (now - sa) <= window_s
        if pre or post:
            dense.append(pair)
    return dense


def _dense_row(oi_data, pair, exchange_name, funding_rates, now_ts):
    """Build one dense OI row from a fetch_open_interest result, or None if empty.
    Pure — runs in a worker thread, mutates nothing shared (reads read-only funding_rates)."""
    if oi_data is None:
        return None
    oi_amt = oi_data.get("openInterestAmount")
    mark = (funding_rates.get(pair["symbol"]) or {}).get("markPrice")
    oi_usd = oi_amt * mark if oi_amt is not None and mark is not None else None
    return {
        "exchange": exchange_name, "asset_id": pair["asset_id"], "symbol": pair["symbol"],
        "timestamp": now_ts, "open_interest": oi_amt, "oi_value_usd": oi_usd,
    }


def _binance_dense_fetch(exchange, exchange_name, rate_limiter, dense_pairs, funding_rates,
                         now_ts, thread_name):
    """Sequential instantaneous OI for each dense symbol on `exchange` (main weight pool);
    USD = oi × markPx. The zero-concurrency path — used at `oi_dense_concurrency<=1` or while a
    `_DenseFetchPool` hasn't been built yet. Per-symbol errors isolate (one bad symbol never
    aborts). At a mass settlement (~541 symbols) this overruns the 60 s tick (~140 s), which is
    exactly why `_DenseFetchPool` parallelizes it — but with isolated per-worker sessions."""
    rows = []
    for pair in dense_pairs:
        sym = pair["symbol"]
        try:
            # priority=True: dense OI rides the reserved weight lane (T-007.4) so it never
            # blocks on the candle-kline burst — an untaken instantaneous OI sample is unbackfillable.
            oi_data = paced_call(
                rate_limiter, exchange, BINANCE_OI_WEIGHT, exchange.fetch_open_interest, sym,
                priority=True,
            )
        except RETRIABLE_EXCEPTIONS as exc:
            log.warning("[%s] Retriable error on dense %s: %s", thread_name, sym, exc)
            continue
        except Exception as exc:
            log.warning("[%s] Error on dense %s: %s", thread_name, sym, exc)
            continue
        row = _dense_row(oi_data, pair, exchange_name, funding_rates, now_ts)
        if row is not None:
            rows.append(row)
    return rows


def _close_exchange(exchange):
    """Best-effort release of a ccxt exchange's HTTP session (used on partial pool-init)."""
    try:
        exchange.close()
    except Exception:
        pass


class _DenseFetchPool:
    """Parallel Binance dense fetch with ISOLATED per-worker sessions. K worker threads each
    borrow their OWN ccxt exchange from a checkout queue, so a concurrent dense fetch never
    shares a `requests.Session` or `last_response_headers` across threads — no shared-session
    thread-safety bet, no limiter header race (the read is request-local per exchange). The
    global `WeightRateLimiter` still gates every call (the weight budget is per-IP). Built once
    and reused; refreshes its exchanges' markets on the same age-gated schedule as the main
    exchange so newly-listed symbols don't fail on stale pooled markets.

    Row ownership stays single-writer: workers return rows, only the main thread appends.
    Invariant: `max_workers == concurrency == len(exchanges)`, so a checkout never blocks and an
    exchange is only ever touched by one thread at a time (`get_nowait` fails fast if broken)."""

    def __init__(self, config: ExchangeConfig, concurrency: int, exchange_factory=create_exchange):
        self._exchanges = []
        try:
            for _ in range(concurrency):
                self._exchanges.append(exchange_factory(config))
        except Exception:
            for ex in self._exchanges:  # partial init → don't leak the sessions already opened
                _close_exchange(ex)
            raise
        self._available = queue.Queue()
        for ex in self._exchanges:
            self._available.put(ex)
        self._pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix=f"{config.name}-dense")

    def _refresh_markets(self, thread_name):
        # Called before any task is submitted, so all exchanges are idle in the queue. Age-gated
        # per exchange; a reload failure logs and leaves that exchange stale (never aborts).
        for ex in self._exchanges:
            try:
                maybe_reload_markets(ex)
            except Exception as exc:
                log.warning("[%s] Dense pool market reload failed (using stale): %s", thread_name, exc)

    def _fetch_one(self, pair, rate_limiter, funding_rates, now_ts, exchange_name):
        ex = self._available.get_nowait()  # fail fast if the K==workers invariant ever breaks
        try:
            # priority=True: dense OI weight lane (T-007.4) — see _binance_dense_fetch.
            oi_data = paced_call(
                rate_limiter, ex, BINANCE_OI_WEIGHT, ex.fetch_open_interest, pair["symbol"],
                priority=True,
            )
            return _dense_row(oi_data, pair, exchange_name, funding_rates, now_ts)
        finally:
            self._available.put(ex)

    def close(self):
        """Shut the pool down and release its exchange sessions. The daemon OI loop keeps the
        pool for the process lifetime; this exists for lifecycle completeness / tests."""
        self._pool.shutdown(wait=False)
        for ex in self._exchanges:
            _close_exchange(ex)

    def fetch(self, dense_pairs, rate_limiter, funding_rates, now_ts, exchange_name, thread_name):
        if not dense_pairs:
            return []
        self._refresh_markets(thread_name)
        rows = []
        futures = {
            self._pool.submit(self._fetch_one, p, rate_limiter, funding_rates, now_ts, exchange_name): p
            for p in dense_pairs
        }
        for fut in as_completed(futures):
            sym = futures[fut]["symbol"]
            try:
                row = fut.result()
            except RETRIABLE_EXCEPTIONS as exc:
                log.warning("[%s] Retriable error on dense %s: %s", thread_name, sym, exc)
                continue
            except Exception as exc:
                log.warning("[%s] Error on dense %s: %s", thread_name, sym, exc)
                continue
            if row is not None:
                rows.append(row)
        return rows


def _binance_baseline_sweep(exchange, exchange_name, rate_limiter, conn, due_pairs,
                            baseline_cursor, baseline_last_fetch, now, thread_name):
    """openInterestHist (separate /futures/data bucket) per due symbol, `since`-based so a
    gap backfills from the last stored baseline point. Self-paced by BINANCE_OI_HIST_SPACING_S
    (weight 0 ⇒ no limiter pacing). Cursor advances to the max ts returned ONLY after a
    successful commit — a fetch/insert failure leaves the cursor unadvanced (no gap)."""
    inserted_total = 0
    for pair in due_pairs:
        sym = pair["symbol"]
        cursor = baseline_cursor.get(sym)
        since_ms = int(cursor * 1000) + 1 if cursor else None
        try:
            hist = paced_call(
                rate_limiter, exchange, BINANCE_OI_HIST_WEIGHT,
                exchange.fetch_open_interest_history, sym, "5m", since_ms, BASELINE_HIST_LIMIT,
            )
        except Exception as exc:
            log.warning("[%s] Baseline fetch failed on %s: %s", thread_name, sym, exc)
            time.sleep(BINANCE_OI_HIST_SPACING_S)
            continue  # cursor unadvanced → retried next sweep, no gap

        rows = []
        max_ts = cursor or 0.0
        for h in hist:
            ts_ms = h.get("timestamp")
            if ts_ms is None:
                continue
            ts_s = ts_ms / 1000
            rows.append({
                "exchange": exchange_name, "asset_id": pair["asset_id"], "symbol": sym,
                "timestamp": ts_s, "open_interest": h.get("openInterestAmount"),
                "oi_value_usd": h.get("openInterestValue"),
            })
            max_ts = max(max_ts, ts_s)

        try:
            inserted_total += _insert_oi(conn, rows)
        except Exception as exc:
            log.warning("[%s] Baseline insert failed on %s: %s", thread_name, sym, exc)
            if not conn.closed:
                safe_rollback(conn)
            time.sleep(BINANCE_OI_HIST_SPACING_S)
            continue  # cursor unadvanced → no gap

        baseline_last_fetch[sym] = now  # advance due-clock only after success
        if rows:
            baseline_cursor[sym] = max_ts  # advance cursor only after commit
        time.sleep(BINANCE_OI_HIST_SPACING_S)
    return inserted_total


def _oi_loop_binance(exchange, config: ExchangeConfig, dsn: str, rate_limiter: RateLimiter):
    """Binance two-source OI: a 5-min baseline via openInterestHist (separate weight
    bucket) + sub-5-min dense openInterest for symbols within ±window of settlement."""
    thread_name = f"{config.name}/oi"

    # Reserve-contention signal (T-007.4): the binding constraint is the dense weight-RATE, not
    # the instantaneous in-flight — a reserve that covers `oi_dense_concurrency × weight` (6) can
    # still be too small if the in-window dense set is large. So instead of a startup in-flight
    # check we watch it at RUNTIME: when a cycle's dense weight (in-window symbols × per-call
    # weight) exceeds the reserve, dense may contend with candles at the cap. Edge-triggered so an
    # 11-min mass settlement logs once, not once per cycle. 0 = lane off (no signal).
    reserve = config.rate_limit_priority_reserve
    dense_over_reserve = False

    conn = connect_db(dsn)
    prev_next_ft: dict = {}      # symbol → last-seen nextFundingTime (ms), for change-detection
    settlement_at: dict = {}     # symbol → wall-clock time.time() of last detected settlement
    baseline_cursor: dict = {}   # symbol → last baseline data ts (seconds)
    baseline_last_fetch: dict = {}  # symbol → `now` of last baseline sweep (due-clock)
    seeded = False
    dense_pool = None  # built lazily below when oi_dense_concurrency > 1; retried on failure

    while True:
        # Lazy-build the isolated-session dense pool; retry each cycle on failure so a transient
        # init miss self-heals instead of disabling parallelism until restart (degraded =
        # sequential dense on the main exchange, which still collects, just slower).
        if dense_pool is None and config.oi_dense_concurrency > 1:
            try:
                dense_pool = _DenseFetchPool(config, config.oi_dense_concurrency)
                log.info("[%s] Dense pool ready: %d worker exchanges", thread_name, config.oi_dense_concurrency)
            except Exception:
                log.exception("[%s] Dense pool init failed; DEGRADED to sequential dense this cycle", thread_name)


        if conn.closed:
            try:
                conn = connect_db(dsn)
            except RuntimeError:
                log.error("[%s] DB reconnect failed, will retry next cycle", thread_name)
                time.sleep(30)
                continue

        try:
            pairs = _get_active_pairs(conn, config.name)
        except Exception:
            log.exception("[%s] Failed to query active pairs", thread_name)
            safe_rollback(conn)
            _sleep_until_next(config.oi_poll_interval_s, OI_POLL_BUFFER_S)
            continue

        if not pairs:
            log.info("[%s] No active pairs, sleeping", thread_name)
            _heartbeat(conn, config, thread_name)
            _sleep_until_next(config.oi_poll_interval_s, OI_POLL_BUFFER_S)
            continue

        if not seeded:
            try:
                baseline_cursor = _seed_baseline_cursor(conn, config.name)
                log.info("[%s] Seeded baseline cursor for %d symbols", thread_name, len(baseline_cursor))
                seeded = True  # only after success — a failed seed retries next cycle so
                # DB-backed restart backfill is not silently downgraded to since=None
            except Exception:
                log.exception("[%s] Baseline cursor seed failed, retrying next cycle", thread_name)
                if not conn.closed:
                    safe_rollback(conn)

        active = {p["symbol"]: p for p in pairs}
        # Prune cross-tick state to currently-active symbols so the maps track the active
        # set, not every symbol ever seen (listing churn would otherwise grow them unbounded).
        _prune_binance_state(active.keys(), prev_next_ft, settlement_at, baseline_cursor, baseline_last_fetch)
        now = time.time()
        cycle_start = time.monotonic()

        try:
            # priority=True: the premiumIndex bulk is the dense-window trigger — it gates the
            # whole dense cycle, so it rides the same weight lane (T-007.4) as the dense fetches
            # below; blocking it at the low cap would strand the dense burst before it starts.
            funding_rates = paced_call(
                rate_limiter, exchange, BINANCE_FUNDING_BULK_WEIGHT, exchange.fetch_funding_rates,
                priority=True,
            )
        except Exception as exc:
            log.warning("[%s] premiumIndex bulk failed, skipping cycle: %s", thread_name, exc)
            if not conn.closed:
                safe_rollback(conn)
            _heartbeat(conn, config, thread_name)
            _sleep_until_next(config.oi_poll_interval_s, OI_POLL_BUFFER_S)
            continue

        dense = _binance_dense_set(
            funding_rates, active, now, config.oi_settlement_window_s, prev_next_ft, settlement_at,
        )
        # Rate-based reserve-contention warning (edge-triggered): the dense weight this cycle vs
        # the reserve band. Logs once on the rising edge of a breach so a mass settlement isn't
        # spammed, and re-arms when the set falls back under the reserve.
        dense_weight = len(dense) * BINANCE_OI_WEIGHT
        if reserve and dense_weight > reserve:
            if not dense_over_reserve:
                log.warning(
                    "[%s] dense weight=%d exceeds rate_limit_priority_reserve=%d — dense may "
                    "contend with candles at the weight cap; raise rate_limit_priority_reserve",
                    thread_name, dense_weight, reserve,
                )
            dense_over_reserve = True
        else:
            dense_over_reserve = False
        if dense_pool is not None:
            dense_rows = dense_pool.fetch(dense, rate_limiter, funding_rates, now, config.name, thread_name)
        else:
            dense_rows = _binance_dense_fetch(
                exchange, config.name, rate_limiter, dense, funding_rates, now, thread_name,
            )
        try:
            dense_inserted = _insert_oi(conn, dense_rows)
        except Exception as exc:
            log.warning("[%s] Dense insert failed: %s", thread_name, exc)
            if not conn.closed:
                safe_rollback(conn)
            dense_inserted = 0

        # Baseline runs ONLY once the cursor is seeded — otherwise a since=None sweep would
        # advance the cursor from a "latest" fetch and skip the restart gap (claim 4). Dense
        # above is cursor-independent, so it still runs while a failed seed retries.
        base_inserted, due = 0, []
        if seeded:
            batch = _baseline_batch_size(len(pairs), config.oi_poll_interval_s, config.oi_baseline_interval_s)
            due = [
                p for p in pairs
                if now - baseline_last_fetch.get(p["symbol"], 0) >= config.oi_baseline_interval_s
            ][:batch]
            base_inserted = _binance_baseline_sweep(
                exchange, config.name, rate_limiter, conn, due,
                baseline_cursor, baseline_last_fetch, now, thread_name,
            )

        cycle_s = time.monotonic() - cycle_start
        # weight = (server rolling-minute used, reserved in-flight) — direct evidence the dense
        # burst stays under the priority cap (T-007.4). None when the limiter tracks no weight
        # (interval limiter, or the None limiter in unit tests).
        weight = rate_limiter.used_snapshot() if rate_limiter is not None else None
        log.info(
            "[%s] Poll: dense=%d (set=%d) baseline=%d (due=%d) cycle_duration_s=%.1f weight=%s",
            thread_name, dense_inserted, len(dense), base_inserted, len(due), cycle_s, weight,
        )
        _heartbeat(conn, config, thread_name)
        _sleep_until_next(config.oi_poll_interval_s, OI_POLL_BUFFER_S)


# ---------------------------------------------------------------------------
# Entry point — dispatches to the right strategy
# ---------------------------------------------------------------------------


def oi_loop(exchange, config: ExchangeConfig, dsn: str, rate_limiter: RateLimiter):
    """OI collection loop. Dispatches by exchange: HL native bulk, Bybit/OKX bulk,
    Binance two-source; every `has_oi` exchange maps to exactly one strategy."""
    if config.ccxt_class == "hyperliquid":
        _oi_loop_hyperliquid(config, dsn)
    elif config.name in ("bybit", "okx"):
        _oi_loop_bulk(exchange, config, dsn, rate_limiter)
    elif config.name == "binance":
        _oi_loop_binance(exchange, config, dsn, rate_limiter)
    else:  # fail fast — a new has_oi venue must declare its strategy, not inherit Binance's
        raise ValueError(f"No OI strategy for exchange '{config.name}' (has_oi with no loop)")
