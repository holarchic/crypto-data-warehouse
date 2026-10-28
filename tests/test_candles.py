"""Tests for collector/candles.py, the REST candle feed: the poll_limit weight diet and its wiring
into _collect_pair, the per-table INSERTs, the bar-and-cursor transaction, and candle_loop's pacing.
`candles` is a real module under conftest (only `db`, `main`, `oi` and `backoff` are stubbed), so the
tests patch its own bindings.
"""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import candles as candles_mod
import pytest
import status
from candles import (
    CANDLE_LIMIT_MARGIN,
    CANDLE_LIMIT_MIN,
    CANDLE_SEED_LIMIT,
    poll_limit,
)
from rate_limiter import binance_kline_weight

from tests.fakes import RecordingConn

# --- poll_limit pure helper ---------------------------------------------------


def test_poll_limit_steady_state_is_weight_1():
    # Caught up (one interval since last candle) → tiny limit → Binance kline weight 1 (was 5).
    interval_s = 300
    now_ms = 1_700_000_000_000
    since_ms = now_ms - interval_s * 1000  # exactly one interval back
    limit = poll_limit(since_ms, now_ms, interval_s)
    assert limit <= CANDLE_LIMIT_MARGIN + 1
    assert binance_kline_weight(limit) == 1


def test_poll_limit_sizes_to_gap_plus_margin():
    # A pair down K cycles fetches ~K + MARGIN candles (exact gap recovery, not a flat 1000).
    interval_s = 300
    k = 40
    now_ms = 1_700_000_000_000
    since_ms = now_ms - k * interval_s * 1000
    assert poll_limit(since_ms, now_ms, interval_s) == k + CANDLE_LIMIT_MARGIN


def test_poll_limit_caps_at_seed_limit():
    # A huge gap is capped at the per-call ceiling (recovery preserved exactly as flat-1000 gave).
    interval_s = 300
    now_ms = 1_700_000_000_000
    since_ms = now_ms - 10_000 * interval_s * 1000
    assert poll_limit(since_ms, now_ms, interval_s) == CANDLE_SEED_LIMIT


def test_poll_limit_floors_on_clock_skew():
    # now < since (clock skew) → floor, never 0/negative.
    interval_s = 300
    now_ms = 1_700_000_000_000
    since_ms = now_ms + 5 * interval_s * 1000
    assert poll_limit(since_ms, now_ms, interval_s) == CANDLE_LIMIT_MIN


# --- _collect_pair wiring ----------------------------------------------------


def _run_collect_pair(candles_mod, monkeypatch, last_timestamp, weight_budget, poll_max_bars=None, results=None,
                      scan_from=None):
    """Drive the real _collect_pair with fetch_candles patched to capture its `limit` (and every
    call in `captured["calls"]`). `results` lists what successive fetches return (default: [])."""
    captured = {"calls": []}
    pending = list(results or [])

    def _fake_fetch(exchange, symbol, market_info, exchange_name, timeframe, interval_s,
                    rate_limiter, since_ms=None, limit=1000, until_ms=None):
        captured["limit"] = limit
        captured["since_ms"] = since_ms
        captured["calls"].append({"since_ms": since_ms, "until_ms": until_ms})
        return pending.pop(0) if pending else []  # empty → insert_candles/status paths are no-ops

    monkeypatch.setattr(candles_mod, "fetch_candles", _fake_fetch)
    monkeypatch.setattr(candles_mod, "insert_candles", lambda conn, candles, table: len(candles))
    monkeypatch.setattr(candles_mod, "upsert_status_success", lambda *a, **k: None)
    # Freeze wall clock so the poll_limit gap is deterministic.
    fixed_now = 1_700_000_000.0
    monkeypatch.setattr(candles_mod.time, "time", lambda: fixed_now)

    config = SimpleNamespace(name="binance", market_type="perp", rate_limit_weight_budget=weight_budget,
                             poll_max_bars=poll_max_bars)
    tf = SimpleNamespace(name="5m", interval_s=300, table="candles")
    pair = {"asset_id": "BTC", "symbol": "BTC/USDT:USDT", "native_symbol": "BTCUSDT",
            "market_type": "perp", "last_timestamp": last_timestamp}
    candles_mod._collect_pair(object(), pair, config, tf, None, MagicMock(), "binance/5m", {} if scan_from is None else scan_from)
    return captured, fixed_now


def test_collect_pair_seed_uses_full_limit(monkeypatch):
    # Seed (no stored candle) fetches max history regardless of venue.
    captured, _ = _run_collect_pair(candles_mod, monkeypatch, last_timestamp=None, weight_budget=2400)
    assert captured["since_ms"] is None
    assert captured["limit"] == CANDLE_SEED_LIMIT


def test_collect_pair_poll_weight_bound_uses_diet(monkeypatch):
    # Poll on a weight-bound venue (Binance) sizes the limit to the gap — small, not 1000.
    interval_s = 300
    stored = datetime.fromtimestamp(1_700_000_000.0 - 3 * interval_s, tz=timezone.utc)
    captured, now = _run_collect_pair(candles_mod, monkeypatch, last_timestamp=stored, weight_budget=2400)
    expected = poll_limit(captured["since_ms"], int(now * 1000), interval_s)
    assert captured["limit"] == expected
    assert captured["limit"] < CANDLE_SEED_LIMIT  # diet applied


def test_collect_pair_poll_non_weight_bound_keeps_full_limit(monkeypatch):
    # Poll on a non-weight-bound venue keeps the flat 1000 — diet is Binance-only.
    interval_s = 300
    stored = datetime.fromtimestamp(1_700_000_000.0 - 3 * interval_s, tz=timezone.utc)
    captured, _ = _run_collect_pair(candles_mod, monkeypatch, last_timestamp=stored, weight_budget=None)
    assert captured["limit"] == CANDLE_SEED_LIMIT


def _window_end(since_ms, interval_s, bars):
    """Last ms of the `bars`-th candle bucket counted from the one containing `since_ms`."""
    bar_ms = interval_s * 1000
    return (since_ms // bar_ms + bars) * bar_ms - 1


def _hl_buckets(start_ms, end_ms, interval_s):
    """What HL's candleSnapshot answers: every candle whose bucket contains a time in
    [start, end], both ends included (the SDK's recorded response pads both ends)."""
    bar_ms = interval_s * 1000
    return [{"t": t, "T": t + bar_ms - 1, "s": "BTC", "i": "5m", "o": "1", "c": "1", "h": "1", "l": "1",
             "v": "1", "n": 1}
            for t in range(start_ms // bar_ms * bar_ms, end_ms // bar_ms * bar_ms + 1, bar_ms)]


def test_collect_pair_poll_caps_the_window_at_poll_max_bars(monkeypatch):
    # HL charges by bars returned: a poll 3 h behind asks for 60 bars, not since→now.
    stored = datetime.fromtimestamp(1_700_000_000.0 - 36 * 300, tz=timezone.utc)
    bar = {"timestamp": 1_699_990_000.0}
    captured, _ = _run_collect_pair(candles_mod, monkeypatch, last_timestamp=stored, weight_budget=None,
                                    poll_max_bars=60, results=[[bar]])
    since = captured["calls"][0]["since_ms"]
    assert captured["calls"] == [{"since_ms": since, "until_ms": _window_end(since, 300, 60)}]


def test_collect_pair_capped_seed_walks_the_seed_depth_forward(monkeypatch):
    # A cursorless pair on a capped venue starts CANDLE_SEED_LIMIT bars back, one 60-bar window a
    # poll, not with a 1000-bar tail request.
    captured, now = _run_collect_pair(candles_mod, monkeypatch, last_timestamp=None, weight_budget=None,
                                      poll_max_bars=60)
    since = int(now * 1000) - CANDLE_SEED_LIMIT * 300 * 1000
    assert captured["calls"] == [{"since_ms": since, "until_ms": _window_end(since, 300, 60)}]


def test_collect_pair_uncapped_seed_is_a_tail_request(monkeypatch):
    captured, _ = _run_collect_pair(candles_mod, monkeypatch, last_timestamp=None, weight_budget=None)
    assert captured["calls"] == [{"since_ms": None, "until_ms": None}]


def test_collect_pair_empty_capped_window_moves_the_scan_past_it(monkeypatch):
    # A cursor 10 days behind (older than the venue keeps, say): the first 60-bar window is empty.
    # The next poll asks for the following 60 bars, never for since→now in one heavy request.
    stored = datetime.fromtimestamp(1_700_000_000.0 - 10 * 86400, tz=timezone.utc)
    scan_from = {}
    captured, _ = _run_collect_pair(candles_mod, monkeypatch, last_timestamp=stored, weight_budget=None,
                                    poll_max_bars=60, scan_from=scan_from)
    since = captured["calls"][0]["since_ms"]
    first_end = _window_end(since, 300, 60)
    assert captured["calls"] == [{"since_ms": since, "until_ms": first_end}]
    assert scan_from == {"BTC/USDT:USDT": first_end + 1}
    bar = {"timestamp": (first_end + 600_000) / 1000}
    captured, _ = _run_collect_pair(candles_mod, monkeypatch, last_timestamp=stored, weight_budget=None,
                                    poll_max_bars=60, scan_from=scan_from, results=[[bar]])
    assert captured["calls"] == [{"since_ms": first_end + 1, "until_ms": _window_end(first_end + 1, 300, 60)}]
    assert scan_from == {}  # bars found: the stored cursor leads again


def test_collect_pair_caught_up_empty_window_does_not_move_the_scan(monkeypatch):
    # Caught up (the 60-bar window reaches past now): an empty answer is just no new bar yet.
    stored = datetime.fromtimestamp(1_700_000_000.0 - 300, tz=timezone.utc)
    scan_from = {}
    captured, _ = _run_collect_pair(candles_mod, monkeypatch, last_timestamp=stored, weight_budget=None,
                                    poll_max_bars=60, scan_from=scan_from)
    assert len(captured["calls"]) == 1
    assert scan_from == {}


def test_capped_hl_requests_return_at_most_poll_max_bars_raw_candles(monkeypatch):
    # Through real ccxt and an HL that pads both ends: a catch-up poll and a seed each get at most
    # poll_max_bars raw candles back, so each weighs 20 + ceil(60 / 60) = 21.
    import ccxt

    exchange = ccxt.hyperliquid({"options": {"fetchMarkets": {"types": ["swap"]}}})
    raw_counts = []

    def post_info(request, *a, **k):
        if request["type"] == "candleSnapshot":
            bars = _hl_buckets(request["req"]["startTime"], request["req"]["endTime"], 300)
            raw_counts.append(len(bars))
            return bars
        return {
            "metaAndAssetCtxs": [{"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 40}]},
                                 [{"markPx": "100", "dayNtlVlm": "1", "funding": "0", "openInterest": "1",
                                   "oraclePx": "100", "premium": "0", "prevDayPx": "100", "midPx": "100",
                                   "impactPxs": ["99", "101"], "dayBaseVlm": "1"}]],
            "spotMeta": {"tokens": [], "universe": []},
        }[request["type"]]

    exchange.publicPostInfo = post_info
    exchange.load_markets()
    monkeypatch.setattr(candles_mod, "insert_candles", lambda conn, candles, table: len(candles))
    monkeypatch.setattr(candles_mod, "upsert_status_success", lambda *a, **k: None)
    config = SimpleNamespace(name="hyperliquid", market_type="perp", rate_limit_weight_budget=None, poll_max_bars=60)
    tf = SimpleNamespace(name="5m", interval_s=300, table="candles")
    behind = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() - 86400, tz=timezone.utc)  # catch-up
    for last in (behind, None):  # then a seed
        pair = {"asset_id": "BTC", "symbol": "BTC/USDC:USDC", "native_symbol": "", "market_type": "perp",
                "last_timestamp": last}
        candles_mod._collect_pair(exchange, pair, config, tf, candles_mod.RateLimiter(0), MagicMock(), "hyperliquid/5m", {})
    assert raw_counts == [60, 60]


# --- candle-table routing (T-015) ---------------------------------------------


def test_insert_sql_is_keyed_by_table():
    # One INSERT per candle table, targeting the right table (built from CANDLE_TABLES at import).
    assert set(candles_mod.INSERT_SQL) == {"candles", "candles_1m"}
    assert "INTO candles (" in candles_mod.INSERT_SQL["candles"]
    assert "INTO candles_1m (" in candles_mod.INSERT_SQL["candles_1m"]


def test_insert_candles_selects_table(monkeypatch):
    # insert_candles(table=...) executes the SQL for that table; unknown table fails fast.
    executed = {}

    class _Cur:
        rowcount = 1
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, row): executed["sql"] = sql

    class _Conn:
        def cursor(self): return _Cur()
        def commit(self): pass

    row = {"exchange": "binance", "market_type": "perp", "asset_id": "BTC", "symbol": "B",
           "native_symbol": "B", "timeframe": "1m", "timestamp": 1.0, "open": 1, "high": 1,
           "low": 1, "close": 1, "volume": 1, "quote_volume": None, "trades": None}
    candles_mod.insert_candles(_Conn(), [row], "candles_1m")
    assert "INTO candles_1m (" in executed["sql"]
    import pytest
    with pytest.raises(KeyError):
        candles_mod.insert_candles(_Conn(), [row], "no_such_table")


def test_collect_pair_threads_tf_table(monkeypatch):
    # _collect_pair must route the insert to tf.table (so 1m lands in candles_1m).
    captured = {}
    monkeypatch.setattr(candles_mod, "fetch_candles", lambda *a, **k: [{"timestamp": 1.0}])
    monkeypatch.setattr(candles_mod, "insert_candles",
                        lambda conn, candles, table="candles": captured.setdefault("table", table) or 1)
    monkeypatch.setattr(candles_mod, "upsert_status_success", lambda *a, **k: None)
    monkeypatch.setattr(candles_mod.time, "time", lambda: 1_700_000_000.0)
    config = SimpleNamespace(name="binance", market_type="perp", rate_limit_weight_budget=None, poll_max_bars=None)
    tf = SimpleNamespace(name="1m", interval_s=60, table="candles_1m")
    pair = {"asset_id": "BTC", "symbol": "B", "native_symbol": "B",
            "market_type": "perp", "last_timestamp": None}
    candles_mod._collect_pair(object(), pair, config, tf, None, MagicMock(), "binance/1m", {})
    assert captured["table"] == "candles_1m"


# --- one transaction per pair: bars and cursor commit together -------------------------


def _bars(n):
    return [{"exchange": "binance", "market_type": "perp", "asset_id": "BTC", "symbol": "B", "native_symbol": "B",
             "timeframe": "5m", "timestamp": 1_699_999_000.0 + 300 * i, "open": 1, "high": 1, "low": 1, "close": 1,
             "volume": 1, "quote_volume": None, "trades": None} for i in range(n)]


def _collect_into(candles_mod, monkeypatch, conn):
    monkeypatch.setattr(candles_mod, "fetch_candles", lambda *a, **k: _bars(2))
    config = SimpleNamespace(name="binance", market_type="perp", rate_limit_weight_budget=None, poll_max_bars=None)
    tf = SimpleNamespace(name="5m", interval_s=300, table="candles")
    pair = {"asset_id": "BTC", "symbol": "B", "native_symbol": "B", "market_type": "perp", "last_timestamp": None}
    candles_mod._collect_pair(object(), pair, config, tf, None, conn, "binance/5m", {})
    return config, tf, pair


def test_collect_pair_commits_bars_and_cursor_once(monkeypatch):
    conn = RecordingConn()
    _collect_into(candles_mod, monkeypatch, conn)
    assert conn.kinds() == ["execute", "execute", "execute", "commit"]
    assert all("INSERT INTO candles (" in e.sql for e in conn.events[:2])
    assert conn.events[2].sql == status.UPSERT_SUCCESS_SQL


class _CursorWriteFails(RecordingConn):
    def execute(self, sql, params=None):
        super().execute(sql, params)
        if sql == status.UPSERT_SUCCESS_SQL:
            raise RuntimeError("cursor write failed")


def test_a_failed_cursor_write_rolls_the_bars_back_then_commits_the_error(monkeypatch):
    monkeypatch.setattr(candles_mod, "safe_rollback", lambda conn: conn.rollback())  # db.safe_rollback's effect
    conn = _CursorWriteFails()
    with pytest.raises(RuntimeError) as failure:
        _collect_into(candles_mod, monkeypatch, conn)
    assert "commit" not in conn.kinds()
    config, tf, pair = SimpleNamespace(name="binance", market_type="perp"), SimpleNamespace(name="5m"), {"symbol": "B"}
    candles_mod._record_pair_error(conn, config, pair, tf, failure.value)
    tail = conn.events[3:]
    assert [e.kind for e in tail] == ["rollback", "execute", "commit"]
    assert tail[1].sql == status.UPSERT_ERROR_SQL


# --- candle_loop pacing: each cycle paces from its own start ---------------------


class _StopLoop(Exception):
    pass


def _drive_candle_loop(candles_mod, monkeypatch, tf, cycles, connect=None):
    """Run candle_loop for `cycles` pacing calls against fakes and a ticking clock (1000, 1001, …);
    return the recorded `sleep_until_next_slot` arguments."""
    clock = {"t": 999.0}

    def now():
        clock["t"] += 1
        return clock["t"]

    calls = []

    def pace(*a):
        calls.append(a)
        if len(calls) >= cycles:
            raise _StopLoop

    conn = SimpleNamespace(closed=False)
    monkeypatch.setattr(candles_mod.time, "time", now)
    monkeypatch.setattr(candles_mod.time, "sleep", lambda s: None)
    monkeypatch.setattr(candles_mod, "connect_db", connect or (lambda dsn: conn))
    monkeypatch.setattr(candles_mod, "maybe_reload_markets", lambda ex: None)
    monkeypatch.setattr(candles_mod, "query_active_pairs", lambda c, n, t: [{"symbol": "BTC/USDT:USDT"}])
    # The fake poll reads the clock, as real polls do, so a start taken at cycle end reads later.
    monkeypatch.setattr(candles_mod, "_collect_pair", lambda *a: candles_mod.time.time())
    monkeypatch.setattr(candles_mod, "sleep_until_next_slot", pace)
    cfg = SimpleNamespace(name="binance", market_type="perp")
    try:
        candles_mod.candle_loop(object(), cfg, tf, "dsn", None)
    except _StopLoop:
        pass
    return calls


def test_candle_loop_paces_each_cycle_from_its_start(monkeypatch):
    tf = SimpleNamespace(name="5m", interval_s=300, buffer_s=30, table="candles")
    calls = _drive_candle_loop(candles_mod, monkeypatch, tf, cycles=2)
    # 5m runs its first cycle at once (no wait); each wait then targets the slot after the start
    # of the cycle just ended: 1000, then 1002 (the poll in between reads 1001).
    assert calls == [(300, 30, 1000.0, "binance/5m"), (300, 30, 1002.0, "binance/5m")]


def test_candle_loop_deferred_first_cycle_waits_for_the_slot_after_now(monkeypatch):
    tf = SimpleNamespace(name="1h", interval_s=3600, buffer_s=60, table="candles")
    calls = _drive_candle_loop(candles_mod, monkeypatch, tf, cycles=2)
    assert calls == [(3600, 60, None, "binance/1h"), (3600, 60, 1000.0, "binance/1h")]


def test_candle_loop_drops_scan_state_for_deactivated_pairs(monkeypatch):
    cycles = iter([["A", "B"], ["A"], ["A"]])
    seen = []

    def collect(exchange, pair, config, tf, rate_limiter, conn, thread_name, scan_from):
        seen.append(sorted(scan_from))
        scan_from[pair["symbol"]] = 1  # as an empty capped window would

    tf = SimpleNamespace(name="5m", interval_s=300, buffer_s=30, table="candles")
    _drive_candle_loop(candles_mod, monkeypatch, tf, cycles=2)  # installs the fakes
    monkeypatch.setattr(candles_mod, "query_active_pairs", lambda c, n, t: [{"symbol": s} for s in next(cycles)])
    monkeypatch.setattr(candles_mod, "_collect_pair", collect)
    calls = []

    def pace(*a):
        calls.append(a)
        if len(calls) >= 2:
            raise _StopLoop

    monkeypatch.setattr(candles_mod, "sleep_until_next_slot", pace)
    try:
        candles_mod.candle_loop(object(), SimpleNamespace(name="binance", market_type="perp"), tf, "dsn", None)
    except _StopLoop:
        pass
    # Cycle 1 sees A then B; cycle 2 (only A active) has dropped B's entry before polling A.
    assert seen == [[], ["A"], ["A"]]


def test_candle_loop_reconnect_failure_retries_from_the_failed_cycles_start(monkeypatch):
    # A failed reconnect sleeps 30 s and loops: the next wait targets the slot after the failed
    # iteration's start, so retries keep to the slot grid instead of hammering a down DB.
    conn = SimpleNamespace(closed=True)
    attempts = {"n": 0}

    def connect(dsn):
        attempts["n"] += 1
        if attempts["n"] == 2:  # the first call is the loop's initial connect
            raise RuntimeError("db down")
        return conn

    tf = SimpleNamespace(name="5m", interval_s=300, buffer_s=30, table="candles")
    calls = _drive_candle_loop(candles_mod, monkeypatch, tf, cycles=1, connect=connect)
    assert attempts["n"] == 2
    assert calls == [(300, 30, 1000.0, "binance/5m")]
