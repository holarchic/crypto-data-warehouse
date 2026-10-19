"""Tests for collector/candles.py (poll_limit weight diet) and its wiring into
_collect_pair. The pure helper is tested directly; the wiring is tested against the
REAL main module (conftest stubs `main` as a MagicMock, so we load main.py in
isolation via importlib to exercise the actual limit selection, not a mock).
"""

import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import config as _config_mod  # real collector module — gives us the collector dir
from candles import (
    CANDLE_LIMIT_MARGIN,
    CANDLE_LIMIT_MIN,
    CANDLE_SEED_LIMIT,
    poll_limit,
)
from rate_limiter import binance_kline_weight

_MAIN_PATH = Path(_config_mod.__file__).parent / "main.py"


def _load_real_main():
    """Load collector/main.py under a private name so we run the real _collect_pair /
    fetch_candles without disturbing the shared `main` stub other tests rely on."""
    spec = importlib.util.spec_from_file_location("main_real", _MAIN_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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


# --- _collect_pair wiring (real main) -----------------------------------------


def _run_collect_pair(main_mod, monkeypatch, last_timestamp, weight_budget, poll_max_bars=None, results=None):
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

    monkeypatch.setattr(main_mod, "fetch_candles", _fake_fetch)
    monkeypatch.setattr(main_mod, "insert_candles", lambda conn, candles, table: len(candles))
    monkeypatch.setattr(main_mod, "upsert_status_success", lambda *a, **k: None)
    # Freeze wall clock so the poll_limit gap is deterministic.
    fixed_now = 1_700_000_000.0
    monkeypatch.setattr(main_mod.time, "time", lambda: fixed_now)

    config = SimpleNamespace(name="binance", market_type="perp", rate_limit_weight_budget=weight_budget,
                             poll_max_bars=poll_max_bars)
    tf = SimpleNamespace(name="5m", interval_s=300, table="candles")
    pair = {"asset_id": "BTC", "symbol": "BTC/USDT:USDT", "native_symbol": "BTCUSDT",
            "market_type": "perp", "last_timestamp": last_timestamp}
    main_mod._collect_pair(object(), pair, config, tf, None, None, "binance/5m")
    return captured, fixed_now


def test_collect_pair_seed_uses_full_limit(monkeypatch):
    # Seed (no stored candle) fetches max history regardless of venue.
    main_mod = _load_real_main()
    captured, _ = _run_collect_pair(main_mod, monkeypatch, last_timestamp=None, weight_budget=2400)
    assert captured["since_ms"] is None
    assert captured["limit"] == CANDLE_SEED_LIMIT


def test_collect_pair_poll_weight_bound_uses_diet(monkeypatch):
    # Poll on a weight-bound venue (Binance) sizes the limit to the gap — small, not 1000.
    main_mod = _load_real_main()
    interval_s = 300
    stored = datetime.fromtimestamp(1_700_000_000.0 - 3 * interval_s, tz=timezone.utc)
    captured, now = _run_collect_pair(main_mod, monkeypatch, last_timestamp=stored, weight_budget=2400)
    expected = poll_limit(captured["since_ms"], int(now * 1000), interval_s)
    assert captured["limit"] == expected
    assert captured["limit"] < CANDLE_SEED_LIMIT  # diet applied


def test_collect_pair_poll_non_weight_bound_keeps_full_limit(monkeypatch):
    # Poll on a non-weight-bound venue keeps the flat 1000 — diet is Binance-only.
    main_mod = _load_real_main()
    interval_s = 300
    stored = datetime.fromtimestamp(1_700_000_000.0 - 3 * interval_s, tz=timezone.utc)
    captured, _ = _run_collect_pair(main_mod, monkeypatch, last_timestamp=stored, weight_budget=None)
    assert captured["limit"] == CANDLE_SEED_LIMIT


def test_collect_pair_poll_caps_the_window_at_poll_max_bars(monkeypatch):
    # HL charges by bars returned: a poll 3 h behind asks for 60 bars, not since→now.
    main_mod = _load_real_main()
    stored = datetime.fromtimestamp(1_700_000_000.0 - 36 * 300, tz=timezone.utc)
    bar = {"timestamp": 1_699_990_000.0}
    captured, _ = _run_collect_pair(main_mod, monkeypatch, last_timestamp=stored, weight_budget=None,
                                    poll_max_bars=60, results=[[bar]])
    since = captured["calls"][0]["since_ms"]
    assert captured["calls"] == [{"since_ms": since, "until_ms": since + 60 * 300 * 1000}]


def test_collect_pair_seed_is_not_window_capped(monkeypatch):
    main_mod = _load_real_main()
    captured, _ = _run_collect_pair(main_mod, monkeypatch, last_timestamp=None, weight_budget=None,
                                    poll_max_bars=60)
    assert captured["calls"] == [{"since_ms": None, "until_ms": None}]


def test_collect_pair_capped_empty_window_steps_over_with_one_uncapped_poll(monkeypatch):
    # A pair 10 days behind whose next 60 bars are empty would otherwise never advance its cursor.
    main_mod = _load_real_main()
    stored = datetime.fromtimestamp(1_700_000_000.0 - 10 * 86400, tz=timezone.utc)
    captured, _ = _run_collect_pair(main_mod, monkeypatch, last_timestamp=stored, weight_budget=None,
                                    poll_max_bars=60)
    since = captured["calls"][0]["since_ms"]
    assert captured["calls"] == [{"since_ms": since, "until_ms": since + 60 * 300 * 1000},
                                 {"since_ms": since, "until_ms": None}]


def test_collect_pair_caught_up_empty_window_does_not_refetch(monkeypatch):
    # Caught up (the 60-bar window reaches past now): an empty answer is just no new bar yet.
    main_mod = _load_real_main()
    stored = datetime.fromtimestamp(1_700_000_000.0 - 300, tz=timezone.utc)
    captured, _ = _run_collect_pair(main_mod, monkeypatch, last_timestamp=stored, weight_budget=None,
                                    poll_max_bars=60)
    assert len(captured["calls"]) == 1


def test_fetch_candles_until_bounds_the_real_hl_request():
    # Through real ccxt: `until_ms` becomes candleSnapshot's endTime; without it ccxt asks since→now.
    import ccxt

    main_mod = _load_real_main()
    exchange = ccxt.hyperliquid({"options": {"fetchMarkets": {"types": ["swap"]}}})
    requests = []

    def post_info(request, *a, **k):
        requests.append(request)
        return {
            "metaAndAssetCtxs": [{"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 40}]},
                                 [{"markPx": "100", "dayNtlVlm": "1", "funding": "0", "openInterest": "1",
                                   "oraclePx": "100", "premium": "0", "prevDayPx": "100", "midPx": "100",
                                   "impactPxs": ["99", "101"], "dayBaseVlm": "1"}]],
            "spotMeta": {"tokens": [], "universe": []},
            "candleSnapshot": [],
        }[request["type"]]

    exchange.publicPostInfo = post_info
    exchange.load_markets()
    market_info = {"asset_id": "BTC", "native_symbol": "", "market_type": "perp"}
    since = 1_000_000
    main_mod.fetch_candles(exchange, "BTC/USDC:USDC", market_info, "hyperliquid", "5m", 300,
                           main_mod.RateLimiter(0), since_ms=since, until_ms=since + 60 * 300 * 1000)
    assert requests[-1]["req"]["startTime"] == since
    assert requests[-1]["req"]["endTime"] == since + 60 * 300 * 1000


# --- candle-table routing (T-015) ---------------------------------------------


def test_insert_sql_is_keyed_by_table():
    # One INSERT per candle table, targeting the right table (built from CANDLE_TABLES at import).
    main_mod = _load_real_main()
    assert set(main_mod.INSERT_SQL) == {"candles", "candles_1m"}
    assert "INTO candles (" in main_mod.INSERT_SQL["candles"]
    assert "INTO candles_1m (" in main_mod.INSERT_SQL["candles_1m"]


def test_insert_candles_selects_table(monkeypatch):
    # insert_candles(table=...) executes the SQL for that table; unknown table fails fast.
    main_mod = _load_real_main()
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
    main_mod.insert_candles(_Conn(), [row], "candles_1m")
    assert "INTO candles_1m (" in executed["sql"]
    import pytest
    with pytest.raises(KeyError):
        main_mod.insert_candles(_Conn(), [row], "no_such_table")


def test_collect_pair_threads_tf_table(monkeypatch):
    # _collect_pair must route the insert to tf.table (so 1m lands in candles_1m).
    main_mod = _load_real_main()
    captured = {}
    monkeypatch.setattr(main_mod, "fetch_candles", lambda *a, **k: [{"timestamp": 1.0}])
    monkeypatch.setattr(main_mod, "insert_candles",
                        lambda conn, candles, table="candles": captured.setdefault("table", table) or 1)
    monkeypatch.setattr(main_mod, "upsert_status_success", lambda *a, **k: None)
    monkeypatch.setattr(main_mod.time, "time", lambda: 1_700_000_000.0)
    config = SimpleNamespace(name="binance", market_type="perp", rate_limit_weight_budget=None)
    tf = SimpleNamespace(name="1m", interval_s=60, table="candles_1m")
    pair = {"asset_id": "BTC", "symbol": "B", "native_symbol": "B",
            "market_type": "perp", "last_timestamp": None}
    main_mod._collect_pair(object(), pair, config, tf, None, object(), "binance/1m")
    assert captured["table"] == "candles_1m"


# --- collect_loop pacing: each cycle paces from its own start ---------------------


class _StopLoop(Exception):
    pass


def _drive_collect_loop(main_mod, monkeypatch, tf, cycles, connect=None):
    """Run collect_loop for `cycles` pacing calls against fakes and a ticking clock (1000, 1001, …);
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
    monkeypatch.setattr(main_mod.time, "time", now)
    monkeypatch.setattr(main_mod.time, "sleep", lambda s: None)
    monkeypatch.setattr(main_mod, "connect_db", connect or (lambda dsn: conn))
    monkeypatch.setattr(main_mod, "maybe_reload_markets", lambda ex: None)
    monkeypatch.setattr(main_mod, "query_active_pairs", lambda c, n, t: [{"symbol": "BTC/USDT:USDT"}])
    # The fake poll reads the clock, as real polls do, so a start taken at cycle end reads later.
    monkeypatch.setattr(main_mod, "_collect_pair", lambda *a: main_mod.time.time())
    monkeypatch.setattr(main_mod, "sleep_until_next_slot", pace)
    cfg = SimpleNamespace(name="binance", market_type="perp")
    try:
        main_mod.collect_loop(object(), cfg, tf, "dsn", None)
    except _StopLoop:
        pass
    return calls


def test_collect_loop_paces_each_cycle_from_its_start(monkeypatch):
    main_mod = _load_real_main()
    tf = SimpleNamespace(name="5m", interval_s=300, buffer_s=30, table="candles")
    calls = _drive_collect_loop(main_mod, monkeypatch, tf, cycles=2)
    # 5m runs its first cycle at once (no wait); each wait then targets the slot after the start
    # of the cycle just ended: 1000, then 1002 (the poll in between reads 1001).
    assert calls == [(300, 30, 1000.0, "binance/5m"), (300, 30, 1002.0, "binance/5m")]


def test_collect_loop_deferred_first_cycle_waits_for_the_slot_after_now(monkeypatch):
    main_mod = _load_real_main()
    tf = SimpleNamespace(name="1h", interval_s=3600, buffer_s=60, table="candles")
    calls = _drive_collect_loop(main_mod, monkeypatch, tf, cycles=2)
    assert calls == [(3600, 60, None, "binance/1h"), (3600, 60, 1000.0, "binance/1h")]


def test_collect_loop_reconnect_failure_retries_from_the_failed_cycles_start(monkeypatch):
    # A failed reconnect sleeps 30 s and loops: the next wait targets the slot after the failed
    # iteration's start, so retries keep to the slot grid instead of hammering a down DB.
    main_mod = _load_real_main()
    conn = SimpleNamespace(closed=True)
    attempts = {"n": 0}

    def connect(dsn):
        attempts["n"] += 1
        if attempts["n"] == 2:  # the first call is the loop's initial connect
            raise RuntimeError("db down")
        return conn

    tf = SimpleNamespace(name="5m", interval_s=300, buffer_s=30, table="candles")
    calls = _drive_collect_loop(main_mod, monkeypatch, tf, cycles=1, connect=connect)
    assert attempts["n"] == 2
    assert calls == [(300, 30, 1000.0, "binance/5m")]
