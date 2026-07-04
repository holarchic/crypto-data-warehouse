"""Tests for collector/premium_index.py — the Binance premium-index poll (T-010).

Pure helpers (row build, insert, cursor seed) run against a fake conn; `_collect_premium_pair`
runs against a fake exchange + real RateLimiter(0) so the forward-only-vs-diet `since`/`limit`
selection and cursor advance are exercised without a live wire. Loop wiring (has_premium_index
launches the thread) is covered in test_candles-style real-main loading (see test at bottom).
"""

from pathlib import Path

import premium_index as pi
import pytest
from candles import poll_limit
from rate_limiter import RateLimiter, binance_kline_weight

PAIR = {"asset_id": "BTC", "symbol": "BTC/USDT:USDT", "native_symbol": "BTCUSDT"}
INTERVAL = pi.PREMIUM_INTERVAL_S


# --- fakes -------------------------------------------------------------------


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))
        if self._conn.raise_on and self._conn.raise_on in sql:
            raise RuntimeError("injected insert failure")

    def fetchall(self):
        return self._conn.fetch_rows

    @property
    def rowcount(self):
        return 1


class FakeConn:
    def __init__(self, raise_on=None, fetch_rows=None):
        self.closed = False
        self.executed = []
        self.commits = 0
        self.raise_on = raise_on
        self.fetch_rows = fetch_rows or []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1


class FakeExchange:
    def __init__(self, bars):
        self.bars = bars
        self.calls = []
        self.last_response_headers = {}
        self.has = {"fetchPremiumIndexOHLCV": True}  # Binance capability; guard checks this

    def fetch_premium_index_ohlcv(self, symbol, timeframe, since, limit):
        self.calls.append({"symbol": symbol, "timeframe": timeframe, "since": since, "limit": limit})
        return self.bars


# --- _build_premium_rows -----------------------------------------------------


def test_build_premium_rows_maps_ohlc_ignores_volume_drops_forming_bar():
    now_ms = 2_000_000_000_000
    closed_ms = now_ms - 2 * INTERVAL * 1000       # fully closed bar
    forming_ms = now_ms - (INTERVAL // 2) * 1000   # still forming → ts+interval > now
    raw = [
        [closed_ms, 0.001, 0.002, 0.0005, 0.0015, 999.0],   # 6th field (volume) is dummy
        [forming_ms, 0.003, 0.004, 0.002, 0.0035, 0.0],
    ]
    rows = pi._build_premium_rows(raw, PAIR, "binance", now_ms)
    assert len(rows) == 1  # forming bar dropped
    r = rows[0]
    assert r == {
        "exchange": "binance", "asset_id": "BTC", "symbol": "BTC/USDT:USDT",
        "native_symbol": "BTCUSDT", "timestamp": closed_ms / 1000,
        "premium_open": 0.001, "premium_high": 0.002, "premium_low": 0.0005, "premium_close": 0.0015,
    }
    assert "volume" not in r and "premium_volume" not in r  # the dummy 6th field is not persisted


def test_build_premium_rows_empty_when_only_forming():
    now_ms = 2_000_000_000_000
    raw = [[now_ms - 10_000, 0.001, 0.001, 0.001, 0.001, 0.0]]  # within the current interval
    assert pi._build_premium_rows(raw, PAIR, "binance", now_ms) == []


# --- _insert_premium ---------------------------------------------------------


def test_insert_premium_commits_and_counts():
    rows = [
        {"exchange": "binance", "asset_id": "BTC", "symbol": "S", "native_symbol": "SUSDT",
         "timestamp": 1.0, "premium_open": 0.0, "premium_high": 0.0, "premium_low": 0.0, "premium_close": 0.0},
        {"exchange": "binance", "asset_id": "ETH", "symbol": "E", "native_symbol": "EUSDT",
         "timestamp": 2.0, "premium_open": 0.0, "premium_high": 0.0, "premium_low": 0.0, "premium_close": 0.0},
    ]
    conn = FakeConn()
    assert pi._insert_premium(conn, rows) == 2
    assert conn.commits == 1
    sqls = [sql for sql, _ in conn.executed]
    assert all("INTO premium_index" in s and "ON CONFLICT DO NOTHING" in s for s in sqls)


def test_insert_premium_empty_no_commit():
    conn = FakeConn()
    assert pi._insert_premium(conn, []) == 0
    assert conn.commits == 0


# --- _seed_cursors -----------------------------------------------------------


def test_seed_cursors_parses_and_skips_none():
    conn = FakeConn(fetch_rows=[("BTC/USDT:USDT", 100.0), ("ETH/USDT:USDT", None)])
    cursors = pi._seed_cursors(conn, "binance")
    assert cursors == {"BTC/USDT:USDT": 100.0}  # None-ts symbol skipped


# --- _collect_premium_pair ---------------------------------------------------


def _closed_bar(ts_ms):
    return [ts_ms, 0.001, 0.002, 0.0005, 0.0015, 0.0]


def test_collect_pair_first_run_is_forward_only():
    # No cursor → since=None, limit=PREMIUM_SEED_LIMIT (a few recent bars, NOT the 1000 candle seed).
    now_ms = 2_000_000_000_000
    ex = FakeExchange([_closed_bar(now_ms - 2 * INTERVAL * 1000)])
    conn = FakeConn()
    cursors = {}
    pi._collect_premium_pair(ex, PAIR, _cfg(), RateLimiter(0), conn, cursors, now_ms)
    call = ex.calls[0]
    assert call["since"] is None
    assert call["limit"] == pi.PREMIUM_SEED_LIMIT
    assert call["timeframe"] == pi.PREMIUM_TIMEFRAME


def test_collect_pair_caught_up_uses_weight_diet():
    # With a cursor a few bars back → since=cursor+1, limit sized to the gap (weight 1, not 5).
    now_ms = 2_000_000_000_000
    cursor_s = now_ms / 1000 - 3 * INTERVAL
    ex = FakeExchange([_closed_bar(now_ms - 2 * INTERVAL * 1000)])
    conn = FakeConn()
    cursors = {PAIR["symbol"]: cursor_s}
    pi._collect_premium_pair(ex, PAIR, _cfg(), RateLimiter(0), conn, cursors, now_ms)
    call = ex.calls[0]
    assert call["since"] == int(cursor_s * 1000) + 1
    assert call["limit"] == poll_limit(call["since"], now_ms, INTERVAL)
    assert binance_kline_weight(call["limit"]) == 1  # diet applied


def test_collect_pair_advances_cursor_to_newest_closed_bar():
    now_ms = 2_000_000_000_000
    t0 = now_ms - 3 * INTERVAL * 1000
    t1 = now_ms - 2 * INTERVAL * 1000
    ex = FakeExchange([_closed_bar(t0), _closed_bar(t1)])
    conn = FakeConn()
    cursors = {}
    fetched, inserted = pi._collect_premium_pair(ex, PAIR, _cfg(), RateLimiter(0), conn, cursors, now_ms)
    assert (fetched, inserted) == (2, 2)
    assert cursors[PAIR["symbol"]] == t1 / 1000  # advanced to the newest closed bar


# --- _sleep_until_next_boundary ----------------------------------------------


def test_sleep_until_next_boundary_math(monkeypatch):
    slept = {}
    monkeypatch.setattr(pi.time, "time", lambda: 1000.0)
    monkeypatch.setattr(pi.time, "sleep", lambda s: slept.setdefault("s", s))
    # next 300s boundary after 1000 is 1200, + buffer 15 → 1215 → sleep 215s
    pi._sleep_until_next_boundary(300, 15)
    assert slept["s"] == 215.0


# --- helpers -----------------------------------------------------------------


def _cfg():
    from types import SimpleNamespace
    return SimpleNamespace(name="binance", market_type="perp")


# --- run() wiring (source-pin — run() is a blocking daemon entry point) -------


def test_run_wires_premium_thread_behind_flag():
    """The premium loop is imported and launched only when has_premium_index is set. Source-pinned
    because run() blocks forever; a live launch test would hang (matches the api LOOP_TIMEFRAMES pin)."""
    src = (Path(pi.__file__).parent / "main.py").read_text()
    assert "from premium_index import premium_index_loop" in src
    assert "config.has_premium_index" in src
    assert "target=premium_index_loop" in src


# --- loop control flow (max_cycles-bounded) ----------------------------------


def _patch_loop(monkeypatch, conn, pairs, seed_fn, hb):
    monkeypatch.setattr(pi, "_sleep_until_next_boundary", lambda *a: None)
    monkeypatch.setattr(pi, "maybe_reload_markets", lambda *a: None)
    monkeypatch.setattr(pi, "connect_db", lambda *a, **k: conn)
    monkeypatch.setattr(pi, "query_active_pairs", lambda *a, **k: pairs)
    monkeypatch.setattr(pi, "_seed_cursors", seed_fn)
    monkeypatch.setattr(pi, "upsert_loop_heartbeat", lambda *a, **k: hb.__setitem__("n", hb["n"] + 1))
    monkeypatch.setattr(pi, "safe_rollback", lambda *a: None)


def test_loop_seed_failure_skips_poll_and_heartbeats(monkeypatch):
    # A failed cursor seed must NOT fall through to a forward-only poll — that would skip the
    # restart gap. The cycle heartbeats (liveness) + retries the seed next cycle, never fetching.
    def _boom(*a, **k):
        raise RuntimeError("db down during seed")
    conn, ex, hb = FakeConn(), FakeExchange([_closed_bar(1_600_000_000_000)]), {"n": 0}
    pairs = [dict(PAIR, market_type="perp", last_timestamp=None)]
    _patch_loop(monkeypatch, conn, pairs, _boom, hb)
    pi.premium_index_loop(ex, _cfg(), "dsn", RateLimiter(0), max_cycles=1)
    assert ex.calls == []   # never polled forward-only over the gap
    assert hb["n"] == 1     # liveness maintained while the seed retries


def test_loop_seeds_empty_then_polls(monkeypatch):
    # A clean seed (empty table → {}) proceeds to the forward-only poll (fetch happens).
    conn, ex, hb = FakeConn(), FakeExchange([_closed_bar(1_600_000_000_000)]), {"n": 0}
    pairs = [dict(PAIR, market_type="perp", last_timestamp=None)]
    _patch_loop(monkeypatch, conn, pairs, lambda *a: {}, hb)
    pi.premium_index_loop(ex, _cfg(), "dsn", RateLimiter(0), max_cycles=1)
    assert len(ex.calls) == 1              # polled the one pair
    assert ex.calls[0]["since"] is None    # forward-only on the empty table
    assert hb["n"] == 1


def test_loop_reactivated_symbol_resumes_from_db_cursor(monkeypatch):
    # A symbol with existing premium rows (returned by the seed) resumes from its DB cursor — NOT a
    # forward-only since=None poll. This is the deactivate→reactivate gap-skip guard: an active
    # symbol missing from the in-memory cursor map is re-seeded from the DB before first-run.
    cursor_ts = 1_600_000_000.0
    conn, ex, hb = FakeConn(), FakeExchange([_closed_bar(1_600_000_500_000)]), {"n": 0}
    pairs = [dict(PAIR, market_type="perp", last_timestamp=None)]
    _patch_loop(monkeypatch, conn, pairs, lambda *a: {PAIR["symbol"]: cursor_ts}, hb)
    pi.premium_index_loop(ex, _cfg(), "dsn", RateLimiter(0), max_cycles=1)
    assert len(ex.calls) == 1
    assert ex.calls[0]["since"] == int(cursor_ts * 1000) + 1   # resumed from DB, not since=None


def test_loop_raises_for_venue_without_premium_index_support():
    # Fail fast on a misconfig: has_premium_index on a venue with no premiumIndexKlines must raise,
    # not heartbeat-as-alive while every fetch fails silently (matches oi_loop / build_liq_spec).
    ex = FakeExchange([])
    ex.has = {}  # no fetchPremiumIndexOHLCV capability
    with pytest.raises(ValueError, match="Binance-only"):
        pi.premium_index_loop(ex, _cfg(), "dsn", RateLimiter(0), max_cycles=1)
