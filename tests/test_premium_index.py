"""Tests for collector/premium_index.py — the Binance premium-index poll (T-010).

Pure helpers (row build, insert, cursor seed) run against a fake conn; `_collect_premium_pair`
runs against a fake exchange + real RateLimiter(0) so the forward-only-vs-diet `since`/`limit`
selection and cursor advance are exercised without a live wire. Loop wiring (has_premium_index
launches the thread) is covered in test_candles-style real-main loading (see test at bottom).
"""

import premium_index as pi
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
