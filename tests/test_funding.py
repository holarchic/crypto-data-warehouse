"""funding.py: settled history read from each venue's raw response, through real ccxt with only
the HTTP transport stubbed (`exchange.fetch` answers from a fake venue that honours the request's
bounds), and the hourly poll built on it."""

import dataclasses
import json
import urllib.parse
from datetime import UTC, datetime
from types import SimpleNamespace

import ccxt
import funding
import pytest
from funding import (
    HOUR_MS,
    FullPage,
    MalformedRecord,
    Settlement,
    fetch_binance_all,
    fetch_symbol_history,
    funding_rows,
)

from tests.fakes import RecordingConn
from tests.instruments import instrument

T0 = 1_792_713_600_000  # 2026-10-21 00:00 UTC
MARKETS = {
    "binance": {"symbol": "BTCUSDT"},
    "bybit": {"category": "linear", "symbol": "BTCUSDT"},
    "okx": {"instId": "BTC-USDT-SWAP"},
    "hyperliquid": {"coin": "BTC"},
    "dydx": {"market": "BTC-USD"},
}


def hourly(n, step_h=1, start=T0, jitter_ms=5):
    """`n` settlements `step_h` apart from `start`, each `jitter_ms` late, rates 1e-5, 2e-5, …"""
    return [(start + i * step_h * HOUR_MS + jitter_ms, (i + 1) * 1e-5) for i in range(n)]


def _iso(ms):
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class FakeVenue:
    """Answers each venue's funding-history request from `history` [(raw_ms, rate)], honouring
    the request's bounds and page size the way the venue does. `extra` records are appended to
    every answer, `override` replaces the answer for the n-th request."""

    def __init__(self, venue, history, extra=(), override=None):
        self.venue, self.history, self.extra, self.override = venue, history, list(extra), override or {}
        self.requests, self.urls = [], []

    def __call__(self, url, method="GET", headers=None, body=None):
        params = json.loads(body) if body else {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).items()}
        self.requests.append(params)
        self.urls.append(url)
        if len(self.requests) - 1 in self.override:
            return self.override[len(self.requests) - 1]
        return getattr(self, f"_{self.venue}")(params)

    def _between(self, lo, hi, newest_first=False, limit=None):
        rows = [(t, r) for t, r in self.history if lo <= t <= hi]
        rows = sorted(rows, reverse=newest_first)
        return rows[:limit] if limit else rows

    def _binance(self, p):
        rows = self._between(int(p["startTime"]), int(p.get("endTime", 10**15)), limit=int(p["limit"]))
        return [{"symbol": p.get("symbol", "BTCUSDT"), "fundingTime": t, "fundingRate": str(r)} for t, r in rows] + self.extra

    def _bybit(self, p):
        rows = self._between(int(p["startTime"]), int(p["endTime"]), newest_first=True, limit=int(p["limit"]))
        recs = [{"symbol": p["symbol"], "fundingRateTimestamp": str(t), "fundingRate": str(r)} for t, r in rows]
        return {"retCode": 0, "result": {"list": recs + self.extra}}

    def _okx(self, p):
        rows = self._between(int(p["before"]) + 1, int(p["after"]) - 1, newest_first=True, limit=int(p["limit"]))
        recs = [{"fundingTime": str(t), "realizedRate": str(r), "fundingRate": "0.0001"} for t, r in rows]
        return {"code": "0", "data": recs + self.extra}

    def _hyperliquid(self, p):
        return [{"coin": p["coin"], "time": t, "fundingRate": str(r)} for t, r in self._between(p["startTime"], p["endTime"])] + self.extra

    def _dydx(self, p):
        until = funding._iso_to_ms(p["effectiveBeforeOrAt"])
        rows = self._between(0, until, newest_first=True, limit=int(p["limit"]))
        return {"historicalFunding": [{"effectiveAt": _iso(t), "rate": str(r)} for t, r in rows] + self.extra}


CCXT_CLASS = {"binance": "binanceusdm"}  # as exchanges.yaml runs it


def exchange_for(venue, fake):
    ex = getattr(ccxt, CCXT_CLASS.get(venue, venue))({"enableRateLimit": False})
    ex.fetch = fake
    return ex


def direct(fn, params):
    return fn(params)


def fetch(venue, history, since, until, **kw):
    fake = FakeVenue(venue, history, extra=kw.pop("extra", ()), override=kw.pop("override", None))
    result = fetch_symbol_history(exchange_for(venue, fake), venue, MARKETS[venue], since, until, direct, **kw)
    return result, fake


# --- fetch_symbol_history ---

@pytest.mark.parametrize("venue", list(MARKETS))
def test_each_venue_returns_the_window_truncated_to_the_second(venue):
    history = hourly(10, jitter_ms=5)
    result, fake = fetch(venue, history, T0, T0 + 9 * HOUR_MS + 999)
    assert result == [Settlement(T0 + i * HOUR_MS, (i + 1) * 1e-5) for i in range(10)]
    if venue == "dydx":  # the market is a path parameter
        assert all("/historicalFunding/BTC-USD?" in url for url in fake.urls)
    else:
        assert all(MARKETS[venue].items() <= r.items() for r in fake.requests)


def test_request_bounds_per_venue():
    _, b = fetch("binance", [], T0, T0 + HOUR_MS)
    assert b.requests == [{"symbol": "BTCUSDT", "startTime": str(T0), "endTime": str(T0 + HOUR_MS), "limit": "1000"}]
    _, o = fetch("okx", [], T0, T0 + HOUR_MS)
    assert (o.requests[0]["before"], o.requests[0]["after"]) == (str(T0 - 1), str(T0 + HOUR_MS + 1))
    _, h = fetch("hyperliquid", [], T0, T0 + HOUR_MS)
    assert (h.requests[0]["startTime"], h.requests[0]["endTime"], h.requests[0]["type"]) == (T0, T0 + HOUR_MS, "fundingHistory")
    _, d = fetch("dydx", [], T0, T0 + HOUR_MS)
    assert d.requests[0]["effectiveBeforeOrAt"] == _iso(T0 + HOUR_MS)


def test_dydx_block_time_jitter_truncates():
    result, _ = fetch("dydx", hourly(3, jitter_ms=262), T0, T0 + 3 * HOUR_MS)
    assert [s.ts_ms for s in result] == [T0, T0 + HOUR_MS, T0 + 2 * HOUR_MS]


def test_hl_catch_up_walks_18h_windows_without_full_page():
    result, fake = fetch("hyperliquid", hourly(100), T0, T0 + 100 * HOUR_MS)
    assert len(result) == 100
    spans = [r["endTime"] - r["startTime"] for r in fake.requests]
    assert len(fake.requests) == 6 and max(spans) == 18 * HOUR_MS - 1


def test_bybit_catch_up_walks_150h_windows_without_full_page():
    result, fake = fetch("bybit", hourly(300), T0, T0 + 300 * HOUR_MS)
    assert len(result) == 300 and len(fake.requests) == 3


def test_hl_repair_window_of_401_items_passes():
    result, fake = fetch("hyperliquid", hourly(401), T0, T0 + 400 * HOUR_MS + 999, window_h=401)
    assert len(result) == 401 and len(fake.requests) == 1


@pytest.mark.parametrize("venue", ["binance", "bybit", "okx"])
def test_a_page_at_the_cap_is_full(venue):
    cap = funding.VENUES[venue].page_cap
    with pytest.raises(FullPage):
        fetch(venue, hourly(cap + 50, step_h=1, jitter_ms=0), T0, T0 + 2000 * HOUR_MS, window_h=cap + 50)


def test_dydx_full_page_reaching_the_window_start_is_complete():
    # 100 rows at or before the window end, the oldest before the window start: complete.
    result, _ = fetch("dydx", hourly(150), T0 + 60 * HOUR_MS, T0 + 120 * HOUR_MS)
    assert [s.ts_ms for s in result] == [T0 + i * HOUR_MS for i in range(60, 120)]


def test_dydx_full_page_inside_the_window_is_cut_off():
    with pytest.raises(FullPage):
        fetch("dydx", hourly(150, step_h=1, jitter_ms=0), T0, T0 + 140 * HOUR_MS, window_h=145)


@pytest.mark.parametrize("venue", list(MARKETS))
@pytest.mark.parametrize("defect", ["no time", "no rate", "nan rate"])
def test_a_malformed_record_fails_the_symbol(venue, defect):
    time_field, rate_field = funding.VENUES[venue].time_field, funding.VENUES[venue].rate_field
    bad = {time_field: _iso(T0) if venue == "dydx" else str(T0), rate_field: "1e-5", "symbol": "BTCUSDT"}
    if defect == "no time":
        del bad[time_field]
    elif defect == "no rate":
        del bad[rate_field]
    else:
        bad[rate_field] = "NaN"
    with pytest.raises(MalformedRecord):
        fetch(venue, hourly(3), T0, T0 + 3 * HOUR_MS, extra=[bad])


def test_a_malformed_record_in_a_later_window_fails_after_earlier_ones_succeed():
    later = {1: [{"coin": "BTC", "time": T0 + 20 * HOUR_MS}]}  # no rate, second request
    with pytest.raises(MalformedRecord):
        fetch("hyperliquid", hourly(40), T0, T0 + 40 * HOUR_MS, override=later)


def test_dydx_malformed_row_older_than_the_window_still_fails():
    with pytest.raises(MalformedRecord):
        fetch("dydx", hourly(5, start=T0 + 50 * HOUR_MS), T0 + 50 * HOUR_MS, T0 + 55 * HOUR_MS,
              extra=[{"effectiveAt": _iso(T0), "rate": None}])


def test_okx_reads_the_realized_rate_never_the_prediction():
    result, _ = fetch("okx", [(T0, 3e-5)], T0, T0 + HOUR_MS)
    assert result == [Settlement(T0, 3e-5)]  # the fake's fundingRate is 0.0001
    missing = {"fundingTime": str(T0 + 1), "fundingRate": "0.0001"}
    with pytest.raises(MalformedRecord):
        fetch("okx", [], T0, T0 + HOUR_MS, extra=[missing])


# --- fetch_binance_all ---

def binance_all(rows_by_page):
    pages = iter(rows_by_page)
    ex = ccxt.binanceusdm({"enableRateLimit": False})
    requests = []

    def fake(url, method="GET", headers=None, body=None):
        requests.append({k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).items()})
        return [{"symbol": s, "fundingTime": t, "fundingRate": str(r)} for s, t, r in next(pages)]

    ex.fetch = fake
    return ex, requests


def test_binance_all_pages_on_raw_ms_and_keeps_same_second_symbols(monkeypatch):
    monkeypatch.setitem(funding.VENUES, "binance", dataclasses.replace(funding.VENUES["binance"], page_cap=3))
    page1 = [("AUSDT", T0 + 2, 1e-5), ("BUSDT", T0 + 5, 2e-5), ("CUSDT", T0 + 5, 3e-5)]
    page2 = [("CUSDT", T0 + 5, 3e-5), ("DUSDT", T0 + 7, 4e-5)]  # the boundary second, re-fetched
    ex, requests = binance_all([page1, page2])
    result = fetch_binance_all(ex, T0, direct)
    assert [r["startTime"] for r in requests] == [str(T0), str(T0 + 5)]
    assert result == {s: [Settlement(T0, r)] for s, r in
                      [("AUSDT", 1e-5), ("BUSDT", 2e-5), ("CUSDT", 3e-5), ("DUSDT", 4e-5)]}


def test_binance_all_non_advancing_page_steps_once_then_fails(monkeypatch):
    monkeypatch.setitem(funding.VENUES, "binance", dataclasses.replace(funding.VENUES["binance"], page_cap=2))
    stuck = [("AUSDT", T0, 1e-5), ("BUSDT", T0, 1e-5)]
    ex, requests = binance_all([stuck, stuck, stuck])
    with pytest.raises(FullPage):
        fetch_binance_all(ex, T0, direct)
    assert [r["startTime"] for r in requests] == [str(T0), str(T0 + 1)]


def test_binance_all_record_without_symbol_fails():
    ex, _ = binance_all([[(None, T0, 1e-5)]])
    with pytest.raises(MalformedRecord):
        fetch_binance_all(ex, T0, direct)


# --- funding_rows ---

def test_interval_is_the_gap_to_the_previous_settlement():
    pair = instrument("IOST/USDT:USDT", "IOST")
    times = [T0, T0 + 4 * HOUR_MS, T0 + 8 * HOUR_MS, T0 + 9 * HOUR_MS, T0 + 10 * HOUR_MS]
    rows = funding_rows(pair, "binance", T0 - 4 * HOUR_MS, [Settlement(t, 1e-5) for t in times])
    assert [r["interval_hours"] for r in rows] == [4, 4, 4, 1, 1]
    assert rows[0] == {"exchange": "binance", "asset_id": "IOST", "symbol": "IOST/USDT:USDT",
                       "timestamp": T0 / 1000, "rate": 1e-5, "interval_hours": 4}
    assert funding_rows(pair, "binance", None, [Settlement(T0, 1e-5)])[0]["interval_hours"] is None


def test_rows_never_carry_a_predicted_rate():
    assert "predicted_rate" not in funding_rows(instrument(), "okx", None, [Settlement(T0, 1e-5)])[0]
    assert "NULL, %(interval_hours)s" in funding.INSERT_FUNDING_SQL


# --- poll_settled ---

class PollConn(RecordingConn):
    """Answers the cursor query with `cursors` (symbol -> ms) and counts each insert as one row."""

    def __init__(self, cursors):
        super().__init__(rows=list(cursors.items()))

    def execute(self, sql, params=None):
        super().execute(sql, params)
        self.rowcount = 1

    def inserted(self):
        return [e.params for e in self.events if e.sql == funding.INSERT_FUNDING_SQL]


def poll(monkeypatch, venue, fake, instruments, cursors, now_ms, markets=None):
    monkeypatch.setattr(funding.time, "time", lambda: now_ms / 1000)
    monkeypatch.setattr(funding, "RETRIABLE_EXCEPTIONS", (ConnectionError,))
    monkeypatch.setattr(funding, "safe_rollback", lambda conn: conn.rollback())
    ex = exchange_for(venue, fake)
    markets = markets or {}
    ex.market = lambda symbol: markets[symbol]
    cfg = SimpleNamespace(name=venue, ccxt_class=CCXT_CLASS.get(venue, venue))
    conn = PollConn(cursors)
    monkeypatch.setattr(funding, "paced_call", lambda rl, ex_, w, fn, *a, **k: fn(*a, **k))
    inserted = funding.poll_settled(ex, cfg, conn, instruments, rate_limiter=None)
    return inserted, conn


BTC = instrument("BTC/USDT:USDT", "BTC")
ETH = instrument("ETH/USDT:USDT", "ETH")
NEW = instrument("NEW/USDT:USDT", "NEW")
BIN_MARKETS = {"BTC/USDT:USDT": {"id": "BTCUSDT"}, "ETH/USDT:USDT": {"id": "ETHUSDT"},
               "NEW/USDT:USDT": {"id": "NEWUSDT"}, "OLD/USDT:USDT": {"id": "OLDUSDT"}}


def test_binance_poll_walks_from_the_oldest_active_cursor_and_inserts_only_newer_rows(monkeypatch):
    now = T0 + 24 * HOUR_MS
    history = {"BTCUSDT": hourly(6, step_h=4, jitter_ms=3), "ETHUSDT": hourly(3, step_h=8, jitter_ms=4)}
    requests = []

    def fake(url, method="GET", headers=None, body=None):
        q = {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).items()}
        requests.append(q)
        return [{"symbol": s, "fundingTime": t, "fundingRate": str(r)}
                for s, rows in history.items() for t, r in rows if t >= int(q["startTime"])]

    cursors = {"BTC/USDT:USDT": T0 + 12 * HOUR_MS, "ETH/USDT:USDT": T0 + 8 * HOUR_MS}
    inserted, conn = poll(monkeypatch, "binance", fake, [BTC, ETH], cursors, now, BIN_MARKETS)
    assert requests == [{"startTime": str(T0 + 8 * HOUR_MS + 1000), "limit": "1000"}]
    rows = conn.inserted()
    assert [(r["symbol"], r["timestamp"] * 1000, r["interval_hours"]) for r in rows] == [
        ("BTC/USDT:USDT", T0 + 16 * HOUR_MS, 4), ("BTC/USDT:USDT", T0 + 20 * HOUR_MS, 4),
        ("ETH/USDT:USDT", T0 + 16 * HOUR_MS, 8)]
    assert inserted == 3
    assert conn.kinds()[:2] == ["execute", "commit"]  # the cursor read ends before the network walk


def test_binance_poll_new_symbol_reaches_back_100h_and_never_past_7_days(monkeypatch):
    now = T0 + 300 * HOUR_MS
    requests = []

    def fake(url, method="GET", headers=None, body=None):
        requests.append(urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["startTime"][0])
        return []

    poll(monkeypatch, "binance", fake, [NEW], {}, now, BIN_MARKETS)
    assert requests == [str(now - 100 * HOUR_MS)]
    requests.clear()
    stale = {"NEW/USDT:USDT": now - 200 * HOUR_MS}
    poll(monkeypatch, "binance", fake, [NEW], stale, now, BIN_MARKETS)
    assert requests == [str(now - 168 * HOUR_MS)]


def test_per_symbol_poll_starts_one_second_after_the_cursor(monkeypatch):
    now = T0 + 10 * HOUR_MS
    fake = FakeVenue("hyperliquid", hourly(10))
    markets = {"BTC/USDT:USDT": {"baseName": "BTC"}}
    _, conn = poll(monkeypatch, "hyperliquid", fake, [BTC], {"BTC/USDT:USDT": T0 + 7 * HOUR_MS}, now, markets)
    assert fake.requests[0]["startTime"] == T0 + 7 * HOUR_MS + 1000
    assert [(r["timestamp"] * 1000, r["interval_hours"]) for r in conn.inserted()] == [
        (T0 + 8 * HOUR_MS, 1), (T0 + 9 * HOUR_MS, 1)]
    assert all(r["endTime"] - r["startTime"] < 18 * HOUR_MS for r in fake.requests)


def test_a_failing_symbol_writes_nothing_and_the_next_still_polls(monkeypatch):
    now = T0 + 3 * HOUR_MS
    fake = FakeVenue("hyperliquid", hourly(3))
    markets = {"BTC/USDT:USDT": {"baseName": "BAD"}, "ETH/USDT:USDT": {"baseName": "ETH"}}
    original = fake._hyperliquid
    fake._hyperliquid = lambda p: [{"coin": "BAD", "time": T0}] if p["coin"] == "BAD" else original(p)
    inserted, conn = poll(monkeypatch, "hyperliquid", fake, [BTC, ETH], {}, now, markets)
    assert {r["symbol"] for r in conn.inserted()} == {"ETH/USDT:USDT"}
    assert inserted == 3
    assert "rollback" in conn.kinds()


def test_no_path_reads_the_current_funding_endpoint():
    source = open(funding.__file__).read()
    assert "fetch_funding_rates" not in source and "fetch_funding_rate(" not in source
