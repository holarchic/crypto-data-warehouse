"""funding_repair: report, fail-closed apply and HL pacing, against an in-memory funding table and a
fake venue behind real ccxt (only the transport stubbed). The SQL itself runs against TimescaleDB in
tests/integration/check_funding_repair.py."""

from datetime import UTC, datetime

import ccxt
import funding_repair as fr
import pytest
from funding import HOUR_MS
from funding_repair import Report, StoredRow, compare, compare_tail

from tests.funding_fakes import T0, FakeVenue, exchange_for, hourly

SYMBOL = "BTC/USDT:USDT"


class TableConn:
    """funding_rates for one exchange as {(symbol, ts_ms): (rate, interval)}, answering the repair's
    SQL by identity and applying its delete and upsert."""

    def __init__(self, rows, exchange="binance", lock_free=True, candle_lock=lambda: False):
        self.table, self.exchange = dict(rows), exchange
        self.lock_free, self.candle_lock = lock_free, candle_lock
        self.result, self.deletes, self.commits, self.rollbacks = [], [], 0, 0

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if sql == fr.LOCK_SQL:
            self.result = [(self.lock_free,)]
        elif sql == fr.LOCK_HELD_SQL:
            assert params["key"] == fr.HL_CANDLE_REPAIR_LOCK_KEY
            self.result = [(self.candle_lock(),)]
        elif sql == fr.SYMBOLS_SQL:
            symbols = sorted({s for s, _ in self.table})
            self.result = [(s, s.split("/")[0], min(t for x, t in self.table if x == s),
                            max(t for x, t in self.table if x == s)) for s in symbols]
        elif sql == fr.STORED_SQL:
            lo, hi = params["lo_s"] * 1000, params["hi_s"] * 1000
            self.result = sorted((t, r, i) for (s, t), (r, i) in self.table.items()
                                 if s == params["symbol"] and lo <= t < hi)
        elif sql == fr.DELETE_SQL:
            self.deletes.append(params)
            for key in [k for k in self.table if k[0] == params["symbol"] and k[1] < params["hi_s"] * 1000]:
                del self.table[key]
        else:
            raise AssertionError(sql)

    def executemany(self, sql, rows):
        assert sql == fr.UPSERT_SQL
        for r in rows:
            self.table[(r["symbol"], round(r["timestamp"] * 1000))] = (r["rate"], r["interval_hours"])

    def fetchone(self):
        return self.result[0]

    def fetchall(self):
        return self.result

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class Clock:
    def __init__(self, start):
        self.now = start
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def run(conn, fake, until_ms, apply=False, venue="binance", market=None):
    exchange = exchange_for(venue, fake)
    exchange.market = market or (lambda symbol: {"id": "BTCUSDT", "baseName": "BTC", "linear": True})
    clock = Clock(T0 / 1000 + 12 * 3600)
    code = fr.run(conn, lambda: exchange, venue, until_ms, apply, None, sleep=clock.sleep, clock=clock)
    return code, clock.sleeps


def report_for(conn, fake, until_ms, venue="binance"):
    exchange = exchange_for(venue, fake)
    exchange.market = lambda symbol: {"id": "BTCUSDT", "baseName": "BTC", "linear": True}
    pacer = fr.Pacer(venue, sleep=lambda s: None)
    stored = fr.stored_symbols(conn, venue)[0]
    return fr.repair_symbol(conn, exchange, venue, stored, until_ms, pacer, apply=False)


def four_hourly(n, start=T0, jitter_ms=5):
    return hourly(n, step_h=4, start=start, jitter_ms=jitter_ms)


def canonical(history, first_interval=None):
    """The table rows the venue `history` should end as: second-truncated, interval from the gap."""
    rows, prev = {}, None
    for raw, rate in history:
        t = raw // 1000 * 1000
        rows[(SYMBOL, t)] = (rate, first_interval if prev is None else round((t - prev) / HOUR_MS))
        prev = t
    return rows


# --- compare ---

def test_report_counts_each_defect():
    rates = [1e-5, 2e-5, 3e-5, 4e-5, 5e-5, 6e-5]
    venue = [{"timestamp": (T0 + i * 4 * HOUR_MS) / 1000, "rate": rates[i],
              "interval_hours": 4 if i else None} for i in range(6)]
    stored = [
        StoredRow(T0 - 8 * HOUR_MS, 1e-5, 8),  # older than the venue's history
        StoredRow(T0, 1e-5, 4),  # the first settlement: no fetched predecessor
        StoredRow(T0 + 2 * HOUR_MS, 9e-5, 8),  # a shifted stamp
        StoredRow(T0 + 4 * HOUR_MS, 2e-5, 4),  # equal
        StoredRow(T0 + 4 * HOUR_MS + 2, 2e-5, 8),  # its millisecond duplicate
        StoredRow(T0 + 8 * HOUR_MS, 1e-4, 4),  # the floor rate
        StoredRow(T0 + 12 * HOUR_MS, 4e-5, 8),  # the venue default interval
        # T0 + 16 h missing
        StoredRow(T0 + 20 * HOUR_MS + 5, 6e-5, 4),  # right rate, millisecond stamp
    ]
    report = Report(SYMBOL)
    compare(stored, venue, report)
    assert (report.equal, report.differ, report.missing, report.unmatched, report.pre_depth,
            report.interval_unverified) == (1, 3, 1, 2, 1, 1)


def test_tail_counts_wrong_rates_and_accepts_jitter():
    venue = [{"timestamp": (T0 + 12 * HOUR_MS) / 1000, "rate": 1e-5, "interval_hours": 1}]
    assert compare_tail([StoredRow(T0 + 12 * HOUR_MS, 1e-5, 1)], venue) == 0
    assert compare_tail([StoredRow(T0 + 12 * HOUR_MS, 1e-4, 1)], venue) == 1
    assert compare_tail([StoredRow(T0 + 13 * HOUR_MS, 1e-5, 1)], venue) == 1
    assert compare_tail([StoredRow(T0 + 13 * HOUR_MS, None, 1)], venue) == 1  # NULL on no settlement


def test_the_repair_upsert_replaces_a_conflicting_row():
    # The collector's insert does nothing on conflict; the repair's must win (behaviour pinned in
    # tests/integration/check_funding_repair.py against TimescaleDB).
    assert "ON CONFLICT (exchange, symbol, timestamp) DO UPDATE" in fr.UPSERT_SQL
    assert "interval_hours = EXCLUDED.interval_hours" in fr.UPSERT_SQL


# --- apply ---

def legacy_table(history):
    """What the old poll stored for a 4h Binance symbol: each settlement's rate 4 h early, plus
    the startup seed's millisecond-stamped copies of the first two."""
    rows = {(SYMBOL, raw // 1000 * 1000 - 4 * HOUR_MS): (rate, 8) for raw, rate in history}
    rows.update({(SYMBOL, raw): (rate, 8) for raw, rate in history[:2]})
    return rows


def test_apply_rebuilds_the_symbol_and_a_second_report_is_all_equal():
    history = four_hourly(12)
    until = T0 + 40 * HOUR_MS
    conn = TableConn(legacy_table(history))
    code, _ = run(conn, FakeVenue("binance", history), until, apply=True)
    assert code == 0
    assert conn.deletes == [{"exchange": "binance", "symbol": SYMBOL, "hi_s": (until + 1000) / 1000}]
    assert conn.table == canonical(history[:11])
    report = report_for(conn, FakeVenue("binance", history), until)
    assert (report.differ, report.missing, report.unmatched, report.pre_depth, report.tail_unmatched) == (0,) * 5
    assert report.equal == 11


def test_until_is_inclusive_by_canonical_second_and_newer_rows_survive():
    history = four_hourly(4)  # T0 .. T0+12h, each 5 ms late
    until = T0 + 8 * HOUR_MS
    later = (SYMBOL, T0 + 12 * HOUR_MS)
    rate_at_until = history[2][1]
    rows = {(SYMBOL, T0 + 8 * HOUR_MS): (9e-5, 8), (SYMBOL, T0 + 8 * HOUR_MS + 5): (rate_at_until, 8), later: (4e-5, 4)}
    conn = TableConn(rows)
    run(conn, FakeVenue("binance", history), until, apply=True)
    # the venue's .005 settlement at `until` is fetched and kept; its interval comes from the
    # settlement read 24 h before the first stored row
    assert conn.table[(SYMBOL, T0 + 8 * HOUR_MS)] == (rate_at_until, 4)
    assert (SYMBOL, T0 + 8 * HOUR_MS + 5) not in conn.table
    assert conn.table[later] == (4e-5, 4)
    assert (SYMBOL, T0) not in conn.table  # venue history before the first stored row isn't added


def test_retention_advancing_after_apply_is_pre_depth_and_interval_unverified():
    history = four_hourly(10)
    until = T0 + 36 * HOUR_MS
    conn = TableConn(canonical(history))
    report = report_for(conn, FakeVenue("binance", history[2:]), until)  # the venue dropped two
    assert (report.pre_depth, report.interval_unverified, report.differ, report.unmatched) == (2, 1, 0, 0)


def test_tail_rows_are_reported_and_never_written():
    history = four_hourly(6)
    until = T0 + 12 * HOUR_MS
    rows = canonical(history)
    rows[(SYMBOL, T0 + 20 * HOUR_MS)] = (1e-4, 4)  # after until, wrong rate
    conn = TableConn(rows)
    report = report_for(conn, FakeVenue("binance", history), until)
    assert report.tail_unmatched == 1
    run(conn, FakeVenue("binance", history), until, apply=True)
    assert conn.table[(SYMBOL, T0 + 20 * HOUR_MS)] == (1e-4, 4)


def test_a_failed_fetch_leaves_the_symbol_untouched_and_the_run_fails():
    history = hourly(450)  # two 400 h repair windows
    rows = {(SYMBOL, T0 + i * HOUR_MS): (1e-4, 8) for i in range(450)}
    conn = TableConn(rows, exchange="hyperliquid")
    fake = FakeVenue("hyperliquid", history, override={1: [{"coin": "BTC", "time": T0 + 400 * HOUR_MS}]})  # no rate
    code, _ = run(conn, fake, T0 + 449 * HOUR_MS, apply=True, venue="hyperliquid")
    assert code == 1 and len(fake.requests) == 2
    assert conn.deletes == [] and conn.table == rows


def test_a_malformed_symbol_fails_alone_and_the_next_is_repaired():
    eth = "ETH/USDT:USDT"
    rows = {(SYMBOL, T0): (1e-4, 8), (eth, T0): (1e-4, 8)}
    conn = TableConn(rows)
    good = FakeVenue("binance", four_hourly(3))

    def venue(url, method="GET", headers=None, body=None):
        if "symbol=BTCUSDT" in url:
            return [{"symbol": "BTCUSDT", "fundingTime": float("inf"), "fundingRate": "1e-5"}]
        return good(url, method, headers, body)

    market = {SYMBOL: {"id": "BTCUSDT"}, eth: {"id": "ETHUSDT"}}
    code, _ = run(conn, venue, T0 + 8 * HOUR_MS, apply=True, market=market.__getitem__)
    assert code == 1
    assert conn.table[(SYMBOL, T0)] == (1e-4, 8)  # untouched
    assert conn.table[(eth, T0)] == (four_hourly(3)[0][1], None)  # rebuilt


def test_an_unfetchable_symbol_is_left_alone():
    rows = {(SYMBOL, T0): (1e-4, 8)}
    conn = TableConn(rows)

    def no_market(symbol):
        raise ccxt.BadSymbol(symbol)

    code, _ = run(conn, FakeVenue("binance", four_hourly(3)), T0 + 8 * HOUR_MS, apply=True, market=no_market)
    assert code == 0 and conn.deletes == [] and conn.table == rows


def test_a_held_repair_lock_stops_the_run():
    conn = TableConn({}, lock_free=False)
    code, _ = run(conn, FakeVenue("binance", []), T0)
    assert code == 1


@pytest.mark.parametrize("venue", ["binance", "bybit", "okx", "dydx"])
def test_each_request_is_followed_by_the_venue_spacing(venue):
    conn = TableConn({(SYMBOL, T0): (1e-5, None)}, exchange=venue)
    _, sleeps = run(conn, FakeVenue(venue, four_hourly(2)), T0 + 4 * HOUR_MS, venue=venue)
    assert sleeps and set(sleeps) == {fr.REPAIR_SPACING_S[venue]}


# --- HL gate ---

def hl_gate(start, candle_lock):
    clock = Clock(start)
    conn = TableConn({}, exchange="hyperliquid", candle_lock=lambda: candle_lock(clock.now))
    return fr.HlGate(conn, clock, clock.sleep), clock


def noon(minutes=0):
    return datetime(2026, 10, 23, 12, tzinfo=UTC).timestamp() + minutes * 60


def test_hl_gate_waits_60s_after_start():
    gate, clock = hl_gate(noon(), lambda now: False)
    gate.wait()
    assert clock.now == noon(1)


def test_hl_gate_waits_out_the_candle_lock_then_60s_more():
    release = noon(5)
    gate, clock = hl_gate(noon(2), lambda now: now < release)
    gate.wait()
    assert release + fr.HL_COOLDOWN_S <= clock.now < release + fr.HL_COOLDOWN_S + fr.HL_BLOCKED_POLL_S


def test_hl_gate_holds_through_the_pre_repair_window():
    start = datetime(2026, 10, 23, 6, 9, 30, tzinfo=UTC).timestamp()
    gate, clock = hl_gate(start, lambda now: False)
    gate.wait()
    end = datetime(2026, 10, 23, 6, 35, tzinfo=UTC).timestamp()
    assert end + fr.HL_COOLDOWN_S <= clock.now < end + fr.HL_COOLDOWN_S + fr.HL_BLOCKED_POLL_S


def test_hl_markets_load_and_every_request_wait_on_the_gate():
    clock = Clock(noon())
    events = []
    conn = TableConn({(SYMBOL, T0): (1e-5, None)}, exchange="hyperliquid")
    fake = FakeVenue("hyperliquid", hourly(2))

    def factory():
        events.append(("markets", clock.now))
        exchange = exchange_for("hyperliquid", lambda *a, **k: (events.append(("request", clock.now)), fake(*a, **k))[1])
        exchange.market = lambda symbol: {"baseName": "BTC"}
        return exchange

    fr.run(conn, factory, "hyperliquid", T0 + HOUR_MS, False, None, sleep=clock.sleep, clock=clock)
    assert events[0] == ("markets", noon(1))  # not before the start cooldown
    times = [t for _, t in events]
    assert all(b - a >= 60 for a, b in zip(times, times[1:], strict=False))


def test_until_must_be_a_whole_second_with_a_timezone():
    assert fr.parse_until("2026-10-27T13:00:00Z") == datetime(2026, 10, 27, 13, tzinfo=UTC).timestamp() * 1000
    for bad in ("2026-10-27T13:00:00", "2026-10-27T13:00:00.5Z"):
        with pytest.raises(Exception):
            fr.parse_until(bad)
