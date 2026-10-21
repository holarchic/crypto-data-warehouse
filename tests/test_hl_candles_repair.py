"""Tests for collector/hl_candles_repair.py: the window and cursor rules, parsing and coverage, pacing,
and `run` against a fake HL, a fake DB and a fake clock. The real writer, SQL, bootstrap and lock run
against TimescaleDB in tests/integration/check_hl_candles_repair.py (conftest stubs psycopg here)."""

import math

import hl_candles_repair as hl
import pytest
from config import CANDLE_REPAIR, LOOP_INTERVALS
from feed_health import paging_threshold_s

from tests.instruments import instrument

H = hl.HOUR_MS
M = hl.MINUTE_MS
NOW = 1_792_400_000_000 // M * M + 30_000  # mid-minute, so `end` floors
END = NOW // M * M - 2 * M


# ---- window ----


def test_window_without_cursor_is_26h():
    w = hl.window(NOW, None)
    assert (w.req, w.end, w.lost) == (END - 26 * H, END, None)


def test_window_self_heals_back_to_cursor_minus_overlap():
    w = hl.window(NOW, END - 50 * H)
    assert (w.req, w.lost) == (END - 52 * H, None)


def test_window_recent_cursor_keeps_26h():
    assert hl.window(NOW, END - 1 * H).req == END - 26 * H


def test_window_clipping_only_the_overlap_is_not_loss():
    w = hl.window(NOW, END - 71 * H)
    assert (w.req, w.lost) == (END - 72 * H, None)
    w = hl.window(NOW, END - 72 * H)
    assert (w.req, w.lost) == (END - 72 * H, None)


def test_window_cursor_past_the_cap_is_a_lost_span():
    w = hl.window(NOW, END - 100 * H)
    assert (w.req, w.lost) == (END - 72 * H, (END - 100 * H, END - 72 * H))


def test_slices_are_oldest_first_contiguous_and_at_most_26h():
    assert hl.slices(END - 26 * H, END) == [(END - 26 * H, END)]
    s = hl.slices(END - 72 * H, END)
    assert s == [(END - 72 * H, END - 46 * H), (END - 46 * H, END - 20 * H), (END - 20 * H, END)]


def test_failed_bootstrap_pages_before_its_anchor_reaches_the_cap():
    page_after_ms = paging_threshold_s(LOOP_INTERVALS[CANDLE_REPAIR]) * 1000
    assert hl.DEFAULT_LOOKBACK_MS + page_after_ms < hl.CAP_MS


# ---- pacing ----


def test_snapshot_weight():
    assert hl.snapshot_weight(0, 26 * H) == 46
    assert hl.snapshot_weight(0, 3 * H) == 23
    assert hl.snapshot_weight(0, 61 * M) == 22


@pytest.mark.parametrize("per_min, gap", [(20, 138), (10, 276), (5, 552)])
def test_next_call_at_spaces_46_weight_calls(per_min, gap):
    assert hl.next_call_at(100.0, 46, per_min) == pytest.approx(100.0 + gap)


class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.sleeps = []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


def test_pacer_charges_every_call_and_halves_on_429():
    clock = FakeClock()
    p = hl.Pacer(clock, clock.sleep)
    p.charge(20)  # meta: no wait on the first call
    p.charge(46)  # waits meta's 60 s
    assert clock.sleeps == [pytest.approx(60.0)]
    p.rate_limited()
    p.rate_limited()
    p.rate_limited()
    assert (p.per_min, p.http429) == (5.0, 3)
    assert clock.sleeps[1:] == [hl.BACKOFF_429_S] * 3


# ---- parsing and coverage ----


def bar(t, n=5, coin="BTC", **over):
    b = {"t": t, "T": t + M - 1, "s": coin, "i": "1m", "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10", "n": n}
    b.update(over)
    return b


def test_parse_keeps_traded_in_window_bars_and_counts_every_listed_minute():
    s, e = END - 4 * M, END
    bars = [bar(s - M), bar(s), bar(s + M, n=0), bar(s + 2 * M), bar(s + 3 * M), bar(e)]
    p = hl.parse_snapshot(bars, "BTC", s, e)
    assert not p.malformed
    assert p.raw_minutes == [s, s + M, s + 2 * M, s + 3 * M]
    assert [b["t"] for b in p.traded] == [s, s + 2 * M, s + 3 * M]
    assert p.traded[0] == {"t": s, "n": 5, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0}


@pytest.mark.parametrize("bad", [
    {"o": "x"}, {"h": "NaN"}, {"v": "inf"}, {"n": "5"}, {"t": END - 4 * M + 1}, {"s": "ETH"}, {"t": None},
    {"c": False}, {"o": True}, {"n": True}, {"t": True},
])
def test_parse_flags_a_malformed_bar_and_keeps_nothing(bad):
    s = END - 4 * M
    broken = {**bar(s + M), **bad}
    p = hl.parse_snapshot([bar(s), broken], "BTC", s, END)
    assert p.malformed and p.traded == []


def test_parse_flags_a_non_list_answer():
    assert hl.parse_snapshot({"error": "x"}, "BTC", 0, M).malformed


@pytest.mark.parametrize("minutes, expected", [
    ([0, 1, 2, 3], "complete"),
    ([2, 3], "leading"),
    ([], "empty"),
    ([0, 1, 3], "hole"),
    ([0, 1, 2], "hole"),  # ends early
])
def test_coverage(minutes, expected):
    assert hl.coverage([m * M for m in minutes], 0, 4 * M) == expected


def r(status, conflict=False):
    return {"status": status, "stored_conflict": conflict, "inserted": 0, "updated": 0}


@pytest.mark.parametrize("statuses, ok", [
    (["complete"], True),
    (["leading"], True),
    (["empty", "leading", "complete"], True),
    (["empty", "complete"], True),
    (["empty"], False),
    (["empty", "empty"], False),
    (["complete", "leading"], False),
    (["leading", "leading"], False),
    (["complete", "empty"], False),
    (["hole"], False),
    (["malformed"], False),
    (["failed"], False),
    (["expired", "complete"], False),
])
def test_coin_complete(statuses, ok):
    assert hl.coin_complete([r(s) for s in statuses]) is ok


def test_coin_complete_refuses_a_stored_row_rest_lacks():
    assert not hl.coin_complete([r("complete", conflict=True)])
    assert not hl.coin_complete([r("empty", conflict=True), r("complete")])


# ---- run against a fake world ----


PAIRS = [
    instrument(symbol="BTC/USDC:USDC", asset_id="BTC", native_symbol="0", market_type="perp"),
    instrument(symbol="KPEPE/USDC:USDC", asset_id="KPEPE", native_symbol="9", market_type="perp"),
]
NAMES = {"BTC": "BTC", "KPEPE": "kPEPE"}


class World:
    """HL, the warehouse and the clock in one, patched over the module's I/O."""

    def __init__(self, monkeypatch, cursor=None, pairs=PAIRS, wall_ms=NOW):
        self.cursor = cursor
        self.pairs = pairs
        self.lock_free = True
        self.stored = {}  # symbol -> set of minutes
        self.bootstrapped = []
        self.advanced = []
        self.writes = []
        self.listed_from = {}  # coin -> first listed minute
        self.untraded = set()  # (coin, minute) with n == 0
        self.missing = set()  # (coin, minute) REST omits
        self.malformed = set()  # coin whose answers are malformed
        self.fail = set()  # coin whose fetch fails
        self.rate_limit_next = 0
        self.alerts = []
        self.deliver = True
        self.calls = []
        self.clock = FakeClock()
        self.wall0 = wall_ms
        monkeypatch.setattr(hl, "take_lock", lambda conn: self.lock_free)
        monkeypatch.setattr(hl, "read_cursor", lambda conn: self.cursor)
        monkeypatch.setattr(hl, "bootstrap_cursor", self._bootstrap)
        monkeypatch.setattr(hl, "active_pairs", lambda conn: self.pairs)
        monkeypatch.setattr(hl, "stored_minutes", lambda conn, pair, s, e: {
            t for t in self.stored.get(pair.symbol, set()) if s <= t < e})
        monkeypatch.setattr(hl, "write_bars", self._write)
        monkeypatch.setattr(hl, "advance_cursor", lambda conn, end: self.advanced.append(end))

    def _bootstrap(self, conn, cursor):
        self.bootstrapped.append(cursor)
        self.cursor = cursor

    def _write(self, conn, pair, bars):
        self.writes.append((pair.symbol, [b["t"] for b in bars]))
        return 0, len(bars)

    def fetch(self, coin, s, e):
        self.calls.append((coin, s, e, self.clock.t))
        if self.rate_limit_next:
            self.rate_limit_next -= 1
            raise hl.RateLimited()
        if coin in self.fail:
            raise hl.FetchError("boom")
        if coin in self.malformed:
            return [bar(s, coin=coin, o="x")]
        first = max(s, self.listed_from.get(coin, s))
        return [bar(t, coin=coin, n=0 if (coin, t) in self.untraded else 3)
                for t in range(first, e, M) if (coin, t) not in self.missing]

    def deps(self):
        def alert(text):
            self.alerts.append(text)
            return self.deliver
        return hl.Deps(connect=lambda: object(), fetch_snapshot=self.fetch, fetch_meta=lambda: NAMES,
                       send_alert=alert, sleep=self.clock.sleep, clock=self.clock,
                       wall_ms=lambda: self.wall0 + int(self.clock.t * 1000))

    def run(self, hours=None):
        return hl.run(NOW, hours, self.deps())


def test_run_all_complete_advances_to_end(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    assert w.run() == 0
    assert w.advanced == [END]
    assert [c[:3] for c in w.calls] == [("BTC", END - 26 * H, END), ("kPEPE", END - 26 * H, END)]


def test_run_paces_snapshot_calls(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    w.run()
    assert w.calls[1][3] - w.calls[0][3] == pytest.approx(46 * 60 / 20)


def test_first_run_seeds_the_cursor_before_fetching(monkeypatch):
    w = World(monkeypatch, cursor=None)
    w.fail = {"BTC"}
    assert w.run() == 1
    assert w.bootstrapped == [END - 26 * H] and w.advanced == []


def test_a_failed_first_run_is_covered_by_the_next(monkeypatch):
    w = World(monkeypatch, cursor=None)
    w.fail = {"BTC"}
    w.run()
    anchor = w.bootstrapped[0]
    later = hl.window(NOW + 24 * H, w.cursor)
    assert later.req <= anchor and later.lost is None


def test_run_coin_failure_keeps_the_cursor(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    w.fail = {"kPEPE"}
    assert w.run() == 1
    assert w.advanced == []


def test_run_boolean_price_writes_nothing_and_keeps_the_cursor(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    real = w.fetch
    w.fetch = lambda coin, s, e: [{**b, "c": False} if coin == "kPEPE" else b for b in real(coin, s, e)]
    assert w.run() == 1
    assert [s for s, _ in w.writes] == ["BTC/USDC:USDC"] and w.advanced == []


def test_run_malformed_writes_nothing_for_that_slice(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    w.malformed = {"kPEPE"}
    assert w.run() == 1
    assert [s for s, _ in w.writes] == ["BTC/USDC:USDC"] and w.advanced == []


def test_run_hole_in_both_sources_is_incomplete_but_written(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    w.missing = {("BTC", END - 5 * H)}  # REST and storage both lack it
    assert w.run() == 1
    assert w.advanced == [] and len(w.writes) == 2


def test_run_untraded_minutes_still_complete_coverage(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    w.untraded = {("BTC", END - 5 * H), ("BTC", END - 5 * H + M)}
    assert w.run() == 0
    btc = dict(w.writes)["BTC/USDC:USDC"]
    assert END - 5 * H not in btc and len(btc) == 26 * 60 - 2


def test_run_stored_row_rest_lacks_is_incomplete(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    w.stored = {"BTC/USDC:USDC": {END - 3 * H}}
    w.untraded = {("BTC", END - 3 * H)}
    assert w.run() == 1 and w.advanced == []


def test_run_leading_gap_is_a_listing(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    w.listed_from = {"kPEPE": END - 5 * H}
    assert w.run() == 0 and w.advanced == [END]


def test_run_coin_listed_in_a_later_slice_advances(monkeypatch):
    w = World(monkeypatch, cursor=END - 46 * H)  # two slices
    w.listed_from = {"kPEPE": END - 12 * H}
    assert w.run() == 0 and w.advanced == [END]


def test_run_unexplained_empty_answer_is_incomplete(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    w.listed_from = {"kPEPE": END}  # REST answers nothing
    assert w.run() == 1 and w.advanced == []


def test_run_empty_universe_fails(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H, pairs=[])
    assert w.run() == 1 and w.calls == [] and w.advanced == []


def test_run_meta_name_missing_is_a_coin_failure(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H,
              pairs=PAIRS + [instrument(symbol="NEW/USDC:USDC", asset_id="NEW", native_symbol="", market_type="perp")])
    assert w.run() == 1 and w.advanced == []


def test_run_429_retries_counts_and_exits_1(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    w.rate_limit_next = 1
    assert w.run() == 1
    assert w.advanced == [END]  # complete, so the cursor moves; the 429 still flags the run
    assert hl.BACKOFF_429_S in w.clock.sleeps


def test_run_refuses_a_slice_that_aged_past_rest_through_pacing(monkeypatch):
    # A 72 h catch-up whose budget a 429 halved: by the time the oldest slice reaches its later coins,
    # that slice starts more than 82 h back and is refused rather than certified.
    many = [instrument(symbol=f"C{i}/USDC:USDC", asset_id=f"C{i}", native_symbol="", market_type="perp")
            for i in range(161)]
    w = World(monkeypatch, cursor=END - 70 * H, pairs=many)
    w.rate_limit_next = 1
    deps = w.deps()
    deps.fetch_meta = lambda: {f"C{i}": f"C{i}" for i in range(161)}
    assert hl.run(NOW, None, deps) == 1
    oldest = [c for c in w.calls if c[1] == END - 72 * H]
    assert len(oldest) < 161 and w.advanced == []


def test_run_lost_span_alerts_and_rebases_when_delivered(monkeypatch):
    w = World(monkeypatch, cursor=END - 100 * H)
    assert w.run() == 1
    assert len(w.alerts) == 1 and "past REST's reach" in w.alerts[0]
    assert w.advanced == [END]


def test_run_lost_span_keeps_the_cursor_when_the_alert_fails(monkeypatch):
    w = World(monkeypatch, cursor=END - 100 * H)
    w.deliver = False
    assert w.run() == 1
    assert w.advanced == []


@pytest.mark.parametrize("cursor_h, hours, advances", [
    (1, 3, False),    # required range is 26 h
    (48, 1, False),
    (48, 72, True),
    (100, 72, True),  # lost span: alerted, then rebased
])
def test_manual_runs_follow_the_same_completion_rule(monkeypatch, cursor_h, hours, advances):
    w = World(monkeypatch, cursor=END - cursor_h * H)
    w.run(hours=hours)
    assert (w.advanced == [END]) is advances


def test_run_lock_held_does_nothing(monkeypatch):
    w = World(monkeypatch, cursor=END - 20 * H)
    w.lock_free = False
    assert w.run() == 0
    assert w.calls == [] and w.bootstrapped == [] and w.advanced == []


def test_reconnect_without_the_lock_aborts(monkeypatch):
    class OperationalError(Exception):
        pass

    monkeypatch.setattr(hl.psycopg, "OperationalError", OperationalError)
    w = World(monkeypatch, cursor=END - 20 * H)

    class DeadConn:
        closed = True

    def stored(conn, pair, s, e):
        raise OperationalError("server closed the connection")

    monkeypatch.setattr(hl, "stored_minutes", stored)
    locks = iter([True, False])
    monkeypatch.setattr(hl, "take_lock", lambda conn: next(locks))
    deps = w.deps()
    deps.connect = DeadConn
    with pytest.raises(hl.LockLost):
        hl.run(NOW, None, deps)
    assert len(w.calls) == 1 and w.writes == [] and w.advanced == []


def test_upsert_sql_pins_the_change_guard_and_insert_flag():
    sql = " ".join(hl.UPSERT_REPAIR_SQL.split())
    assert "IS DISTINCT FROM" in sql and "RETURNING (xmax = 0) AS inserted" in sql
    assert "quote_volume = " not in sql  # the WS never sets it; the repair leaves it alone


def test_meta_bridge_maps_asset_ids_to_hl_names(monkeypatch):
    monkeypatch.setattr(hl, "_post", lambda payload: {"universe": [{"name": "BTC"}, {"name": "kPEPE"}]})
    assert hl.fetch_meta_names() == {"BTC": "BTC", "KPEPE": "kPEPE"}


def test_weight_rounds_up_per_60_minutes():
    assert hl.snapshot_weight(0, 1560 * M) == 20 + math.ceil(1560 / 60)
