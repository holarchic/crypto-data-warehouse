"""Tests for collector/oi.py — HL 1-min cadence + asset_ctxs bundle dual-write.

The repo has NO real-DB test harness (conftest stubs psycopg/db/status/backoff with
MagicMock). So DB-engine behaviour (real ON-CONFLICT dedup, real rollback) is verified
in prod; here we test cc's own logic: parsing, the sleep boundary, and the loop's
insert/commit/rollback discipline via a fake conn/cursor that records calls.
"""

import sys

# conftest stubs `oi` to a MagicMock; remove the stub so we test the real module.
sys.modules.pop("oi", None)

import oi  # noqa: E402, I001  (must follow stub removal)


# --- Slice A: pure unit -------------------------------------------------------


def test_to_float_numeric_and_string():
    assert oi._to_float("1.5") == 1.5
    assert oi._to_float(2.0) == 2.0
    assert oi._to_float("0") == 0.0  # zero is valid — NOT coalesced to None
    assert oi._to_float(0) == 0.0


def test_to_float_absent_is_none():
    # Legitimately absent → None (stored as SQL NULL); HL omits fields on illiquid assets.
    assert oi._to_float(None) is None
    assert oi._to_float("") is None


def test_to_float_malformed_raises():
    # Present-but-garbage → _CtxParseError (caller drops the asset), NOT a silent None.
    import pytest
    with pytest.raises(oi._CtxParseError):
        oi._to_float("abc", "openInterest")
    with pytest.raises(oi._CtxParseError):
        oi._to_float(float("nan"), "markPx")
    with pytest.raises(oi._CtxParseError):
        oi._to_float("NaN", "funding")
    with pytest.raises(oi._CtxParseError):
        oi._to_float(True, "openInterest")  # bool is not a real HL numeric
    with pytest.raises(oi._CtxParseError):
        oi._to_float(float("inf"), "markPx")  # non-finite


def test_parse_hl_ctx_impact_pxs_non_list_raises():
    import pytest
    with pytest.raises(oi._CtxParseError, match="impactPxs"):
        oi._parse_hl_ctx({"openInterest": "1.0", "impactPxs": {"bid": 1}})


def _full_ctx():
    return {
        "funding": "0.0000125",
        "openInterest": "12345.6",
        "prevDayPx": "60000.0",
        "dayNtlVlm": "1000000.0",
        "premium": "0.0001",
        "oraclePx": "61000.0",
        "markPx": "61010.0",
        "midPx": "61005.0",
        "impactPxs": ["61000.0", "61010.0"],
    }


def test_parse_hl_ctx_full_bundle():
    b = oi._parse_hl_ctx(_full_ctx())
    assert b["funding"] == 0.0000125
    assert b["open_interest"] == 12345.6
    assert b["prev_day_px"] == 60000.0
    assert b["day_ntl_vlm"] == 1000000.0
    assert b["premium"] == 0.0001
    assert b["oracle_px"] == 61000.0
    assert b["mark_px"] == 61010.0
    assert b["mid_px"] == 61005.0
    assert b["impact_bid_px"] == 61000.0
    assert b["impact_ask_px"] == 61010.0
    # exactly the 10 seed float columns, no extras
    assert set(b) == {
        "funding", "open_interest", "prev_day_px", "day_ntl_vlm", "premium",
        "oracle_px", "mark_px", "mid_px", "impact_bid_px", "impact_ask_px",
    }


def test_parse_hl_ctx_missing_fields_none():
    ctx = {"openInterest": "5.0", "markPx": "10.0"}  # premium/midPx/funding/... absent
    b = oi._parse_hl_ctx(ctx)
    assert b["open_interest"] == 5.0
    assert b["mark_px"] == 10.0
    assert b["premium"] is None
    assert b["mid_px"] is None
    assert b["funding"] is None


def test_parse_hl_ctx_impact_pxs_variants():
    # null (HL sends null on illiquid assets, not just absent) → both None
    assert oi._parse_hl_ctx({"impactPxs": None})["impact_bid_px"] is None
    assert oi._parse_hl_ctx({"impactPxs": None})["impact_ask_px"] is None
    # short (one element) → bid set, ask None
    short = oi._parse_hl_ctx({"impactPxs": ["7.0"]})
    assert short["impact_bid_px"] == 7.0
    assert short["impact_ask_px"] is None
    # absent key → both None
    assert oi._parse_hl_ctx({})["impact_bid_px"] is None


def test_sleep_until_next_computes_boundary(monkeypatch):
    slept = {}
    monkeypatch.setattr(oi.time, "time", lambda: 1000.0)
    monkeypatch.setattr(oi.time, "sleep", lambda s: slept.setdefault("s", s))
    oi._sleep_until_next(60, 5)
    # next 60s boundary after 1000 is 1020, +5 buffer = 1025 → sleep 25s
    assert slept["s"] == 25.0


def test_sleep_until_next_clamps_buffer_to_half_interval(monkeypatch):
    # 30s interval + 30s buffer would target the NEXT boundary (doubling the period);
    # the clamp caps buffer at interval//2=15 → ~30s cadence, not ~60s.
    slept = {}
    monkeypatch.setattr(oi.time, "time", lambda: 1000.0)
    monkeypatch.setattr(oi.time, "sleep", lambda s: slept.setdefault("s", s))
    oi._sleep_until_next(30, 30)
    # next 30s boundary after 1000 is 1020, +min(30,15)=15 → 1035 → sleep 35s (< a full 60s)
    assert slept["s"] == 35.0


# --- Slice B: insert/commit/rollback discipline via a fake conn ---------------


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        self._conn.executed.append((sql, params))
        if self._conn.raise_on and self._conn.raise_on in sql:
            raise RuntimeError("injected insert failure")

    @property
    def rowcount(self):
        return 1


class FakeConn:
    def __init__(self, raise_on=None):
        self.closed = False
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.raise_on = raise_on

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_insert_hl_snapshot_one_commit():
    oi_rows = [{"exchange": "hyperliquid", "asset_id": "BTC", "symbol": "BTC/USDC:USDC",
                "timestamp": 1.0, "open_interest": 2.0, "oi_value_usd": 20.0}]
    ctx_rows = [{"asset_id": "BTC", "symbol": "BTC/USDC:USDC", "timestamp": 1.0,
                 "funding": 0.1, "open_interest": 2.0, "prev_day_px": 3.0, "day_ntl_vlm": 4.0,
                 "premium": 5.0, "oracle_px": 6.0, "mark_px": 10.0, "mid_px": 8.0,
                 "impact_bid_px": 9.0, "impact_ask_px": 10.0}]
    conn = FakeConn()
    oi_n, ctx_n = oi._insert_hl_snapshot(conn, oi_rows, ctx_rows)
    assert (oi_n, ctx_n) == (1, 1)
    assert conn.commits == 1  # exactly one commit owns both inserts
    sqls = [sql for sql, _ in conn.executed]
    assert any("INTO open_interest" in s for s in sqls)
    assert any("INTO hl_asset_ctxs" in s and "ON CONFLICT DO NOTHING" in s for s in sqls)


def test_insert_hl_snapshot_ctx_failure_no_commit():
    # ctx insert raises → the single commit is never reached (caller rolls back).
    conn = FakeConn(raise_on="INTO hl_asset_ctxs")
    oi_rows = [{"exchange": "hyperliquid", "asset_id": "BTC", "symbol": "S",
                "timestamp": 1.0, "open_interest": 2.0, "oi_value_usd": None}]
    ctx_rows = [{"asset_id": "BTC", "symbol": "S", "timestamp": 1.0, "funding": None,
                 "open_interest": 2.0, "prev_day_px": None, "day_ntl_vlm": None,
                 "premium": None, "oracle_px": None, "mark_px": None, "mid_px": None,
                 "impact_bid_px": None, "impact_ask_px": None}]
    try:
        oi._insert_hl_snapshot(conn, oi_rows, ctx_rows)
    except RuntimeError:
        pass
    assert conn.commits == 0


def test_build_hl_snapshots_assembly_and_derivation():
    universe, ctxs = _fixture_universe_ctxs()
    asset_to_pair = {p["asset_id"]: p for p in _pairs()}
    oi_rows, ctx_rows = oi._build_hl_snapshots(universe, ctxs, asset_to_pair, "hyperliquid", 42.0)
    assert len(oi_rows) == 3 and len(ctx_rows) == 3
    oi_by = {r["asset_id"]: r for r in oi_rows}
    ctx_by = {r["asset_id"]: r for r in ctx_rows}
    # BTC both present → oi*mark; ILLIQ no markPx → None
    assert oi_by["BTC"]["oi_value_usd"] == 12345.6 * 61010.0
    assert oi_by["ILLIQ"]["oi_value_usd"] is None
    assert ctx_by["ILLIQ"]["mark_px"] is None
    # shared open_interest + timestamp across the two tables
    for aid in ("BTC", "ETH", "ILLIQ"):
        assert oi_by[aid]["open_interest"] == ctx_by[aid]["open_interest"]
        assert oi_by[aid]["timestamp"] == ctx_by[aid]["timestamp"] == 42.0


def test_build_hl_snapshots_skips_unknown_assets():
    universe = [{"name": "BTC"}, {"name": "UNLISTED"}]
    ctxs = [_full_ctx(), _full_ctx()]
    asset_to_pair = {"BTC": {"asset_id": "BTC", "symbol": "BTC/USDC:USDC"}}
    oi_rows, ctx_rows = oi._build_hl_snapshots(universe, ctxs, asset_to_pair, "hyperliquid", 1.0)
    assert [r["asset_id"] for r in oi_rows] == ["BTC"]
    assert len(ctx_rows) == 1


def test_build_hl_snapshots_captures_1000x_token_via_upper_bridge():
    # Regression: HL names a 1000× token `kPEPE` (leading-k lowercased); the active-pair map is keyed on
    # cc's ccxt-canonical `KPEPE`. The `.upper()` bridge must match it (else it's silently dropped from
    # both tables — the pre-fix bug). Storage diverges by design: open_interest = canonical `KPEPE`
    # (cross-venue), hl_asset_ctxs = HL-native `kPEPE` (= backtester seed coin, T-011 passthrough).
    universe = [{"name": "kPEPE"}]
    ctxs = [_full_ctx()]
    asset_to_pair = {"KPEPE": {"asset_id": "KPEPE", "symbol": "KPEPE/USDC:USDC"}}
    oi_rows, ctx_rows = oi._build_hl_snapshots(universe, ctxs, asset_to_pair, "hyperliquid", 1.0)
    assert len(oi_rows) == 1 and len(ctx_rows) == 1
    assert oi_rows[0]["asset_id"] == "KPEPE"    # ccxt-canonical (cross-venue open_interest)
    assert ctx_rows[0]["asset_id"] == "kPEPE"   # HL-native = seed coin (hl_asset_ctxs)
    assert ctx_rows[0]["symbol"] == "KPEPE/USDC:USDC"


def test_build_hl_snapshots_null_name_dropped_not_aborted():
    # A present-but-null `name` must drop that one asset (parse-or-drop), NOT raise AttributeError from
    # `.upper()` and abort the whole cycle. A valid coin in the same response still lands.
    universe = [{"name": None}, {"name": "BTC"}]
    ctxs = [_full_ctx(), _full_ctx()]
    asset_to_pair = {"BTC": {"asset_id": "BTC", "symbol": "BTC/USDC:USDC"}}
    oi_rows, ctx_rows = oi._build_hl_snapshots(universe, ctxs, asset_to_pair, "hyperliquid", 1.0)
    assert [r["asset_id"] for r in ctx_rows] == ["BTC"]   # null-name asset dropped, BTC survives


def test_build_hl_snapshots_length_mismatch_raises():
    import pytest
    with pytest.raises(ValueError, match="length mismatch"):
        oi._build_hl_snapshots([{"name": "BTC"}, {"name": "ETH"}], [_full_ctx()], {}, "hyperliquid", 1.0)


def test_build_hl_snapshots_drops_malformed_asset():
    # ETH has a garbage openInterest → dropped; BTC (well-formed) still lands.
    universe = [{"name": "BTC"}, {"name": "ETH"}]
    bad = _full_ctx()
    bad["openInterest"] = "not-a-number"
    ctxs = [_full_ctx(), bad]
    asset_to_pair = {
        "BTC": {"asset_id": "BTC", "symbol": "BTC/USDC:USDC"},
        "ETH": {"asset_id": "ETH", "symbol": "ETH/USDC:USDC"},
    }
    oi_rows, ctx_rows = oi._build_hl_snapshots(universe, ctxs, asset_to_pair, "hyperliquid", 1.0)
    assert [r["asset_id"] for r in oi_rows] == ["BTC"]  # ETH dropped, not stored as NULL
    assert [r["asset_id"] for r in ctx_rows] == ["BTC"]


def test_build_hl_snapshots_non_dict_row_dropped_not_aborted():
    # A non-dict ctx (row-container drift) drops that asset per-asset — it does NOT
    # escape as a raw exception that would abort the whole cycle. BTC still lands.
    universe = [{"name": "BTC"}, {"name": "ETH"}]
    ctxs = [_full_ctx(), ["not", "a", "dict"]]
    asset_to_pair = {
        "BTC": {"asset_id": "BTC", "symbol": "BTC/USDC:USDC"},
        "ETH": {"asset_id": "ETH", "symbol": "ETH/USDC:USDC"},
    }
    oi_rows, ctx_rows = oi._build_hl_snapshots(universe, ctxs, asset_to_pair, "hyperliquid", 1.0)
    assert [r["asset_id"] for r in oi_rows] == ["BTC"]
    assert [r["asset_id"] for r in ctx_rows] == ["BTC"]


def _fixture_universe_ctxs():
    """One well-formed cycle: BTC full, ETH missing premium/mid, ILLIQ null impact + no markPx."""
    universe = [{"name": "BTC"}, {"name": "ETH"}, {"name": "ILLIQ"}]
    ctxs = [
        _full_ctx(),
        {"funding": "0.0", "openInterest": "500.0", "prevDayPx": "3000.0",
         "dayNtlVlm": "9.0", "oraclePx": "3001.0", "markPx": "3002.0",
         "impactPxs": ["3000.0", "3002.0"]},  # no premium, no midPx
        {"openInterest": "5.0", "impactPxs": None},  # no markPx → oi_value_usd None
    ]
    return universe, ctxs


def _pairs():
    return [
        {"asset_id": "BTC", "symbol": "BTC/USDC:USDC"},
        {"asset_id": "ETH", "symbol": "ETH/USDC:USDC"},
        {"asset_id": "ILLIQ", "symbol": "ILLIQ/USDC:USDC"},
    ]


class _StopLoop(Exception):
    pass


def _drive_one_cycle(monkeypatch, conn, universe, ctxs, pairs):
    """Run exactly one iteration of _oi_loop_hyperliquid against a fake conn."""
    from types import SimpleNamespace
    monkeypatch.setattr(oi, "connect_db", lambda dsn: conn)
    monkeypatch.setattr(oi, "_get_active_pairs", lambda c, name: pairs)
    monkeypatch.setattr(oi, "_fetch_hl_meta_and_ctxs", lambda: (universe, ctxs))
    # `db` is a conftest MagicMock, so `oi.safe_rollback` is a no-op unless another test file happens to
    # un-stub it first (cross-file ordering) — pin it to the fake conn's rollback so the rollback test is
    # order-independent (was: passed only in the full suite, failed running test_oi.py alone).
    monkeypatch.setattr(oi, "safe_rollback", lambda c: c.rollback())
    hb = {"n": 0}
    monkeypatch.setattr(oi, "_heartbeat", lambda *a: hb.__setitem__("n", hb["n"] + 1))

    def stop(*a):
        raise _StopLoop

    monkeypatch.setattr(oi, "_sleep_until_next", stop)
    monkeypatch.setattr(oi.time, "time", lambda: 1_700_000_000.0)
    cfg = SimpleNamespace(name="hyperliquid", market_type="perp", ccxt_class="hyperliquid",
                          oi_poll_interval_s=60)
    try:
        oi._oi_loop_hyperliquid(cfg, "dsn")
    except _StopLoop:
        pass
    return hb


def _rows_for(conn, sql_marker):
    return [params for sql, params in conn.executed if sql_marker in sql]


def test_dual_write_happy_path(monkeypatch):
    universe, ctxs = _fixture_universe_ctxs()
    conn = FakeConn()
    _drive_one_cycle(monkeypatch, conn, universe, ctxs, _pairs())

    oi_rows = _rows_for(conn, "INTO open_interest")
    ctx_rows = _rows_for(conn, "INTO hl_asset_ctxs")
    assert len(oi_rows) == 3
    assert len(ctx_rows) == 3
    # exactly ONE dual-write commit (heartbeat is patched out), no rollback
    assert conn.commits == 1
    assert conn.rollbacks == 0

    # shared open_interest + mark_px between the two tables, per asset
    oi_by_asset = {r["asset_id"]: r for r in oi_rows}
    ctx_by_asset = {r["asset_id"]: r for r in ctx_rows}
    for aid in ("BTC", "ETH", "ILLIQ"):
        assert oi_by_asset[aid]["open_interest"] == ctx_by_asset[aid]["open_interest"]
        assert oi_by_asset[aid]["timestamp"] == ctx_by_asset[aid]["timestamp"]

    # oi_value_usd derivation: BTC both present → oi*mark; ILLIQ no markPx → None
    assert oi_by_asset["BTC"]["oi_value_usd"] == 12345.6 * 61010.0
    assert oi_by_asset["ILLIQ"]["oi_value_usd"] is None
    assert ctx_by_asset["ILLIQ"]["mark_px"] is None


def test_dual_write_atomic_rollback(monkeypatch):
    # Fail on the ctx insert → the dual-write commit is never reached, rollback fires,
    # so NEITHER table keeps the cycle (real atomicity is Postgres'; this proves cc
    # issues one commit / rolls back on failure).
    universe, ctxs = _fixture_universe_ctxs()
    conn = FakeConn(raise_on="INTO hl_asset_ctxs")
    _drive_one_cycle(monkeypatch, conn, universe, ctxs, _pairs())
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_zip_mismatch_aborts_cycle(monkeypatch):
    # universe longer than ctxs → abort: no inserts, no commit (heartbeat still runs).
    universe = [{"name": "BTC"}, {"name": "ETH"}, {"name": "EXTRA"}]
    ctxs = [_full_ctx(), _full_ctx()]  # length 2 != 3
    conn = FakeConn()
    hb = _drive_one_cycle(monkeypatch, conn, universe, ctxs, _pairs())
    assert conn.executed == []
    assert conn.commits == 0
    assert hb["n"] == 1  # heartbeat (liveness) still fired


# --- Slice D: Bybit/OKX bulk parse (T-007.2) ---------------------------------


def _bulk_active():
    return {
        "BTC/USDT:USDT": {"asset_id": "BTC", "symbol": "BTC/USDT:USDT"},
        "ETH/USDT:USDT": {"asset_id": "ETH", "symbol": "ETH/USDT:USDT"},
    }


def test_parse_bulk_oi_bybit_reads_info_and_skips_inactive():
    # OI is only in ticker["info"], not the unified ticker. USD is native.
    tickers = {
        "BTC/USDT:USDT": {"info": {"openInterest": "1000.5", "openInterestValue": "60000000"}},
        "ETH/USDT:USDT": {"info": {"openInterest": "50.0", "openInterestValue": "150000"}},
        "DOGE/USDT:USDT": {"info": {"openInterest": "9", "openInterestValue": "9"}},  # inactive
    }
    rows, dropped = oi._parse_bulk_oi_bybit(tickers, _bulk_active(), "bybit", 42.0)
    assert dropped == []
    by = {r["asset_id"]: r for r in rows}
    assert set(by) == {"BTC", "ETH"}  # DOGE not active → skipped
    assert by["BTC"]["open_interest"] == 1000.5
    assert by["BTC"]["oi_value_usd"] == 60000000.0
    assert by["BTC"]["timestamp"] == 42.0


def test_parse_bulk_oi_bybit_absent_usd_is_none_malformed_dropped():
    tickers = {
        "BTC/USDT:USDT": {"info": {"openInterest": "5.0"}},              # no USD → None
        "ETH/USDT:USDT": {"info": {"openInterest": "not-a-number"}},     # malformed → drop
    }
    rows, dropped = oi._parse_bulk_oi_bybit(tickers, _bulk_active(), "bybit", 1.0)
    assert [r["asset_id"] for r in rows] == ["BTC"]
    assert rows[0]["oi_value_usd"] is None
    assert len(dropped) == 1 and "ETH" in dropped[0]


def test_parse_bulk_oi_okx_usd_from_price_map():
    # OKX bulk has no oiUsd → compute from price map (amount × last).
    ois = {
        "BTC/USDT:USDT": {"openInterestAmount": 100.0, "openInterestValue": None},
        "ETH/USDT:USDT": {"openInterestAmount": 20.0, "openInterestValue": None},
    }
    price_map = {"BTC/USDT:USDT": 60000.0}  # ETH price missing → USD None
    rows = oi._parse_bulk_oi_okx(ois, price_map, _bulk_active(), "okx", 7.0)
    by = {r["asset_id"]: r for r in rows}
    assert by["BTC"]["oi_value_usd"] == 100.0 * 60000.0
    assert by["ETH"]["oi_value_usd"] is None
    assert by["ETH"]["open_interest"] == 20.0


def test_parse_bulk_oi_okx_native_value_preferred():
    # If ccxt ever populates openInterestValue, use it (don't recompute).
    ois = {"BTC/USDT:USDT": {"openInterestAmount": 100.0, "openInterestValue": 123.0}}
    rows = oi._parse_bulk_oi_okx(ois, {"BTC/USDT:USDT": 999.0}, _bulk_active(), "okx", 1.0)
    assert rows[0]["oi_value_usd"] == 123.0


# --- Slice E: Binance two-source (T-007.2) -----------------------------------

NOW = 1_700_000_000.0
WINDOW = 900


def _fr(next_funding_s, mark=None):
    """A ccxt funding-rate record: fundingTimestamp (ms), optional markPrice."""
    r = {"fundingTimestamp": int(next_funding_s * 1000)}
    if mark is not None:
        r["markPrice"] = mark
    return r


def _binance_active():
    return {
        "BTC/USDT:USDT": {"asset_id": "BTC", "symbol": "BTC/USDT:USDT"},
        "ETH/USDT:USDT": {"asset_id": "ETH", "symbol": "ETH/USDT:USDT"},
        "XRP/USDT:USDT": {"asset_id": "XRP", "symbol": "XRP/USDT:USDT"},
    }


def test_dense_set_pre_window_and_out_of_window():
    active = _binance_active()
    fr = {
        "BTC/USDT:USDT": _fr(NOW + 600),    # 10 min out → in ±15m pre-window
        "ETH/USDT:USDT": _fr(NOW + WINDOW),  # exactly 15m → boundary, included (<=)
        "XRP/USDT:USDT": _fr(NOW + 5000),   # far out → not dense
    }
    prev, sett = {}, {}
    dense = oi._binance_dense_set(fr, active, NOW, WINDOW, prev, sett)
    assert {p["asset_id"] for p in dense} == {"BTC", "ETH"}
    # state populated for all seen symbols (enables next-tick change-detection)
    assert set(prev) == set(active)


def test_dense_set_post_window_via_change_detection():
    active = {"BTC/USDT:USDT": {"asset_id": "BTC", "symbol": "BTC/USDT:USDT"}}
    # nextFundingTime far future (pre-window false), but it JUST changed vs prior tick →
    # a settlement occurred → post-window keeps it dense.
    prev = {"BTC/USDT:USDT": int((NOW - 100) * 1000)}  # old next-funding
    sett = {}
    fr = {"BTC/USDT:USDT": _fr(NOW + 28800)}  # rolled to +8h
    dense = oi._binance_dense_set(fr, active, NOW, WINDOW, prev, sett)
    assert [p["asset_id"] for p in dense] == ["BTC"]
    assert sett["BTC/USDT:USDT"] == NOW  # settlement stamped


def test_dense_set_first_tick_no_post_window():
    # Empty prev (restart): a far-future settlement is NOT dense (post can't fire yet).
    active = {"BTC/USDT:USDT": {"asset_id": "BTC", "symbol": "BTC/USDT:USDT"}}
    fr = {"BTC/USDT:USDT": _fr(NOW + 28800)}
    dense = oi._binance_dense_set(fr, active, NOW, WINDOW, {}, {})
    assert dense == []


def test_baseline_batch_size():
    assert oi._baseline_batch_size(700, 60, 1800) == 24   # ceil(700/30)
    assert oi._baseline_batch_size(5, 60, 1800) == 1       # small set → 1
    assert oi._baseline_batch_size(700, 120, 60) == 700    # tick>interval → 1 tick sweep


class _FakeExchange:
    def __init__(self, hist=None, oi_by_symbol=None, raise_hist=None, raise_oi=None):
        self._hist = hist or {}
        self._oi = oi_by_symbol or {}
        self._raise_hist = raise_hist
        self._raise_oi = raise_oi or set()
        self.last_response_headers = None

    def fetch_open_interest_history(self, symbol, timeframe, since, limit):
        if self._raise_hist and symbol in self._raise_hist:
            raise RuntimeError("hist boom")
        return self._hist.get(symbol, [])

    def fetch_open_interest(self, symbol):
        if symbol in self._raise_oi:
            raise RuntimeError("oi boom")
        return self._oi.get(symbol)


def _patch_paced_and_sleep(monkeypatch):
    """Stub `paced_call` to run `fn` directly, and `sleep` to a no-op. Records each call as
    `(fn_name, priority)` and returns that list, so wiring tests can assert the dense lane's
    `priority=True`. Mirrors the real `paced_call`: `priority` is consumed here, NOT forwarded
    to `fn` (a ccxt method would reject it) — this is also what keeps these fakes valid now that
    the dense sites pass `priority=True`."""
    calls = []

    def _spy(rl, ex, w, fn, *a, priority=False, **k):
        calls.append((getattr(fn, "__name__", str(fn)), priority))
        return fn(*a, **k)

    monkeypatch.setattr(oi, "paced_call", _spy)
    monkeypatch.setattr(oi.time, "sleep", lambda s: None)
    # conftest stubs `backoff`, so oi.RETRIABLE_EXCEPTIONS is a MagicMock (uncatchable).
    # Install a real tuple so `except RETRIABLE_EXCEPTIONS` is valid; a raised RuntimeError
    # then falls to the generic `except Exception` skip path.
    monkeypatch.setattr(oi, "RETRIABLE_EXCEPTIONS", (ConnectionError,))
    return calls


def test_baseline_sweep_advances_cursor_after_commit(monkeypatch):
    _patch_paced_and_sleep(monkeypatch)
    ex = _FakeExchange(hist={"BTC/USDT:USDT": [
        {"timestamp": 1_700_000_300_000, "openInterestAmount": 10.0, "openInterestValue": 100.0},
        {"timestamp": 1_700_000_600_000, "openInterestAmount": 11.0, "openInterestValue": 110.0},
    ]})
    conn = FakeConn()
    cursor, last_fetch = {}, {}
    due = [{"asset_id": "BTC", "symbol": "BTC/USDT:USDT"}]
    n = oi._binance_baseline_sweep(ex, "binance", None, conn, due, cursor, last_fetch, NOW, "binance/oi")
    assert n == 2
    assert cursor["BTC/USDT:USDT"] == 1_700_000_600.0  # advanced to max ts (seconds)
    assert last_fetch["BTC/USDT:USDT"] == NOW
    # native USD carried through (openInterestValue), rows hit open_interest
    oi_rows = _rows_for(conn, "INTO open_interest")
    assert oi_rows[0]["oi_value_usd"] == 100.0


def test_baseline_sweep_since_from_cursor(monkeypatch):
    _patch_paced_and_sleep(monkeypatch)
    seen = {}

    class _Ex(_FakeExchange):
        def fetch_open_interest_history(self, symbol, timeframe, since, limit):
            seen["since"] = since
            return []

    conn = FakeConn()
    cursor = {"BTC/USDT:USDT": 1_700_000_600.0}
    oi._binance_baseline_sweep(_Ex(), "binance", None, conn, [{"asset_id": "BTC", "symbol": "BTC/USDT:USDT"}],
                               cursor, {}, NOW, "binance/oi")
    assert seen["since"] == 1_700_000_600_000 + 1  # cursor(s)→ms + 1


def test_baseline_sweep_fetch_error_leaves_cursor_unadvanced(monkeypatch):
    _patch_paced_and_sleep(monkeypatch)
    ex = _FakeExchange(raise_hist={"BTC/USDT:USDT"})
    conn = FakeConn()
    cursor = {"BTC/USDT:USDT": 500.0}
    last_fetch = {}
    n = oi._binance_baseline_sweep(ex, "binance", None, conn, [{"asset_id": "BTC", "symbol": "BTC/USDT:USDT"}],
                                   cursor, last_fetch, NOW, "binance/oi")
    assert n == 0
    assert cursor["BTC/USDT:USDT"] == 500.0        # unchanged → no gap
    assert "BTC/USDT:USDT" not in last_fetch        # stays due → retried next sweep


def test_dense_fetch_sequential_usd_and_skips(monkeypatch):
    # Sequential path (oi_dense_concurrency<=1 / pre-pool fallback): USD from markPx; ETH has no
    # mark → None (not a crash).
    _patch_paced_and_sleep(monkeypatch)
    ex = _FakeExchange(oi_by_symbol={
        "BTC/USDT:USDT": {"openInterestAmount": 100.0},
        "ETH/USDT:USDT": {"openInterestAmount": 50.0},
    })
    dense_pairs = [
        {"asset_id": "BTC", "symbol": "BTC/USDT:USDT"},
        {"asset_id": "ETH", "symbol": "ETH/USDT:USDT"},
    ]
    fr = {"BTC/USDT:USDT": _fr(NOW, mark=60000.0), "ETH/USDT:USDT": _fr(NOW)}
    rows = oi._binance_dense_fetch(ex, "binance", None, dense_pairs, fr, NOW, "binance/oi")
    by = {r["asset_id"]: r for r in rows}
    assert by["BTC"]["oi_value_usd"] == 100.0 * 60000.0
    assert by["ETH"]["oi_value_usd"] is None


def test_dense_fetch_sequential_error_isolation_and_empty(monkeypatch):
    _patch_paced_and_sleep(monkeypatch)
    ex = _FakeExchange(oi_by_symbol={"A/USDT:USDT": {"openInterestAmount": 1.0}},
                       raise_oi={"B/USDT:USDT"})
    pairs = [{"asset_id": "A", "symbol": "A/USDT:USDT"}, {"asset_id": "B", "symbol": "B/USDT:USDT"}]
    rows = oi._binance_dense_fetch(ex, "binance", None, pairs, {}, NOW, "binance/oi")
    assert [r["symbol"] for r in rows] == ["A/USDT:USDT"]  # B raised → skipped, A lands
    assert oi._binance_dense_fetch(ex, "binance", None, [], {}, NOW, "binance/oi") == []


def test_dense_fetch_sequential_uses_priority_lane(monkeypatch):
    # Every dense per-symbol fetch rides the priority weight lane (T-007.4).
    calls = _patch_paced_and_sleep(monkeypatch)
    ex = _FakeExchange(oi_by_symbol={f"C{i}/USDT:USDT": {"openInterestAmount": 1.0} for i in range(3)})
    pairs = [{"asset_id": f"C{i}", "symbol": f"C{i}/USDT:USDT"} for i in range(3)]
    oi._binance_dense_fetch(ex, "binance", None, pairs, {}, NOW, "binance/oi")
    assert calls == [("fetch_open_interest", True)] * 3  # all dense calls priority=True


# --- _DenseFetchPool: isolated per-worker sessions ---------------------------


class _PoolFakeExchange:
    """A per-worker fake exchange. Records close()/reload; flags if the SAME instance is entered
    concurrently (the checkout queue must prevent that)."""

    def __init__(self, oi_by_symbol, raise_oi, shared):
        self._oi = oi_by_symbol
        self._raise = raise_oi
        self._shared = shared           # shared dict across the K exchanges for assertions
        self._in_use = False
        self.closed = False

    def fetch_open_interest(self, symbol):
        if self._in_use:               # two threads in one exchange → checkout invariant broken
            self._shared["concurrent_reuse"] = True
        self._in_use = True
        try:
            import time as _t
            _t.sleep(0.002)            # widen the window so a real violation would be caught
            if symbol in self._raise:
                raise RuntimeError("oi boom")
            return self._oi.get(symbol)
        finally:
            self._in_use = False

    def load_markets(self, reload=False):
        self._shared["reloads"] = self._shared.get("reloads", 0) + 1

    def close(self):
        self.closed = True


def _pool_factory(oi_by_symbol, raise_oi=frozenset(), shared=None, fail_after=None):
    made = []

    def factory(_config):
        if fail_after is not None and len(made) >= fail_after:
            raise RuntimeError("factory boom")
        ex = _PoolFakeExchange(oi_by_symbol, raise_oi, shared if shared is not None else {})
        made.append(ex)
        return ex

    factory.made = made
    return factory


def _mk_pool(monkeypatch, factory, concurrency=4):
    _patch_paced_and_sleep(monkeypatch)
    cfg = _cfg_binance()
    return oi._DenseFetchPool(cfg, concurrency, exchange_factory=factory)


def _cfg_binance():
    from types import SimpleNamespace
    return SimpleNamespace(name="binance")


def test_dense_pool_fetches_all_no_concurrent_reuse(monkeypatch):
    shared = {}
    syms = [f"C{i}/USDT:USDT" for i in range(30)]
    factory = _pool_factory({s: {"openInterestAmount": float(i)} for i, s in enumerate(syms)}, shared=shared)
    pool = _mk_pool(monkeypatch, factory, concurrency=4)
    try:
        pairs = [{"asset_id": f"C{i}", "symbol": s} for i, s in enumerate(syms)]
        fr = {s: _fr(NOW, mark=2.0) for s in syms}
        rows = pool.fetch(pairs, None, fr, NOW, "binance", "binance/oi")
        assert {r["symbol"] for r in rows} == set(syms)           # every symbol landed
        assert shared.get("concurrent_reuse") is not True         # no exchange used by 2 threads
        assert len(factory.made) == 4                             # K dedicated exchanges
    finally:
        pool.close()


def test_dense_pool_error_isolation_and_market_refresh(monkeypatch):
    shared = {}
    syms = [f"C{i}/USDT:USDT" for i in range(12)]
    factory = _pool_factory(
        {s: {"openInterestAmount": float(i)} for i, s in enumerate(syms)},
        raise_oi={"C3/USDT:USDT"}, shared=shared,
    )
    pool = _mk_pool(monkeypatch, factory, concurrency=3)
    try:
        pairs = [{"asset_id": f"C{i}", "symbol": s} for i, s in enumerate(syms)]
        # market refresh runs before submit — force reload by pretending markets are ancient.
        monkeypatch.setattr(oi, "maybe_reload_markets", lambda ex: ex.load_markets(reload=True))
        rows = pool.fetch(pairs, None, {s: _fr(NOW, mark=1.0) for s in syms}, NOW, "binance", "binance/oi")
        assert "C3/USDT:USDT" not in {r["symbol"] for r in rows}  # failing symbol skipped
        assert len(rows) == 11                                    # the other 11 landed
        assert shared.get("reloads") == 3                         # refreshed all 3 pooled exchanges
    finally:
        pool.close()


def test_dense_pool_uses_priority_lane(monkeypatch):
    # Every pooled dense fetch rides the priority weight lane (T-007.4). Order is immaterial
    # (as_completed), so assert the multiset: all entries priority=True, one per symbol.
    calls = _patch_paced_and_sleep(monkeypatch)
    syms = [f"C{i}/USDT:USDT" for i in range(5)]
    factory = _pool_factory({s: {"openInterestAmount": float(i)} for i, s in enumerate(syms)})
    pool = oi._DenseFetchPool(_cfg_binance(), 3, exchange_factory=factory)
    try:
        pairs = [{"asset_id": f"C{i}", "symbol": s} for i, s in enumerate(syms)]
        pool.fetch(pairs, None, {s: _fr(NOW, mark=1.0) for s in syms}, NOW, "binance", "binance/oi")
        assert len(calls) == 5 and all(c == ("fetch_open_interest", True) for c in calls)
    finally:
        pool.close()


def test_dense_pool_empty_set(monkeypatch):
    pool = _mk_pool(monkeypatch, _pool_factory({}), concurrency=2)
    try:
        assert pool.fetch([], None, {}, NOW, "binance", "binance/oi") == []
    finally:
        pool.close()


def test_dense_pool_partial_init_closes_built_exchanges(monkeypatch):
    # Factory fails on the 3rd exchange → the 2 already built are close()d, pool not published.
    import pytest
    factory = _pool_factory({}, fail_after=2)
    _patch_paced_and_sleep(monkeypatch)
    with pytest.raises(RuntimeError, match="factory boom"):
        oi._DenseFetchPool(_cfg_binance(), 6, exchange_factory=factory)
    assert len(factory.made) == 2 and all(ex.closed for ex in factory.made)


def test_oi_loop_raises_on_unknown_oi_venue():
    from types import SimpleNamespace

    import pytest
    cfg = SimpleNamespace(name="kraken", ccxt_class="kraken")
    with pytest.raises(ValueError, match="No OI strategy"):
        oi.oi_loop(object(), cfg, "dsn", None)


def test_binance_loop_skips_baseline_when_seed_fails(monkeypatch):
    # Seed failure ⇒ seeded stays False ⇒ baseline sweep must NOT run this cycle (a
    # since=None sweep would skip the restart gap); dense still runs.
    from types import SimpleNamespace
    conn = FakeConn()
    pairs = [{"asset_id": "BTC", "symbol": "BTC/USDT:USDT"}]
    monkeypatch.setattr(oi, "connect_db", lambda dsn: conn)
    monkeypatch.setattr(oi, "_get_active_pairs", lambda c, name: pairs)

    def _seed_boom(*a):
        raise RuntimeError("seed boom")

    monkeypatch.setattr(oi, "_seed_baseline_cursor", _seed_boom)
    paced_calls = []

    def _spy(rl, ex, w, fn, *a, priority=False, **k):
        paced_calls.append((getattr(fn, "__name__", str(fn)), priority))
        return fn(*a, **k)

    monkeypatch.setattr(oi, "paced_call", _spy)
    monkeypatch.setattr(oi, "_binance_dense_set", lambda *a: [])
    monkeypatch.setattr(oi, "_binance_dense_fetch", lambda *a: [])
    swept = {"n": 0}
    monkeypatch.setattr(oi, "_binance_baseline_sweep", lambda *a: swept.__setitem__("n", swept["n"] + 1) or 0)
    hb = {"n": 0}
    monkeypatch.setattr(oi, "_heartbeat", lambda *a: hb.__setitem__("n", hb["n"] + 1))

    def stop(*a):
        raise _StopLoop

    monkeypatch.setattr(oi, "_sleep_until_next", stop)

    def fetch_funding_rates():  # named (not a lambda) so the spy records the real fn name
        return {}

    ex = SimpleNamespace(fetch_funding_rates=fetch_funding_rates, last_response_headers=None)
    cfg = SimpleNamespace(name="binance", market_type="perp", ccxt_class="binanceusdm",
                          oi_poll_interval_s=60, oi_settlement_window_s=900,
                          oi_baseline_interval_s=1800, oi_dense_concurrency=1,
                          rate_limit_priority_reserve=0)  # sequential path, lane off
    try:
        oi._oi_loop_binance(ex, cfg, "dsn", None)
    except _StopLoop:
        pass
    assert swept["n"] == 0   # baseline sweep skipped (seed failed)
    assert hb["n"] == 1      # liveness heartbeat still fired
    # The premiumIndex trigger rides the priority weight lane (T-007.4).
    assert ("fetch_funding_rates", True) in paced_calls


def _drive_binance_reserve_cycle(monkeypatch, caplog, dense_pairs, reserve):
    """Drive one _oi_loop_binance cycle with a controlled dense set + reserve, capturing WARNINGs.
    Returns the caplog records. The runtime reserve-contention check counts len(dense)×weight."""
    import logging
    from types import SimpleNamespace
    conn = FakeConn()
    monkeypatch.setattr(oi, "connect_db", lambda dsn: conn)
    monkeypatch.setattr(oi, "_get_active_pairs", lambda c, name: [{"asset_id": "BTC", "symbol": "BTC/USDT:USDT"}])
    monkeypatch.setattr(oi, "_seed_baseline_cursor", lambda *a: {})
    monkeypatch.setattr(oi, "_binance_dense_set", lambda *a: list(dense_pairs))
    monkeypatch.setattr(oi, "_binance_dense_fetch", lambda *a: [])
    monkeypatch.setattr(oi, "paced_call", lambda rl, ex, w, fn, *a, priority=False, **k: fn(*a, **k))
    monkeypatch.setattr(oi, "_binance_baseline_sweep", lambda *a: 0)
    monkeypatch.setattr(oi, "_heartbeat", lambda *a: None)

    def stop(*a):
        raise _StopLoop

    monkeypatch.setattr(oi, "_sleep_until_next", stop)

    def fetch_funding_rates():
        return {}

    ex = SimpleNamespace(fetch_funding_rates=fetch_funding_rates, last_response_headers=None)
    cfg = SimpleNamespace(name="binance", market_type="perp", ccxt_class="binanceusdm",
                          oi_poll_interval_s=60, oi_settlement_window_s=900,
                          oi_baseline_interval_s=1800, oi_dense_concurrency=1,
                          rate_limit_priority_reserve=reserve)
    with caplog.at_level(logging.WARNING):
        try:
            oi._oi_loop_binance(ex, cfg, "dsn", None)
        except _StopLoop:
            pass
    return caplog.records


def test_binance_loop_warns_when_dense_weight_exceeds_reserve(monkeypatch, caplog):
    # T-007.4 runtime check: a cycle whose dense weight (len(dense)×OI-weight) exceeds the
    # reserve band logs a contention warning (the rate-based signal, not the old in-flight one).
    dense = [{"asset_id": f"A{i}", "symbol": f"A{i}/USDT:USDT"} for i in range(5)]  # weight 5
    records = _drive_binance_reserve_cycle(monkeypatch, caplog, dense, reserve=2)  # 5 > 2 → warn
    assert any("exceeds rate_limit_priority_reserve" in r.message for r in records)


def test_binance_loop_reserve_warning_is_edge_triggered_across_cycles(monkeypatch, caplog):
    # The warning is edge-triggered: it fires once on the rising edge of a breach, stays silent
    # while the breach persists, re-arms when the dense set falls back under the reserve, and
    # fires again on the next rise. Drive breach → breach → recover → breach = exactly 2 warnings.
    import logging
    from types import SimpleNamespace

    def _pairs(n):
        return [{"asset_id": f"A{i}", "symbol": f"A{i}/USDT:USDT"} for i in range(n)]

    dense_by_cycle = [_pairs(5), _pairs(5), _pairs(0), _pairs(5)]  # weights 5,5,0,5 vs reserve 2
    it = iter(dense_by_cycle)
    conn = FakeConn()
    monkeypatch.setattr(oi, "connect_db", lambda dsn: conn)
    monkeypatch.setattr(oi, "_get_active_pairs", lambda c, name: [{"asset_id": "BTC", "symbol": "BTC/USDT:USDT"}])
    monkeypatch.setattr(oi, "_seed_baseline_cursor", lambda *a: {})
    monkeypatch.setattr(oi, "_binance_dense_set", lambda *a: next(it))
    monkeypatch.setattr(oi, "_binance_dense_fetch", lambda *a: [])
    monkeypatch.setattr(oi, "paced_call", lambda rl, ex, w, fn, *a, priority=False, **k: fn(*a, **k))
    monkeypatch.setattr(oi, "_binance_baseline_sweep", lambda *a: 0)
    monkeypatch.setattr(oi, "_heartbeat", lambda *a: None)

    def stop_when_drained(*a):
        # Let the loop run until the dense-set sequence is exhausted, then break out.
        if next(_remaining, None) is None:
            raise _StopLoop

    _remaining = iter(range(len(dense_by_cycle) - 1))  # allow N cycles, stop before the (N+1)th set
    monkeypatch.setattr(oi, "_sleep_until_next", stop_when_drained)

    def fetch_funding_rates():
        return {}

    ex = SimpleNamespace(fetch_funding_rates=fetch_funding_rates, last_response_headers=None)
    cfg = SimpleNamespace(name="binance", market_type="perp", ccxt_class="binanceusdm",
                          oi_poll_interval_s=60, oi_settlement_window_s=900,
                          oi_baseline_interval_s=1800, oi_dense_concurrency=1,
                          rate_limit_priority_reserve=2)
    with caplog.at_level(logging.WARNING):
        try:
            oi._oi_loop_binance(ex, cfg, "dsn", None)
        except (_StopLoop, StopIteration):
            pass
    warns = [r for r in caplog.records if "exceeds rate_limit_priority_reserve" in r.message]
    assert len(warns) == 2  # cycle 1 (rise) + cycle 4 (re-armed rise); cycles 2/3 silent


def test_binance_loop_silent_when_dense_weight_within_reserve(monkeypatch, caplog):
    # Dense weight under the reserve band → no contention warning (avoids false alarms every cycle).
    dense = [{"asset_id": f"A{i}", "symbol": f"A{i}/USDT:USDT"} for i in range(5)]  # weight 5
    records = _drive_binance_reserve_cycle(monkeypatch, caplog, dense, reserve=600)  # 5 < 600 → silent
    assert not any("exceeds rate_limit_priority_reserve" in r.message for r in records)


def test_prune_binance_state_drops_inactive_symbols():
    prev = {"BTC/USDT:USDT": 1, "GONE/USDT:USDT": 2}
    cursor = {"BTC/USDT:USDT": 5.0, "GONE/USDT:USDT": 6.0}
    oi._prune_binance_state({"BTC/USDT:USDT"}, prev, cursor)
    assert set(prev) == {"BTC/USDT:USDT"}
    assert set(cursor) == {"BTC/USDT:USDT"}


def test_seed_baseline_cursor_sql_isolates_aligned_rows():
    # Regression guard (codex-r2 HIGH): the seed query must filter to 5-min-aligned rows
    # so dense now-rows can't poison the cursor.
    assert "% 300 = 0" in oi.SEED_BASELINE_CURSOR_SQL.replace("%%", "%")
    assert "EXTRACT(EPOCH FROM timestamp)::numeric" in oi.SEED_BASELINE_CURSOR_SQL
