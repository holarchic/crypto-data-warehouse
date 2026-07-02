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


def test_to_float_missing_and_bad():
    assert oi._to_float(None) is None
    assert oi._to_float("") is None
    assert oi._to_float("abc") is None


def test_to_float_nan_is_none():
    # NaN maps to None — the SQL-NULL contract (export converts NULL→NaN).
    assert oi._to_float(float("nan")) is None
    assert oi._to_float("NaN") is None


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


def test_insert_hl_ctxs_commit_flag():
    rows = [{
        "asset_id": "BTC", "symbol": "BTC/USDC:USDC", "timestamp": 1.0,
        "funding": 0.1, "open_interest": 2.0, "prev_day_px": 3.0, "day_ntl_vlm": 4.0,
        "premium": 5.0, "oracle_px": 6.0, "mark_px": 7.0, "mid_px": 8.0,
        "impact_bid_px": 9.0, "impact_ask_px": 10.0,
    }]
    conn = FakeConn()
    assert oi._insert_hl_ctxs(conn, rows, commit=True) == 1
    assert conn.commits == 1
    assert "ON CONFLICT DO NOTHING" in conn.executed[0][0]

    conn2 = FakeConn()
    oi._insert_hl_ctxs(conn2, rows, commit=False)
    assert conn2.commits == 0  # caller commits

    assert oi._insert_hl_ctxs(FakeConn(), [], commit=True) == 0


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
    hb = {"n": 0}
    monkeypatch.setattr(oi, "_heartbeat", lambda *a: hb.__setitem__("n", hb["n"] + 1))

    def stop(*a):
        raise _StopLoop

    monkeypatch.setattr(oi, "_sleep_until_next", stop)
    monkeypatch.setattr(oi.time, "time", lambda: 1_700_000_000.0)
    cfg = SimpleNamespace(name="hyperliquid", market_type="perp", ccxt_class="hyperliquid")
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
