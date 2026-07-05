"""Tests for collector/hl_candles.py — the HL WS 1m candle spec (chunk 2) + gap-backfill (chunk 3)."""

import json
from datetime import datetime, timezone

import hl_candles
from config import CANDLE_WS
from hl_candles import (
    _NO_REPAIR,
    BACKFILL_MAX_FETCHES,
    BACKFILL_TRAILING_CYCLE,
    INSERT_CANDLES_1M_SQL,
    _check_sub_ceiling,
    _gap_since_ms,
    _parse_hl_candle,
    _repair_candidates,
    _run_backfill_sweep,
    _trailing_share,
    build_hl_candle_spec,
)


def _dt(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)

# Two HL pairs: asset_id is the coin name (= WS `s`); native_symbol is the numeric asset index.
_PAIRS = [
    {"symbol": "BTC/USDC:USDC", "asset_id": "BTC", "native_symbol": "0", "market_type": "perp"},
    {"symbol": "AAVE/USDC:USDC", "asset_id": "AAVE", "native_symbol": "28", "market_type": "perp"},
]


def _candle_frame(coin="BTC", t=1_720_000_000_000, o="100", h="110", lo="90", c="105", v="12.5", n=42):
    return json.dumps({"channel": "candle",
                       "data": {"t": t, "T": t + 60_000, "s": coin, "i": "1m",
                                "o": o, "h": h, "l": lo, "c": c, "v": v, "n": n}})


# --- topics + framing ------------------------------------------------------

def test_topics_for_pairs_returns_coin_ids_and_rebuilds_map():
    spec = build_hl_candle_spec()
    coins = spec.topics_for_pairs(_PAIRS)
    assert coins == ["BTC", "AAVE"]  # coin ids (asset_id), not full subscribe messages
    # The map is now populated so parse_message can resolve — proven via a parse below.
    rows = spec.parse_message(_candle_frame("AAVE"))
    assert rows[0]["symbol"] == "AAVE/USDC:USDC"
    assert rows[0]["native_symbol"] == "28"


def test_subscribe_frames_wrap_each_coin_id():
    spec = build_hl_candle_spec()
    frames = spec.subscribe_frames(["BTC", "AAVE"])
    assert len(frames) == 2
    msg = json.loads(frames[0])
    assert msg == {"method": "subscribe",
                   "subscription": {"type": "candle", "coin": "BTC", "interval": "1m"}}


def test_unsubscribe_frames_use_unsubscribe_method():
    spec = build_hl_candle_spec()
    msg = json.loads(spec.unsubscribe_frames(["BTC"])[0])
    assert msg["method"] == "unsubscribe"
    assert msg["subscription"]["coin"] == "BTC"


# --- parse -----------------------------------------------------------------

def test_parse_candle_frame_to_row():
    asset_map = {"BTC": {"symbol": "BTC/USDC:USDC", "native_symbol": "0", "market_type": "perp"}}
    rows = _parse_hl_candle(_candle_frame("BTC"), asset_map)
    assert len(rows) == 1
    row = rows[0]
    assert row["exchange"] == "hyperliquid"
    assert row["asset_id"] == "BTC"           # s → asset_id
    assert row["symbol"] == "BTC/USDC:USDC"
    assert row["native_symbol"] == "0"
    assert row["timeframe"] == "1m"
    assert row["timestamp"] == 1_720_000_000_000 / 1000.0  # t (open ms) → epoch seconds
    assert (row["open"], row["high"], row["low"], row["close"]) == (100.0, 110.0, 90.0, 105.0)
    assert row["volume"] == 12.5
    assert row["quote_volume"] is None
    assert row["trades"] == 42


def test_parse_unknown_coin_dropped():
    # No map entry for the coin → unplaceable identity → drop (not stored with a guessed id).
    assert _parse_hl_candle(_candle_frame("DOGE"), {"BTC": {"symbol": "x", "native_symbol": "0", "market_type": "perp"}}) == []


def test_parse_bad_open_time_dropped():
    asset_map = {"BTC": {"symbol": "BTC/USDC:USDC", "native_symbol": "0", "market_type": "perp"}}
    frame = json.dumps({"channel": "candle", "data": {"t": "not-a-number", "s": "BTC", "o": "1"}})
    assert _parse_hl_candle(frame, asset_map) == []


def test_parse_null_magnitude_still_lands():
    # Bad OHLCV → NULL column, but the row (identity + time) still lands.
    asset_map = {"BTC": {"symbol": "BTC/USDC:USDC", "native_symbol": "0", "market_type": "perp"}}
    frame = json.dumps({"channel": "candle", "data": {"t": 1_720_000_000_000, "s": "BTC",
                                                      "o": None, "h": None, "l": None, "c": None, "v": None, "n": None}})
    rows = _parse_hl_candle(frame, asset_map)
    assert len(rows) == 1
    assert rows[0]["open"] is None and rows[0]["trades"] is None


def test_control_frames_yield_no_rows():
    asset_map = {"BTC": {"symbol": "x", "native_symbol": "0", "market_type": "perp"}}
    for control in (
        json.dumps({"channel": "subscriptionResponse", "data": {"method": "subscribe"}}),
        json.dumps({"channel": "pong"}),
        json.dumps({"channel": "trades", "data": []}),   # a non-candle channel
        json.dumps({"channel": "candle", "data": []}),   # candle channel but data not a dict
    ):
        assert _parse_hl_candle(control, asset_map) == []


# --- upsert SQL + spec fields ----------------------------------------------

def test_insert_sql_is_upsert_on_the_candles_1m_pk():
    sql = " ".join(INSERT_CANDLES_1M_SQL.split())
    assert "INSERT INTO candles_1m" in sql
    # Conflict target must be the candles_1m PK, and it must DO UPDATE (converge the forming bar),
    # NOT DO NOTHING (which would freeze the first partial bar).
    assert "ON CONFLICT (exchange, market_type, symbol, timeframe, timestamp) DO UPDATE SET" in sql
    assert "DO NOTHING" not in sql
    for col in ("open", "high", "low", "close", "volume", "quote_volume", "trades"):
        assert f"{col} = EXCLUDED.{col}" in sql


def test_spec_sets_ping_frame_and_interval():
    # ws.py only sends app pings when the INTERVAL is set — frame alone is dead config.
    spec = build_hl_candle_spec()
    assert spec.loop_tf == CANDLE_WS
    assert json.loads(spec.ping_frame) == {"method": "ping"}
    assert spec.ping_interval_s == 30.0


def test_ceiling_guard_warns_near_cap(caplog):
    assert _check_sub_ceiling(["BTC"] * 161) == 161      # far under → no warning
    with caplog.at_level("WARNING"):
        _check_sub_ceiling(["c"] * 950)
    assert any("approaching the per-IP ceiling" in r.message for r in caplog.records)


# ===========================================================================
# Gap-backfill (chunk 3)
# ===========================================================================

_NOW_MS = 1_720_000_000_000  # a fixed "now" for the since-rule tests
_MIN = 60_000


def test_gap_since_returns_gap_after_boundary_bar():
    # A mid-window gap → since = the bar BEFORE the gap (gap_after), so the fetch overwrites the stale
    # boundary bar AND fills the hole — not gap_after + 1.
    gap_after_ms = _NOW_MS - 30 * _MIN
    row = {"last_ts": _dt(_NOW_MS - _MIN), "earliest_gap_after": _dt(gap_after_ms)}
    assert _gap_since_ms(row, _NOW_MS) == gap_after_ms


def test_gap_since_trailing_catch_up():
    # No gap but behind the tape by >1.5 bars → since = last_ts + 1 bar.
    last_ms = _NOW_MS - 10 * _MIN
    row = {"last_ts": _dt(last_ms), "earliest_gap_after": None}
    assert _gap_since_ms(row, _NOW_MS) == last_ms + _MIN


def test_gap_since_no_repair_when_current():
    # Up to date (last bar just closed, no gap) → no gap-repair candidate.
    row = {"last_ts": _dt(_NOW_MS - _MIN), "earliest_gap_after": None}
    assert _gap_since_ms(row, _NOW_MS) is _NO_REPAIR


def test_repair_candidates_selects_gaps_and_zero_row_coins():
    universe = [
        {"symbol": "A", "asset_id": "A"},   # has a gap
        {"symbol": "B", "asset_id": "B"},   # up to date → excluded
        {"symbol": "C", "asset_id": "C"},   # absent from scan → zero-row → seed
    ]
    scan = {
        "A": {"last_ts": _dt(_NOW_MS - _MIN), "earliest_gap_after": _dt(_NOW_MS - 20 * _MIN)},
        "B": {"last_ts": _dt(_NOW_MS - _MIN), "earliest_gap_after": None},
    }
    cands = dict((p["symbol"], since) for p, since in _repair_candidates(universe, scan, _NOW_MS))
    assert set(cands) == {"A", "C"}                 # B (up to date) excluded
    assert cands["A"] == _NOW_MS - 20 * _MIN        # gap_after boundary
    assert cands["C"] is None                       # zero-row → seed (since=None)


def test_trailing_share_covers_whole_universe_over_cycle():
    universe = [{"symbol": str(i), "asset_id": str(i)} for i in range(25)]
    covered = []
    for sweep in range(BACKFILL_TRAILING_CYCLE):
        covered.extend(p["symbol"] for p in _trailing_share(universe, sweep))
    # Every coin covered exactly once across a full rotation — no gaps, no double-repair.
    assert sorted(covered, key=int) == [p["symbol"] for p in universe]
    assert len(covered) == len(set(covered)) == 25


class _DummyConn:
    closed = False
    def commit(self): pass
    def rollback(self): pass


def _wire_sweep(monkeypatch, universe, scan, bars_per_fetch):
    """Stub the sweep's DB + fetch touchpoints; return a list capturing each fetch's since_ms."""
    import main
    seen = []
    monkeypatch.setattr(hl_candles, "query_active_pairs", lambda conn, name, tf: universe)
    monkeypatch.setattr(hl_candles, "_scan_candles_1m", lambda conn: scan)
    monkeypatch.setattr(hl_candles, "_insert_candles_1m", lambda conn, rows: len(rows))
    monkeypatch.setattr(hl_candles, "_heartbeat", lambda *a: None)

    def fake_fetch(exchange, symbol, mi, name, tf, interval_s, rl, since_ms=None, limit=1000):
        seen.append(since_ms)
        return [{"x": 1}] * bars_per_fetch
    monkeypatch.setattr(main, "fetch_candles", fake_fetch)
    return seen


def _cfg():
    from config import load_config
    return load_config("hyperliquid")


def test_sweep_fetch_cap_bounds_volume(monkeypatch):
    # 100 zero-row coins → 100 gap candidates; 100 bars/fetch keeps the BAR cap slack so the FETCH
    # cap (40) is the binding bound.
    universe = [{"symbol": str(i), "asset_id": str(i), "native_symbol": "0", "market_type": "perp"} for i in range(100)]
    seen = _wire_sweep(monkeypatch, universe, {}, bars_per_fetch=100)
    repaired, remaining = _run_backfill_sweep(object(), _cfg(), _DummyConn(), None, 0, "t")
    assert len(seen) == BACKFILL_MAX_FETCHES        # capped at 40 fetches
    assert remaining == 100 - BACKFILL_MAX_FETCHES  # the rest deferred to later sweeps


def test_sweep_zero_gaps_only_fetches_trailing_share(monkeypatch):
    # All 100 coins up to date → no gap candidates; only the rotating 1/CYCLE trailing share is fetched
    # (steady-state REST is just that tiny share, not the whole universe).
    universe = [{"symbol": str(i), "asset_id": str(i), "native_symbol": "0", "market_type": "perp"} for i in range(100)]
    scan = {str(i): {"last_ts": _dt(_NOW_MS - _MIN), "earliest_gap_after": None} for i in range(100)}
    # freeze now so the scan rows read as current
    monkeypatch.setattr(hl_candles.time, "time", lambda: _NOW_MS / 1000.0)
    seen = _wire_sweep(monkeypatch, universe, scan, bars_per_fetch=3)
    _run_backfill_sweep(object(), _cfg(), _DummyConn(), None, 0, "t")
    assert len(seen) == 100 // BACKFILL_TRAILING_CYCLE   # 10 trailing coins, no gap fetches
