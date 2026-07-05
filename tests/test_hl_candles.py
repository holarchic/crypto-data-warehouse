"""Tests for collector/hl_candles.py — the HL WS 1m candle spec (T-015.1 chunk 2)."""

import json

from config import CANDLE_WS
from hl_candles import (
    INSERT_CANDLES_1M_SQL,
    _check_sub_ceiling,
    _parse_hl_candle,
    build_hl_candle_spec,
)

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
