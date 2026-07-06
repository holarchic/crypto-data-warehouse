"""Tests for collector/hl_candles.py — the HL WS-only 1m candle spec."""

import json

import hl_candles
from config import CANDLE_WS
from hl_candles import (
    INSERT_CANDLES_1M_SQL,
    _check_sub_ceiling,
    _parse_hl_candle,
    build_hl_candle_spec,
)

# DB rows carry cc's canonical (ccxt-uppercase) asset_id; ccxt `info.name` is HL's authoritative WS
# coin name — equal for normals, lowercase-`k` for 1000× tokens (KPEPE→kPEPE).
_PAIRS = [
    {"symbol": "BTC/USDC:USDC", "asset_id": "BTC", "native_symbol": "0", "market_type": "perp"},
    {"symbol": "AAVE/USDC:USDC", "asset_id": "AAVE", "native_symbol": "28", "market_type": "perp"},
    {"symbol": "KPEPE/USDC:USDC", "asset_id": "KPEPE", "native_symbol": "15", "market_type": "perp"},
]


class _FakeExchange:
    def __init__(self, markets):
        self.markets = markets


def _mkt(symbol, base, name, idx):
    return {"symbol": symbol, "base": base, "swap": True, "id": idx, "info": {"name": name}}


_EX = _FakeExchange({
    "BTC/USDC:USDC": _mkt("BTC/USDC:USDC", "BTC", "BTC", "0"),
    "AAVE/USDC:USDC": _mkt("AAVE/USDC:USDC", "AAVE", "AAVE", "28"),
    "KPEPE/USDC:USDC": _mkt("KPEPE/USDC:USDC", "KPEPE", "kPEPE", "15"),  # HL name lowercases the k
})


def _amap(**hl_to_asset):
    """Build an asset_map keyed on HL WS name → canonical row fields (matches _rebuild_map's shape)."""
    return {hl: {"asset_id": aid, "symbol": f"{aid}/USDC:USDC", "native_symbol": "0", "market_type": "perp"}
            for hl, aid in hl_to_asset.items()}


def _candle_frame(coin="BTC", t=1_720_000_000_000, o="100", h="110", lo="90", c="105", v="12.5", n=42):
    return json.dumps({"channel": "candle",
                       "data": {"t": t, "T": t + 60_000, "s": coin, "i": "1m",
                                "o": o, "h": h, "l": lo, "c": c, "v": v, "n": n}})


# --- topics + framing + name mapping ---------------------------------------

def test_topics_resolve_hl_names_including_1000x_token():
    spec = build_hl_candle_spec(_EX)
    coins = spec.topics_for_pairs(_PAIRS)
    # KPEPE (cc asset_id) subscribes as the HL name kPEPE; normals unchanged. Subscribing KPEPE would
    # make HL drop the whole connection.
    assert sorted(coins) == ["AAVE", "BTC", "kPEPE"]
    # A kPEPE candle frame routes back through the map to the CANONICAL asset_id (KPEPE), not "kPEPE".
    rows = spec.parse_message(_candle_frame("kPEPE"))
    assert rows[0]["asset_id"] == "KPEPE"
    assert rows[0]["symbol"] == "KPEPE/USDC:USDC"


def test_hl_coin_names_only_lowercases_1000x_tokens():
    from hl_candles import hl_coin_names
    names = hl_coin_names(_EX)
    assert names["KPEPE"] == "kPEPE"   # 1000× token → HL's lowercase-k name
    assert names["BTC"] == "BTC"       # normal → unchanged
    assert names["AAVE"] == "AAVE"


def test_subscribe_frames_wrap_each_coin_id():
    spec = build_hl_candle_spec(_EX)
    frames = spec.subscribe_frames(["BTC", "AAVE"])
    assert len(frames) == 2
    msg = json.loads(frames[0])
    assert msg == {"method": "subscribe",
                   "subscription": {"type": "candle", "coin": "BTC", "interval": "1m"}}


def test_unsubscribe_frames_use_unsubscribe_method():
    spec = build_hl_candle_spec(_EX)
    msg = json.loads(spec.unsubscribe_frames(["kPEPE"])[0])
    assert msg["method"] == "unsubscribe"
    assert msg["subscription"]["coin"] == "kPEPE"


# --- parse -----------------------------------------------------------------

def test_parse_candle_frame_to_row():
    rows = _parse_hl_candle(_candle_frame("BTC"), _amap(BTC="BTC"))
    assert len(rows) == 1
    row = rows[0]
    assert row["exchange"] == "hyperliquid"
    assert row["asset_id"] == "BTC"           # s → map → canonical asset_id
    assert row["symbol"] == "BTC/USDC:USDC"
    assert row["timeframe"] == "1m"
    assert row["timestamp"] == 1_720_000_000_000 / 1000.0  # t (open ms) → epoch seconds
    assert (row["open"], row["high"], row["low"], row["close"]) == (100.0, 110.0, 90.0, 105.0)
    assert row["volume"] == 12.5
    assert row["quote_volume"] is None
    assert row["trades"] == 42


def test_parse_unknown_coin_dropped():
    # No map entry for the coin → unplaceable identity → drop (not stored with a guessed id).
    assert _parse_hl_candle(_candle_frame("DOGE"), _amap(BTC="BTC")) == []


def test_parse_bad_open_time_dropped():
    frame = json.dumps({"channel": "candle", "data": {"t": "not-a-number", "s": "BTC", "o": "1"}})
    assert _parse_hl_candle(frame, _amap(BTC="BTC")) == []


def test_parse_null_magnitude_still_lands():
    # Bad OHLCV → NULL column, but the row (identity + time) still lands.
    frame = json.dumps({"channel": "candle", "data": {"t": 1_720_000_000_000, "s": "BTC",
                                                      "o": None, "h": None, "l": None, "c": None, "v": None, "n": None}})
    rows = _parse_hl_candle(frame, _amap(BTC="BTC"))
    assert len(rows) == 1
    assert rows[0]["open"] is None and rows[0]["trades"] is None


def test_control_frames_yield_no_rows():
    asset_map = _amap(BTC="BTC")
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
    spec = build_hl_candle_spec(_EX)
    assert spec.loop_tf == CANDLE_WS
    assert json.loads(spec.ping_frame) == {"method": "ping"}
    assert spec.ping_interval_s == 30.0


def test_ceiling_guard_warns_near_documented_ceiling(caplog):
    assert _check_sub_ceiling(["c"] * 161) == 161        # the current universe, far under → no warning
    assert not caplog.records
    with caplog.at_level("WARNING"):
        _check_sub_ceiling(["c"] * 950)                   # nearing the documented ~1000 per-IP ceiling
    assert any("ceiling" in r.message for r in caplog.records)


# --- run() wiring (source-pin — run() is a blocking daemon entry point) -------

def test_run_wires_candle_ws_only_behind_flag():
    """The single HL 1m WS thread is imported + launched only when has_candle_ws is set, and NO REST
    backfill thread exists (WS-only, T-015.1). Source-pinned because run() blocks forever (matches the
    premium/liq wiring pins)."""
    from pathlib import Path
    src = (Path(hl_candles.__file__).parent / "main.py").read_text()
    assert "from hl_candles import build_hl_candle_spec" in src
    assert "config.has_candle_ws" in src
    # One WS thread over the whole universe; the spec takes `exchange` (for the HL coin-name map).
    assert "args=(build_hl_candle_spec(exchange), config, dsn)" in src
    # WS-only: no live REST backfill thread.
    assert "hl_candle_backfill_loop" not in src
    assert "candle-backfill" not in src
