"""Tests for collector/liquidations.py — Binance + Bybit liquidation WS feed specs."""

import json
import logging

import liquidations as liq
from config import LIQUIDATION_WS

PAIRS = [
    {"native_symbol": "BTCUSDT", "asset_id": "BTC", "symbol": "BTC/USDT:USDT"},
    {"native_symbol": "ETHUSDT", "asset_id": "ETH", "symbol": "ETH/USDT:USDT"},
]


# --- Binance parser --------------------------------------------------------


def _binance_frame(**o):
    base = {"s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "q": "0.014", "p": "9910",
            "ap": "9950", "z": "0.010", "T": 1568014460893}
    base.update(o)
    return json.dumps({"e": "forceOrder", "E": 1568014460893, "o": base})


def test_binance_parse_long_liquidation():
    m = {"BTCUSDT": "BTC"}
    rows = liq._parse_binance_liq(_binance_frame(S="SELL"), m)
    assert len(rows) == 1
    r = rows[0]
    assert r == {"venue": "binance", "asset_id": "BTC", "liquidated_side": "long",
                 "px": 9950.0, "sz": 0.010, "timestamp": 1568014460.893}


def test_binance_parse_short_liquidation():
    rows = liq._parse_binance_liq(_binance_frame(S="BUY"), {"BTCUSDT": "BTC"})
    assert rows[0]["liquidated_side"] == "short"  # BUY order = a short was liquidated


def test_binance_parse_untracked_symbol_dropped():
    assert liq._parse_binance_liq(_binance_frame(s="DOGEUSDT"), {"BTCUSDT": "BTC"}) == []


def test_binance_parse_non_forceorder_frame_ignored():
    assert liq._parse_binance_liq(json.dumps({"e": "somethingElse"}), {"BTCUSDT": "BTC"}) == []


def test_binance_parse_bad_side_dropped():
    assert liq._parse_binance_liq(_binance_frame(S="???"), {"BTCUSDT": "BTC"}) == []


def test_binance_parse_malformed_magnitude_lands_null():
    # Bad px/sz → the row still lands (directional sidecar); ap falls back to p, z to q.
    rows = liq._parse_binance_liq(_binance_frame(ap="", p="junk", z=None, q="bad"), {"BTCUSDT": "BTC"})
    assert len(rows) == 1
    assert rows[0]["px"] is None and rows[0]["sz"] is None
    assert rows[0]["asset_id"] == "BTC" and rows[0]["liquidated_side"] == "long"


# --- Bybit parser ----------------------------------------------------------


def _bybit_frame(data):
    return json.dumps({"topic": "allLiquidation.BTCUSDT", "type": "snapshot", "ts": 1739502303204, "data": data})


def test_bybit_parse_unrolls_data_array_both_sides():
    m = {"BTCUSDT": "BTC", "ETHUSDT": "ETH"}
    raw = _bybit_frame([
        {"T": 1739502302929, "s": "BTCUSDT", "S": "Buy", "v": "0.003", "p": "96475.10"},
        {"T": 1739502302930, "s": "ETHUSDT", "S": "Sell", "v": "1.5", "p": "2600"},
    ])
    rows = liq._parse_bybit_liq(raw, m)
    assert [(r["asset_id"], r["liquidated_side"], r["px"], r["sz"]) for r in rows] == [
        ("BTC", "long", 96475.10, 0.003),   # Buy = long liquidated
        ("ETH", "short", 2600.0, 1.5),      # Sell = short liquidated
    ]
    assert rows[0]["timestamp"] == 1739502302.929


def test_bybit_control_frame_no_data_ignored():
    # Subscribe ack and pong carry no `data` array → [].
    assert liq._parse_bybit_liq(json.dumps({"success": True, "op": "subscribe"}), {}) == []
    assert liq._parse_bybit_liq(json.dumps({"op": "pong", "args": ["1739"]}), {}) == []


def test_bybit_untracked_skipped_tracked_kept():
    m = {"BTCUSDT": "BTC"}
    raw = _bybit_frame([
        {"T": 1739502302929, "s": "DOGEUSDT", "S": "Buy", "v": "1", "p": "0.1"},   # untracked → skip
        {"T": 1739502302930, "s": "BTCUSDT", "S": "Buy", "v": "0.003", "p": "96475"},
    ])
    rows = liq._parse_bybit_liq(raw, m)
    assert [r["asset_id"] for r in rows] == ["BTC"]


# --- Shared native→asset map wiring ----------------------------------------


def test_binance_spec_topics_for_pairs_populates_map_despite_empty_topics():
    spec = liq.build_binance_liq_spec()
    topics = spec.topics_for_pairs(PAIRS)
    assert topics == []                                   # all-market: no per-symbol topics
    rows = spec.parse_message(_binance_frame(s="ETHUSDT", S="SELL"))
    assert rows[0]["asset_id"] == "ETH"                   # resolved via the map populated above


def test_bybit_spec_topics_and_resolution_and_rebuild():
    spec = liq.build_bybit_liq_spec()
    topics = spec.topics_for_pairs(PAIRS)
    assert set(topics) == {"allLiquidation.BTCUSDT", "allLiquidation.ETHUSDT"}
    assert spec.parse_message(_bybit_frame([
        {"T": 1739502302929, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "9"}]))[0]["asset_id"] == "BTC"
    # A pair-set change rebuilds the map: the old native no longer resolves.
    spec.topics_for_pairs([{"native_symbol": "SOLUSDT", "asset_id": "SOL"}])
    assert spec.parse_message(_bybit_frame([
        {"T": 1739502302929, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "9"}])) == []


# --- Bybit subscribe framing + ceiling guard -------------------------------


def test_bybit_subscribe_frames_batched_under_chunk():
    topics = [f"allLiquidation.SYM{i}USDT" for i in range(250)]
    frames = liq._bybit_subscribe_frames(topics)
    assert len(frames) == 3                                # 100 + 100 + 50
    payload = json.loads(frames[0])
    assert payload["op"] == "subscribe" and len(payload["args"]) == 100
    assert json.loads(liq._bybit_unsubscribe_frames(topics)[0])["op"] == "unsubscribe"


def test_bybit_ceiling_guard_warns_near_limit(caplog):
    small = [f"allLiquidation.S{i}" for i in range(100)]   # ~1.5k chars → no warning
    with caplog.at_level(logging.WARNING):
        liq._check_bybit_ceiling(small)
    assert not any("ceiling" in r.message for r in caplog.records)
    caplog.clear()
    big = [f"allLiquidation.{'X' * 20}{i}" for i in range(600)]  # ~20k chars → warn
    with caplog.at_level(logging.WARNING):
        total = liq._check_bybit_ceiling(big)
    assert total >= liq.BYBIT_ARGS_CHAR_WARN
    assert any("ceiling" in r.message for r in caplog.records)


# --- insert_batch ----------------------------------------------------------


class _FakeCursor:
    def __init__(self, calls):
        self._calls = calls
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        self._calls.append((sql, params))


class _FakeConn:
    def __init__(self):
        self.calls = []
        self.commits = 0

    def cursor(self):
        return _FakeCursor(self.calls)

    def commit(self):
        self.commits += 1


def test_insert_liquidations_binds_grade_c_and_seven_columns():
    conn = _FakeConn()
    events = liq._parse_binance_liq(_binance_frame(), {"BTCUSDT": "BTC"})
    n = liq._insert_liquidations(conn, events)
    assert n == 1 and conn.commits == 1
    sql, params = conn.calls[0]
    assert "grade" in sql and "'c'" in sql                 # grade hardcoded C in the SQL
    assert set(params) == {"timestamp", "venue", "asset_id", "liquidated_side", "px", "sz"}
    assert "oid" not in params and "tid" not in params     # HL columns not bound for C rows


def test_insert_liquidations_empty_is_noop():
    conn = _FakeConn()
    assert liq._insert_liquidations(conn, []) == 0
    assert conn.commits == 0


# --- Spec-builder constants (regression guard for the routed Binance URL) ---


def test_binance_spec_constants():
    spec = liq.build_binance_liq_spec()
    assert spec.url == "wss://fstream.binance.com/market/ws/!forceOrder@arr"  # routed /market path
    assert spec.loop_tf == LIQUIDATION_WS
    assert spec.ping_interval_s is None                    # protocol pings only
    assert spec.proactive_reconnect_s == liq.BINANCE_PROACTIVE_RECONNECT_S


def test_bybit_spec_constants():
    spec = liq.build_bybit_liq_spec()
    assert spec.url == "wss://stream.bybit.com/v5/public/linear"
    assert spec.loop_tf == LIQUIDATION_WS
    assert spec.ping_interval_s == 20.0
    assert json.loads(spec.ping_frame) == {"op": "ping"}
    assert spec.proactive_reconnect_s is None
