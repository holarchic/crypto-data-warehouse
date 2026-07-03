"""Tests for collector/ws.py — the generic WS engine.

No real socket or DB: conftest stubs db/status/backoff as MagicMock, so we monkeypatch
`ws.connect_db`/`ws.query_active_pairs`/`ws.upsert_loop_heartbeat` and drive a scripted
`FakeWS` + an injected clock. Buffer mechanics test the factored helpers (`_flush`,
`_active_pairs`, `_ensure_conn`, `_cap_buffer`) directly; timer/reconnect behavior drives
the full `ws_loop`. Real DB-engine behavior (ON CONFLICT, real rollback) is prod-verified.
"""

import json
from unittest.mock import MagicMock, call

import ws  # ws is not stubbed by conftest → the real module
from config import ExchangeConfig
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException

# --- Test doubles ------------------------------------------------------------


class FakeConn:
    def __init__(self, closed=False):
        self.closed = closed
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class FakeWS:
    """Scripted socket. Each script step is a (kind, value) tuple consumed by one recv():
    ("msg", str) returns a data frame; ("timeout", _) raises WebSocketTimeoutException (quiet);
    ("jump", dt) advances the shared clock by dt and raises timeout; ("close", _) sets
    connected=False and returns "" (graceful close); ("drop", _) raises a closed exception.
    An exhausted script raises closed (a drop) so a session always terminates.
    """

    def __init__(self, script, clock_box):
        self.script = list(script)
        self.clock_box = clock_box
        self.connected = True
        self.sent = []
        self.close_calls = 0
        self.shutdown_calls = 0

    def settimeout(self, s):
        pass

    def recv(self):
        if not self.script:
            raise WebSocketConnectionClosedException("script exhausted")
        kind, val = self.script.pop(0)
        if kind == "jump":
            self.clock_box[0] += val
            raise WebSocketTimeoutException()
        if kind == "timeout":
            raise WebSocketTimeoutException()
        if kind == "close":
            self.connected = False
            return ""
        if kind == "drop":
            raise WebSocketConnectionClosedException("drop")
        return val  # "msg"

    def send(self, frame):
        self.sent.append(frame)

    def close(self, **kw):
        self.close_calls += 1
        self.connected = False

    def shutdown(self):
        self.shutdown_calls += 1
        self.connected = False


def _msg(events):
    return ("msg", json.dumps(events))


def _clock():
    box = [0.0]
    return box, (lambda: box[0])


def _connector(ws_list):
    it = iter(ws_list)

    def _connect(url, timeout=None):
        return next(it)

    return _connect


def _config(name="bybit", market_type="perp"):
    return ExchangeConfig(name=name, ccxt_class=name, btc_symbol="BTC/USDT:USDT",
                          market_type=market_type, rate_limit_ms=200, enabled=True)


def _bybit_subs(topics):
    topics = list(topics)
    return [json.dumps({"op": "subscribe", "args": topics[i:i + 10]}) for i in range(0, len(topics), 10)]


def _bybit_unsubs(topics):
    topics = list(topics)
    return [json.dumps({"op": "unsubscribe", "args": topics})] if topics else []


def _spec(insert_batch, *, venue="bybit", parse_message=None, **overrides):
    if venue == "binance":
        base = dict(topics_for_pairs=lambda pairs: ["!forceOrder@arr"],
                    subscribe_frames=lambda topics: [], unsubscribe_frames=lambda topics: [],
                    ping_frame=None, ping_interval_s=None, proactive_reconnect_s=86400.0)
    else:
        base = dict(topics_for_pairs=lambda pairs: [f"allLiquidation.{p['native_symbol']}" for p in pairs],
                    subscribe_frames=_bybit_subs, unsubscribe_frames=_bybit_unsubs,
                    ping_frame='{"op":"ping"}', ping_interval_s=20.0, proactive_reconnect_s=None)
    base.update(name="liquidations", loop_tf="liq_ws", url=f"wss://fake/{venue}",
                parse_message=parse_message or (lambda raw: json.loads(raw)), insert_batch=insert_batch)
    base.update(overrides)
    return ws.WSFeedSpec(**base)


def _patch_db(monkeypatch, pairs=None, conn=None):
    """Wire connect_db → a FakeConn and query_active_pairs → a fixed pair list (or side_effect)."""
    conn = conn or FakeConn()
    monkeypatch.setattr(ws, "connect_db", lambda dsn, **kw: conn)
    if callable(pairs):
        monkeypatch.setattr(ws, "query_active_pairs", pairs)
    else:
        monkeypatch.setattr(ws, "query_active_pairs", lambda c, name, tf: list(pairs or []))
    hb = MagicMock()
    monkeypatch.setattr(ws, "upsert_loop_heartbeat", hb)
    return conn, hb


# --- Flush mechanics ---------------------------------------------------------


def test_flush_by_size(monkeypatch):
    conn, _ = _patch_db(monkeypatch)
    insert = MagicMock(return_value=600)
    spec = _spec(insert, venue="binance")
    _, clk = _clock()
    # one frame parsing to 600 events → buffer >= WS_BATCH_MAX(500) → flush in-iteration
    fake = FakeWS([_msg([{"i": k} for k in range(600)])], [0.0])
    ws.ws_loop(spec, _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    assert insert.called
    assert len(insert.call_args_list[0].args[1]) == 600


def test_flush_by_interval(monkeypatch):
    _patch_db(monkeypatch)
    insert = MagicMock(return_value=5)
    spec = _spec(insert, venue="binance")
    box, clk = _clock()
    # 5 events (< WS_BATCH_MAX) then a clock jump past WS_BATCH_INTERVAL(1.0s) → flush
    fake = FakeWS([_msg([{"i": k} for k in range(5)]), ("jump", 1.5)], box)
    ws.ws_loop(spec, _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    assert insert.called
    assert len(insert.call_args_list[0].args[1]) == 5


def test_tail_flush_on_disconnect(monkeypatch):
    _patch_db(monkeypatch)
    insert = MagicMock(return_value=3)
    spec = _spec(insert, venue="binance")
    # 3 events (no size/interval flush) then a drop → tail-flush in ws_loop's finally
    fake = FakeWS([_msg([{"i": 1}, {"i": 2}, {"i": 3}]), ("drop", None)], [0.0])
    _, clk = _clock()
    ws.ws_loop(spec, _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    assert insert.called
    assert len(insert.call_args_list[-1].args[1]) == 3


def test_insert_failure_retains_buffer_and_rolls_back():
    conn = FakeConn()
    spec = _spec(MagicMock(side_effect=RuntimeError("db")), venue="binance")
    runtime = ws._WSRuntime(conn=conn, buffer=[{"i": 1}, {"i": 2}])
    ws._flush(runtime, spec, "dsn", "t")
    assert runtime.buffer == [{"i": 1}, {"i": 2}]  # retained for retry
    assert conn.rollbacks == 1


def test_insert_failure_caps_buffer():
    conn = FakeConn()
    spec = _spec(MagicMock(side_effect=RuntimeError("db")), venue="binance")
    runtime = ws._WSRuntime(conn=conn, buffer=[{"i": k} for k in range(ws.WS_MAX_BUFFER + 1000)])
    ws._flush(runtime, spec, "dsn", "t")
    assert len(runtime.buffer) == ws.WS_MAX_BUFFER
    assert runtime.buffer[0] == {"i": 1000}  # oldest 1000 dropped, newest kept


# --- Reconnect / backoff -----------------------------------------------------


def test_reconnect_resubscribes(monkeypatch):
    _patch_db(monkeypatch, pairs=[{"native_symbol": "BTCUSDT"}])
    spec = _spec(MagicMock(return_value=0), venue="bybit")
    ws1, ws2 = FakeWS([("drop", None)], [0.0]), FakeWS([("drop", None)], [0.0])
    _, clk = _clock()
    ws.ws_loop(spec, _config(), "dsn", connect_fn=_connector([ws1, ws2]), clock=clk, max_sessions=2)
    # both sessions sent an initial subscribe frame for BTCUSDT (resubscribe on reconnect)
    assert any("allLiquidation.BTCUSDT" in f for f in ws1.sent)
    assert any("allLiquidation.BTCUSDT" in f for f in ws2.sent)


def test_topics_survive_reconnect_during_db_outage(monkeypatch):
    """A per-symbol feed that reconnects while the DB is down must re-subscribe its LAST-KNOWN
    topics (cached in _WSRuntime), not come back subscribed to nothing until the DB returns."""
    pairs = MagicMock(side_effect=[
        [{"native_symbol": "BTCUSDT"}],  # session1 initial subscribe → caches {BTCUSDT}
        RuntimeError("db down"),          # session2 initial subscribe → _active_pairs returns None
    ])
    conn = FakeConn()
    monkeypatch.setattr(ws, "connect_db", lambda dsn, **kw: conn)
    monkeypatch.setattr(ws, "query_active_pairs", pairs)
    monkeypatch.setattr(ws, "upsert_loop_heartbeat", MagicMock())
    ws1, ws2 = FakeWS([("drop", None)], [0.0]), FakeWS([("drop", None)], [0.0])
    _, clk = _clock()
    ws.ws_loop(_spec(MagicMock(), venue="bybit"), _config(), "dsn",
               connect_fn=_connector([ws1, ws2]), clock=clk, max_sessions=2)
    assert any("allLiquidation.BTCUSDT" in f for f in ws2.sent)  # cached topics re-subscribed despite DB down


def test_connect_timeout_feeds_backoff(monkeypatch):
    _patch_db(monkeypatch)
    backoff = MagicMock()
    monkeypatch.setattr(ws, "backoff_sleep", backoff)

    def _boom(url, timeout=None):
        raise WebSocketTimeoutException("handshake stalled")

    _, clk = _clock()
    ws.ws_loop(_spec(MagicMock(), venue="binance"), _config(), "dsn", connect_fn=_boom, clock=clk, max_sessions=1)
    backoff.assert_called_once_with(1)  # connect failure → error_count 1 → backoff, loop did not wedge


def test_proactive_reconnect_no_backoff(monkeypatch):
    _patch_db(monkeypatch)
    backoff = MagicMock()
    monkeypatch.setattr(ws, "backoff_sleep", backoff)
    spec = _spec(MagicMock(return_value=0), venue="binance", proactive_reconnect_s=100.0)
    box, clk = _clock()
    fake = FakeWS([("jump", 101)], box)  # jump past proactive deadline → clean return
    ws.ws_loop(spec, _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    backoff.assert_not_called()  # clean end resets nothing to fear — error_count stayed 0
    assert fake.shutdown_calls == 1  # socket released


def test_graceful_close_backs_off(monkeypatch):
    """A server-initiated graceful close backs off (unlike proactive reconnect) — a venue that
    accepts the handshake then immediately closes every session must not tight-spin."""
    _patch_db(monkeypatch)
    backoff = MagicMock()
    monkeypatch.setattr(ws, "backoff_sleep", backoff)
    box, clk = _clock()
    fake = FakeWS([("close", None)], box)
    ws.ws_loop(_spec(MagicMock(), venue="binance"), _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    backoff.assert_called_once_with(1)


def test_error_count_resets_on_healthy_reconnect(monkeypatch):
    _patch_db(monkeypatch)
    backoff = MagicMock()
    monkeypatch.setattr(ws, "backoff_sleep", backoff)
    spec = _spec(MagicMock(return_value=0), venue="binance")
    # each session subscribes (resets error_count to 0) then drops (→1). Without the reset the
    # second backoff would be call(2); the reset makes both call(1).
    ws1, ws2 = FakeWS([("drop", None)], [0.0]), FakeWS([("drop", None)], [0.0])
    _, clk = _clock()
    ws.ws_loop(spec, _config(), "dsn", connect_fn=_connector([ws1, ws2]), clock=clk, max_sessions=2)
    assert backoff.call_args_list == [call(1), call(1)]


# --- Timers: ping / heartbeat ------------------------------------------------


def test_ping_timer_fires_bybit_not_binance(monkeypatch):
    _patch_db(monkeypatch, pairs=[{"native_symbol": "BTCUSDT"}])
    box, clk = _clock()
    fake = FakeWS([("jump", 25)], box)  # past ping_interval_s(20)
    ws.ws_loop(_spec(MagicMock(), venue="bybit"), _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    assert '{"op":"ping"}' in fake.sent

    box2, clk2 = _clock()
    fake2 = FakeWS([("jump", 25)], box2)
    ws.ws_loop(_spec(MagicMock(), venue="binance"), _config(), "dsn", connect_fn=_connector([fake2]), clock=clk2, max_sessions=1)
    assert fake2.sent == []  # no app-level ping for Binance


def test_heartbeat_fires_while_silent(monkeypatch):
    _, hb = _patch_db(monkeypatch)
    box, clk = _clock()
    # zero data frames — only a clock jump past WS_HEARTBEAT_INTERVAL(60). Connection-based,
    # not event-based: a silent feed is healthy and must still heartbeat.
    fake = FakeWS([("jump", 65)], box)
    ws.ws_loop(_spec(MagicMock(), venue="binance"), _config("binance"), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    assert hb.called
    assert hb.call_args.args[1:] == ("binance", "perp", "liq_ws")


# --- Subscription management -------------------------------------------------


def test_initial_subscribe_with_pairs(monkeypatch):
    _patch_db(monkeypatch, pairs=[{"native_symbol": "BTCUSDT"}, {"native_symbol": "ETHUSDT"}])
    box, clk = _clock()
    fake = FakeWS([("drop", None)], box)
    ws.ws_loop(_spec(MagicMock(), venue="bybit"), _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    frame = fake.sent[0]
    assert "allLiquidation.BTCUSDT" in frame and "allLiquidation.ETHUSDT" in frame


def test_initial_subscribe_zero_pairs_enters_recv(monkeypatch):
    _patch_db(monkeypatch, pairs=[])  # no active pairs yet
    insert = MagicMock(return_value=1)
    box, clk = _clock()
    # no subscribe frames sent, but the session still enters the recv loop and processes a msg
    fake = FakeWS([_msg([{"i": 1}]), ("jump", 1.5)], box)
    ws.ws_loop(_spec(insert, venue="bybit"), _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    assert fake.sent == []  # nothing to subscribe to
    assert insert.called  # recv loop still ran


def test_resubscribe_on_pair_set_change(monkeypatch):
    pairs = MagicMock(side_effect=[
        [{"native_symbol": "BTCUSDT"}],                              # initial subscribe
        [{"native_symbol": "BTCUSDT"}, {"native_symbol": "ETHUSDT"}],  # resubscribe: +ETH
    ])
    _patch_db(monkeypatch, pairs=pairs)
    box, clk = _clock()
    fake = FakeWS([("jump", 65)], box)  # past WS_RESUB_CHECK_INTERVAL(60)
    ws.ws_loop(_spec(MagicMock(), venue="bybit"), _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    # a subscribe frame for the newly-added ETH topic was sent (not for the unchanged BTC)
    eth_frames = [f for f in fake.sent if "allLiquidation.ETHUSDT" in f and "subscribe" in f and "unsubscribe" not in f]
    assert eth_frames


def test_subscribe_arg_cap_batching():
    topics = [f"allLiquidation.S{i}" for i in range(23)]
    frames = _bybit_subs(topics)  # venue owns the ≤10-arg cap
    assert len(frames) == 3  # 10 + 10 + 3
    assert all(len(json.loads(f)["args"]) <= 10 for f in frames)


# --- Session edges: graceful close, parse-or-drop, DB recovery ---------------


def test_graceful_close_ends_session(monkeypatch):
    _patch_db(monkeypatch)
    insert = MagicMock(return_value=1)
    spec = _spec(insert, venue="binance")
    box, clk = _clock()
    # a data frame, then a graceful server close ("" + connected=False) → session ends, tail-flush
    fake = FakeWS([_msg([{"i": 1}]), ("close", None)], box)
    ws.ws_loop(spec, _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    assert fake.shutdown_calls == 1  # released via _safe_close on the graceful-close path
    assert insert.called  # buffered event tail-flushed


def test_parse_error_skips_frame_session_survives(monkeypatch):
    _patch_db(monkeypatch)
    insert = MagicMock(return_value=1)

    def parse(raw):
        if raw == "BAD":
            raise ValueError("garbage frame")
        return json.loads(raw)

    spec = _spec(insert, venue="binance", parse_message=parse)
    box, clk = _clock()
    fake = FakeWS([("msg", "BAD"), _msg([{"i": 9}]), ("drop", None)], box)
    ws.ws_loop(spec, _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
    # bad frame dropped, good frame survived and flushed → the session did not die on the parse error
    assert insert.called
    assert insert.call_args_list[-1].args[1] == [{"i": 9}]


def test_ensure_conn_reconnects_on_closed(monkeypatch):
    reopened = FakeConn(closed=False)
    monkeypatch.setattr(ws, "connect_db", lambda dsn, **kw: reopened)
    runtime = ws._WSRuntime(conn=FakeConn(closed=True), buffer=[])
    assert ws._ensure_conn(runtime, "dsn") is True
    assert runtime.conn is reopened


def test_ensure_conn_is_nonblocking(monkeypatch):
    """A mid-session DB reconnect must be single-shot — connect_db's default 10×5s retry would
    freeze the recv loop (no draining/ping/heartbeat) for ~50s, defeating socket survival."""
    calls = []

    def fake_connect(dsn, max_retries=10, retry_delay=5):
        calls.append((max_retries, retry_delay))
        return FakeConn()

    monkeypatch.setattr(ws, "connect_db", fake_connect)
    ws._ensure_conn(ws._WSRuntime(conn=None, buffer=[]), "dsn")
    assert calls == [(1, 0)]  # never the blocking default


def test_mid_session_db_reconnect_flushes(monkeypatch):
    reopened = FakeConn(closed=False)
    monkeypatch.setattr(ws, "connect_db", lambda dsn, **kw: reopened)
    insert = MagicMock(return_value=2)
    spec = _spec(insert, venue="binance")
    # conn dropped mid-session; _flush's _ensure_conn reconnects and the buffer flushes
    runtime = ws._WSRuntime(conn=FakeConn(closed=True), buffer=[{"i": 1}, {"i": 2}])
    ws._flush(runtime, spec, "dsn", "t")
    assert runtime.conn is reopened
    assert insert.call_args.args[0] is reopened
    assert runtime.buffer == []


def test_buffer_survives_reconnect(monkeypatch):
    _patch_db(monkeypatch)
    # session-1 in-session flush AND its tail-flush both fail (DB down); session-2 succeeds.
    insert = MagicMock(side_effect=[RuntimeError("db down"), RuntimeError("still down"), 3])
    spec = _spec(insert, venue="binance")
    ws1 = FakeWS([_msg([{"i": 1}, {"i": 2}]), ("jump", 1.5), ("drop", None)], [0.0])
    ws2 = FakeWS([_msg([{"i": 3}]), ("jump", 1.5)], [0.0])
    _, clk = _clock()
    ws.ws_loop(spec, _config(), "dsn", connect_fn=_connector([ws1, ws2]), clock=clk, max_sessions=2)
    # the session-2 successful insert carried the session-1 events forward (buffer survived the drop)
    last_events = insert.call_args_list[-1].args[1]
    assert {"i": 1} in last_events and {"i": 2} in last_events and {"i": 3} in last_events


def test_active_pairs_commits_to_close_read_txn(monkeypatch):
    conn, _ = _patch_db(monkeypatch, pairs=[{"native_symbol": "BTCUSDT"}])
    runtime = ws._WSRuntime(conn=conn, buffer=[])
    pairs = ws._active_pairs(runtime, _config(), "dsn", "t")
    assert pairs == [{"native_symbol": "BTCUSDT"}]
    assert conn.commits == 1  # read txn closed → no idle-in-transaction on a quiet WS conn


# --- Two divergent specs drive one engine (abstraction validation) -----------


def test_two_divergent_specs_drive_engine(monkeypatch):
    _patch_db(monkeypatch, pairs=[{"native_symbol": "BTCUSDT"}])
    for venue in ("binance", "bybit"):
        insert = MagicMock(return_value=1)
        box, clk = _clock()
        fake = FakeWS([_msg([{"i": 1}]), ("jump", 1.5), ("drop", None)], box)
        ws.ws_loop(_spec(insert, venue=venue), _config(), "dsn", connect_fn=_connector([fake]), clock=clk, max_sessions=1)
        assert insert.called, f"{venue}-shape spec failed to drive the engine"
