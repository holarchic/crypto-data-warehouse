"""T-014 regression: a db restart must not kill a collector loop thread.

Both outages (2026-07-03, 2026-07-04) had the same mechanism — a graceful `db` restart broke the
loops' long-lived connections; the ensuing DB op raised `OperationalError`, and an unguarded
rollback/write in the error handler re-raised it out of the loop, killing the thread. These drive the
two real remaining escape paths (funding bulk insert; candle per-pair status write) and assert the
loop survives and reconnects on the next cycle. The `safe_rollback` swallow itself is proved in
test_db.py against the REAL helper (here `db` is stubbed, so `safe_rollback` is a no-op MagicMock —
which is fine: these tests prove the try/except GUARDS make the loop survive, not the swallow)."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import config as _config_mod
import funding


class _StopLoop(Exception):
    """Break out of a `while True` loop-drive after N cycles."""


class FakeConn:
    def __init__(self):
        self.closed = False

    def rollback(self):
        pass

    def commit(self):
        pass


class _FakeOpError(Exception):
    """Stands in for psycopg.OperationalError raised when the server drops the connection."""


def _load_real_main():
    """Load collector/main.py under a private name (conftest stubs `main` as a MagicMock), matching
    tests/test_candles.py — so we exercise the real collect_loop without mutating the shared stub."""
    spec = importlib.util.spec_from_file_location(
        "main_t014_real", Path(_config_mod.__file__).parent / "main.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_funding_loop_survives_db_restart_during_bulk_insert(monkeypatch):
    # _poll_bulk's bulk insert sits outside the fetch handler; a db restart there used to raise
    # straight out of funding_loop. Drive it: the insert raises + drops the conn on cycle 1; the
    # loop must survive and reconnect on cycle 2 (top-of-cycle `if conn.closed:`).
    conns = []

    def fake_connect(dsn):
        c = FakeConn()
        conns.append(c)
        return c

    def boom_insert(conn, rows):
        conn.closed = True  # server dropped the connection mid-insert
        raise _FakeOpError("the connection is lost")

    cycles = {"n": 0}

    def stop(*a):
        cycles["n"] += 1
        if cycles["n"] >= 2:
            raise _StopLoop

    monkeypatch.setattr(funding, "connect_db", fake_connect)
    monkeypatch.setattr(funding, "_get_active_pairs", lambda c, name: [{"symbol": "BTC/USDT:USDT", "asset_id": "BTC"}])
    monkeypatch.setattr(funding, "_seed_funding", lambda *a: None)
    monkeypatch.setattr(funding, "_parse_funding_record", lambda *a, **k: None)
    monkeypatch.setattr(funding, "_insert_funding", boom_insert)
    monkeypatch.setattr(funding, "paced_call", lambda rl, ex, w, fn, *a, **k: fn(*a, **k))
    monkeypatch.setattr(funding, "_heartbeat", lambda *a: None)
    monkeypatch.setattr(funding, "_sleep_until_next_hour", stop)

    ex = SimpleNamespace(fetch_funding_rates=lambda: {"BTC/USDT:USDT": {}}, last_response_headers=None)
    cfg = SimpleNamespace(name="binance", ccxt_class="binanceusdm", market_type="perp")
    try:
        funding.funding_loop(ex, cfg, "dsn", None)
    except _StopLoop:
        pass
    # Loop survived the insert failure (no propagation) AND reconnected (initial + ≥1 reconnect).
    assert len(conns) >= 2


def test_collect_loop_survives_status_write_failure_on_db_restart(monkeypatch):
    # The per-pair error handler writes status via upsert_status_error — a DB write that can raise
    # on a db restart. _record_pair_error must swallow it; the loop must survive and reconnect.
    main = _load_real_main()
    conns = []

    def fake_connect(dsn):
        c = FakeConn()
        conns.append(c)
        return c

    def boom_collect(*a):
        raise RuntimeError("collect boom")  # non-retriable → `except Exception` → _record_pair_error

    upserts = {"n": 0}

    def boom_upsert(conn, *a, **k):
        upserts["n"] += 1
        conn.closed = True  # server dropped the connection mid status-write
        raise _FakeOpError("status write lost")

    cycles = {"n": 0}

    def stop(*a):
        cycles["n"] += 1
        if cycles["n"] >= 2:
            raise _StopLoop

    monkeypatch.setattr(main, "RETRIABLE_EXCEPTIONS", (ValueError,))  # real tuple, not the backoff stub
    monkeypatch.setattr(main, "connect_db", fake_connect)
    monkeypatch.setattr(main, "maybe_reload_markets", lambda ex: None)
    monkeypatch.setattr(main, "query_active_pairs", lambda c, n, tf: [
        {"symbol": "BTC/USDT:USDT", "asset_id": "BTC", "native_symbol": "BTCUSDT",
         "market_type": "perp", "last_timestamp": None},
    ])
    monkeypatch.setattr(main, "_collect_pair", boom_collect)
    monkeypatch.setattr(main, "upsert_status_error", boom_upsert)
    monkeypatch.setattr(main, "sleep_until_next_boundary", stop)

    tf = SimpleNamespace(name="5m", interval_s=300, buffer_s=30)
    cfg = SimpleNamespace(name="binance", market_type="perp", rate_limit_weight_budget=2400)
    try:
        main.collect_loop(object(), cfg, tf, "dsn", None)
    except _StopLoop:
        pass
    assert upserts["n"] >= 1   # the status-write escape path was exercised
    assert len(conns) >= 2     # loop survived the raise AND reconnected
