"""Tests for collector/db.py — markets reload helper."""

import sys
import time
from types import SimpleNamespace

# conftest stubs `db` to a MagicMock; remove the stub so we test the real module.
sys.modules.pop("db", None)

import db  # noqa: E402  (must follow stub removal)
from db import create_exchange, maybe_reload_markets  # noqa: E402


def _fake_exchange(loaded_at, n_markets=10):
    calls = {"reload": 0}

    def load_markets(reload=False):
        if reload:
            calls["reload"] += 1

    ex = SimpleNamespace(
        markets={f"SYM{i}/USDT:USDT": {} for i in range(n_markets)},
        load_markets=load_markets,
        _cc_markets_loaded_at=loaded_at,
        _calls=calls,
    )
    return ex


def test_skip_when_fresh():
    ex = _fake_exchange(loaded_at=time.time() - 100)
    assert maybe_reload_markets(ex, interval_s=86400) is False
    assert ex._calls["reload"] == 0


def test_reload_when_stale():
    stale_at = time.time() - 90000
    ex = _fake_exchange(loaded_at=stale_at)
    assert maybe_reload_markets(ex, interval_s=86400) is True
    assert ex._calls["reload"] == 1
    assert ex._cc_markets_loaded_at > stale_at


def test_reload_missing_timestamp_treated_as_stale():
    ex = _fake_exchange(loaded_at=0)
    delattr(ex, "_cc_markets_loaded_at")
    assert maybe_reload_markets(ex, interval_s=86400) is True
    assert ex._calls["reload"] == 1


def test_create_exchange_closes_session_on_load_markets_failure(monkeypatch):
    # If load_markets raises, create_exchange must close the just-built session and re-raise —
    # else a retrying caller (the dense pool's per-cycle init) leaks one session per attempt.
    import pytest

    closed = {"n": 0}

    class _Boom:
        def __init__(self, opts):
            pass

        def load_markets(self, reload=False):
            raise RuntimeError("load boom")

        def close(self):
            closed["n"] += 1

    monkeypatch.setattr(db.ccxt, "boomex", _Boom, raising=False)
    cfg = SimpleNamespace(ccxt_class="boomex", options={})
    with pytest.raises(RuntimeError, match="load boom"):
        create_exchange(cfg)
    assert closed["n"] == 1  # session released, no leak


def test_concurrent_calls_reload_once():
    """Lock + age gate must prevent thread storms from triggering N reloads."""
    import threading

    ex = _fake_exchange(loaded_at=time.time() - 90000)

    def hit():
        maybe_reload_markets(ex, interval_s=86400)

    threads = [threading.Thread(target=hit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert ex._calls["reload"] == 1
