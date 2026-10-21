"""The collector's process root (collector/main.py): the feed table and the supervisor that exits the
process when a feed thread stops, so Docker restarts the collector. conftest stubs `main` as a
MagicMock, so the real module is loaded under a private name."""

import importlib.util
import logging
import subprocess
import sys
import textwrap
import threading
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import config as _config_mod
import pytest

COLLECTOR = Path(_config_mod.__file__).parent
DEADLINE_S = 5


def _load_real_main():
    spec = importlib.util.spec_from_file_location("main_feeds_real", COLLECTOR / "main.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


main = _load_real_main()

FLAGS = ("has_funding", "has_oi", "has_liquidation_ws", "has_premium_index", "has_candle_ws")
TF_5M = SimpleNamespace(name="5m")
TF_1H = SimpleNamespace(name="1h")


def _config(name="okx", **flags):
    return SimpleNamespace(name=name, timeframes=[TF_5M, TF_1H], **{f: flags.get(f, False) for f in FLAGS})


def _bound(feed):
    """A feed as (name, loop, args): what its thread will call."""
    return feed.name, feed.run.func, feed.run.args


@pytest.fixture
def specs(monkeypatch):
    """Spec builders return sentinels recording what they were built from."""
    monkeypatch.setattr(main, "build_liq_spec", lambda venue: ("liq-spec", venue))
    monkeypatch.setattr(main, "build_hl_candle_spec", lambda exchange: ("hl-candle-spec", exchange))


# --- build_feeds ---------------------------------------------------------------


def test_no_flags_gives_one_candle_feed_per_timeframe(specs):
    cfg, exchange = _config(), object()
    feeds = main.build_feeds(cfg, exchange, "dsn", "limiter")
    assert [_bound(f) for f in feeds] == [
        ("okx-5m", main.candle_loop, (exchange, cfg, TF_5M, "dsn", "limiter")),
        ("okx-1h", main.candle_loop, (exchange, cfg, TF_1H, "dsn", "limiter")),
    ]


@pytest.mark.parametrize("flag, name, target, args", [
    ("has_funding", "okx-funding", "funding_loop", ("ex", "cfg", "dsn", "limiter")),
    ("has_oi", "okx-oi", "oi_loop", ("ex", "cfg", "dsn", "limiter")),
    ("has_liquidation_ws", "okx-liq", "ws_loop", (("liq-spec", "okx"), "cfg", "dsn")),
    ("has_premium_index", "okx-premium", "premium_index_loop", ("ex", "cfg", "dsn", "limiter")),
    ("has_candle_ws", "okx-candle-ws", "ws_loop", (("hl-candle-spec", "ex"), "cfg", "dsn")),
])
def test_each_flag_adds_exactly_its_feed(specs, flag, name, target, args):
    cfg, exchange = _config(**{flag: True}), object()
    resolve = {"ex": exchange, "cfg": cfg}
    expected_args = tuple(
        tuple(resolve.get(part, part) for part in a) if isinstance(a, tuple) else resolve.get(a, a) for a in args
    )
    feeds = main.build_feeds(cfg, exchange, "dsn", "limiter")
    assert [f.name for f in feeds] == ["okx-5m", "okx-1h", name]
    assert _bound(feeds[-1]) == (name, getattr(main, target), expected_args)


def test_all_flags_launch_in_order(specs):
    feeds = main.build_feeds(_config(**dict.fromkeys(FLAGS, True)), object(), "dsn", "limiter")
    assert [f.name for f in feeds] == [
        "okx-5m", "okx-1h", "okx-funding", "okx-oi", "okx-liq", "okx-premium", "okx-candle-ws",
    ]


def test_hl_1m_is_one_ws_feed_and_no_rest_candle_feed(specs):
    """HL 1m streams over one WS connection for the whole universe; it adds no per-timeframe REST feed."""
    cfg, exchange = _config(name="hyperliquid", has_candle_ws=True), object()
    feeds = main.build_feeds(cfg, exchange, "dsn", "limiter")
    assert [_bound(f) for f in feeds] == [
        ("hyperliquid-5m", main.candle_loop, (exchange, cfg, TF_5M, "dsn", "limiter")),
        ("hyperliquid-1h", main.candle_loop, (exchange, cfg, TF_1H, "dsn", "limiter")),
        ("hyperliquid-candle-ws", main.ws_loop, (("hl-candle-spec", exchange), cfg, "dsn")),
    ]


# --- wait_for_dead_feed ---------------------------------------------------------


class _FakeThread:
    def __init__(self, name, alive_polls):
        self.name, self.alive_polls = name, alive_polls

    def is_alive(self):
        self.alive_polls -= 1
        return self.alive_polls >= 0


def test_wait_returns_the_first_dead_thread(monkeypatch):
    sleeps = []
    monkeypatch.setattr(main, "time", SimpleNamespace(sleep=sleeps.append))
    threads = [_FakeThread("a", alive_polls=99), _FakeThread("b", alive_polls=3)]
    assert main.wait_for_dead_feed(threads, 7) == "b"
    assert sleeps == [7, 7, 7]  # polled while every thread lived, then returned without sleeping


# --- run(): real threads, os._exit intercepted ----------------------------------


class _Exited(Exception):
    def __init__(self, code):
        self.code = code


@pytest.fixture
def harness(monkeypatch):
    """run() with its setup stubbed, os._exit raising, and pytest's log handlers kept open. Collects the
    test's Events and threads, then releases and joins them."""
    events, threads = [], []

    def _exit(code):
        raise _Exited(code)

    monkeypatch.setattr(main, "load_config", lambda: SimpleNamespace(name="test", timeframes=[TF_5M]))
    monkeypatch.setattr(main, "create_exchange", lambda cfg: None)
    monkeypatch.setattr(main, "build_dsn", lambda: "dsn")
    monkeypatch.setattr(main, "build_rate_limiter", lambda cfg: None)
    monkeypatch.setattr(main, "os", SimpleNamespace(_exit=_exit))
    monkeypatch.setattr(logging, "shutdown", lambda: None)
    monkeypatch.setattr(threading, "excepthook", lambda args: None)  # the dying feed's traceback
    monkeypatch.setattr(main, "SUPERVISE_POLL_S", 0.01)
    real_thread = threading.Thread

    def _tracked_thread(*a, **k):
        t = real_thread(*a, **k)
        threads.append(t)
        return t

    monkeypatch.setattr(main, "threading", SimpleNamespace(Thread=_tracked_thread))
    yield SimpleNamespace(events=events, set_feeds=lambda feeds: monkeypatch.setattr(main, "build_feeds", lambda *a: feeds))
    for event in events:
        event.set()
    for t in threads:
        t.join(DEADLINE_S)
        assert not t.is_alive(), f"test thread {t.name} outlived its test"


def _raises():
    raise RuntimeError("feed died")


def _returns():
    return None


@pytest.mark.parametrize("target", [_raises, _returns], ids=["raises", "returns"])
def test_run_exits_1_when_a_feed_stops(harness, caplog, target):
    harness.set_feeds([main.Feed("test-feed", target)])
    with caplog.at_level(logging.CRITICAL), pytest.raises(_Exited) as exited:
        main.run()
    assert exited.value.code == 1
    assert "Feed thread test-feed stopped" in caplog.text


def test_run_supervises_every_feed_not_just_the_first(harness, monkeypatch, caplog):
    """The first feed lives throughout; a later one stops only after the supervisor has polled three
    times. run() must not exit before it stops, and then must name it."""
    first_release, polled_thrice = threading.Event(), threading.Event()
    harness.events.extend([first_release, polled_thrice])
    polls = {"n": 0}
    stopped = {"at_poll": None}

    def _sleep(seconds):
        polls["n"] += 1
        if polls["n"] >= 3:
            polled_thrice.set()
        threading.Event().wait(seconds)

    def _later():
        assert polled_thrice.wait(DEADLINE_S)
        stopped["at_poll"] = polls["n"]

    monkeypatch.setattr(main, "time", SimpleNamespace(sleep=_sleep))
    harness.set_feeds([
        main.Feed("test-first", partial(first_release.wait, DEADLINE_S)),
        main.Feed("test-later", _later),
    ])
    with caplog.at_level(logging.CRITICAL), pytest.raises(_Exited) as exited:
        main.run()
    assert exited.value.code == 1
    assert stopped["at_poll"] is not None and stopped["at_poll"] >= 3
    assert "Feed thread test-later stopped" in caplog.text
    assert "test-first stopped" not in caplog.text


# --- os._exit in a real interpreter, with an executor worker mid-call ----------------

_BLOCKED_WORKER_SCRIPT = textwrap.dedent("""
    import concurrent.futures, sys, threading, types
    from unittest.mock import MagicMock
    sys.path.insert(0, sys.argv[1])
    for mod in ["psycopg", "psycopg.rows", "psycopg_pool", "dotenv", "db", "oi", "backoff"]:
        sys.modules[mod] = MagicMock()
    import main

    entered, release = threading.Event(), threading.Event()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    pool.submit(lambda: (entered.set(), release.wait()))
    assert entered.wait(5), "executor worker never started"

    def die():
        raise RuntimeError("feed died")

    main.load_config = lambda: types.SimpleNamespace(name="test", timeframes=[types.SimpleNamespace(name="5m")])
    main.build_rate_limiter = lambda cfg: None
    main.build_feeds = lambda *a: [main.Feed("test-dies", die)]
    main.SUPERVISE_POLL_S = 0.05
    main.run()
""")


def test_exit_does_not_wait_for_a_blocked_executor_worker():
    """sys.exit would join the blocked worker at interpreter shutdown and hang; os._exit must not."""
    result = subprocess.run(
        [sys.executable, "-c", _BLOCKED_WORKER_SCRIPT, str(COLLECTOR)],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 1, result.stderr
    assert "Feed thread test-dies stopped; exiting so Docker restarts the collector" in result.stderr
