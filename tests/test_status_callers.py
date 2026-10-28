"""Every caller of a status.py write commits it: status.py never ends a transaction, so a write
its caller forgets to commit is lost when the loop's connection resets."""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import funding
import hl_candles_repair
import premium_index
import pytest
import status
import ws

from tests.fakes import RecordingConn

if isinstance(sys.modules.get("oi"), MagicMock):  # conftest stubs `oi`; these tests need the real one
    del sys.modules["oi"]
import oi  # noqa: E402, I001

CONFIG = SimpleNamespace(name="binance", market_type="perp")


@pytest.mark.parametrize("module", [funding, oi, premium_index], ids=lambda m: m.__name__)
def test_poll_loop_heartbeat_commits(module):
    conn = RecordingConn()
    module._heartbeat(conn, CONFIG, "t")
    assert conn.kinds() == ["execute", "commit"]
    assert conn.events[0].sql == status.UPSERT_LOOP_HEARTBEAT_SQL


def test_ws_heartbeat_commits():
    runtime = ws._WSRuntime(conn=RecordingConn(), buffer=[])
    ws._heartbeat(runtime, CONFIG, SimpleNamespace(loop_tf="liq_ws"), "dsn", "t")
    assert runtime.conn.kinds() == ["execute", "commit"]
    assert runtime.conn.events[0].params["loop"] == "liq_ws"


def test_repair_cursor_advance_commits():
    conn = RecordingConn()
    hl_candles_repair.advance_cursor(conn, 1_700_000_000_000)
    assert conn.kinds() == ["execute", "commit"]
    assert conn.events[0].sql == status.UPSERT_SUCCESS_SQL
