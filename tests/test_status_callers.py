"""Every caller of a status.py write commits it: status.py never ends a transaction, so a write
its caller forgets to commit is lost when the loop's connection resets."""

import sys
from types import SimpleNamespace

import funding
import hl_candles_repair
import premium_index
import pytest
import status
import ws

from tests.test_status import RecordingConn

sys.modules.pop("oi", None)  # conftest stubs `oi`; these tests need the real module
import oi  # noqa: E402, I001

CONFIG = SimpleNamespace(name="binance", market_type="perp")


class _Conn(RecordingConn):
    closed = False


@pytest.mark.parametrize("module", [funding, oi, premium_index], ids=lambda m: m.__name__)
def test_poll_loop_heartbeat_commits(module):
    conn = _Conn()
    module._heartbeat(conn, CONFIG, "t")
    assert [e[0] for e in conn.events] == ["execute", "commit"]
    assert conn.events[0][1] == status.UPSERT_LOOP_HEARTBEAT_SQL


def test_ws_heartbeat_commits():
    runtime = ws._WSRuntime(conn=_Conn(), buffer=[])
    ws._heartbeat(runtime, CONFIG, SimpleNamespace(loop_tf="liq_ws"), "dsn", "t")
    assert [e[0] for e in runtime.conn.events] == ["execute", "commit"]
    assert runtime.conn.events[0][2]["loop"] == "liq_ws"


def test_repair_cursor_advance_commits():
    conn = _Conn()
    hl_candles_repair.advance_cursor(conn, 1_700_000_000_000)
    assert [e[0] for e in conn.events] == ["execute", "commit"]
    assert conn.events[0][1] == status.UPSERT_SUCCESS_SQL
