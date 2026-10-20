"""Tests for collector/status.py: helpers run inside the caller's transaction, and
`active_instruments` is the one reader of a venue's instrument universe."""

import re
from datetime import date
from pathlib import Path

import pytest
import status
from status import UNIVERSE_TIMEFRAME, Instrument, active_instruments


class RecordingConn:
    """Records every execute and every transaction call; fetchall answers `rows`."""

    rowcount = 0
    description = ()

    def __init__(self, rows=()):
        self.rows = list(rows)
        self.events = []

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.events.append(("execute", sql, params))

    def executemany(self, sql, rows):
        self.events.append(("executemany", sql, rows))

    def fetchall(self):
        return self.rows

    def commit(self):
        self.events.append(("commit",))

    def rollback(self):
        self.events.append(("rollback",))


HELPERS = [
    lambda c: status.upsert_status_success(c, "binance", "perp", "BTC/USDT:USDT", "5m", 1.0, asset_id="BTC"),
    lambda c: status.upsert_status_error(c, "binance", "perp", "BTC/USDT:USDT", "5m", "boom"),
    lambda c: status.upsert_loop_heartbeat(c, "binance", "perp", "oi"),
    lambda c: status.upsert_discovery_pair(c, "binance", "perp", "BTC/USDT:USDT", "5m", "BTC", "BTCUSDT", 1.0),
    lambda c: status.deactivate_stale_pairs(c, "binance", "5m", ["BTC/USDT:USDT"]),
    lambda c: status.record_discovery_volumes(c, date(2026, 10, 20), {"BTC": 1.0}),
    lambda c: status.query_active_asset_ids(c),
    lambda c: status.query_recent_max_volumes(c, date(2026, 10, 13)),
    lambda c: status.query_active_pairs(c, "binance", "5m"),
    lambda c: status.active_instruments(c, "binance"),
]


@pytest.mark.parametrize("helper", HELPERS, ids=[
    "success", "error", "heartbeat", "discovery_pair", "deactivate", "record_volumes",
    "asset_ids", "recent_max", "active_pairs", "active_instruments"])
def test_no_helper_ends_the_callers_transaction(helper):
    conn = RecordingConn()
    helper(conn)
    kinds = [event[0] for event in conn.events]
    assert kinds and set(kinds) <= {"execute", "executemany"}


def test_status_module_never_commits_or_rolls_back():
    source = Path(status.__file__).read_text()
    assert not re.search(r"\.(commit|rollback)\(", source)


def test_active_instruments_reads_the_universe_timeframe_as_instruments():
    conn = RecordingConn(rows=[("BTC/USDT:USDT", "BTC", "BTCUSDT", "perp"),
                               ("ETH/USDT:USDT", None, None, "perp")])
    instruments = active_instruments(conn, "binance")
    assert instruments == [Instrument("BTC/USDT:USDT", "BTC", "BTCUSDT", "perp"),
                           Instrument("ETH/USDT:USDT", None, None, "perp")]
    (_, sql, params), = conn.events
    assert params == {"exchange": "binance", "timeframe": UNIVERSE_TIMEFRAME}
    assert "last_timestamp" not in sql
    assert UNIVERSE_TIMEFRAME == "5m"


def test_instrument_is_immutable():
    instrument = Instrument("BTC/USDT:USDT", "BTC", "BTCUSDT", "perp")
    with pytest.raises(AttributeError):
        instrument.symbol = "ETH/USDT:USDT"


def test_collector_success_write_never_reactivates_a_pair():
    """Hysteresis reads `active`, so only discovery may set it: a collector cycle holding a pre-discovery
    pair list must not resurrect a pair discovery just dropped (it would then re-enter at the keep threshold)."""
    on_conflict = status.UPSERT_SUCCESS_SQL.split("DO UPDATE SET", 1)[1]
    assert not re.search(r"\bactive\s*=", on_conflict)
