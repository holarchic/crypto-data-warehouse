"""Tests for collector/status.py: helpers run inside the caller's transaction, and
`active_instruments` is the one reader of a venue's instrument universe."""

import re
from datetime import date
from pathlib import Path

import pytest
import status
from status import UNIVERSE_TIMEFRAME, Instrument, active_instruments

from tests.fakes import RecordingConn

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
    assert conn.kinds() and set(conn.kinds()) <= {"execute", "executemany"}


def test_status_module_never_commits_or_rolls_back():
    source = Path(status.__file__).read_text()
    assert not re.search(r"\.(commit|rollback)\(", source)


def test_active_instruments_reads_the_universe_timeframe_as_instruments():
    conn = RecordingConn(rows=[("BTC/USDT:USDT", "BTC", "BTCUSDT", "perp"),
                               ("ETH/USDT:USDT", None, None, "perp")])
    instruments = active_instruments(conn, "binance")
    assert instruments == [Instrument("BTC/USDT:USDT", "BTC", "BTCUSDT", "perp"),
                           Instrument("ETH/USDT:USDT", None, None, "perp")]
    (query,) = conn.events
    assert query.params == {"exchange": "binance", "timeframe": UNIVERSE_TIMEFRAME}
    assert "last_timestamp" not in query.sql
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


COLLECTOR = Path(status.__file__).parent
UNIVERSE_CONSUMERS = ["funding.py", "oi.py", "premium_index.py", "ws.py", "hl_candles_repair.py"]


def test_the_universe_has_one_reader():
    """Feeds take the venue's instruments from active_instruments; only the candle loop reads the
    per-timeframe rows (with their cursor) through query_active_pairs."""
    callers = {path.name for path in COLLECTOR.glob("*.py")
               if path.name != "status.py" and re.search(r"\bquery_active_pairs\(", path.read_text())}
    assert callers == {"candles.py"}
    for name in UNIVERSE_CONSUMERS:
        assert re.search(r"\bactive_instruments\(", (COLLECTOR / name).read_text()), name
