"""Tests for collector/discovery.py — union qualification with hysteresis over a keep window."""

import importlib.util
import re
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import discovery
import pytest
from config import load_discovery_config
from discovery import keep_window_start, qualify_assets

ENTRY = 1_000_000
KEEP = 500_000
ROOT = Path(__file__).resolve().parents[1]


def test_new_asset_enters_at_threshold():
    assert qualify_assets({"A": ENTRY}, {}, set(), ENTRY, KEEP) == {"A"}


def test_inactive_asset_between_keep_and_entry_stays_out():
    assert qualify_assets({"A": 900_000}, {}, set(), ENTRY, KEEP) == set()


def test_active_asset_between_keep_and_entry_is_kept():
    assert qualify_assets({"A": 600_000}, {}, {"A"}, ENTRY, KEEP) == {"A"}


def test_active_asset_below_keep_is_dropped():
    assert qualify_assets({"A": 400_000}, {}, {"A"}, ENTRY, KEEP) == set()


def test_keep_equal_to_entry_with_no_history_is_a_single_threshold():
    volumes = {"active_dip": 900_000, "active_high": 1_200_000, "new_dip": 900_000}
    assert qualify_assets(volumes, {}, {"active_dip", "active_high"}, ENTRY, ENTRY) == {"active_high"}


def test_active_asset_on_a_weekend_dip_is_kept_by_the_window_max():
    assert qualify_assets({"A": 300_000}, {"A": 800_000}, {"A"}, ENTRY, KEEP) == {"A"}


def test_active_asset_whose_window_max_is_below_keep_is_dropped():
    assert qualify_assets({"A": 300_000}, {"A": 400_000}, {"A"}, ENTRY, KEEP) == set()


def test_entry_reads_tonight_only_not_the_window():
    assert qualify_assets({"A": 900_000}, {"A": 2_000_000}, set(), ENTRY, KEEP) == set()


def test_asset_no_venue_lists_tonight_never_qualifies():
    assert qualify_assets({}, {"A": 2_000_000}, {"A"}, ENTRY, KEEP) == set()


def test_keep_window_start_spans_tonight_and_the_previous_runs():
    assert keep_window_start(date(2026, 10, 18), 7) == date(2026, 10, 12)
    assert keep_window_start(date(2026, 10, 18), 1) == date(2026, 10, 18)


def test_shipped_config_keeps_on_a_seven_night_window():
    assert load_discovery_config()["volume_keep_window_days"] == 7


def _discovery_volumes_ddl(path):
    match = re.search(r"CREATE TABLE IF NOT EXISTS discovery_volumes \(.*?\n\);", path.read_text(), re.DOTALL)
    assert match, f"{path.name}: discovery_volumes CREATE TABLE not found"
    return match.group(0)


def test_init_sql_and_migration_012_create_the_same_discovery_volumes_table():
    init = _discovery_volumes_ddl(ROOT / "db" / "init.sql")
    migration = _discovery_volumes_ddl(ROOT / "db" / "migrations" / "012-discovery-volumes.sql")
    assert init == migration
    assert "PRIMARY KEY (run_date, asset_id)" in init


def _real_status_module():
    """conftest stubs `status` for host runs; load the real file (stdlib imports only) to pin its SQL."""
    path = ROOT / "collector" / "status.py"
    spec = importlib.util.spec_from_file_location("status_real", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_collector_success_write_never_reactivates_a_pair():
    """Hysteresis reads `active`, so only discovery may set it: a collector cycle holding a pre-discovery
    pair list must not resurrect a pair discovery just dropped (it would then re-enter at the keep threshold)."""
    on_conflict = _real_status_module().UPSERT_SUCCESS_SQL.split("DO UPDATE SET", 1)[1]
    assert not re.search(r"\bactive\s*=", on_conflict)


# --- discover() wiring: status/db are conftest stubs, so each run patches fresh mocks -----------

RUN_DATE = date(2026, 10, 18)


def _config(**overrides):
    return {"volume_threshold_usd": ENTRY, "volume_keep_threshold_usd": KEEP, "volume_keep_window_days": 7, **overrides}


class _FixedClock:
    @staticmethod
    def now(tz=None):
        assert tz is timezone.utc
        return datetime(2026, 10, 18, 2, 0, tzinfo=timezone.utc)


def _venue(name):
    return SimpleNamespace(name=name, market_type="perp", timeframes=[SimpleNamespace(name="5m"), SimpleNamespace(name="1h")])


def _ticker(asset_id, volume):
    return {"asset_id": asset_id, "symbol": f"{asset_id}/USDT:USDT", "market_type": "perp",
            "native_symbol": f"{asset_id}USDT", "volume": volume}


def _tickers(venue):
    """OLD is active and dips to $300k tonight; NEW enters at $2M (its max across venues)."""
    return {
        "binance": [_ticker("OLD", 300_000), _ticker("NEW", 2_000_000)],
        "bybit": [_ticker("NEW", 1_500_000)],
    }[venue]


def _run_discover(discovery_config, recent_max_volumes, record_error=None, query_error=None):
    """Run discover() with every IO patched. `calls.mock_calls` is the ordered trace of status/db calls,
    connection commits and close included (`conn` is `calls.conn`)."""
    calls = MagicMock()
    conn = calls.conn
    calls.record.side_effect = record_error
    calls.query_recent.side_effect = query_error
    calls.query_recent.return_value = recent_max_volumes
    calls.query_active.return_value = {"OLD"}
    calls.deactivate.return_value = 0
    exit_code = None
    with (
        patch.object(discovery, "datetime", _FixedClock),
        patch.object(discovery, "load_all_configs", return_value=[_venue("binance"), _venue("bybit")]),
        patch.object(discovery, "load_discovery_config", return_value=discovery_config),
        patch.object(discovery, "create_exchange"),
        patch.object(discovery, "fetch_exchange_tickers", side_effect=lambda _ex, config: _tickers(config.name)),
        patch.object(discovery, "build_dsn"),
        patch.object(discovery, "connect_db", return_value=conn),
        patch.object(discovery, "safe_rollback", calls.rollback),
        patch.object(discovery, "record_discovery_volumes", calls.record),
        patch.object(discovery, "query_recent_max_volumes", calls.query_recent),
        patch.object(discovery, "query_active_asset_ids", calls.query_active),
        patch.object(discovery, "upsert_discovery_pair", calls.upsert),
        patch.object(discovery, "deactivate_stale_pairs", calls.deactivate),
    ):
        try:
            discovery.discover()
        except SystemExit as exc:
            exit_code = exc.code
    return calls, conn, exit_code


def _upsert(conn, asset_id, exchange, timeframe, volume):
    return call.upsert(conn, exchange=exchange, market_type="perp", symbol=f"{asset_id}/USDT:USDT",
                       timeframe=timeframe, asset_id=asset_id, native_symbol=f"{asset_id}USDT", volume=volume)


def _upserted(calls):
    return {(c.kwargs["asset_id"], c.kwargs["exchange"]) for c in calls.upsert.call_args_list}


def test_discover_keeps_an_active_asset_on_history_and_records_before_any_upsert():
    calls, conn, exit_code = _run_discover(_config(), recent_max_volumes={"OLD": 800_000})

    assert exit_code is None
    both, new_only = ["NEW/USDT:USDT", "OLD/USDT:USDT"], ["NEW/USDT:USDT"]
    assert calls.mock_calls == [
        call.record(conn, RUN_DATE, {"OLD": 300_000, "NEW": 2_000_000}),
        call.query_recent(conn, date(2026, 10, 12)),
        call.query_active(conn),
        _upsert(conn, "NEW", "binance", "5m", 2_000_000),
        _upsert(conn, "NEW", "binance", "1h", 2_000_000),
        _upsert(conn, "NEW", "bybit", "5m", 1_500_000),
        _upsert(conn, "NEW", "bybit", "1h", 1_500_000),
        _upsert(conn, "OLD", "binance", "5m", 300_000),
        _upsert(conn, "OLD", "binance", "1h", 300_000),
        call.conn.commit(),
        call.deactivate(conn, "binance", "5m", both),
        call.deactivate(conn, "binance", "1h", both),
        call.deactivate(conn, "bybit", "5m", new_only),
        call.deactivate(conn, "bybit", "1h", new_only),
        call.conn.commit(),
        call.conn.close(),
    ]


@pytest.mark.parametrize("window", ["absent", 0])
def test_discover_without_a_usable_window_reads_tonight_only(window):
    config = _config(volume_keep_window_days=window)
    if window == "absent":
        del config["volume_keep_window_days"]
    calls, conn, _exit_code = _run_discover(config, recent_max_volumes={})

    calls.query_recent.assert_called_once_with(conn, RUN_DATE)
    assert _upserted(calls) == {("NEW", "binance"), ("NEW", "bybit")}


@pytest.mark.parametrize("bad", ["7", 7.5, True, None])
def test_discover_rejects_a_malformed_window(bad):
    with pytest.raises(ValueError, match="volume_keep_window_days"):
        _run_discover(_config(volume_keep_window_days=bad), recent_max_volumes={})


@pytest.mark.parametrize("failing", ["record", "query"])
def test_discover_history_failure_rolls_back_before_touching_the_universe(failing):
    error = RuntimeError('relation "discovery_volumes" does not exist')
    calls, conn, exit_code = _run_discover(_config(), {}, **{f"{failing}_error": error})

    assert exit_code == 1
    calls.rollback.assert_called_once_with(conn)
    conn.close.assert_called_once()
    conn.commit.assert_not_called()
    calls.upsert.assert_not_called()
    calls.deactivate.assert_not_called()


# --- fetch_exchange_tickers: venues without fetchTickers ---------------------------


class _TickerExchange:
    def __init__(self, has_fetch_tickers):
        self.has = {"fetchTickers": has_fetch_tickers}
        self.markets = {"BTC/USD:USD": {"base": "BTC", "swap": True, "id": "BTC-USD"}}
        self.calls = 0

    def fetch_tickers(self, params=None):
        self.calls += 1
        return {"BTC/USD:USD": {"quoteVolume": 5_000_000}}


def _perp_config(name):
    return SimpleNamespace(name=name, market_type="perp", ccxt_class=name)


def test_venue_without_fetch_tickers_is_skipped_before_any_call(caplog):
    exchange = _TickerExchange(has_fetch_tickers=False)
    with caplog.at_level("INFO", logger="discovery"):
        assert discovery.fetch_exchange_tickers(exchange, _perp_config("dydx")) == []
    assert exchange.calls == 0
    assert "[dydx] ccxt has no fetchTickers" in caplog.text
    assert "Traceback" not in caplog.text and not any(r.exc_info for r in caplog.records)


def test_venue_with_fetch_tickers_is_still_fetched():
    exchange = _TickerExchange(has_fetch_tickers=True)
    rows = discovery.fetch_exchange_tickers(exchange, _perp_config("binance"))
    assert exchange.calls == 1
    assert [r["asset_id"] for r in rows] == ["BTC"]
