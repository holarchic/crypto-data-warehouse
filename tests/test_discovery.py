"""Tests for collector/discovery.py — union qualification with hysteresis over a keep window."""

import importlib.util
import re
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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


def test_keep_equal_to_entry_is_a_single_threshold():
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
CONFIG = {"volume_threshold_usd": ENTRY, "volume_keep_threshold_usd": KEEP, "volume_keep_window_days": 7}


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


# OLD is active and dips to $300k tonight; NEW enters at $2M (its max across venues).
TICKERS = {
    "binance": [_ticker("OLD", 300_000), _ticker("NEW", 2_000_000)],
    "bybit": [_ticker("NEW", 1_500_000)],
}


def _run_discover(discovery_config, recent_max_volumes, record_error=None, query_error=None):
    """Run discover() with every IO patched. `calls` records the order of status/db calls."""
    calls = MagicMock()
    conn = MagicMock()
    calls.record.side_effect = record_error
    calls.query_recent.side_effect = query_error
    calls.query_recent.return_value = recent_max_volumes
    calls.query_active.return_value = {"OLD"}
    exit_code = None
    with (
        patch.object(discovery, "datetime", _FixedClock),
        patch.object(discovery, "load_all_configs", return_value=[_venue("binance"), _venue("bybit")]),
        patch.object(discovery, "load_discovery_config", return_value=discovery_config),
        patch.object(discovery, "create_exchange"),
        patch.object(discovery, "fetch_exchange_tickers", side_effect=lambda _ex, config: TICKERS[config.name]),
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


def _upserted(calls):
    return {(c.kwargs["asset_id"], c.kwargs["exchange"]) for c in calls.upsert.call_args_list}


def test_discover_keeps_an_active_asset_on_history_and_records_before_any_upsert():
    calls, conn, exit_code = _run_discover(CONFIG, recent_max_volumes={"OLD": 800_000})

    assert exit_code is None
    names = [c[0] for c in calls.mock_calls]
    assert names.index("record") < names.index("query_recent") < names.index("upsert")
    calls.record.assert_called_once_with(conn, RUN_DATE, {"OLD": 300_000, "NEW": 2_000_000})
    calls.query_recent.assert_called_once_with(conn, date(2026, 10, 12))
    assert _upserted(calls) == {("OLD", "binance"), ("NEW", "binance"), ("NEW", "bybit")}
    assert conn.commit.call_count == 2


def test_discover_without_a_window_key_reads_tonight_only():
    config = {k: v for k, v in CONFIG.items() if k != "volume_keep_window_days"}
    calls, conn, _exit_code = _run_discover(config, recent_max_volumes={})

    calls.query_recent.assert_called_once_with(conn, RUN_DATE)
    assert _upserted(calls) == {("NEW", "binance"), ("NEW", "bybit")}


@pytest.mark.parametrize("failing", ["record", "query"])
def test_discover_history_failure_rolls_back_before_touching_the_universe(failing):
    error = RuntimeError('relation "discovery_volumes" does not exist')
    calls, conn, exit_code = _run_discover(CONFIG, {}, **{f"{failing}_error": error})

    assert exit_code == 1
    calls.rollback.assert_called_once_with(conn)
    conn.close.assert_called_once()
    conn.commit.assert_not_called()
    calls.upsert.assert_not_called()
    calls.deactivate.assert_not_called()
