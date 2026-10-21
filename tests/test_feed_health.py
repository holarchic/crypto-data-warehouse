"""Tests for collector/feed_health.py: the two staleness rules and the shared gap query."""

import pytest
from config import CANDLE_TABLES
from feed_health import gap_query, is_stale_for_paging, is_stale_for_status, paging_threshold_s


@pytest.mark.parametrize(("interval_s", "threshold"), [(60, 1860), (300, 2100), (3600, 7200), (64800, 129600)])
def test_paging_threshold_floors_short_intervals_at_30_minutes(interval_s, threshold):
    assert paging_threshold_s(interval_s) == threshold


def test_paging_rule_boundaries():
    assert is_stale_for_paging(None, None) is False and is_stale_for_paging(10**9, None) is False
    assert is_stale_for_paging(None, 300) is True                 # never succeeded
    assert is_stale_for_paging(0, 300) is False
    assert is_stale_for_paging(2100, 300) is False                # exactly at the threshold
    assert is_stale_for_paging(2101, 300) is True


def test_status_rule_boundaries():
    assert is_stale_for_status(None, None) is False and is_stale_for_status(10**9, None) is False
    assert is_stale_for_status(None, 60) is True
    assert is_stale_for_status(0, 60) is False
    assert is_stale_for_status(120, 60) is False                  # exactly two intervals
    assert is_stale_for_status(121, 60) is True


def test_status_rule_is_stricter_than_paging_below_the_floor():
    """A 1m pair five minutes behind is stale to a /status reader, not to the monitor."""
    assert is_stale_for_status(300, 60) is True
    assert is_stale_for_paging(300, 60) is False


@pytest.mark.parametrize("table", CANDLE_TABLES)
def test_gap_query_reads_its_table_without_an_asset_filter(table):
    sql, params = gap_query(table, exchange="binance", timeframe="5m", interval_s=300, lookback_count=10)
    assert f"FROM {table}\n" in sql
    assert "asset_id" not in sql
    assert params == {"exchange": "binance", "timeframe": "5m", "lookback_s": 3000, "interval_s": 300}


@pytest.mark.parametrize("table", CANDLE_TABLES)
def test_gap_query_filters_by_asset_only_when_given_one(table):
    sql, params = gap_query(table, exchange="binance", timeframe="1m", interval_s=60, lookback_count=5,
                            asset_id="BTC")
    assert f"FROM {table}\n" in sql
    assert "AND asset_id = %(asset_id)s" in sql
    assert params == {"exchange": "binance", "timeframe": "1m", "lookback_s": 300, "interval_s": 60,
                      "asset_id": "BTC"}


def test_gap_query_keeps_an_empty_asset_id_as_a_filter():
    sql, params = gap_query("candles", exchange="x", timeframe="5m", interval_s=300, lookback_count=1, asset_id="")
    assert "asset_id = %(asset_id)s" in sql and params["asset_id"] == ""


def test_gap_query_rejects_a_table_outside_the_allowlist():
    with pytest.raises(KeyError):
        gap_query("funding_rates", exchange="x", timeframe="5m", interval_s=300, lookback_count=1)
