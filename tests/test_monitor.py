"""Tests for collector/monitor.py — pure function tests for staleness, gaps, dedup, webhooks."""

import json
import time

from monitor import (
    build_webhook_payload,
    compute_staleness,
    format_gap_alert,
    format_stale_alert,
    should_alert,
)

TF_INTERVALS = {"5m": 300, "15m": 900, "1h": 3600, "1d": 86400}


# --- compute_staleness ---


def test_compute_staleness_none_stale():
    rows = [
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 100, "error_count": 0},
        {"exchange": "binance", "symbol": "ETH/USDT:USDT", "timeframe": "5m", "seconds_since_success": 500, "error_count": 0},
    ]
    assert compute_staleness(rows, TF_INTERVALS) == []


def test_compute_staleness_one_stale():
    # 5m threshold = 300 + max(300, 1800) = 2100s
    rows = [
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 100, "error_count": 0},
        {"exchange": "binance", "symbol": "ETH/USDT:USDT", "timeframe": "5m", "seconds_since_success": 2500, "error_count": 3},
    ]
    stale = compute_staleness(rows, TF_INTERVALS)
    assert len(stale) == 1
    assert stale[0]["symbol"] == "ETH/USDT:USDT"


def test_compute_staleness_null_last_success():
    rows = [
        {"exchange": "bybit", "symbol": "BTC/USDT:USDT", "timeframe": "1h", "seconds_since_success": None, "error_count": 0},
    ]
    stale = compute_staleness(rows, TF_INTERVALS)
    assert len(stale) == 1


def test_compute_staleness_at_threshold():
    """Exactly at threshold = not stale (needs >). 5m threshold = 2100s."""
    rows = [
        {"exchange": "okx", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 2100, "error_count": 0},
    ]
    assert compute_staleness(rows, TF_INTERVALS) == []


def test_compute_staleness_different_timeframes():
    """5m pair stale, 1d pair fine — only 5m returned.
    5m threshold = 2100s; 1d threshold = 86400 + max(86400, 1800) = 172800s.
    """
    rows = [
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 2500, "error_count": 0},
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "1d", "seconds_since_success": 50000, "error_count": 0},
    ]
    stale = compute_staleness(rows, TF_INTERVALS)
    assert len(stale) == 1
    assert stale[0]["timeframe"] == "5m"


def test_compute_staleness_unknown_timeframe_skipped():
    """Rows with timeframes not in the interval map are ignored."""
    rows = [
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "2m", "seconds_since_success": 1, "error_count": 0},
    ]
    assert compute_staleness(rows, TF_INTERVALS) == []


# --- format_stale_alert ---


def test_format_stale_alert_single():
    stale = [{"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 700, "error_count": 2}]
    text = format_stale_alert(stale)
    assert "1 stale" in text
    assert "binance" in text
    assert "BTC/USDT:USDT" in text
    assert "5m" in text


def test_format_stale_alert_multiple():
    stale = [
        {"exchange": "binance", "symbol": "BTC/USDT:USDT", "timeframe": "5m", "seconds_since_success": 700, "error_count": 0},
        {"exchange": "bybit", "symbol": "ETH/USDT:USDT", "timeframe": "1h", "seconds_since_success": None, "error_count": 5},
    ]
    text = format_stale_alert(stale)
    assert "2 stale" in text
    assert "never" in text


def test_format_stale_alert_empty():
    assert format_stale_alert([]) == ""


# --- format_gap_alert ---


def test_format_gap_alert():
    gaps = [
        {"symbol": "BTC/USDT:USDT", "gap_after": "2026-03-29T10:00:00+00:00", "gap_before": "2026-03-29T10:15:00+00:00", "missing_count": 2},
    ]
    text = format_gap_alert("binance", "5m", gaps)
    assert "1 candle gap" in text
    assert "binance" in text
    assert "BTC/USDT:USDT" in text


def test_format_gap_alert_empty():
    assert format_gap_alert("binance", "5m", []) == ""


# --- should_alert (dedup) ---


def test_should_alert_first_call():
    cooldowns = {}
    assert should_alert("test_key", cooldowns, 3600) is True
    assert "test_key" in cooldowns


def test_should_alert_within_cooldown():
    cooldowns = {"test_key": time.time()}
    assert should_alert("test_key", cooldowns, 3600) is False


def test_should_alert_after_cooldown():
    cooldowns = {"test_key": time.time() - 4000}
    assert should_alert("test_key", cooldowns, 3600) is True


# --- build_webhook_payload ---


def test_build_webhook_payload():
    payload = build_webhook_payload("hello")
    data = json.loads(payload)
    assert data["text"] == "hello"
    assert data["content"] == "hello"
    assert isinstance(payload, bytes)
