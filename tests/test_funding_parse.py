"""Tests for funding._parse_funding_record — timestamp semantics + interval math."""

from funding import DEFAULT_FUNDING_INTERVAL, FUNDING_INTERVAL_HOURS, _parse_funding_record

PAIR = {"asset_id": "BTC", "symbol": "BTC/USDT:USDT"}


# --- Bulk endpoint (from_bulk=True) ---

def test_bulk_subtracts_interval_from_funding_timestamp():
    """Bulk: fundingTimestamp is NEXT settlement → subtract interval to get last."""
    record = {"fundingTimestamp": 1700000000000, "fundingRate": 0.0001, "nextFundingRate": 0.0002}
    result = _parse_funding_record(record, "binance", PAIR, from_bulk=True)
    expected_ts = (1700000000000 - DEFAULT_FUNDING_INTERVAL * 3600 * 1000) / 1000
    assert result["timestamp"] == expected_ts
    assert result["rate"] == 0.0001
    assert result["predicted_rate"] == 0.0002


def test_bulk_hyperliquid_1h_interval():
    """HL has 1h funding interval — should subtract 1h, not 8h."""
    record = {"fundingTimestamp": 1700000000000, "fundingRate": 0.0005, "nextFundingRate": None}
    result = _parse_funding_record(record, "hyperliquid", PAIR, from_bulk=True)
    expected_ts = (1700000000000 - 1 * 3600 * 1000) / 1000
    assert result["timestamp"] == expected_ts
    assert result["interval_hours"] == 1


def test_bulk_none_funding_timestamp_returns_none():
    record = {"fundingTimestamp": None, "fundingRate": 0.0001}
    assert _parse_funding_record(record, "binance", PAIR, from_bulk=True) is None


# --- History endpoint (from_bulk=False, default) ---

def test_history_uses_timestamp_directly():
    """History: timestamp = actual settlement time."""
    record = {"timestamp": 1700000000000, "fundingRate": -0.0003, "nextFundingRate": None}
    result = _parse_funding_record(record, "binance", PAIR)
    assert result["timestamp"] == 1700000000000 / 1000
    assert result["rate"] == -0.0003


def test_history_none_timestamp_returns_none():
    record = {"timestamp": None, "fundingRate": 0.0001}
    assert _parse_funding_record(record, "binance", PAIR) is None


# --- Common fields ---

def test_output_fields_complete():
    record = {"timestamp": 1700000000000, "fundingRate": 0.0001, "nextFundingRate": 0.0002}
    result = _parse_funding_record(record, "bybit", PAIR)
    assert set(result.keys()) == {"exchange", "asset_id", "symbol", "timestamp", "rate", "predicted_rate", "interval_hours"}
    assert result["exchange"] == "bybit"
    assert result["asset_id"] == "BTC"
    assert result["symbol"] == "BTC/USDT:USDT"


def test_default_interval_is_8h():
    assert DEFAULT_FUNDING_INTERVAL == 8


def test_hyperliquid_interval_is_1h():
    assert FUNDING_INTERVAL_HOURS.get("hyperliquid") == 1
