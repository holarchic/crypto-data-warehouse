"""Instrument fixtures for consumer tests: only the fields a test names matter."""

from status import Instrument


def instrument(symbol="BTC/USDT:USDT", asset_id=None, native_symbol=None, market_type="perp") -> Instrument:
    return Instrument(symbol=symbol, asset_id=asset_id, native_symbol=native_symbol, market_type=market_type)
