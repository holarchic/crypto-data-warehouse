"""Tests for collector/symbols.py — resolve_asset_id."""

from symbols import resolve_asset_id

# --- Override path ---

def test_override_exact_match():
    assert resolve_asset_id("BCHSV", {"BCHSV": "BSV"}) == "BSV"


def test_override_takes_priority_over_multiplier():
    """Even if the base looks like a multiplier, override wins."""
    assert resolve_asset_id("1000SHIB", {"1000SHIB": "SHIBOVERRIDE"}) == "SHIBOVERRIDE"


def test_override_missing_key_falls_through():
    assert resolve_asset_id("ETH", {"BCHSV": "BSV"}) == "ETH"


# --- Multiplier strip path ---

def test_multiplier_strip_1000():
    assert resolve_asset_id("1000SHIB") == "SHIB"


def test_multiplier_strip_10():
    assert resolve_asset_id("10PEPE") == "PEPE"


def test_multiplier_strip_1():
    assert resolve_asset_id("1MBABYDOGE") == "MBABYDOGE"


def test_multiplier_no_match_alpha_prefix():
    """Pure alpha string should not trigger strip."""
    assert resolve_asset_id("BTC") == "BTC"


def test_multiplier_no_match_pure_digits():
    """Pure digits — no alpha remainder, no match."""
    assert resolve_asset_id("12345") == "12345"


# --- Passthrough path ---

def test_passthrough_standard():
    assert resolve_asset_id("ETH") == "ETH"


def test_passthrough_none_overrides():
    assert resolve_asset_id("SOL", None) == "SOL"
