"""Symbol normalization — resolve asset_id from ccxt market base."""

import re


def resolve_asset_id(base: str, overrides: dict | None = None) -> str:
    """Map a ccxt market base to a canonical asset_id.

    Rules applied in order:
    1. YAML overrides (e.g. BCHSV → BSV)
    2. Multiplier strip: numeric prefix + alpha remainder → strip prefix (e.g. 1000SHIB → SHIB)
    3. Default: return base as-is (covers 95% — BTC, ETH, SOL, etc.)

    Multiplier does NOT affect OHLCV values — ccxt returns correct prices.
    asset_id is for grouping only.
    """
    if overrides and base in overrides:
        return overrides[base]

    match = re.match(r"^(\d+)([A-Za-z].+)$", base)
    if match:
        return match.group(2)

    return base
