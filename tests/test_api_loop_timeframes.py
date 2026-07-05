"""Regression guard for the .1→.2 handoff: the API must exclude EVERY per-loop liveness
sentinel from its candle-facing `/status` queries. `api/main.py` can't be imported here
(FastAPI isn't in the collector test env), so parse its `LOOP_TIMEFRAMES` literal from source
and assert it matches `config.LOOP_INTERVALS` keys exactly — the invariant that would silently
drift if a new loop pseudo-TF (like `liq_ws`) is added to the collector but not the API.
"""

import ast
from pathlib import Path

from config import LOOP_INTERVALS

_API_MAIN = Path(__file__).resolve().parents[1] / "api" / "main.py"


def _api_loop_timeframes() -> set[str]:
    tree = ast.parse(_API_MAIN.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "LOOP_TIMEFRAMES" for t in node.targets
        ):
            return set(ast.literal_eval(node.value))
    raise AssertionError("LOOP_TIMEFRAMES not found in api/main.py")


def test_api_loop_timeframes_match_config_loop_intervals():
    assert _api_loop_timeframes() == set(LOOP_INTERVALS)  # includes funding, oi, liq_ws


def test_api_excludes_liq_ws_sentinel():
    assert "liq_ws" in _api_loop_timeframes()


def test_api_excludes_premium_sentinel():
    # T-010: the premium poll's liveness sentinel must not leak into candle-facing /status.
    assert "premium" in _api_loop_timeframes()


def test_api_excludes_candle_ws_and_backfill_sentinels():
    # T-015.1: HL 1m is streamed, not polled — its two loop sentinels are its liveness surface and
    # must be excluded from candle-facing /status (no per-pair HL 1m status rows exist).
    tfs = _api_loop_timeframes()
    assert "candle_ws" in tfs
    assert "candle_backfill" in tfs
