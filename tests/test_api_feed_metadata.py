"""api/main.py reads its feed metadata from collector/config.py (copied into the api image beside
it) instead of mirroring it, and names /status's staleness rule. Read with `ast`: the test harness
stubs `main` and the db modules (tests/conftest.py), and api/main.py can't be imported beside
collector/main.py. tests/integration/check_api_metadata.sh runs the same assertions inside the built
image."""

import ast
from pathlib import Path

import config

API_MAIN = Path(__file__).resolve().parents[1] / "api" / "main.py"
TREE = ast.parse(API_MAIN.read_text())
DERIVED = {"TIMEFRAME_INTERVALS", "CANDLE_TABLE_BY_TIMEFRAME", "LOOP_TIMEFRAMES"}


def _assignment(name):
    for node in TREE.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return node.value
    raise AssertionError(f"{name} is not assigned in api/main.py")


def _function(name):
    node = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == name)
    namespace: dict = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(API_MAIN), "exec"), namespace)  # noqa: S102
    return namespace[name]


def test_no_feed_metadata_is_written_out_as_a_literal():
    imports = {alias.name for n in TREE.body if isinstance(n, ast.Import) for alias in n.names}
    assert "config" in imports
    for node in TREE.body:
        if isinstance(node, ast.Assign):
            names = {t.id for t in node.targets if isinstance(t, ast.Name)}
            if names & DERIVED:
                assert not isinstance(node.value, ast.Dict | ast.Tuple | ast.List | ast.Set), ast.unparse(node)


def test_intervals_and_loop_names_come_from_config():
    assert ast.unparse(_assignment("TIMEFRAME_INTERVALS")) == "config.load_timeframe_intervals() | RETIRED_TIMEFRAME_INTERVALS"
    assert ast.unparse(_assignment("LOOP_TIMEFRAMES")) == "tuple(config.LOOP_INTERVALS)"
    assert ast.unparse(_assignment("_CANDLE_TABLES")) == "config.load_candle_tables()"


def test_retired_timeframes_are_not_collected():
    """A timeframe back in exchanges.yaml must leave the retired list, or its interval has two owners."""
    retired = ast.literal_eval(_assignment("RETIRED_TIMEFRAME_INTERVALS"))
    assert retired == {"1d": 86400, "3d": 259200, "1w": 604800}
    assert not set(retired) & set(config.load_timeframe_intervals())


def test_status_staleness_rule():
    stale = _function("_status_is_stale")
    assert stale(None, None) is False and stale(10_000, None) is False   # unknown timeframe
    assert stale(None, 60) is True                                        # never succeeded
    assert stale(0, 60) is False
    assert stale(120, 60) is False                                        # exactly two intervals
    assert stale(121, 60) is True


def test_status_sets_is_stale_through_the_named_rule():
    get_status = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == "get_status")
    assigns = [n for n in ast.walk(get_status) if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Subscript) and ast.unparse(t.slice) == "'is_stale'" for t in n.targets)]
    assert len(assigns) == 1
    call = assigns[0].value
    assert isinstance(call, ast.Call) and ast.unparse(call.func) == "_status_is_stale"


def test_every_configured_interval_is_a_positive_whole_number_of_seconds():
    """The api and the monitor take these from exchanges.yaml as they are; a quoted, null or zero
    interval would break /status, the gap check and the monitor's staleness alike."""
    intervals = config.load_timeframe_intervals()
    assert intervals
    for timeframe, seconds in intervals.items():
        assert type(seconds) is int and seconds > 0, (timeframe, seconds)


def test_candle_table_routes_through_config():
    candle_table = _function("_candle_table")
    routes = config.load_candle_tables()
    candle_table.__globals__["_CANDLE_TABLES"] = routes
    for timeframe, table in routes.items():
        assert candle_table(timeframe) == table
    assert candle_table("1m") == "candles_1m"
    assert candle_table("1d") == "candles"            # retired: stored in the default table
