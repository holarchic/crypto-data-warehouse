"""api/main.py reads its feed metadata from collector/config.py and its staleness and gap rules from
collector/feed_health.py (both copied into the api image beside it) instead of mirroring them. Read with `ast`: the test harness
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


def _get(name):
    return next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == name)


def test_main_imports_feed_health_and_no_name_shadows_it():
    """`main` also defines a `health()` endpoint; the module's own name keeps the two apart."""
    imports = {alias.asname or alias.name for n in TREE.body if isinstance(n, ast.Import) for alias in n.names}
    assert "feed_health" in imports
    defined = {n.name for n in TREE.body if isinstance(n, ast.FunctionDef | ast.ClassDef)}
    defined |= {t.id for n in TREE.body if isinstance(n, ast.Assign) for t in n.targets if isinstance(t, ast.Name)}
    assert "feed_health" not in defined


def test_status_sets_is_stale_through_the_named_rule():
    assigns = [n for n in ast.walk(_get("get_status")) if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Subscript) and ast.unparse(t.slice) == "'is_stale'" for t in n.targets)]
    assert len(assigns) == 1
    call = assigns[0].value
    assert isinstance(call, ast.Call) and ast.unparse(call.func) == "feed_health.is_stale_for_status"


def test_status_gaps_runs_the_shared_gap_query():
    gaps = _get("get_status_gaps")
    calls = [ast.unparse(n.func) for n in ast.walk(gaps) if isinstance(n, ast.Call)]
    assert "feed_health.gap_query" in calls
    assert "LEAD(" not in ast.unparse(gaps)
    assert "LEAD(" not in API_MAIN.read_text()


def test_candle_table_routes_through_config():
    candle_table = _function("_candle_table")
    routes = config.load_candle_tables()
    candle_table.__globals__["_CANDLE_TABLES"] = routes
    for timeframe, table in routes.items():
        assert candle_table(timeframe) == table
    assert candle_table("1m") == "candles_1m"
    assert candle_table("1d") == "candles"            # retired: stored in the default table
