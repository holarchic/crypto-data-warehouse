"""The API reports pyproject.toml's release version. `api/main.py` can't be imported beside
`collector/main.py`, so the wiring is read with `ast` (as in test_api_candle_tables.py) and the
version expression is evaluated against the repo's own layout."""

import ast
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API_MAIN = ROOT / "api" / "main.py"

RELEASE_VERSION_SOURCE = (
    'tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())["project"]["version"]'
)


def _tree() -> ast.Module:
    return ast.parse(API_MAIN.read_text())


def _release_version_value() -> ast.expr:
    for node in _tree().body:
        if isinstance(node, ast.Assign) and [t.id for t in node.targets if isinstance(t, ast.Name)] == [
            "RELEASE_VERSION"
        ]:
            return node.value
    raise AssertionError("RELEASE_VERSION not assigned at module level in api/main.py")


def test_release_version_reads_pyprojects_version():
    value = _release_version_value()

    assert ast.unparse(value) == ast.unparse(ast.parse(RELEASE_VERSION_SOURCE, mode="eval").body)
    version = eval(compile(ast.Expression(value), str(API_MAIN), "eval"), {
        "tomllib": tomllib, "Path": Path, "__file__": str(API_MAIN),
    })
    assert version == tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    assert re.fullmatch(r"\d+\.\d+\.\d+", version)


def test_fastapi_reports_the_release_version():
    calls = [
        node for node in ast.walk(_tree())
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "FastAPI"
    ]

    assert len(calls) == 1
    version = [kw.value for kw in calls[0].keywords if kw.arg == "version"]
    assert len(version) == 1
    assert isinstance(version[0], ast.Name) and version[0].id == "RELEASE_VERSION"
