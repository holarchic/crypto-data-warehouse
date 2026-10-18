"""Tests for scripts/webhook-lib.sh, driven through real bash, curl and a local HTTP server."""

import json
import shutil
import socket
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

LIB = Path(__file__).resolve().parent.parent / "scripts" / "webhook-lib.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("curl") is None, reason="needs bash and curl"
)


def run_lib(script, arg, webhook_url=None):
    """Runs `script` after sourcing the library, under the scripts' own strict mode; $1 = arg."""
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"}
    if webhook_url is not None:
        env["WEBHOOK_URL"] = webhook_url
    return subprocess.run(
        ["bash", "-c", f'set -euo pipefail; . "{LIB}"; {script}', "lib", str(arg)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def loaded_url(tmp_path, env_line=None, webhook_url=None):
    if env_line is not None:
        (tmp_path / ".env").write_text(f"POSTGRES_USER=candles\n{env_line}\nAPI_PORT=8000\n")
    result = run_lib('load_webhook_url "$1"; printf "%s" "$WEBHOOK_URL"', tmp_path, webhook_url)
    assert result.returncode == 0, result.stderr
    return result.stdout


@pytest.mark.parametrize(
    "env_line",
    [
        # The prod .env shape: quoted, with a trailing space after the closing quote.
        'WEBHOOK_URL="https://hooks.example/abc" ',
        'WEBHOOK_URL="https://hooks.example/abc"',
        "WEBHOOK_URL=https://hooks.example/abc",
        "WEBHOOK_URL=  https://hooks.example/abc\t",
    ],
)
def test_load_webhook_url_trims_like_compose(tmp_path, env_line):
    assert loaded_url(tmp_path, env_line) == "https://hooks.example/abc"


def test_load_webhook_url_prefers_the_environment(tmp_path):
    url = loaded_url(tmp_path, 'WEBHOOK_URL="https://from-env-file"', webhook_url="https://from-env")
    assert url == "https://from-env"


def test_load_webhook_url_without_env_file_or_key(tmp_path):
    assert loaded_url(tmp_path) == ""
    assert loaded_url(tmp_path, "OTHER=1") == ""


class _Capture(BaseHTTPRequestHandler):
    bodies = []

    def do_POST(self):
        length = int(self.headers["Content-Length"])
        self.bodies.append(self.rfile.read(length))
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def webhook_server():
    _Capture.bodies = []
    server = HTTPServer(("127.0.0.1", 0), _Capture)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/hook", _Capture.bodies
    server.shutdown()


def test_webhook_post_sends_slack_and_discord_keys(webhook_server):
    url, bodies = webhook_server
    message = 'backup ($host): restic "check" failed \\ exit 1 🟠'
    result = run_lib('webhook_post "$1"', message, webhook_url=url)
    assert result.returncode == 0, result.stderr
    assert [json.loads(body) for body in bodies] == [{"text": message, "content": message}]


def test_webhook_post_reports_an_unreachable_webhook():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
    result = run_lib('webhook_post "$1"', "x", webhook_url=f"http://127.0.0.1:{closed_port}/hook")
    assert result.returncode != 0


def test_webhook_post_without_a_webhook_is_a_no_op():
    assert run_lib('webhook_post "$1"', "x", webhook_url="").returncode == 0
