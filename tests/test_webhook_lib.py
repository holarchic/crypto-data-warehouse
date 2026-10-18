"""Tests for scripts/webhook-lib.sh, driven through real bash, curl and a local HTTP server."""

import json
import os
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


def run_lib(script, *args, webhook_url=None):
    """Runs `script` after sourcing the library, under the scripts' own strict mode."""
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"}
    if webhook_url is not None:
        env["WEBHOOK_URL"] = webhook_url
    return subprocess.run(
        ["bash", "-c", f'set -euo pipefail; . "{LIB}"; {script}', "lib", *map(str, args)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def url_from_env_file(tmp_path, env_line=None, webhook_url=None):
    if env_line is not None:
        (tmp_path / ".env").write_text(f"POSTGRES_USER=candles\n{env_line}\nAPI_PORT=8000\n")
    result = run_lib('webhook_url "$1"', tmp_path, webhook_url=webhook_url)
    assert result.returncode == 0, result.stderr
    return result.stdout


@pytest.mark.parametrize(
    "env_line",
    [
        # The prod .env shape: quoted, with a trailing space after the closing quote.
        'WEBHOOK_URL="https://hooks.example/abc" ',
        'WEBHOOK_URL="https://hooks.example/abc"',
        "WEBHOOK_URL='https://hooks.example/abc'",
        "WEBHOOK_URL=https://hooks.example/abc",
        "WEBHOOK_URL=  https://hooks.example/abc\t",
        "WEBHOOK_URL=https://hooks.example/abc # discord",
        'WEBHOOK_URL="https://hooks.example/abc" # discord',
        "  WEBHOOK_URL = https://hooks.example/abc",
    ],
)
def test_webhook_url_reads_env_like_compose(tmp_path, env_line):
    assert url_from_env_file(tmp_path, env_line) == "https://hooks.example/abc"


def test_webhook_url_keeps_a_hash_inside_the_url(tmp_path):
    assert url_from_env_file(tmp_path, "WEBHOOK_URL=https://hooks.example/a#b") == (
        "https://hooks.example/a#b"
    )


def test_webhook_url_prefers_the_environment(tmp_path):
    url = url_from_env_file(tmp_path, 'WEBHOOK_URL="https://from-file"', webhook_url="https://from-env")
    assert url == "https://from-env"


def test_webhook_url_without_env_file_or_key(tmp_path):
    assert url_from_env_file(tmp_path) == ""
    assert url_from_env_file(tmp_path, "OTHER=1") == ""


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads any file")
def test_webhook_url_warns_on_an_unreadable_env_file(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text('WEBHOOK_URL="https://hooks.example/abc"\n')
    env_file.chmod(0)
    try:
        result = run_lib('webhook_url "$1"', tmp_path)
    finally:
        env_file.chmod(0o600)
    assert (result.returncode, result.stdout) == (0, "")
    assert "cannot read" in result.stderr


@pytest.fixture
def webhook_server():
    """A local webhook; yields its URL and the list of JSON bodies it received."""
    bodies = []

    class Capture(BaseHTTPRequestHandler):
        def do_POST(self):
            bodies.append(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Capture)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/hook", bodies
    server.shutdown()


@pytest.mark.parametrize(
    "message",
    [
        'backup ($host): restic "check" failed \\ exit 1 🟠',
        "line one\nline two\ttabbed\rreturn \x01\x1f",
    ],
)
def test_webhook_post_sends_slack_and_discord_keys(webhook_server, message):
    url, bodies = webhook_server
    result = run_lib('webhook_post "$1" "$2"', url, message)
    assert result.returncode == 0, result.stderr
    assert [json.loads(body) for body in bodies] == [{"text": message, "content": message}]


def test_webhook_post_reports_an_unreachable_webhook():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
    assert run_lib('webhook_post "$1" x', f"http://127.0.0.1:{closed_port}/hook").returncode == 1


def test_webhook_post_without_a_webhook_is_not_a_delivery():
    assert run_lib('webhook_post "" x').returncode == 2
