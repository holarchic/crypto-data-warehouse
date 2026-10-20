"""Tests for deploy/host-monitor.sh's memory and container checks, driven through real bash
with a stub `docker`/`df` on PATH and a local webhook."""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from tests.test_webhook_lib import serve

SCRIPT = Path(__file__).resolve().parent.parent / "deploy" / "host-monitor.sh"
SERVICES = [
    "collector-hyperliquid", "collector-binance", "collector-bybit", "collector-okx",
    "collector-coinbase", "collector-dydx", "monitor", "api", "db",
]

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("curl") is None, reason="needs bash and curl"
)


class Host:
    """A fake host: meminfo, `compose ps` output, and the monitor's reminder files."""

    def __init__(self, root):
        self.root = root
        self.bin = root / "bin"
        self.bin.mkdir()
        # `compose ps` prints $FAKE_PS_FILE, or fails when it is absent.
        (self.bin / "docker").write_text('#!/bin/sh\n[ -f "$FAKE_PS_FILE" ] && cat "$FAKE_PS_FILE"\n')
        # `df` reports $FAKE_DF_PCT for every mount, or fails when it is unset.
        (self.bin / "df").write_text(
            '#!/bin/sh\n[ -n "$FAKE_DF_PCT" ] || exit 1\nprintf "Use%%\\n %s%%\\n" "$FAKE_DF_PCT"\n'
        )
        self.disk_pct = ""
        for stub in ("docker", "df"):
            (self.bin / stub).chmod(0o755)
        self.meminfo = root / "meminfo"
        self.ps = root / "ps"
        self.memory_alert = root / "memory-alert"
        self.container_alert = root / "container-alert"
        self.memory_mb(4000)
        self.running(SERVICES)

    def memory_mb(self, mb):
        self.meminfo.write_text(f"MemTotal: 8000000 kB\nMemAvailable: {mb * 1024} kB\n")

    def running(self, services):
        self.ps.write_text("".join(f"{s}\n" for s in services))

    def run(self, webhook_url):
        env = {
            "PATH": f"{self.bin}:/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
            "WEBHOOK_URL": webhook_url,
            "FAKE_PS_FILE": str(self.ps),
            "FAKE_DF_PCT": self.disk_pct,
            "MEMINFO_FILE": str(self.meminfo),
            "MEMORY_ALERT_FILE": str(self.memory_alert),
            "CONTAINER_ALERT_FILE": str(self.container_alert),
            "DISK_STATE_DIR": str(self.root / "disk"),
            "RESTIC_ENV_FILE": str(self.root / "no-restic.env"),
        }
        result = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True,
                                timeout=60)
        assert result.returncode == 0, result.stdout + result.stderr
        return result


@pytest.fixture
def host(tmp_path):
    return Host(tmp_path)


@pytest.fixture
def webhook():
    server, bodies = serve(204)
    yield f"http://127.0.0.1:{server.server_port}/hook", bodies
    server.shutdown()
    server.server_close()


def pages(bodies):
    sent = []
    while not bodies.empty():
        sent.append(json.loads(bodies.get())["content"])
    return sent


def test_healthy_host_sends_nothing(host, webhook):
    url, bodies = webhook
    host.run(url)
    assert pages(bodies) == []


def test_low_memory_does_not_skip_the_container_check(host, webhook):
    url, bodies = webhook
    host.memory_mb(300)
    host.running([s for s in SERVICES if s != "api"])
    host.run(url)
    sent = pages(bodies)
    assert len(sent) == 2
    assert "Available memory: 300MB" in sent[0]
    assert sent[1].endswith("Containers down: api")


def test_a_fresh_memory_reminder_does_not_mute_a_container_page(host, webhook):
    url, bodies = webhook
    host.memory_mb(300)
    host.memory_alert.touch()
    host.running([s for s in SERVICES if s != "db"])
    host.run(url)
    sent = pages(bodies)
    assert len(sent) == 1
    assert sent[0].endswith("Containers down: db")


def test_services_match_by_exact_name(host, webhook):
    url, bodies = webhook
    host.running([s for s in SERVICES if s not in ("api", "db")] + ["api-old", "dbx"])
    host.run(url)
    assert pages(bodies)[0].endswith("Containers down: api db")


def test_a_failed_compose_ps_pages_as_a_failed_probe(host, webhook):
    url, bodies = webhook
    host.ps.unlink()
    host.run(url)
    sent = pages(bodies)
    assert len(sent) == 1
    assert "Container check failed" in sent[0]
    assert "Containers down" not in sent[0]


def test_unreadable_meminfo_skips_memory_but_still_checks_containers(host, webhook):
    url, bodies = webhook
    host.meminfo.unlink()
    host.running(SERVICES[1:])
    result = host.run(url)
    assert "memory not checked" in result.stdout
    sent = pages(bodies)
    assert len(sent) == 1
    assert sent[0].endswith("Containers down: collector-hyperliquid")


def test_a_delivered_page_waits_an_hour_before_repeating(host, webhook):
    url, bodies = webhook
    host.memory_mb(300)
    host.run(url)
    assert len(pages(bodies)) == 1
    assert host.memory_alert.exists()
    host.run(url)
    assert pages(bodies) == []
    hour_ago = time.time() - 3601
    os.utime(host.memory_alert, (hour_ago, hour_ago))
    host.run(url)
    assert len(pages(bodies)) == 1


def test_an_undelivered_disk_escalation_is_retried_next_run(host, webhook):
    host.disk_pct = "80"
    failing, _ = serve(500)
    try:
        result = host.run(f"http://127.0.0.1:{failing.server_port}/hook")
    finally:
        failing.shutdown()
        failing.server_close()
    assert "page not delivered" in result.stdout
    assert list((host.root / "disk").iterdir()) == []
    url, bodies = webhook
    host.run(url)
    sent = pages(bodies)
    assert len(sent) == 2
    assert all("crossed 75%" in page for page in sent)
    host.run(url)
    assert pages(bodies) == []


def test_an_undelivered_page_is_retried_next_run(host, webhook):
    failing, _ = serve(500)
    try:
        host.running(SERVICES[:-1])
        result = host.run(f"http://127.0.0.1:{failing.server_port}/hook")
    finally:
        failing.shutdown()
        failing.server_close()
    assert "page not delivered" in result.stdout
    assert not host.container_alert.exists()
    url, bodies = webhook
    host.run(url)
    assert pages(bodies)[0].endswith("Containers down: db")
    assert host.container_alert.exists()
