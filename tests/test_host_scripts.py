"""Tests for the host scripts' compose calls and scripts/backup.sh's dump publication, driven
through real bash with a stub `docker` on PATH."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.test_webhook_lib import serve

ROOT = Path(__file__).resolve().parent.parent
HOST_SCRIPTS = sorted([*ROOT.glob("scripts/*.sh"), *ROOT.glob("deploy/*.sh")])

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("curl") is None, reason="needs bash and curl"
)

# Logs each call's argv as one line, then its stdin under "stdin:" when it is SQL for psql.
# pg_dump writes $FAKE_DUMP; pg_restore exits $FAKE_RESTORE_EXIT; psql -At prints a version.
STUB_DOCKER = r"""#!/bin/bash
echo "argv: $*" >> "$DOCKER_LOG"
case " $* " in
    *" pg_dump "*) printf '%s' "$FAKE_DUMP" ;;
    *" pg_restore "*) cat > /dev/null; exit "${FAKE_RESTORE_EXIT:-0}" ;;
    *" -Atc "*) echo 2.26.2 ;;
    *" psql "*) { echo "stdin:"; cat; } >> "$DOCKER_LOG" ;;
esac
"""


@pytest.mark.parametrize("script", HOST_SCRIPTS, ids=lambda p: p.name)
def test_every_compose_call_carries_the_prod_override(script):
    for number, line in enumerate(script.read_text().splitlines(), 1):
        if line.lstrip().startswith("#") or "docker compose" not in line:
            continue
        # `docker compose version` and quoted prose name compose without running a project.
        if re.search(r'docker compose version|"[^"]*docker compose[^"]*"', line):
            continue
        assert "docker-compose.prod.yml" in line, f"{script.name}:{number}: {line.strip()}"


@pytest.fixture
def host(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(STUB_DOCKER)
    (bin_dir / "docker").chmod(0o755)
    return tmp_path


def run_script(host, script, *args, **env):
    full_env = {
        "PATH": f"{host / 'bin'}:/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
        "DOCKER_LOG": str(host / "docker.log"),
        "WEBHOOK_URL": "",
        **env,
    }
    return subprocess.run(["bash", str(ROOT / script), *args], env=full_env, capture_output=True,
                          text=True, timeout=60)


def docker_calls(host):
    log = host / "docker.log"
    return [line[len("argv: "):] for line in log.read_text().splitlines()
            if line.startswith("argv: ")] if log.exists() else []


def assert_prod_override(calls):
    assert calls
    for call in calls:
        assert f"-f {ROOT}/docker-compose.yml -f {ROOT}/docker-compose.prod.yml" in call, call


def backup_env(host, **env):
    return {"BACKUP_DIR": str(host / "backups"), "BACKUP_LOCK_DIR": str(host / "backup.lock"),
            "FAKE_DUMP": "PGDMP-archive", **env}


def test_backup_publishes_a_verified_dump(host):
    result = run_script(host, "scripts/backup.sh", **backup_env(host))
    assert result.returncode == 0, result.stdout + result.stderr
    files = list((host / "backups").iterdir())
    assert len(files) == 1
    assert re.fullmatch(r"candle_collector-\d{8}-\d{6}-timescaledb-2\.26\.2\.dump", files[0].name)
    assert files[0].read_text() == "PGDMP-archive"
    assert not (host / "backup.lock").exists()
    assert_prod_override(docker_calls(host))


def test_a_dump_that_fails_to_read_back_is_never_published(host):
    server, bodies = serve(204)
    try:
        result = run_script(host, "scripts/backup.sh", **backup_env(
            host, FAKE_RESTORE_EXIT="1", WEBHOOK_URL=f"http://127.0.0.1:{server.server_port}/hook"))
    finally:
        server.shutdown()
        server.server_close()
    assert result.returncode != 0
    assert list((host / "backups").iterdir()) == []
    assert not (host / "backup.lock").exists()
    assert b"Backup failed" in bodies.get(timeout=5)


def test_a_held_lock_stops_the_backup_before_any_dump(host):
    (host / "backup.lock").mkdir()
    result = run_script(host, "scripts/backup.sh", **backup_env(host))
    assert result.returncode == 1
    assert "Another backup holds" in result.stderr
    assert docker_calls(host) == []
    assert (host / "backup.lock").is_dir()


def test_verify_compression_passes_each_table_as_a_psql_variable(host):
    result = run_script(host, "scripts/verify-compression.sh")
    assert result.returncode == 0, result.stdout + result.stderr
    calls = docker_calls(host)
    assert_prod_override(calls)
    assert all(any(f"-v tbl={table}" in call for call in calls)
               for table in ("candles", "funding_rates", "open_interest"))
    log = (host / "docker.log").read_text()
    assert log.count("hypertable_compression_stats(:'tbl')") == 3
    assert "policy_retention" in log
