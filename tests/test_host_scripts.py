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
        # `docker compose version` and alert or log messages name compose without running it.
        if "docker compose version" in line or re.match(r"\s*(echo|remind|info|fail|send_alert)\b", line):
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


# `crontab -l` prints $CRONTAB_FILE, says "no crontab" when it is absent, and fails outright
# when $CRONTAB_BROKEN is set; `crontab -` replaces the file.
STUB_CRONTAB = r"""#!/bin/bash
if [ "$1" = "-l" ]; then
    [ -z "$CRONTAB_BROKEN" ] || { echo "crontab: permission denied" >&2; exit 1; }
    [ -f "$CRONTAB_FILE" ] || { echo "no crontab for test" >&2; exit 1; }
    cat "$CRONTAB_FILE"
else
    cat > "$CRONTAB_FILE"
fi
"""

FOREIGN = [
    "MAILTO=ops",
    "15 3 * * * /usr/local/bin/certbot renew",
    "0 1 * * * /srv/other/candle-collector-tools/run.sh",
]


def legacy_entries(project):
    """The unmarked lines an earlier deploy.sh installed, as they stand on a live host."""
    compose = f"cd {project} && docker compose -f docker-compose.yml -f docker-compose.prod.yml"
    return [
        "# candle-collector: daily discovery (02:00 UTC)",
        f"0 2 * * * {compose} --profile discovery run --rm discovery >> /var/log/x.log 2>&1",
        "# candle-collector: host monitor (every 5 min)",
        f"*/5 * * * * {project}/deploy/host-monitor.sh 2>/dev/null",
        f"30 5 * * * {project}/scripts/backup-offsite.sh >> /var/log/y.log 2>&1",
    ]


@pytest.fixture
def cron_host(host):
    (host / "bin" / "crontab").write_text(STUB_CRONTAB)
    (host / "bin" / "crontab").chmod(0o755)
    return host


def install_cron(host, **env):
    result = run_script(host, "deploy/install-cron.sh", CRONTAB_FILE=str(host / "crontab"), **env)
    return result


def jobs(lines):
    return [line for line in lines if line and not line.startswith("#")]


def test_install_cron_into_an_empty_crontab(cron_host):
    result = install_cron(cron_host)
    assert result.returncode == 0, result.stderr
    lines = (cron_host / "crontab").read_text().splitlines()
    assert lines[0].startswith("# >>> crypto-data-warehouse")
    assert lines[-1] == "# <<< crypto-data-warehouse <<<"
    assert len(jobs(lines)) == 5
    assert all(str(ROOT) in job for job in jobs(lines))
    compose_jobs = [job for job in jobs(lines) if "docker compose" in job]
    assert len(compose_jobs) == 3
    assert all("-f docker-compose.yml -f docker-compose.prod.yml" in job for job in compose_jobs)


def test_install_cron_replaces_legacy_entries_and_keeps_foreign_ones(cron_host):
    # A foreign job that only names the checkout as an argument is not ours.
    foreign = [*FOREIGN, f"0 5 * * * /usr/local/bin/restic backup {ROOT}/backups"]
    (cron_host / "crontab").write_text("\n".join(foreign[:2] + legacy_entries(ROOT) + foreign[2:]) + "\n")
    assert install_cron(cron_host).returncode == 0
    lines = (cron_host / "crontab").read_text().splitlines()
    assert lines[:4] == foreign
    assert len(jobs(lines)) == len(foreign) + 5
    assert sum("deploy/host-monitor.sh" in line for line in lines) == 1
    assert not any(line.startswith("# candle-collector:") for line in lines)


def test_install_cron_is_a_fixed_point(cron_host):
    (cron_host / "crontab").write_text("\n".join(FOREIGN) + "\n")
    install_cron(cron_host)
    first = (cron_host / "crontab").read_text()
    install_cron(cron_host)
    assert (cron_host / "crontab").read_text() == first


def test_install_cron_writes_nothing_when_the_crontab_cannot_be_read(cron_host):
    (cron_host / "crontab").write_text("\n".join(FOREIGN) + "\n")
    result = install_cron(cron_host, CRONTAB_BROKEN="1")
    assert result.returncode == 1
    assert "cannot read the crontab" in result.stderr
    assert (cron_host / "crontab").read_text() == "\n".join(FOREIGN) + "\n"


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
