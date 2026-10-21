"""scripts/ops-lib.sh: the host scripts' one prod compose command, and DB_TOOL reaching the database
as the db container's own POSTGRES_USER and POSTGRES_DB. A stub `docker` runs the real `sh -c`
wrapper with the container's environment (site_user, site_db) and stub Postgres clients that log
their arguments. tests/integration/check_ops_lib.sh runs the same scripts against a real db."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
HOST_SCRIPTS = sorted([*ROOT.glob("scripts/*.sh"), *ROOT.glob("deploy/*.sh")])

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

# backup-offsite.sh runs on the server (bash 5): an empty array under `set -u` needs bash >= 4.4,
# which macOS's /bin/bash (3.2) is not.
SERVER_BASH = shutil.which("bash", path="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin")


def _bash_at_least_4_4(bash):
    out = subprocess.run([bash, "-c", 'echo "${BASH_VERSINFO[0]} ${BASH_VERSINFO[1]}"'], capture_output=True, text=True).stdout.split()
    return (int(out[0]), int(out[1])) >= (4, 4)


needs_server_bash = pytest.mark.skipif(not SERVER_BASH or not _bash_at_least_4_4(SERVER_BASH), reason="needs bash >= 4.4")

# docker: logs "docker <args>", strips `compose -f A -f B`, and for `exec -T db CMD…` runs CMD with
# the container's identity in its environment. Every other subcommand (cp, ps) just logs.
STUB_DOCKER = r"""#!/bin/bash
printf 'docker' >> "$OPS_LOG"; printf ' %s' "$@" >> "$OPS_LOG"; printf '\n' >> "$OPS_LOG"
args=("$@")
while [ "${args[0]}" = compose ] || [ "${args[0]}" = -f ]; do
    if [ "${args[0]}" = -f ]; then args=("${args[@]:2}"); else args=("${args[@]:1}"); fi
done
[ "${args[0]}" = exec ] || exit 0
POSTGRES_USER=site_user POSTGRES_DB=site_db exec "${args[@]:3}"
"""

# A Postgres client: logs "<tool> <args>"; psql answers the version queries, pg_dump writes an
# archive (or fails when $FAKE_DUMP_FAIL is set), pg_restore drains its input.
STUB_CLIENT = r"""#!/bin/bash
tool=$(basename "$0")
printf '%s' "$tool" >> "$OPS_LOG"; printf ' %s' "$@" >> "$OPS_LOG"; printf '\n' >> "$OPS_LOG"
case "$tool" in
    psql)
        case "$*" in
            *extversion*) echo 2.26.2 ;;
            *server_version_num*) echo 16 ;;
            *) cat > /dev/null; echo 0 ;;
        esac ;;
    pg_dump) [ -z "${FAKE_DUMP_FAIL:-}" ] || exit 4; printf 'PGDMP-archive' ;;
    pg_restore) cat > /dev/null ;;
esac
"""


@pytest.fixture
def host(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(STUB_DOCKER)
    for tool in ("psql", "pg_dump", "pg_restore", "pg_isready"):
        (bin_dir / tool).write_text(STUB_CLIENT)
    for stub in bin_dir.iterdir():
        stub.chmod(0o755)
    return tmp_path


def run(host, argv, input=None, **env):
    """Run with the host's own POSTGRES_USER/POSTGRES_DB set wrong: only the container's may reach psql."""
    full_env = {"PATH": f"{host / 'bin'}:/usr/bin:/bin", "OPS_LOG": str(host / "ops.log"), "WEBHOOK_URL": "",
                "POSTGRES_USER": "wrong", "POSTGRES_DB": "wrong", **env}
    return subprocess.run(argv, env=full_env, input=input, capture_output=True, text=True, timeout=60)


def log_lines(host, prefix):
    log = host / "ops.log"
    return [line for line in log.read_text().splitlines() if line.startswith(prefix + " ")] if log.exists() else []


# --- ownership ---------------------------------------------------------------------------------

def _runs_compose(line):
    stripped = line.strip()
    if stripped.startswith("#") or "docker compose" not in line:
        return False
    # Capability probes and alert or log messages name compose without running it.
    return "docker compose version" not in line and not re.match(r"\s*(echo|remind|info|fail|send_alert)\b", line)


@pytest.mark.parametrize("script", HOST_SCRIPTS, ids=lambda p: p.name)
def test_only_ops_lib_and_install_cron_run_docker_compose(script):
    running = [n for n, line in enumerate(script.read_text().splitlines(), 1) if _runs_compose(line)]
    if script.name in ("ops-lib.sh", "install-cron.sh"):
        assert running, script.name
        for number in running:
            line = script.read_text().splitlines()[number - 1]
            assert "docker-compose.yml" in line and "docker-compose.prod.yml" in line, line
    else:
        assert running == [], f"{script.name} builds its own compose command at lines {running}"


@pytest.mark.parametrize("script", [s for s in HOST_SCRIPTS if s.name != "restore-drill.sh"], ids=lambda p: p.name)
def test_no_host_script_names_the_database_identity(script):
    """The user and database come from DB_TOOL. `-d postgres`, table names, messages and artifact
    names stay allowed; restore-drill runs its own throwaway container."""
    text = script.read_text()
    assert not re.search(r"-U\s+candles\b", text)
    assert not re.search(r"-d\s+candle_collector\b", text)
    assert not re.search(r"\b(psql|pg_dump|pg_restore)\b[^\n]*\scandle_collector\s*($|[\\>|])", text, re.M)


# --- DB_TOOL ---------------------------------------------------------------------------------

def test_db_tool_runs_as_the_containers_user_against_its_database(host):
    result = run(host, ["bash", "-c", f'. "{ROOT}/scripts/ops-lib.sh"; "${{DB_TOOL[@]}}" psql -X -Atc "SELECT 1"; '
                        '"${DB_TOOL[@]}" psql -X -d postgres -c "SELECT 2"'])
    assert result.returncode == 0, result.stderr
    assert log_lines(host, "psql") == [
        "psql -U site_user -d site_db -X -Atc SELECT 1",
        "psql -U site_user -d site_db -X -d postgres -c SELECT 2",   # psql takes the last -d
    ]
    (call,) = [line for line in log_lines(host, "docker") if "SELECT 1" in line]
    assert call.startswith(f"docker compose -f {ROOT}/docker-compose.yml -f {ROOT}/docker-compose.prod.yml exec -T db sh -c ")


def test_backup_dumps_the_containers_database(host):
    result = run(host, ["bash", str(ROOT / "scripts/backup.sh")],
                 BACKUP_DIR=str(host / "backups"), BACKUP_LOCK_DIR=str(host / "backup.lock"))
    assert result.returncode == 0, result.stdout + result.stderr
    assert log_lines(host, "pg_dump") == ["pg_dump -U site_user -d site_db --format=custom --compress=6"]
    assert [p.read_text() for p in (host / "backups").iterdir()] == ["PGDMP-archive"]


# --- backup-offsite.sh -------------------------------------------------------------------------

# restic: logs its argv; `backup … --stdin-from-command -- CMD…` runs CMD and fails as it fails.
STUB_RESTIC = r"""#!/bin/bash
printf 'restic' >> "$OPS_LOG"; printf ' %s' "$@" >> "$OPS_LOG"; printf '\n' >> "$OPS_LOG"
if [[ " $* " == *" backup "* ]]; then
    while [ "$1" != "--" ]; do shift; done; shift
    "$@" > /dev/null || exit 1
fi
"""
STUB_FLOCK = "#!/bin/bash\nprintf 'flock %s\\n' \"$*\" >> \"$OPS_LOG\"\n"


@pytest.fixture
def offsite(host):
    """A copy of the scripts/ tree whose backup-offsite.sh differs in two lines: PATH keeps the stubs
    first, and the lock file lives in the temp dir."""
    tree = host / "tree"
    shutil.copytree(ROOT / "scripts", tree / "scripts", ignore=shutil.ignore_patterns("__pycache__", "*.db*"))
    for name in ("docker-compose.yml", "docker-compose.prod.yml"):
        (tree / name).write_text("")
    script = tree / "scripts" / "backup-offsite.sh"
    text = script.read_text()
    for old, new in [('export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"', 'export PATH="$PATH"'),
                     ('LOCK_FILE="/run/lock/candle-collector-backup.lock"', f'LOCK_FILE="{host}/offsite.lock"')]:
        assert text.count(old) == 1, old
        text = text.replace(old, new)
    script.write_text(text)
    for name, body in (("restic", STUB_RESTIC), ("flock", STUB_FLOCK)):
        (host / "bin" / name).write_text(body)
        (host / "bin" / name).chmod(0o755)
    (host / "restic.env").write_text("RESTIC_REPOSITORY=rclone:b2:bucket\nRESTIC_PASSWORD_FILE=/dev/null\n")
    return script


def _offsite(host, script, **env):
    return run(host, [SERVER_BASH, str(script)], RESTIC_ENV_FILE=str(host / "restic.env"),
               BACKUP_OK_MARKER=str(host / "backup-ok"), **env)


@needs_server_bash
def test_offsite_backup_streams_the_containers_database(host, offsite):
    result = _offsite(host, offsite)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Offsite backup complete" in result.stdout
    assert (host / "backup-ok").exists()
    assert log_lines(host, "flock") and log_lines(host, "restic")   # the stubs ran
    (backup,) = [line for line in log_lines(host, "restic") if " backup " in line]
    dump = backup.split(" -- ", 1)[1]
    assert dump.startswith("env -u RESTIC_REPOSITORY -u RESTIC_PASSWORD_FILE docker compose -f ")
    assert dump.endswith(" exec -T db sh -c tool=$1; shift; exec \"$tool\" -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" \"$@\" "
                         "db-tool pg_dump --format=custom --compress=0")
    assert log_lines(host, "pg_dump") == ["pg_dump -U site_user -d site_db --format=custom --compress=0"]


@needs_server_bash
def test_a_failing_offsite_dump_fails_the_run_and_keeps_the_marker(host, offsite):
    (host / "backup-ok").write_text("before")
    result = _offsite(host, offsite, FAKE_DUMP_FAIL="1")
    assert result.returncode != 0
    assert "Offsite backup complete" not in result.stdout
    assert (host / "backup-ok").read_text() == "before"
    assert log_lines(host, "pg_dump")                                # the dump was attempted


# --- restore.sh ------------------------------------------------------------------------------

def test_restore_targets_and_names_the_containers_database(host):
    dump = host / "candle_collector-20261021-000000-timescaledb-2.26.2.dump"
    dump.write_text("PGDMP-archive")
    stdin_log = host / "psql-stdin.log"
    # Record every psql statement with its variables, answering the preflight counts.
    (host / "bin" / "psql").write_text(
        "#!/bin/bash\n"
        "printf 'psql' >> \"$OPS_LOG\"; printf ' %s' \"$@\" >> \"$OPS_LOG\"; printf '\\n' >> \"$OPS_LOG\"\n"
        f"sql=$(cat); printf '%s\\n' \"$sql\" >> {stdin_log}\n"
        "case \"$sql\" in *pg_available_extension_versions*|*pg_tablespace*) echo 1 ;; *) echo 0 ;; esac\n")
    result = run(host, ["bash", str(ROOT / "scripts/restore.sh"), str(dump)], input="yes\n")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "This drops the site_db database" in result.stdout
    psql_calls = log_lines(host, "psql")
    assert any("-d postgres -At -v db=site_db" in call for call in psql_calls)          # session check
    assert any(call.endswith("-d postgres -v db=site_db") for call in psql_calls)       # drop + create
    statements = stdin_log.read_text()
    assert "WHERE datname = :'db'" in statements
    assert 'DROP DATABASE IF EXISTS :"db";' in statements and 'CREATE DATABASE :"db" TEMPLATE template0;' in statements
    assert log_lines(host, "pg_restore")[-1].startswith("pg_restore -U site_user -d site_db --exit-on-error ")
