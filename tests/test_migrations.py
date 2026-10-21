"""The migration ledger: the baseline files, the frozen init.sql, and scripts/migrate.sh driven through
real bash with a stub `docker` on PATH. Real transaction and TimescaleDB behaviour is
tests/integration/check_migrations.sh's job."""

import hashlib
import re
import shutil
import subprocess
from pathlib import Path
from typing import NamedTuple

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS = ROOT / "db" / "migrations"
MIGRATE = ROOT / "scripts" / "migrate.sh"
LIB = ROOT / "scripts" / "migrations-lib.sh"

# A schema change goes in a new migration; init.sql changes only when the baseline moves (with
# db/ledger.sql's list, db/baseline-check.sql's expected catalog and this hash together).
INIT_SQL_SHA256 = "eb6502daa7c7540641b0e4a3f44cbcb0d534d56823354e6d207d54cc34db4296"

needs_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")


class BaselineRow(NamedTuple):
    version: str
    name: str


def _baseline_rows():
    values = (ROOT / "db" / "ledger.sql").read_text().split("VALUES", 1)[1]
    return [BaselineRow(*m) for m in re.findall(r"\('(\d{3})', '([^']+)'\)", values)]


def test_the_baseline_is_every_migration_up_to_it():
    rows = _baseline_rows()
    baseline = max(row.version for row in rows)
    files = sorted(p.name for p in MIGRATIONS.glob("*.sql"))
    assert [row.name for row in rows] == [f for f in files if f[:3] <= baseline]
    assert all(row.version == row.name[:3] for row in rows)


def _lib(*commands, cwd=None):
    return subprocess.run(["bash", "-c", f'. "{LIB}"; ' + " ".join(commands)],
                          capture_output=True, text=True, timeout=30, cwd=cwd)


@needs_bash
def test_the_repo_migrations_pass_the_lib_file_check():
    result = _lib(f'migrations_check_files "{MIGRATIONS}"')
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [str(p) for p in sorted(MIGRATIONS.iterdir())]


def test_init_sql_is_frozen_at_the_baseline():
    digest = hashlib.sha256((ROOT / "db" / "init.sql").read_bytes()).hexdigest()
    assert digest == INIT_SQL_SHA256, (
        "db/init.sql changed. A schema change belongs in a new db/migrations/NNN-<name>.sql applied by "
        "scripts/migrate.sh; edit init.sql only to move the baseline, with db/ledger.sql's list, "
        "db/baseline-check.sql's expected catalog and this hash."
    )


def test_initdb_runs_init_sql_then_ledger_sql():
    db = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]["db"]
    initdb = sorted(Path(v.split(":")[1]).name for v in db["volumes"] if "docker-entrypoint-initdb.d" in v)
    assert initdb == ["init.sql", "ledger.sql"]


# --- scripts/migrate.sh through a stub docker -------------------------------------------------------

# Logs each call's argv ("argv:", arguments joined by \x1f) and psql's stdin ("stdin:"). pg_isready
# fails $FAKE_READY_FAILS times, then succeeds (or always, when FAKE_READY_FAILS=always). psql answers
# the lib's queries from FAKE_* variables.
STUB_DOCKER = r"""#!/bin/bash
(IFS=$'\x1f'; echo "argv: $*") >> "$DOCKER_LOG"
case " $* " in
    *" pg_isready "*)
        n=$(cat "$DOCKER_LOG.ready" 2>/dev/null || echo 0); echo $((n + 1)) > "$DOCKER_LOG.ready"
        [ "${FAKE_READY_FAILS:-0}" = always ] && exit 2
        [ "$n" -ge "${FAKE_READY_FAILS:-0}" ] || exit 2
        exit 0 ;;
    *" psql "*)
        sql=$(cat)
        printf 'stdin:\n%s\n' "$sql" >> "$DOCKER_LOG"
        case "$sql" in
            *"THEN 'adopted'"*) echo "$FAKE_STATE"; exit "${FAKE_STATE_EXIT:-0}" ;;
            *"SELECT version FROM public.schema_migrations"*) printf '%b' "$FAKE_APPLIED" ;;
            *"SELECT count(*) FROM public.schema_migrations"*) echo 7; exit "${FAKE_COUNT_EXIT:-0}" ;;
            *"WITH baseline_tables"*) printf '%b' "$FAKE_CHECK"; exit "${FAKE_CHECK_EXIT:-0}" ;;
            *"CREATE TABLE public.schema_migrations"*) exit "${FAKE_LEDGER_EXIT:-0}" ;;
            *)
                if [ -n "$FAKE_FAIL_ON" ] && [[ "$sql" == *"$FAKE_FAIL_ON"* ]]; then exit 3; fi ;;
        esac ;;
esac
"""


class Call(NamedTuple):
    argv: list[str]
    stdin: str

    @property
    def is_psql(self):
        return any("exec psql " in arg for arg in self.argv)

    @property
    def is_apply(self):
        return self.is_psql and "-1" in self.argv

    def var(self, name):
        """The value of `-v name=…` on this call."""
        return next(arg.split("=", 1)[1] for arg in self.argv if arg.startswith(f"{name}="))


@pytest.fixture
def host(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in [("docker", STUB_DOCKER), ("sleep", "#!/bin/bash\n")]:
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    for version in ("001", "002", "003"):
        (migrations / f"{version}-step.sql").write_text(f"CREATE TABLE t{version} (x INTEGER);\n")
    return tmp_path


def migrate(host, *args, **env):
    """Run migrate.sh against the stub; its migrations dir is the fixture's, through a copy of the lib."""
    scripts = host / "scripts"
    if not scripts.exists():
        scripts.mkdir()
        (scripts / "migrate.sh").write_text(MIGRATE.read_text())
        (scripts / "migrations-lib.sh").write_text(LIB.read_text())
        db = host / "db"
        db.mkdir()
        (db / "migrations").symlink_to(host / "migrations")
        for name in ("ledger.sql", "baseline-check.sql"):
            (db / name).write_text((ROOT / "db" / name).read_text())
    full_env = {
        "PATH": f"{host / 'bin'}:/usr/bin:/bin",
        "DOCKER_LOG": str(host / "docker.log"),
        "MIGRATE_READY_TRIES": "3",
        "FAKE_STATE": "adopted",
        "FAKE_APPLIED": "001\\n",
        "FAKE_CHECK": "",
        **env,
    }
    result = subprocess.run(["bash", str(scripts / "migrate.sh"), *args], env=full_env,
                            capture_output=True, text=True, timeout=30)
    log = (host / "docker.log").read_text() if (host / "docker.log").exists() else ""
    return result, [Call(block.partition("\n")[0].split("\x1f"), block.partition("\n")[2].removeprefix("stdin:\n"))
                    for block in log.split("argv: ")[1:]]


def _applies(calls):
    return [c for c in calls if c.is_apply]


@needs_bash
def test_apply_sends_each_pending_file_in_one_transaction_with_its_ledger_row(host):
    result, calls = migrate(host)
    assert result.returncode == 0, result.stderr
    applies = _applies(calls)
    assert [(c.var("version"), c.var("name")) for c in applies] == [("002", "002-step.sql"), ("003", "003-step.sql")]
    for call in applies:
        assert call.argv[-2:] == ["-f", "-"]
        assert call.var("ON_ERROR_STOP") == "1"
        body = re.fullmatch(
            r"DO (\$cc_[0-9a-f]{16}\$) BEGIN EXECUTE (\$cc_[0-9a-f]{16}\$)(.*)\n\2; END \1;\n"
            r"INSERT INTO public\.schema_migrations \(version, name\) VALUES \(:'version', :'name'\);\n?",
            call.stdin, re.S)
        assert body, call.stdin
        assert body[1] != body[2]
        assert body[3] == f"CREATE TABLE t{call.var('version')} (x INTEGER);"


@needs_bash
def test_psql_and_pg_isready_use_the_containers_user_and_database(host):
    _, calls = migrate(host)
    commands = [arg for c in calls for arg in c.argv if arg.startswith("exec ")]
    assert commands and all('-U "$POSTGRES_USER" -d "$POSTGRES_DB"' in cmd for cmd in commands)
    assert not any("candles" in arg for c in calls for arg in c.argv)


@needs_bash
def test_every_psql_call_stops_on_error(host):
    for args, env in [((), {}), (("--status",), {}), (("--adopt",), {"FAKE_STATE": "pre-ledger"})]:
        (host / "docker.log").unlink(missing_ok=True)
        _, calls = migrate(host, *args, **env)
        psql_calls = [c for c in calls if c.is_psql]
        assert psql_calls, args
        for call in psql_calls:
            assert call.var("ON_ERROR_STOP") == "1", (args, call.argv)


@needs_bash
def test_a_failing_migration_stops_the_run(host):
    result, calls = migrate(host, FAKE_FAIL_ON="CREATE TABLE t002")
    assert result.returncode != 0
    assert "002-step.sql failed" in result.stderr
    assert [c.var("name") for c in _applies(calls)] == ["002-step.sql"]


@needs_bash
def test_status_lists_pending_and_applies_nothing(host):
    result, calls = migrate(host, "--status")
    assert result.returncode == 0, result.stderr
    assert "state: adopted\napplied: 7\npending: 2\n  002-step.sql\n  003-step.sql" in result.stdout
    assert _applies(calls) == []


@needs_bash
def test_status_fails_when_the_applied_count_fails(host):
    result, _ = migrate(host, "--status", FAKE_COUNT_EXIT="2")
    assert result.returncode != 0
    assert "could not read schema_migrations" in result.stderr


@needs_bash
@pytest.mark.parametrize("env, message", [
    ({"FAKE_STATE": "pre-ledger"}, "the database is pre-ledger"),
    ({"FAKE_STATE": "empty"}, "the database is empty"),
    ({"FAKE_APPLIED": ""}, "schema_migrations is empty"),
    ({"FAKE_APPLIED": "001\\n003\\n"}, "002-step.sql is older than applied 003"),
    ({"FAKE_STATE_EXIT": "2"}, "could not read the database state"),
])
def test_apply_refuses_without_sending_a_migration(host, env, message):
    result, calls = migrate(host, **env)
    assert result.returncode != 0
    assert message in result.stderr
    assert _applies(calls) == []


@needs_bash
@pytest.mark.parametrize("bad_file, body, message", [
    ("4-short.sql", "SELECT 1;", "not NNN-<name>.sql"),
    ("002-again.sql", "SELECT 1;", "version 002 is used twice"),
    ("004-dir.sql", None, "not a readable file"),
])
def test_bad_migration_files_are_refused(host, bad_file, body, message):
    target = host / "migrations" / bad_file
    if body is None:
        target.mkdir()
    else:
        target.write_text(body)
    result, calls = migrate(host)
    assert result.returncode != 0
    assert message in result.stderr
    assert _applies(calls) == []


@needs_bash
def test_a_migrations_text_reaches_the_server_untouched(host):
    """psql commands, psql variables and dollar quotes in a migration are its own text: psql sees one
    dollar-quoted literal (the server refuses transaction control in it; check_migrations.sh proves that)."""
    text = "CREATE TABLE t4 (x INTEGER);\n\\q\nSELECT :'version';\nDO $$ BEGIN PERFORM 1; END $$;\nCOMMIT;"
    (host / "migrations" / "004-odd.sql").write_text(text + "\n")
    result, calls = migrate(host)
    assert result.returncode == 0, result.stderr
    last = _applies(calls)[-1]
    assert last.var("name") == "004-odd.sql"
    tag = re.match(r"DO \$cc_[0-9a-f]{16}\$ BEGIN EXECUTE (\$cc_[0-9a-f]{16}\$)", last.stdin)[1]
    assert last.stdin.split(tag)[1] == text + "\n"


def _ledger_sent(calls):
    return any("CREATE TABLE public.schema_migrations" in c.stdin for c in calls)


@needs_bash
def test_adopt_checks_the_baseline_then_records_it(host):
    result, calls = migrate(host, "--adopt", FAKE_STATE="pre-ledger", FAKE_CHECK="column a: text|t\\nno retention policy|t\\n")
    assert result.returncode == 0, result.stderr
    stdins = [c.stdin for c in calls if c.stdin]
    check = next(i for i, s in enumerate(stdins) if "WITH baseline_tables" in s)
    ledger = next(i for i, s in enumerate(stdins) if "CREATE TABLE public.schema_migrations" in s)
    assert check < ledger


@needs_bash
@pytest.mark.parametrize("env, message", [
    ({"FAKE_CHECK": "column a: text|t\\nindex discovery_volumes_pkey: CREATE UNIQUE INDEX|f\\n"},
     "index discovery_volumes_pkey: CREATE UNIQUE INDEX"),
    ({"FAKE_CHECK_EXIT": "3"}, "the baseline check did not run"),
    ({"FAKE_CHECK": ""}, "the baseline check returned no rows"),
    ({"FAKE_STATE": "adopted"}, "only a pre-ledger database is adopted"),
])
def test_adopt_refuses_and_records_nothing(host, env, message):
    result, calls = migrate(host, "--adopt", **{"FAKE_STATE": "pre-ledger", "FAKE_CHECK": "column a: text|t\\n", **env})
    assert result.returncode != 0
    assert message in result.stderr
    assert not _ledger_sent(calls)


@needs_bash
def test_a_failing_ledger_fails_the_adopt(host):
    result, calls = migrate(host, "--adopt", FAKE_STATE="pre-ledger", FAKE_CHECK="column a: text|t\\n", FAKE_LEDGER_EXIT="3")
    assert result.returncode != 0
    assert "recording the baseline failed" in result.stderr
    assert _ledger_sent(calls)


@needs_bash
def test_no_query_runs_before_the_db_answers_over_tcp(host):
    result, calls = migrate(host, FAKE_READY_FAILS="2")
    assert result.returncode == 0, result.stderr
    first_psql = next(i for i, c in enumerate(calls) if c.is_psql)
    readiness = [" ".join(c.argv) for c in calls[:first_psql]]
    assert len(readiness) == 3 and all("pg_isready -q -h 127.0.0.1" in r for r in readiness)

    (host / "docker.log").unlink()
    (host / "docker.log.ready").unlink()
    result, calls = migrate(host, FAKE_READY_FAILS="always")
    assert result.returncode != 0
    assert "did not accept TCP connections" in result.stderr
    assert not any(c.is_psql for c in calls)
