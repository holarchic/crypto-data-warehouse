"""The migration ledger: the baseline files, the frozen init.sql, and scripts/migrate.sh driven through
real bash with a stub `docker` on PATH. Real transaction and TimescaleDB behaviour is
tests/integration/check_migrations.sh's job."""

import hashlib
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS = ROOT / "db" / "migrations"
MIGRATE = ROOT / "scripts" / "migrate.sh"

# A schema change goes in a new migration; init.sql changes only when the baseline moves (with
# db/ledger.sql's list and this hash together).
INIT_SQL_SHA256 = "eb6502daa7c7540641b0e4a3f44cbcb0d534d56823354e6d207d54cc34db4296"

TRANSACTION_CONTROL = re.compile(r"^\s*(BEGIN|COMMIT|ROLLBACK|START\s+TRANSACTION|SAVEPOINT)\s*;", re.I | re.M)


def _baseline_rows():
    values = (ROOT / "db" / "ledger.sql").read_text().split("VALUES", 1)[1]
    return re.findall(r"\('(\d{3})', '([^']+)'\)", values)


def test_the_baseline_is_every_migration_up_to_it():
    rows = _baseline_rows()
    baseline = max(version for version, _ in rows)
    files = sorted(p.name for p in MIGRATIONS.glob("*.sql"))
    assert [name for _, name in rows] == [f for f in files if f[:3] <= baseline]
    assert all(version == name[:3] for version, name in rows)


def test_migration_files_are_numbered_once_and_hold_no_transaction_control():
    names = sorted(p.name for p in MIGRATIONS.iterdir())
    assert all(re.fullmatch(r"\d{3}-[a-z0-9-]+\.sql", n) for n in names), names
    versions = [n[:3] for n in names]
    assert len(versions) == len(set(versions))
    for path in MIGRATIONS.glob("*.sql"):
        assert not TRANSACTION_CONTROL.search(path.read_text()), path.name


def test_init_sql_is_frozen_at_the_baseline():
    digest = hashlib.sha256((ROOT / "db" / "init.sql").read_bytes()).hexdigest()
    assert digest == INIT_SQL_SHA256, (
        "db/init.sql changed. A schema change belongs in a new db/migrations/NNN-<name>.sql applied by "
        "scripts/migrate.sh; edit init.sql only to move the baseline, with db/ledger.sql's list and this hash."
    )


def test_initdb_runs_init_sql_then_ledger_sql():
    db = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]["db"]
    initdb = sorted(Path(v.split(":")[1]).name for v in db["volumes"] if "docker-entrypoint-initdb.d" in v)
    assert initdb == ["init.sql", "ledger.sql"]


# --- scripts/migrate.sh through a stub docker -------------------------------------------------------

pytestmark_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

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
            *"SELECT count(*) FROM public.schema_migrations"*) echo 7 ;;
            *"SELECT 'table ' ||"*) printf '%b' "$FAKE_CHECK"; exit "${FAKE_CHECK_EXIT:-0}" ;;
            *"CREATE TABLE public.schema_migrations"*) exit "${FAKE_LEDGER_EXIT:-0}" ;;
            *)
                if [ -n "$FAKE_FAIL_ON" ] && [[ "$sql" == *"$FAKE_FAIL_ON"* ]]; then exit 3; fi ;;
        esac ;;
esac
"""


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
    full_env = {
        "PATH": f"{host / 'bin'}:/usr/bin:/bin",
        "DOCKER_LOG": str(host / "docker.log"),
        "MIGRATIONS_DIR": str(host / "migrations"),
        "MIGRATE_READY_TRIES": "3",
        "FAKE_STATE": "adopted",
        "FAKE_APPLIED": "001\\n",
        "FAKE_CHECK": "",
        **env,
    }
    result = subprocess.run(["bash", str(MIGRATE), *args], env=full_env, capture_output=True, text=True, timeout=30)
    log = (host / "docker.log").read_text() if (host / "docker.log").exists() else ""
    return result, log


def _calls(log):
    """Each docker call as (argv list, stdin text)."""
    calls = []
    for block in log.split("argv: ")[1:]:
        argv_line, _, rest = block.partition("\n")
        calls.append((argv_line.split("\x1f"), rest.removeprefix("stdin:\n")))
    return calls


def _applies(log):
    return [(argv, stdin) for argv, stdin in _calls(log) if "-1" in argv]


@pytestmark_bash
def test_apply_sends_each_pending_file_in_one_transaction_with_its_ledger_row(host):
    result, log = migrate(host)
    assert result.returncode == 0, result.stderr
    applies = _applies(log)
    for (argv, stdin), version in zip(applies, ("002", "003"), strict=True):
        joined = " ".join(argv)
        assert "psql -X -U candles -d candle_collector -v ON_ERROR_STOP=1 -1" in joined
        assert f"-v version={version} -v name={version}-step.sql -f -" in joined
        assert stdin.startswith(f"CREATE TABLE t{version} (x INTEGER);")
        assert "INSERT INTO public.schema_migrations (version, name) VALUES (:'version', :'name');" in stdin


@pytestmark_bash
def test_every_psql_call_stops_on_error(host):
    for args, env in [((), {}), (("--status",), {}), (("--adopt",), {"FAKE_STATE": "pre-ledger"})]:
        (host / "docker.log").unlink(missing_ok=True)
        _, log = migrate(host, *args, **env)
        psql_calls = [argv for argv, _ in _calls(log) if "psql" in argv]
        assert psql_calls, args
        for argv in psql_calls:
            assert "ON_ERROR_STOP=1" in argv and argv[argv.index("ON_ERROR_STOP=1") - 1] == "-v", (args, argv)


@pytestmark_bash
def test_a_failing_migration_stops_the_run(host):
    result, log = migrate(host, FAKE_FAIL_ON="CREATE TABLE t002")
    assert result.returncode != 0
    assert "002-step.sql failed" in result.stderr
    assert [next(x for x in a if x.startswith("name=")) for a, _ in _applies(log)] == ["name=002-step.sql"]


@pytestmark_bash
def test_status_lists_pending_and_applies_nothing(host):
    result, log = migrate(host, "--status")
    assert result.returncode == 0, result.stderr
    assert "state: adopted" in result.stdout
    assert "pending: 2\n  002-step.sql\n  003-step.sql" in result.stdout
    assert _applies(log) == []


@pytestmark_bash
@pytest.mark.parametrize("env, message", [
    ({"FAKE_STATE": "pre-ledger"}, "the database is pre-ledger"),
    ({"FAKE_STATE": "empty"}, "the database is empty"),
    ({"FAKE_APPLIED": ""}, "schema_migrations is empty"),
    ({"FAKE_APPLIED": "001\\n003\\n"}, "002-step.sql is older than applied 003"),
    ({"FAKE_STATE_EXIT": "2"}, "could not read the database state"),
])
def test_apply_refuses_without_sending_a_migration(host, env, message):
    result, log = migrate(host, **env)
    assert result.returncode != 0
    assert message in result.stderr
    assert _applies(log) == []


@pytestmark_bash
@pytest.mark.parametrize("bad_file, body, message", [
    ("4-short.sql", "SELECT 1;", "not NNN-<name>.sql"),
    ("002-again.sql", "SELECT 1;", "version 002 is used twice"),
    ("004-commits.sql", "CREATE TABLE t (x INTEGER);\nCOMMIT;\n", "holds its own transaction control"),
])
def test_bad_migration_files_are_refused(host, bad_file, body, message):
    (host / "migrations" / bad_file).write_text(body)
    result, log = migrate(host)
    assert result.returncode != 0
    assert message in result.stderr
    assert _applies(log) == []


def _ledger_sent(log):
    return any("CREATE TABLE public.schema_migrations" in stdin for _, stdin in _calls(log))


@pytestmark_bash
def test_adopt_checks_the_baseline_then_records_it(host):
    result, log = migrate(host, "--adopt", FAKE_STATE="pre-ledger", FAKE_CHECK="table candles|t\\nno retention policy|t\\n")
    assert result.returncode == 0, result.stderr
    stdins = [stdin for _, stdin in _calls(log) if stdin]
    check = next(i for i, s in enumerate(stdins) if "SELECT 'table ' ||" in s)
    ledger = next(i for i, s in enumerate(stdins) if "CREATE TABLE public.schema_migrations" in s)
    assert check < ledger


@pytestmark_bash
@pytest.mark.parametrize("env, message", [
    ({"FAKE_CHECK": "table candles|t\\ntable discovery_volumes|f\\n"}, "table discovery_volumes"),
    ({"FAKE_CHECK_EXIT": "3"}, "the baseline check did not run"),
    ({"FAKE_STATE": "adopted"}, "only a pre-ledger database is adopted"),
])
def test_adopt_refuses_and_records_nothing(host, env, message):
    result, log = migrate(host, "--adopt", **{"FAKE_STATE": "pre-ledger", **env})
    assert result.returncode != 0
    assert message in result.stderr
    assert not _ledger_sent(log)


@pytestmark_bash
def test_a_failing_ledger_fails_the_adopt(host):
    result, log = migrate(host, "--adopt", FAKE_STATE="pre-ledger", FAKE_CHECK="table candles|t\\n", FAKE_LEDGER_EXIT="3")
    assert result.returncode != 0
    assert "recording the baseline failed" in result.stderr
    assert _ledger_sent(log)


@pytestmark_bash
def test_no_query_runs_before_the_db_answers_over_tcp(host):
    result, log = migrate(host, FAKE_READY_FAILS="2")
    assert result.returncode == 0, result.stderr
    argvs = [argv for argv, _ in _calls(log)]
    first_psql = next(i for i, a in enumerate(argvs) if "psql" in a)
    assert [("pg_isready" in a, "127.0.0.1" in a) for a in argvs[:first_psql]] == [(True, True)] * 3

    (host / "docker.log").unlink()
    (host / "docker.log.ready").unlink()
    result, log = migrate(host, FAKE_READY_FAILS="always")
    assert result.returncode != 0
    assert "did not accept TCP connections" in result.stderr
    assert not any("psql" in argv for argv, _ in _calls(log))
