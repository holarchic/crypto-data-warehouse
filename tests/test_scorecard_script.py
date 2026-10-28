"""scripts/stability-scorecard.sh through real bash, with stub `docker` and `curl` on PATH: argument
checks, how it reaches the monitor and the database, the latency measurement, and its exit status.
The SQL itself runs against TimescaleDB in tests/integration/check_scorecard.py."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "stability-scorecard.sh"
COMPOSE_PROD = ["compose", "-f", f"{ROOT}/docker-compose.yml", "-f", f"{ROOT}/docker-compose.prod.yml"]
META = json.dumps({"intervals": {"5m": 300}, "volume_threshold_usd": 1000000})
DB_PASS = "=== Database checks ===\ncandle capture|100 %|>= 99.9 %|PASS\nSCORECARD-DB: PASS"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None or shutil.which("python3") is None,
                                reason="needs bash and python3")

# Logs each call's argv as one "argv:" line (arguments joined by \x1f), and psql's stdin under
# "stdin:". The monitor's python prints $FAKE_META or fails with $FAKE_MONITOR_EXIT; sha256sum prints
# the hash in $FAKE_HASHES/<service> (default "same"); `ps` lists $FAKE_SERVICES; psql prints
# $FAKE_PSQL_OUT and exits $FAKE_PSQL_EXIT.
STUB_DOCKER = r"""#!/bin/bash
(IFS=$'\x1f'; echo "argv: $*") >> "$DOCKER_LOG"
case " $* " in
    *" ps --services "*) printf '%s\n' "$FAKE_SERVICES" ;;
    *" monitor python "*) [ "${FAKE_MONITOR_EXIT:-0}" -eq 0 ] || exit "$FAKE_MONITOR_EXIT"; printf '%s\n' "$FAKE_META" ;;
    *" sha256sum "*)
        service=$(sed 's/.* exec -T \([^ ]*\) sha256sum.*/\1/' <<<"$*")
        hash=$(cat "$FAKE_HASHES/$service" 2>/dev/null || echo same)
        for f in config.py exchanges.yaml; do echo "$hash  /app/$f"; done ;;
    *" psql "*)
        printf 'stdin:\n%s\n' "$(cat)" >> "$DOCKER_LOG"
        printf '%s\n' "$FAKE_PSQL_OUT"
        exit "${FAKE_PSQL_EXIT:-0}" ;;
esac
"""

# Answers request N with line N of $FAKE_CURL (the last line repeats): "<http code> <seconds> <rows>",
# writing a JSON array of <rows> to the -o file; "timeout" exits 28 as curl does.
STUB_CURL = r"""#!/bin/bash
out=""
while [ $# -gt 0 ]; do
    case "$1" in -o) out="$2"; shift 2 ;; *) shift ;; esac
done
n=$(( $(cat "$CURL_COUNT" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$CURL_COUNT"
line=$(sed -n "${n}p" "$FAKE_CURL"); [ -n "$line" ] || line=$(tail -1 "$FAKE_CURL")
[ "$line" = timeout ] && exit 28
read -r code seconds rows <<<"$line"
{ printf '['; for ((i = 0; i < rows; i++)); do [ "$i" -eq 0 ] || printf ','; printf '{"close":1.0}'; done; printf ']'; } > "$out"
printf '%s %s' "$code" "$seconds"
"""


@pytest.fixture
def host(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("docker", STUB_DOCKER), ("curl", STUB_CURL)):
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    (tmp_path / "hashes").mkdir()
    return tmp_path


def run(host, *args, curl=("200 0.100 500",), **env):
    (host / "curl.txt").write_text("\n".join(curl) + "\n")
    full_env = {
        "PATH": f"{host / 'bin'}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin",
        "DOCKER_LOG": str(host / "docker.log"),
        "FAKE_META": META,
        "FAKE_SERVICES": "api\ncollector-binance\ncollector-bybit\nmonitor",
        "FAKE_HASHES": str(host / "hashes"),
        "FAKE_PSQL_OUT": DB_PASS,
        "FAKE_CURL": str(host / "curl.txt"),
        "CURL_COUNT": str(host / "curl.count"),
        **env,
    }
    return subprocess.run(["bash", str(SCRIPT), *args], env=full_env, capture_output=True, text=True, timeout=120)


def calls(host):
    log = host / "docker.log"
    if not log.exists():
        return []
    return [line[len("argv: "):].split("\x1f") for line in log.read_text().splitlines() if line.startswith("argv: ")]


def psql_call(host):
    return next(c for c in calls(host) if "psql" in c)


@pytest.mark.parametrize("args", [["--days", "0"], ["--days", "-1"], ["--days", "x"], ["--days"], ["--weeks", "2"]])
def test_bad_arguments_stop_before_any_call(host, args):
    result = run(host, *args)
    assert result.returncode == 1
    assert "usage:" in result.stderr
    assert calls(host) == []


def test_everything_passing_exits_0_with_the_not_measured_list(host):
    result = run(host)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "config consistent: PASS (2 collectors match the monitor)" in result.stdout
    assert "api p95 (500 rows): PASS (0.100 s < 0.500 s, 50 requests)" in result.stdout
    assert "NOT MEASURED: funding rows against venue history" in result.stdout
    assert result.stdout.rstrip().endswith("SCORECARD: PASS (measured checks only)")
    assert (host / "curl.count").read_text().strip() == "50"


def test_metadata_comes_from_the_monitor_and_psql_runs_through_db_tool(host):
    run(host, "--days", "3", SCORECARD_AS_OF="2026-10-20T12:00:00Z")
    every = calls(host)
    assert all(c[:5] == COMPOSE_PROD for c in every), every
    assert any(c[5:9] == ["exec", "-T", "monitor", "python"] for c in every)
    psql = psql_call(host)
    assert psql[5:8] == ["exec", "-T", "db"] and psql[8] == "sh" and psql[-1] != "psql"
    assert "ON_ERROR_STOP=1" in psql
    assert "days=3" in psql and f"meta={META}" in psql and "as_of=2026-10-20T12:00:00Z" in psql
    log = (host / "docker.log").read_text()
    assert (ROOT / "scripts" / "stability-scorecard.sql").read_text().strip() in log


def test_days_defaults_to_the_two_week_gate(host):
    run(host)
    assert "days=14" in psql_call(host)


def test_p95_is_the_48th_of_50_sorted_times(host):
    times = [f"200 0.{i:03d} 500" for i in range(50, 0, -1)]   # 0.050 … 0.001, unsorted
    result = run(host, curl=times)
    assert "api p95 (500 rows): PASS (0.048 s" in result.stdout


def test_a_p95_of_exactly_500_ms_fails(host):
    result = run(host, curl=("200 0.500 500",))
    assert result.returncode == 1
    assert "api p95 (500 rows): FAIL (0.500 s, limit < 0.500 s)" in result.stdout
    assert result.stdout.rstrip().endswith("SCORECARD: FAIL")


@pytest.mark.parametrize(("curl", "reason"), [
    (("200 0.1 500", "500 0.1 500"), "request 2 answered HTTP 500"),
    (("200 0.1 499",), "request 1 returned 499 rows, not 500"),
    (("200 0.001 0",), "request 1 returned 0 rows, not 500"),
    (("timeout",), "request 1 failed"),
])
def test_a_bad_response_fails_the_latency_check(host, curl, reason):
    result = run(host, curl=curl)
    assert result.returncode == 1
    assert f"api p95 (500 rows): FAIL ({reason})" in result.stdout


@pytest.mark.parametrize(("psql_out", "message"), [
    ("candle capture|90 %|>= 99.9 %|FAIL\nSCORECARD-DB: FAIL", None),
    ("candle capture|100 %|>= 99.9 %|PASS", "expected one verdict line, found 0"),
    ("SCORECARD-DB: PASS\nSCORECARD-DB: PASS", "expected one verdict line, found 2"),
])
def test_the_database_verdict_decides(host, psql_out, message):
    result = run(host, FAKE_PSQL_OUT=psql_out)
    assert result.returncode == 1
    assert result.stdout.rstrip().endswith("SCORECARD: FAIL")
    if message:
        assert message in result.stdout


def test_a_failing_query_stops_the_scorecard(host):
    result = run(host, FAKE_PSQL_OUT="ERROR:  canceling statement due to statement timeout", FAKE_PSQL_EXIT="3")
    assert result.returncode == 1
    assert "database checks: FAIL (the scorecard SQL failed)" in result.stdout
    assert not (host / "curl.count").exists()


def test_a_stopped_monitor_stops_the_scorecard(host):
    result = run(host, FAKE_MONITOR_EXIT="1")
    assert result.returncode == 1
    assert "is the monitor running?" in result.stderr
    assert not any("psql" in c for c in calls(host))


def test_a_collector_on_other_config_fails_and_is_named(host):
    (host / "hashes" / "collector-bybit").write_text("older")
    result = run(host)
    assert result.returncode == 1
    assert "config consistent: FAIL (differs from the monitor: collector-bybit)" in result.stdout


def test_no_running_collector_fails_the_config_check(host):
    result = run(host, FAKE_SERVICES="api\nmonitor")
    assert result.returncode == 1
    assert "config consistent: FAIL (no running collector)" in result.stdout
