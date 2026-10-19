"""Tests for scripts/offsite-lib.sh, driven through real bash (and real restic and rclone where
installed)."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

LIB = Path(__file__).resolve().parent.parent / "scripts" / "offsite-lib.sh"
BASE_PATH = "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")

# The two lines scripts/backup-offsite.sh builds the dump command's `env` arguments with.
STRIP_ENV = 'strip=$(restic_strip_env_args); read -r -a STRIP_ENV <<< "$strip"'


def run_lib(script, env_file, path=BASE_PATH):
    """Runs `script` after sourcing the library, under the scripts' own strict mode."""
    return subprocess.run(
        ["bash", "-c", f'set -euo pipefail; . "{LIB}"; {script}'],
        # HOME: restic keeps its cache under it.
        env={"PATH": path, "RESTIC_ENV_FILE": str(env_file), "HOME": str(env_file.parent)},
        capture_output=True,
        text=True,
        timeout=120,
    )


def fake_restic(tmp_path):
    """A `restic` that prints each argument, then the environment it was given."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    restic = bin_dir / "restic"
    restic.write_text('#!/bin/sh\nfor a in "$@"; do echo "arg:$a"; done\nenv | sed "s/^/env:/"\n')
    restic.chmod(0o755)
    return f"{bin_dir}:{BASE_PATH}"


def test_restic_run_starts_rclone_without_hard_delete(tmp_path):
    env_file = tmp_path / "restic.env"
    env_file.write_text("RESTIC_REPOSITORY=rclone:b2:bucket/repo\nRCLONE_CONFIG_B2_KEY=secret\n")
    result = run_lib(
        'restic_run snapshots --json; echo "after:${RCLONE_CONFIG_B2_KEY:-unset}"',
        env_file,
        path=fake_restic(tmp_path),
    )
    assert result.returncode == 0, result.stderr
    args = [line[4:] for line in result.stdout.splitlines() if line.startswith("arg:")]
    assert args == ["-o", "rclone.args=serve restic --stdio", "snapshots", "--json"]
    assert "--b2-hard-delete" not in result.stdout
    assert "env:RCLONE_CONFIG_B2_KEY=secret" in result.stdout.splitlines()
    assert result.stdout.splitlines()[-1] == "after:unset"


def strip_env(env_file):
    """Runs the caller's pair; returns the run and the array, one element per line."""
    result = run_lib(
        f'{STRIP_ENV}; [ ${{#STRIP_ENV[@]}} -eq 0 ] || printf "%s\\n" "${{STRIP_ENV[@]}}"',
        env_file,
    )
    return result, result.stdout.splitlines()


def test_restic_strip_env_args_names_every_assignment_and_no_value(tmp_path):
    env_file = tmp_path / "restic.env"
    env_file.write_text(
        "# restic\n"
        "RESTIC_REPOSITORY=rclone:b2:bucket/candle-collector\n"
        "\n"
        "export RESTIC_PASSWORD_FILE=/etc/candle-collector/restic-password\n"
        "  RCLONE_CONFIG_B2_TYPE=b2\n"
        "not an assignment\n"
        "RCLONE_CONFIG_B2_KEY=canary-value with spaces=and equals\n"
    )
    result, array = strip_env(env_file)
    assert result.returncode == 0, result.stderr
    assert array == [
        "-u", "RESTIC_REPOSITORY",
        "-u", "RESTIC_PASSWORD_FILE",
        "-u", "RCLONE_CONFIG_B2_TYPE",
        "-u", "RCLONE_CONFIG_B2_KEY",
    ]  # fmt: skip
    assert "canary" not in result.stdout


def test_restic_strip_env_args_without_assignments_is_empty(tmp_path):
    env_file = tmp_path / "restic.env"
    env_file.write_text("# nothing yet\n\n")
    result, array = strip_env(env_file)
    assert (result.returncode, array) == (0, [])


def test_restic_strip_env_args_reads_crlf_names_clean(tmp_path):
    env_file = tmp_path / "restic.env"
    env_file.write_bytes(b"RESTIC_REPOSITORY=x\r\nRCLONE_CONFIG_B2_KEY=y\r\n")
    result, array = strip_env(env_file)
    assert result.returncode == 0, result.stderr
    assert array == ["-u", "RESTIC_REPOSITORY", "-u", "RCLONE_CONFIG_B2_KEY"]


def test_restic_strip_env_args_fails_on_a_missing_env_file(tmp_path):
    result = run_lib(f"{STRIP_ENV}; echo reached", tmp_path / "missing.env")
    assert result.returncode != 0
    assert "reached" not in result.stdout


@pytest.mark.parametrize("weekday", range(1, 7))
def test_read_back_subset_is_empty_on_weekdays(tmp_path, weekday):
    result = run_lib(f'echo "[$(read_back_subset {weekday} 43)]"', tmp_path / "unused.env")
    assert (result.returncode, result.stdout) == (0, "[]\n")


@pytest.mark.parametrize(
    "week, subset",
    [("01", "2/4"), ("08", "1/4"), ("09", "2/4"), ("43", "4/4"), ("52", "1/4"), ("53", "2/4")],
)
def test_read_back_subset_rotates_quarters_on_sundays(tmp_path, week, subset):
    result = run_lib(f"read_back_subset 7 {week}", tmp_path / "unused.env")
    assert (result.returncode, result.stdout) == (0, f"{subset}\n")


def restic_with_stdin_from_command():
    restic = shutil.which("restic", path=BASE_PATH)
    if restic is None or shutil.which("rclone", path=BASE_PATH) is None:
        return False
    version = subprocess.run([restic, "version"], capture_output=True, text=True).stdout
    match = re.search(r"restic (\d+)\.(\d+)", version)
    return match is not None and (int(match[1]), int(match[2])) >= (0, 17)


@pytest.mark.skipif(not restic_with_stdin_from_command(), reason="needs restic >= 0.17 and rclone")
def test_dump_command_gets_no_env_file_variable_over_rclone(tmp_path):
    (tmp_path / "password").write_text("test-password\n")
    env_file = tmp_path / "restic.env"
    env_file.write_text(
        f"RESTIC_REPOSITORY=rclone:loc:{tmp_path}/repo\n"
        f"RESTIC_PASSWORD_FILE={tmp_path}/password\n"
        "RCLONE_CONFIG_LOC_TYPE=local\n"
        "RCLONE_CONFIG_LOC_CANARY=canary-7f3a\n"
    )
    backup = "restic_run backup --quiet --stdin-filename env.txt --stdin-from-command --"
    dump = "restic_run dump --quiet latest /env.txt"
    result = run_lib(
        "restic_run init --quiet > /dev/null; "
        f'{STRIP_ENV}; {backup} env "${{STRIP_ENV[@]}}" sh -c env; '
        f"echo ===stripped; {dump}; "
        f"{backup} sh -c env; "
        f"echo ===control; {dump}; "
        "echo ===prune; restic_run forget --quiet --keep-last 1 --prune",
        env_file,
    )
    assert result.returncode == 0, result.stderr
    stripped = result.stdout.split("===stripped\n")[1].split("===control\n")[0]
    control = result.stdout.split("===control\n")[1].split("===prune\n")[0]
    assert "PATH=" in stripped
    file_names = [line.split("=", 1)[0] for line in env_file.read_text().splitlines()]
    assert not [line for line in stripped.splitlines() if line.split("=", 1)[0] in file_names]
    assert "RCLONE_CONFIG_LOC_CANARY=canary-7f3a" in control.splitlines()
