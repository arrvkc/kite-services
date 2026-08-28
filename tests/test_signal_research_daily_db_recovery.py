import os
import subprocess
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_signal_research_daily_db.sh"


def test_signal_research_db_supports_exact_date_without_changing_cron_default(tmp_path):
    fake_docker = tmp_path / "docker"
    captured_sql = tmp_path / "sql"
    captured_args = tmp_path / "args"
    fake_docker.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$@\" > \"$CAPTURED_ARGS\"\n"
        "cat > \"$CAPTURED_SQL\"\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{tmp_path}:{environment['PATH']}",
            "CAPTURED_ARGS": str(captured_args),
            "CAPTURED_SQL": str(captured_sql),
        }
    )

    exact = subprocess.run(
        ["bash", str(SCRIPT), "--run-date", "2026-08-27"],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert exact.returncode == 0
    assert "recovery_run_date=2026-08-27" in captured_args.read_text()
    sql = captured_sql.read_text()
    assert sql.count("e.source_run_date = NULLIF(:'recovery_run_date', '')::date") == 2
    assert "s.run_date = NULLIF(:'recovery_run_date', '')::date" in sql

    default = subprocess.run(
        ["bash", str(SCRIPT)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert default.returncode == 0
    assert "recovery_run_date=" in captured_args.read_text()


def test_signal_research_db_rejects_invalid_recovery_date(tmp_path):
    result = subprocess.run(
        ["bash", str(SCRIPT), "--run-date", "2026-02-30"],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    assert result.returncode == 64
    assert "Invalid --run-date" in result.stderr
