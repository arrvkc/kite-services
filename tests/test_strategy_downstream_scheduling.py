"""Exercise existing report entrypoints with local fixture runtime only."""
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def runtime(tmp_path, script):
    for directory in ("venv/bin", "scripts/runtime"):
        (tmp_path / directory).mkdir(parents=True, exist_ok=True)
    (tmp_path / ".env").write_text("")
    (tmp_path / "scripts/runtime/configure_host_database_runtime.sh").write_text("")
    python = tmp_path / "venv/bin/python"
    python.write_text('''#!/bin/bash
if [[ "$1" = -c ]]; then exec python3 "$@"; fi
printf '%s\\n' "$*" >> "$CALLS"
''')
    python.chmod(0o755)
    for name in ("publish_transition_dashboard_archive.sh", "run_signal_research_daily_db.sh"):
        (tmp_path / "scripts" / name).write_text(f'printf "{name}\\n" >> "$CALLS"\n')
    target = tmp_path / "entry.sh"
    target.write_text((ROOT / "scripts" / script).read_text().replace("/opt/kite_services", str(tmp_path)))
    return target, dict(os.environ, CALLS=str(tmp_path / "calls"), EAJEE_STRATEGY_USER_ID="XJ1877")


def test_transition_exact_date_is_data_only_and_report_mode_never_acquires(tmp_path):
    target, env = runtime(tmp_path, "run_transition_dashboard_pipeline.sh")
    subprocess.run(["bash", str(target), "--run-date", "2026-09-21"], env=env, check=True)
    calls = (tmp_path / "calls").read_text().splitlines()
    assert len(calls) == 2
    assert "generate_transition_report_from_db.py --run-date 2026-09-21 --user-id XJ1877" in calls[0]
    assert calls[1] == "publish_transition_dashboard_archive.sh"
    (tmp_path / "calls").write_text("")
    subprocess.run(["bash", str(target), "--report-only"], env=env, check=True)
    calls = (tmp_path / "calls").read_text()
    assert "send_transition_dashboard_email.py" in calls
    assert "generate_transition" not in calls and "publish_transition" not in calls


def test_signal_scheduled_report_does_not_reacquire_data(tmp_path):
    target, env = runtime(tmp_path, "run_signal_research_daily_report.sh")
    subprocess.run(["bash", str(target), "--report-only"], env=env, check=True)
    calls = (tmp_path / "calls").read_text()
    assert "send_signal_research_email_report.py" in calls
    assert "run_signal_research_daily_db.sh" not in calls
