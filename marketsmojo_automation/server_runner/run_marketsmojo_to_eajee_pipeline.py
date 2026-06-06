import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

def run_step(name, command):
    print(f"\n===== {name} =====")
    result = subprocess.run(
        command,
        cwd=BASE_DIR,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"{name} failed with exit code {result.returncode}")

def main():
    run_step(
        "Generate MarketsMojo HTML",
        [sys.executable, "server_runner/run_server_cycle.py"],
    )

    run_step(
        "Upload HTML to Eajee",
        [sys.executable, "server_runner/upload_mojo_html.py"],
    )

    print("\nPIPELINE_SUCCESS")

if __name__ == "__main__":
    main()
