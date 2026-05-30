import shutil
from pathlib import Path
from datetime import datetime

BASE_DIR = Path.home() / "kite_services"

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

RUNS_BASE_DIR = (
    BASE_DIR
    / "logs"
    / "eajee_web_automation"
    / "runs"
)

RUN_DIR = RUNS_BASE_DIR / RUN_ID

RUN_DIR.mkdir(parents=True, exist_ok=True)

ZIP_FILE = RUNS_BASE_DIR / f"{RUN_ID}.zip"


def zip_run():
    if ZIP_FILE.exists():
        ZIP_FILE.unlink()

    shutil.make_archive(
        str(ZIP_FILE.with_suffix("")),
        "zip",
        root_dir=str(RUN_DIR),
    )

    print(f"ZIP_CREATED={ZIP_FILE}")

    return ZIP_FILE


print(f"RUN_DIR={RUN_DIR}")
