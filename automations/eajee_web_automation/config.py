import os
from pathlib import Path

BASE_DIR = Path(
    os.getenv(
        "KITE_SERVICES_BASE_DIR",
        str(Path.home() / "kite_services"),
    )
)

BASE_URL = "https://eajee.in"
LOGIN_URL = "https://eajee.in/login"
TARGET_URL = "https://eajee.in/data/users"

USERNAME = os.getenv("EAJEE_USERNAME", "eajee_admin")
PASSWORD = os.getenv("EAJEE_PASSWORD", "welcome@123")

LOG_DIR = BASE_DIR / "logs" / "eajee_web_automation"
RUNS_DIR = LOG_DIR / "runs"
SCREENSHOT_DIR = LOG_DIR / "screenshots"

LOG_DIR.mkdir(parents=True, exist_ok=True)
RUNS_DIR.mkdir(parents=True, exist_ok=True)
SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
