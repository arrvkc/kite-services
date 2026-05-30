from pathlib import Path

BASE_URL = "https://eajee.in"

LOGIN_URL = "https://eajee.in/login"

TARGET_URL = "https://eajee.in/data/users"

USERNAME = "eajee_admin"

PASSWORD = "welcome@123"

BASE_DIR = Path.home() / "kite_services"

SCREENSHOT_DIR = (
    BASE_DIR
    / "logs"
    / "eajee_web_automation"
    / "screenshots"
)

SCREENSHOT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)
