# Source Code Walkthrough

config.py
- Base directory abstraction using KITE_SERVICES_BASE_DIR.

browser.py
- Creates Playwright Chromium browser.
- Uses fresh context.

login.py
- Performs EAJEE login.
- Opens target users page.

credentials.py
- Reads credentials from local JSON or PostgreSQL via docker exec.

zerodha_login.py
- Generates TOTP using pyotp.
- Completes Zerodha authentication.

refresh_user_token.py
- Main operational workflow.
- Logs results.
- Captures screenshots.
- Creates ZIP artifacts.

refresh_selected_tokens.py
- Batch orchestrator.
- Generates HTML email reports.

run_context.py
- Creates RUN_DIR and ZIP archive.
