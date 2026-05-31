# EAJEE Zerodha Token Refresh Automation
## Production Operations, Architecture, Deployment and Maintenance Guide

Version: 1.0
Project: kite_services
Module: automations/eajee_web_automation

---

# 1. Executive Summary

This automation refreshes Zerodha Kite access tokens for selected users through the EAJEE web application.

The solution uses Playwright browser automation to:

1. Login to EAJEE.
2. Navigate to the user administration page.
3. Trigger Zerodha Connect.
4. Complete Zerodha login.
5. Generate and submit TOTP.
6. Verify successful token refresh.
7. Log results.
8. Capture artifacts.
9. Send operational email reports.
10. Retain diagnostic evidence.

The solution supports both:

- Mac local development
- Linux production server execution

and is designed for unattended daily execution through cron.

---

# 2. Business Objective

Zerodha access tokens expire regularly and must be refreshed.

Manual refresh requires:

- Opening EAJEE
- Opening Zerodha Connect
- Entering credentials
- Entering TOTP

This process is repetitive and operationally risky.

Automation removes the manual dependency.

---

# 3. Scope

Included:

- EAJEE login
- Zerodha login
- TOTP generation
- Email reporting
- Cron execution
- Run artifact capture
- Automated cleanup

Excluded:

- Order placement
- Trading strategies
- Position management
- Risk engine execution

---

# 4. Production Users

Current production users:

- XJ1877
- SQW865
- OMK569
- DKJ644

New users must be individually verified before addition.

---

# 5. Architecture

Flow:

Cron
→ Python
→ Playwright
→ EAJEE
→ Zerodha
→ Redirect Back
→ Verification
→ Logging
→ Email

---

# 6. Directory Structure

automations/eajee_web_automation/

Files:

- config.py
- browser.py
- login.py
- credentials.py
- zerodha_login.py
- refresh_user_token.py
- refresh_selected_tokens.py
- run_context.py
- inspect_users_page.py
- test_browser_cache.py
- fetch_user_credentials_from_server.sh

---

# 7. Runtime Modes

## Local Mode

Credential source:

secrets/zerodha_<USER>.json

Used for:

- Development
- Debugging
- Testing

## Production Mode

Credential source:

ATMS PostgreSQL users table

Used for:

- Daily execution
- Cron execution

---

# 8. Database Dependency

Database:

atms

Table:

users

Required fields:

- username
- zerodha_user_id
- zerodha_password
- zerodha_totp_hash

Optional:

- kite_api_key
- kite_api_secret
- kite_access_token

---

# 9. TOTP Handling

The system generates TOTP using:

pyotp

The stored value must be the actual secret.

Verification procedure:

1. Generate TOTP from database value.
2. Compare with Google Authenticator.
3. Proceed only if identical.

---

# 10. Browser Design

Browser engine:

Chromium

Framework:

Playwright

Execution mode:

Headless

Production does not require GUI access.

---

# 11. Fresh Browser Principle

Every execution uses:

- New browser
- New context
- No cached cookies
- No local storage reuse
- No session reuse

Purpose:

Avoid hidden dependencies.

---

# 12. Login Flow

EAJEE Login

Open:

https://eajee.in/login

Actions:

- Enter username
- Enter password
- Submit
- Verify redirect

---

# 13. User Discovery

Open:

https://eajee.in/data/users

Locate:

target user row

Locate:

Refresh / Connect link

---

# 14. Zerodha Flow

After refresh click:

Redirect to:

https://kite.zerodha.com/

Actions:

- Enter user ID
- Enter password
- Generate TOTP
- Submit

Expected:

Redirect back to EAJEE.

---

# 15. Success Validation

Successful refresh requires:

- Zerodha login success
- Redirect back to EAJEE
- Final URL verification

Expected:

Status = SUCCESS

---

# 16. Failure Handling

Failures do not stop batch processing.

Example:

User 1 → Success
User 2 → Failure
User 3 → Success
User 4 → Success

Final result:

PARTIAL_FAILURE

---

# 17. Logging Strategy

Main file:

refresh_token_results.csv

Contains:

- timestamp
- user
- status
- stage
- error
- run directory

---

# 18. Run Artifacts

Every run generates:

runs/<RUN_ID>/

Possible contents:

- screenshots
- HTML dumps
- traceback
- page text
- zip archive

---

# 19. ZIP Artifacts

Each run is archived.

Purpose:

- Incident investigation
- Support
- Root cause analysis

---

# 20. Email Reporting

Report includes:

- Overall status
- Success count
- Failure count
- Per-user results

Subjects:

[EAJEE] Zerodha Token Refresh - SUCCESS

[EAJEE] Zerodha Token Refresh - PARTIAL_FAILURE

---

# 21. Production Environment

Base directory:

/opt/kite_services

Logs:

/opt/kite_services/logs/eajee_web_automation

---

# 22. Environment Variables

Required:

SMTP_HOST
SMTP_PORT
SMTP_USER
SMTP_PASSWORD

REPORT_MAIL_TO
REPORT_MAIL_FROM

ATMS_DATABASE_URL

---

# 23. Server Dependencies

Python

Playwright

Chromium

pyotp

---

# 24. Installation

Activate virtual environment.

Install:

pip install playwright pyotp

Install browser:

playwright install chromium

---

# 25. Production Command

EAJEE_CREDENTIAL_MODE=server_db
KITE_SERVICES_BASE_DIR=/opt/kite_services

Run refresh_selected_tokens.py

---

# 26. Cron Schedule

Production cron:

08:15 IST

Equivalent:

02:45 UTC

Daily execution.

---

# 27. Cron Philosophy

Single cron.

Single responsibility.

Workflow:

Refresh
→ Email
→ Cleanup

---

# 28. Cleanup Policy

Retention:

30 days

Delete:

runs older than 30 days

Logged to:

cron.log

---

# 29. Monitoring

Daily checks:

tail -20 refresh_token_results.csv

tail -100 cron.log

---

# 30. Troubleshooting

Common Issues:

Credential missing
TOTP mismatch
Playwright missing
Chromium missing
Selector changed
User row not found
Redirect failure

---

# 31. Security Considerations

Never commit:

- .env
- secrets
- screenshots
- logs
- ZIP artifacts

Protect:

SMTP credentials
TOTP secrets
Zerodha passwords

---

# 32. Upgrade Roadmap

Future improvements:

1. Slack alerts
2. Telegram alerts
3. Config-based user list
4. Retry support
5. API-based refresh
6. Token validation
7. Health dashboard
8. Metrics collection

---

# 33. Operational Verification Checklist

Verify:

- Playwright installed
- Chromium installed
- Single user success
- Four user success
- Email received
- Cron installed
- Cleanup active
- Logs generated

---

# 34. Deployment Procedure

Mac Development
→ Testing
→ Git Commit
→ Git Push
→ Deploy Script
→ Server Verification

Direct server code changes are discouraged.

---

# 35. Summary

This automation is:

- Production Ready
- Headless
- Cron Safe
- Auditable
- Recoverable
- Environment Aware
- Email Monitored
- Suitable for Daily Unattended Execution
