# System Overview

Purpose:
Automate Zerodha token refresh via EAJEE.

Production flow:
Cron -> refresh_selected_tokens.py -> refresh_user_token.py -> EAJEE Login -> Zerodha Login -> TOTP -> Verification -> Email Report.

Current production users:
XJ1877, SQW865, OMK569, DKJ644.
