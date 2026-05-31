# Architecture and Design

Browser Layer
- browser.py creates a fresh Chromium browser and context for every run.

Configuration Layer
- config.py centralizes URLs, credentials and paths.

Credential Layer
- credentials.py supports:
  - local_json
  - server_db
  - auto

Execution Layer
- refresh_user_token.py performs a complete refresh for a single user.

Batch Layer
- refresh_selected_tokens.py executes users sequentially and sends email reports.

Artifact Layer
- run_context.py creates run directories and ZIP archives.
