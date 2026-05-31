# Database and Credentials

Database:
atms

Table:
users

Required fields:
- zerodha_user_id
- zerodha_password
- zerodha_totp_hash

Credential Modes

local_json:
secrets/zerodha_<USER>.json

server_db:
ATMS PostgreSQL users table

auto:
Prefer local JSON, otherwise database.
