# Kite Services — Codex Instructions

## Responsibility

This repository owns strategy, validation, signal, risk and automation engines,
host-run pipelines and selected generated browser reports.

It does not own:

- the ATMS Flask application menu;
- ATMS login or public layouts;
- ATMS route authorization;
- the primary ATMS application logo.

Production composition and deployment belong to the parent ATMS Platform
repository.

## Current EAJEE UI task

For the current favicon task, code changes should be confined to:

- `engines/strategy_deterministic_engine/scripts/generate_transition_html_report.py`
- `scripts/publish_transition_dashboard_archive.sh`
- narrowly focused tests for those outputs
- `AGENTS.md`

Do not modify validation generators unless production platform evidence proves
their standalone HTML is directly served.

## Report rules

Modify report generators, not generated output.

Preserve:

- report data;
- calculations;
- tables;
- schemas;
- output filenames;
- publication paths;
- email behavior;
- cron behavior.

The production transition report may reference the shared root-relative asset:

- `/static/favicon.svg`

Do not copy ATMS templates or brand assets into this repository.

## Engine safety

Do not modify strategy, signal, risk, stop, options or trading calculations as
part of a browser-shell task.

Do not change database contracts, cron sequencing or authentication automation.

Do not run live browser automation or production login workflows during local
UI development.

## Secrets and runtime data

Do not inspect, print, modify or commit:

- `.env`;
- credentials;
- access tokens;
- cookies;
- browser profiles;
- audit payloads;
- logs;
- production data;
- generated CSV, JSON or HTML reports.

## Git safety

Treat all existing tracked and untracked work as valuable.

Do not clean, reset, stash, overwrite or delete unrelated work.

Do not use `git add .` or `git add -A`. Stage explicit paths only.

Never force-push or rewrite history.

## Dependencies and validation

Do not install or upgrade dependencies without explicit instruction.

Before proposing a commit:

- inspect the complete diff;
- run `git diff --check`;
- run focused report-generator tests;
- confirm report data-generation logic is unchanged;
- confirm only intended source files changed;
- leave unrelated failures unchanged and documented.

Never push or deploy from an ordinary coding session.
