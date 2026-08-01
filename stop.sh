#!/bin/bash
cd /opt/kite_services
set -a
. ./.env
set +a
PYTHONPATH=.:services /opt/kite_services/venv/bin/python engines/stop_engine/stop_orchestrator.py "$@"
