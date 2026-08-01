#!/usr/bin/env bash
set -euo pipefail

cd /opt/kite_services

set -a
. /opt/kite_services/.env
set +a

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_fo_universe_validation_csv.py

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_directional_accuracy_report.py

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_directional_accuracy_summary.py

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_strength_lift_report.py

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_validation_dashboard_split.py

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
engines/strategy_validation_engine/scripts/generate_validation_dashboard_payload.py

mkdir -p /home/sivapanduri/instance/strategy_validation

cp \
/opt/kite_services/data/strategy_validation/validation_dashboard_payload.json \
/home/sivapanduri/instance/strategy_validation/validation_dashboard_payload.json
