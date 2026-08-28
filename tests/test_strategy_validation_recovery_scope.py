import subprocess
from datetime import date
from pathlib import Path

import pandas as pd

from engines.strategy_validation_engine.scripts import (
    generate_fo_universe_validation_csv as validation_csv,
)


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_strategy_validation_reports.sh"


def test_validation_generator_filters_future_rows_for_recovery():
    input_rows = pd.DataFrame(
        {
            "symbol": ["ALPHA", "ALPHA"],
            "date": [date(2026, 8, 27), date(2026, 8, 28)],
        }
    )
    bounded = validation_csv.bound_trend_history(input_rows, date(2026, 8, 27))

    assert bounded["date"].tolist() == [date(2026, 8, 27)]
    assert validation_csv.bound_trend_history(input_rows, None) is input_rows


def test_validation_wrapper_passes_recovery_boundary_and_preserves_default():
    invalid = subprocess.run(
        ["bash", str(SCRIPT), "--through-date", "2026-02-30"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert invalid.returncode == 64
    assert "Invalid --through-date" in invalid.stderr

    script_text = SCRIPT.read_text(encoding="utf-8")
    assert 'VALIDATION_DATE_ARGS=(--through-date "$THROUGH_DATE")' in script_text
    assert 'through_date=${THROUGH_DATE:-ALL}' in script_text
