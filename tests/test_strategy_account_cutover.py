import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from engines.strategy_deterministic_engine.scripts import (
    validate_strategy_kite_credentials as credential_validator,
)


ROOT = Path(__file__).resolve().parents[1]


def test_future_transition_and_stop_account_scopes_are_explicit():
    transition = (ROOT / "scripts/run_transition_dashboard_pipeline.sh").read_text()
    stop = (ROOT / "run_stop_cron.sh").read_text()

    assert '--user-id "${EAJEE_STRATEGY_USER_ID:-XJ1877}"' in transition
    assert "--user-id OMK569" not in transition
    assert 'USER_IDS=("XJ1877" "OMK569" "SQW865" "DKJ644")' in stop


def test_wrong_broker_profile_identity_still_fails_closed():
    kite = SimpleNamespace(
        set_access_token=Mock(),
        profile=Mock(return_value={"user_id": "OMK569"}),
    )
    with (
        patch.object(sys, "argv", ["validate", "XJ1877"]),
        patch.object(
            credential_validator,
            "get_kite_credentials",
            return_value=("api", "token"),
        ),
        patch.object(credential_validator, "KiteConnect", return_value=kite),
        pytest.raises(SystemExit, match="reason=wrong_account"),
    ):
        credential_validator.main()
