from automations.eajee_web_automation.refresh_selected_tokens import (
    evaluate_batch_result,
)


def test_exactly_one_persisted_success_per_requested_user_passes():
    rows = [
        {"user_id": "OMK569", "status": "SUCCESS"},
        {"user_id": "DA6170", "status": "SUCCESS"},
    ]

    assert evaluate_batch_result(["OMK569", "DA6170"], rows) == "SUCCESS"


def test_empty_or_missing_persisted_results_fail_closed():
    assert evaluate_batch_result(["OMK569", "DA6170"], []) == "PARTIAL_FAILURE"
    assert (
        evaluate_batch_result(
            ["OMK569", "DA6170"],
            [{"user_id": "OMK569", "status": "SUCCESS"}],
        )
        == "PARTIAL_FAILURE"
    )


def test_duplicate_or_failed_persisted_results_fail_closed():
    assert (
        evaluate_batch_result(
            ["OMK569", "DA6170"],
            [
                {"user_id": "OMK569", "status": "SUCCESS"},
                {"user_id": "OMK569", "status": "SUCCESS"},
            ],
        )
        == "PARTIAL_FAILURE"
    )
    assert (
        evaluate_batch_result(
            ["OMK569", "DA6170"],
            [
                {"user_id": "OMK569", "status": "FAILED"},
                {"user_id": "DA6170", "status": "SUCCESS"},
            ],
        )
        == "PARTIAL_FAILURE"
    )
