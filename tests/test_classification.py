"""Unit tests for record classification into Valid, Corrected, and Quarantine (Section 6.8 & Section 8 of PDF)."""
from __future__ import annotations

from src.quality_rules import classify_errors


def test_quarantine_single_error():
    assert classify_errors(["MISSING_ORDER_ID"]) == "quarantine"
    assert classify_errors(["INVALID_IMPOSSIBLE_DATE"]) == "quarantine"
    assert classify_errors(["CORRUPTED_ITEMS_JSON"]) == "quarantine"


def test_quarantine_multiple_conflicting_errors():
    errors = ["MISSING_ORDER_ID", "INVALID_IMPOSSIBLE_DATE", "UNKNOWN_PRICE"]
    assert classify_errors(errors) == "quarantine"


def test_valid_record_no_errors():
    assert classify_errors([]) == "valid"


def test_quarantine_all_error_codes():
    """Each known error code should result in quarantine."""
    from src.quality_rules import ERROR_CODES
    for code in ERROR_CODES:
        assert classify_errors([code]) == "quarantine", f"Failed for {code}"


def test_corrected_status_distinction():
    """
    classify_errors only distinguishes 'quarantine' vs 'valid'.
    The 'corrected' status is determined in elt_pipeline.py by
    checking if the corrections array has elements.
    Records with no errors but with corrections are 'corrected'.
    Records with no errors and no corrections are 'valid'.
    This is by design: corrections happen at the Spark level.
    """
    # No errors = valid (could be corrected or clean, determined later)
    assert classify_errors([]) == "valid"
    # Any error = quarantine
    assert classify_errors(["EMPTY_ITEMS"]) == "quarantine"
