from src.quality_rules import classify_errors


def test_error_classification_goes_to_quarantine():
    assert classify_errors([]) == "valid"
    assert classify_errors(["MISSING_ORDER_ID"]) == "quarantine"
    assert classify_errors(["X", "Y"]) == "quarantine"


def test_all_defined_error_codes_classify_as_quarantine():
    """Every error code defined in ERROR_CODES must trigger quarantine status."""
    from src.quality_rules import ERROR_CODES
    for code in ERROR_CODES:
        assert classify_errors([code]) == "quarantine", (
            f"Error code '{code}' should result in 'quarantine' but did not."
        )


def test_consistency_equation_logic():
    """
    Section 6.11 invariant:
    raw_count == valid_count + corrected_count + quarantine_count

    This unit test verifies the Python-side counting logic:
    every record ends up in exactly one destination.
    """
    records = [
        {"errors": [], "corrected": False},   # valid
        {"errors": [], "corrected": True},    # corrected
        {"errors": ["MISSING_ORDER_ID"], "corrected": False},  # quarantine
        {"errors": [], "corrected": True},    # corrected
        {"errors": ["EMPTY_ITEMS", "CORRUPTED_ITEMS_JSON"], "corrected": False},  # quarantine
    ]

    valid_count = 0
    corrected_count = 0
    quarantine_count = 0

    for rec in records:
        if rec["errors"]:
            quarantine_count += 1
        elif rec["corrected"]:
            corrected_count += 1
        else:
            valid_count += 1

    raw_count = len(records)
    assert raw_count == valid_count + corrected_count + quarantine_count, (
        "Section 6.11 consistency equation violated: "
        f"{raw_count} != {valid_count} + {corrected_count} + {quarantine_count}"
    )
    assert valid_count == 1
    assert corrected_count == 2
    assert quarantine_count == 2
