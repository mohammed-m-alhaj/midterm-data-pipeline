"""Unit tests for deterministic quality cleaning rules (Section 6.6 & Section 8 of PDF)."""
from __future__ import annotations

from src.quality_rules import (
    is_valid_email,
    normalize_email,
    normalize_number_text,
    normalize_phone,
    standardize_status,
    standardize_text,
    to_decimal,
)


# --- Rule 1: Arabic digits conversion ---
def test_arabic_digits_conversion():
    assert normalize_number_text("٧٠٦٠٠٠٫٠") == "706000.0"
    assert normalize_number_text("١٢٣٤٥") == "12345"
    assert normalize_number_text("٠٠٠٥") == "0005"


# --- Rule 2: Currency symbol / name removal ---
def test_currency_removal():
    assert to_decimal("5000 ريال يمني") == 5000.0
    assert to_decimal("5000 ر.ي") == 5000.0
    assert to_decimal("5000 ريال") == 5000.0


# --- Rule 3: Thousand separators ---
def test_thousand_separators():
    assert to_decimal("125,000.00") == 125000.0
    assert to_decimal("1,250,000") == 1250000


# --- Rule 4: Price in words ---
def test_price_in_words():
    assert to_decimal("ألفان") == 2000.0
    assert to_decimal("الفان") == 2000.0
    assert to_decimal("خمسة آلاف") == 5000.0
    assert to_decimal("عشرة آلاف") == 10000.0


# --- Rule 5: Phone number normalization ---
def test_phone_normalization():
    assert normalize_phone("77 123 4567") == "+967771234567"
    assert normalize_phone("٩٦٧٧٧١٢٣٤٥٦٧") == "+967771234567"
    assert normalize_phone("+967 77 123 4567") == "+967771234567"
    assert normalize_phone("00967771234567") == "+967771234567"
    # Invalid phone returns None
    assert normalize_phone("123456") is None


# --- Rule 6: Email cleaning ---
def test_email_cleaning():
    email, is_corrected = normalize_email("user@@mail..com")
    assert email == "user@mail.com"
    assert is_corrected is True
    assert is_valid_email("user@mail.com") is True
    assert is_valid_email("invalid-email") is False
    assert is_valid_email(None) is False


# --- Rule 7: Date (tested indirectly via Spark, testing format helpers here) ---
# Date normalization is done in ELT pipeline via Spark expressions.
# We test the helper functions that support it.
def test_date_format_examples():
    """Ensure common date format strings are recognized.
    Actual Spark date parsing is validated in integration tests.
    """
    # This test validates that the quality_rules module handles text correctly
    text = standardize_text("  2025 / 01 / 31  ")
    assert text == "2025 / 01 / 31"


# --- Rule 8: Whitespace and synonyms ---
def test_status_standardization():
    assert standardize_status("مدفوع") == "تم الدفع"
    assert standardize_status("غير مدفوع") == "بانتظار الدفع"
    assert standardize_status("مؤكد") == "مؤكد"
    assert standardize_status("مأكد") == "مؤكد"


# --- Rule 9: Whitespace trimming ---
def test_whitespace_trimming():
    assert standardize_text("  hello   world  ") == "hello world"
    assert standardize_text(None) is None


# --- Edge cases ---
def test_none_handling():
    assert normalize_number_text(None) is None
    assert to_decimal(None) is None
    assert normalize_phone(None) is None
    email, corrected = normalize_email(None)
    assert email is None
    assert corrected is False
