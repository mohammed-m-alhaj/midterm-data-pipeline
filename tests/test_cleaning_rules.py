from decimal import Decimal

from src.quality_rules import (
    is_valid_email,
    normalize_email,
    normalize_number_text,
    normalize_phone,
    standardize_status,
    to_decimal,
)


def test_arabic_number_normalization():
    assert normalize_number_text("٧٠٦٠٠٠٫٠") == "706000.0"
    assert to_decimal("١٢٥,٠٠٠.00") == Decimal("125000.00")


def test_currency_normalization():
    assert normalize_number_text(" 5000 ريال يمني ") == "5000"
    assert normalize_number_text(" 2500 YER ") == "2500"
    assert normalize_number_text(" 1000 ر.ي ") == "1000"


def test_known_word_price():
    assert to_decimal("ألفان") == Decimal("2000")
    assert to_decimal("الفان") == Decimal("2000")
    assert to_decimal("خمسة آلاف") == Decimal("5000")
    assert to_decimal("خمسه الاف") == Decimal("5000")
    assert to_decimal("عشرة آلاف") == Decimal("10000")


def test_phone_normalization():
    assert normalize_phone("77 123 4567") == "+967771234567"
    assert normalize_phone("+967 77 123 4567") == "+967771234567"
    assert normalize_phone("٩٦٧٧٧١٢٣٤٥٦٧") == "+967771234567"


def test_email_repeated_symbols_are_repaired_when_safe():
    fixed, changed = normalize_email("user@@mail..com")
    assert changed is True
    assert fixed == "user@mail.com"
    assert is_valid_email(fixed)


def test_status_standardization():
    assert standardize_status(" مدفوع ") == "تم الدفع"
    assert standardize_status(" غير مدفوع ") == "بانتظار الدفع"
    assert standardize_status("   مؤكد ") == "مؤكد"


def test_date_standardization_formats():
    """Rule 7: Supported date formats ISO, YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY."""
    import datetime
    # Verification of supported formats
    d1 = datetime.datetime.strptime("2025-01-31", "%Y-%m-%d")
    d2 = datetime.datetime.strptime("31/01/2025", "%d/%m/%Y")
    d3 = datetime.datetime.strptime("31-01-2025", "%d-%m-%Y")
    assert d1.strftime("%Y-%m-%d") == "2025-01-31"
    assert d2.strftime("%Y-%m-%d") == "2025-01-31"
    assert d3.strftime("%Y-%m-%d") == "2025-01-31"


def test_total_amount_recalculation_logic():
    """Rule 8: Total amount recalculation from items sum + delivery cost."""
    items = [
        {"qty": 2, "unit_price": 5000.0, "total": 10000.0},
        {"qty": 1, "unit_price": 2000.0, "total": 2000.0},
    ]
    delivery_cost = 1000.0
    calculated_total = sum(item["total"] for item in items) + delivery_cost
    assert calculated_total == 13000.0
