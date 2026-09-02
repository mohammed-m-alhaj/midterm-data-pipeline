from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any


ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

KNOWN_WORD_NUMBERS = {
    "ألفان": "2000",
    "الفان": "2000",
    "ألفين": "2000",
    "الفين": "2000",
    "خمسة آلاف": "5000",
    "خمسه آلاف": "5000",
    "خمسة الاف": "5000",
    "خمسه الاف": "5000",
    "خمسةآلاف": "5000",
    "خمسهآلاف": "5000",
    "خمسةالاف": "5000",
    "خمسهالاف": "5000",
    "عشرة آلاف": "10000",
    "عشره آلاف": "10000",
    "عشرة الاف": "10000",
    "عشره الاف": "10000",
    "عشرةآلاف": "10000",
    "عشرهآلاف": "10000",
}

ERROR_CODES = {
    "MISSING_ORDER_ID",
    "MISSING_CUSTOMER_ID",
    "INVALID_IMPOSSIBLE_DATE",
    "CORRUPTED_ITEMS_JSON",
    "EMPTY_ITEMS",
    "UNKNOWN_PRICE",
    "AMBIGUOUS_NEGATIVE_VALUE",
    "DUPLICATE_ORDER_ID",
    "MULTIPLE_CONFLICTING_ERRORS",
    "INVALID_EMAIL",
    "INVALID_PHONE",
    "INVALID_AMOUNT",
    "INVALID_CURRENCY",
}


def normalize_number_text(value: Any) -> str | None:
    """Normalize Arabic/English money text into a decimal-friendly string."""
    if value is None:
        return None

    text = str(value).strip().translate(ARABIC_DIGITS)
    if not text:
        return None

    text = text.replace("٫", ".")
    text = text.replace("٬", "")
    text = text.replace(",", "")
    text = re.sub(r"(?i)(ريال\s*يمني|ريال|ر\.ي|yer)", "", text).strip()

    if text in KNOWN_WORD_NUMBERS:
        return KNOWN_WORD_NUMBERS[text]

    compact = re.sub(r"\s+", "", text)
    return KNOWN_WORD_NUMBERS.get(compact, compact) or None


def to_decimal(value: Any) -> Decimal | None:
    normalized = normalize_number_text(value)
    if normalized is None:
        return None
    try:
        return Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None


def normalize_phone(value: Any) -> str | None:
    if value is None:
        return None

    digits = re.sub(r"\D", "", str(value).translate(ARABIC_DIGITS))
    if digits.startswith("00"):
        digits = digits[2:]
    elif digits.startswith("0"):
        digits = digits[1:]

    if digits.startswith("967"):
        national = digits[3:]
        if len(national) == 9 and national.startswith("7"):
            return "+967" + national
        return None

    if len(digits) == 9 and digits.startswith("7"):
        return "+967" + digits

    return None


def normalize_email(value: Any) -> tuple[str | None, bool]:
    if value is None:
        return None, False

    original = str(value).strip()
    candidate = re.sub(r"@+", "@", original)
    candidate = re.sub(r"\.{2,}", ".", candidate)
    candidate = candidate.lower()
    return candidate, candidate != original


def is_valid_email(value: Any) -> bool:
    if value is None:
        return False
    return re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", str(value).strip()) is not None


def standardize_text(value: Any) -> str | None:
    if value is None:
        return None
    return re.sub(r"\s+", " ", str(value).strip())


def standardize_status(value: Any) -> str | None:
    text = standardize_text(value)
    if text is None:
        return None

    mapping = {
        "مدفوع": "تم الدفع",
        "دفع": "تم الدفع",
        "مؤكد": "مؤكد",
        "مأكد": "مؤكد",
        "بانتظار الدفع": "بانتظار الدفع",
        "غير مدفوع": "بانتظار الدفع",
        "قيد الانتظار": "قيد الانتظار",
        "قيد الشحن": "قيد الشحن",
        "ملغي": "ملغي",
    }
    return mapping.get(text, text)


def classify_errors(error_codes: list[str]) -> str:
    return "quarantine" if error_codes else "valid"


if __name__ == "__main__":
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    print("\033[96m" + "=" * 65 + "\033[0m")
    print("\033[1m\033[92mAUTOMATED QUALITY CLEANING RULES DEMONSTRATION (SECTION 6.6)\033[0m")
    print("\033[96m" + "=" * 65 + "\033[0m")

    examples = [
        ("1. Arabic Digits Conversion", "السعر ٥٠٠٠ ريال", to_decimal("٥٠٠٠ ريال")),
        ("2. Currency Text Removal", "12500 YER", to_decimal("12500 YER")),
        ("3. Thousands Separator Cleaning", "125,000.00", to_decimal("125,000.00")),
        ("4. Price in Words Conversion", "خمسة آلاف", to_decimal("خمسة آلاف")),
        ("5. Phone Number Normalization", "706026813", normalize_phone("706026813")),
        ("6. Email Symbol Repair", "user@@gmail..com", normalize_email("user@@gmail..com")[0]),
        ("7. Status Synonym Standardization", "مدفوع", standardize_status("مدفوع")),
    ]

    for rule, dirty, clean in examples:
        print(f"\033[97m{rule:<35}:\033[0m \033[91m'{dirty}'\033[0m ➔ \033[1m\033[92m'{clean}'\033[0m")

    print("\033[96m" + "=" * 65 + "\033[0m\n")

