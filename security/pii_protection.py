"""PII Protection & Data Masking Engine for ZT-LedgerGuard.

Zero-Trust Principle:
Sensitive personal identifiers, card data, and account numbers must
never appear unmasked in logs, telemetry, public API responses, or
audit events.
"""

from __future__ import annotations

import copy
import re
from typing import Any, Dict, List, Optional, Union

from security.security_config import (
    PII_FIELD_CLASSIFICATION,
    PIIClassificationLevel,
)

# Regular expressions for automatic regex-based PII identification
_CARD_REGEX = re.compile(r"\b(?:\d{4}[-\s]?){3}\d{4}\b|\b\d{13,19}\b")
_EMAIL_REGEX = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")
_PHONE_REGEX = re.compile(r"\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b")
_GENERIC_ACCOUNT_REGEX = re.compile(r"\b\d{10,19}\b")


def get_field_classification(field_name: str) -> PIIClassificationLevel:
    """Return the PII classification level for a given field name."""
    clean_name = field_name.lower().strip()
    return PII_FIELD_CLASSIFICATION.get(clean_name, PIIClassificationLevel.PUBLIC)


def is_sensitive_field(field_name: str) -> bool:
    """Check if a field name is classified as Sensitive or Highly Sensitive."""
    level = get_field_classification(field_name)
    return level in (PIIClassificationLevel.SENSITIVE, PIIClassificationLevel.HIGHLY_SENSITIVE)


def mask_sensitive_value(value: Optional[str], visible_suffix_len: int = 4) -> str:
    """Mask arbitrary string keeping only the final suffix characters visible.

    Args:
        value: The sensitive string to mask.
        visible_suffix_len: Number of trailing characters to preserve (default 4).

    Returns:
        Masked string with asterisks replacing leading characters.
    """
    if value is None:
        return ""
    val_str = str(value).strip()
    total_len = len(val_str)
    if total_len == 0:
        return ""

    if total_len <= visible_suffix_len:
        masked_prefix = "*" * max(0, total_len - 1)
        return masked_prefix + val_str[-1:]

    masked_prefix = "*" * (total_len - visible_suffix_len)
    visible_suffix = val_str[-visible_suffix_len:]
    return f"{masked_prefix}{visible_suffix}"


def mask_account_id(account_id: Optional[str]) -> str:
    """Mask a banking account identifier while preserving standard prefixes.

    Examples:
        'ACC-123456789' -> 'ACC-*****6789'
        '123456789012'  -> '********9012'
        'ACC-001'       -> 'ACC-**1'
    """
    if not account_id:
        return ""

    acc_str = str(account_id).strip()
    if "-" in acc_str:
        prefix, remainder = acc_str.split("-", 1)
        if remainder:
            return f"{prefix}-{mask_sensitive_value(remainder, visible_suffix_len=4)}"
        return mask_sensitive_value(acc_str, visible_suffix_len=4)

    return mask_sensitive_value(acc_str, visible_suffix_len=4)


def mask_card_number(card_number: Optional[str]) -> str:
    """Mask a credit/debit card number, retaining only the last 4 digits.

    Example:
        '9876543212345678'      -> '************5678'
        '9876-5432-1234-5678' -> '************5678'
    """
    if not card_number:
        return ""

    digits_only = re.sub(r"\D", "", str(card_number).strip())
    if len(digits_only) < 4:
        return "****"
    return "*" * (len(digits_only) - 4) + digits_only[-4:]


def mask_phone_number(phone_number: Optional[str]) -> str:
    """Mask a telephone number, retaining only the last 4 digits.

    Example:
        '9876543210' -> '******3210'
    """
    if not phone_number:
        return ""

    phone_str = str(phone_number).strip()
    digits_only = re.sub(r"\D", "", phone_str)
    if len(digits_only) <= 4:
        return "****"

    masked_digits = "*" * (len(digits_only) - 4) + digits_only[-4:]
    if phone_str.startswith("+"):
        return f"+{masked_digits}"
    return masked_digits


def mask_email(email: Optional[str]) -> str:
    """Mask an email address, keeping first character and full domain.

    Example:
        'charu@example.com' -> 'c****@example.com'
        'a@b.com'           -> 'a****@b.com'
    """
    if not email or "@" not in email:
        return "*****"

    parts = email.strip().split("@", 1)
    local_part = parts[0]
    domain_part = parts[1]

    if not local_part:
        return f"****@{domain_part}"

    first_char = local_part[0]
    return f"{first_char}****@{domain_part}"


def mask_government_id(gov_id: Optional[str]) -> str:
    """Mask a national identification or tax number, preserving last 4 characters.

    Example:
        'ABCDE1234F' -> '******234F'
    """
    if not gov_id:
        return ""
    return mask_sensitive_value(str(gov_id).strip(), visible_suffix_len=4)


def mask_pii_value(value: Optional[str], field_type: str = "general") -> str:
    """Dispatches masking according to field type classification."""
    if value is None:
        return ""
    ft = field_type.lower().strip()
    if ft in ("account_id", "account", "bank_account_number", "customer_id", "customer"):
        return mask_account_id(value)
    if ft in ("card_number", "card", "pan"):
        return mask_card_number(value)
    if ft in ("email", "mail"):
        return mask_email(value)
    if ft in ("phone_number", "phone", "mobile"):
        return mask_phone_number(value)
    if ft in ("government_id", "ssn", "national_id", "tax_id"):
        return mask_government_id(value)
    return mask_sensitive_value(value, visible_suffix_len=4)


def sanitize_text(text: str) -> str:
    """Sanitize freeform text by redacting email, card, and numeric account patterns."""
    if not text:
        return ""

    sanitized = text
    # Mask emails
    sanitized = _EMAIL_REGEX.sub(lambda m: mask_email(m.group(0)), sanitized)
    # Mask cards
    sanitized = _CARD_REGEX.sub(lambda m: mask_card_number(m.group(0)), sanitized)
    # Mask numeric account sequences
    sanitized = _GENERIC_ACCOUNT_REGEX.sub(lambda m: mask_account_id(m.group(0)), sanitized)
    return sanitized


def sanitize_payload(payload: Any, deep: bool = True) -> Any:
    """Recursively sanitize a dictionary, list, or primitive payload.

    Guarantees:
        1. Non-mutating: creates a deep copy before masking.
        2. Known sensitive keys are masked according to field type.
        3. String values containing PII patterns are sanitized.
    """
    if payload is None:
        return None

    data = copy.deepcopy(payload) if deep else payload

    if isinstance(data, dict):
        sanitized_dict: Dict[str, Any] = {}
        for key, value in data.items():
            key_str = str(key)
            if is_sensitive_field(key_str):
                # Mask based on key type
                if isinstance(value, str):
                    sanitized_dict[key_str] = mask_pii_value(value, field_type=key_str)
                elif isinstance(value, (int, float)):
                    sanitized_dict[key_str] = mask_pii_value(str(value), field_type=key_str)
                elif isinstance(value, (dict, list)):
                    sanitized_dict[key_str] = sanitize_payload(value, deep=False)
                else:
                    sanitized_dict[key_str] = mask_sensitive_value(str(value))
            else:
                sanitized_dict[key_str] = sanitize_payload(value, deep=False)
        return sanitized_dict

    if isinstance(data, list):
        return [sanitize_payload(item, deep=False) for item in data]

    if isinstance(data, str):
        # Auto-detect PII inside string
        return sanitize_text(data)

    return data
