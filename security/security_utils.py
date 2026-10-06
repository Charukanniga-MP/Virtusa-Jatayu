"""Security Utilities Foundation for ZT-LedgerGuard.

Zero-Trust Principle:
Data hygiene and least-privilege logging are enforced at all entry points.
This module provides Day 1 foundational utilities for masking and sanitizing
sensitive identifiers such as account numbers before output to logs or telemetry.

CRITICAL SECURITY RULES:
NEVER log:
1. Full account numbers (PANs / IBANs / Account IDs)
2. Passwords, secret keys, or credentials
3. API keys or service tokens
4. Authentication tokens (JWTs, session IDs)
5. Sensitive Personal Identifiable Information (PII)

NOTE:
This foundation constitutes Phase 1 data hygiene. Full Zero-Trust architecture,
including mutual TLS, cryptographic ledger verification, and differential privacy,
is scheduled for subsequent phases (Day 8).
"""

import re
from typing import Optional

# Regular expression to identify unmasked numeric account / card sequences (10-19 digits)
_NUMERIC_ACCOUNT_REGEX = re.compile(r"\b\d{10,19}\b")


def mask_sensitive_value(value: Optional[str], visible_suffix_len: int = 4) -> str:
    """Mask a sensitive identifier, keeping only the final visible suffix characters.

    Args:
        value: The raw sensitive string (e.g., account number, secret reference).
        visible_suffix_len: Number of trailing characters to leave unmasked (default 4).

    Returns:
        Masked string with asterisks replacing leading characters.

    Examples:
        >>> mask_sensitive_value("123456789012")
        '********9012'
        >>> mask_sensitive_value("1234")
        '***4'
        >>> mask_sensitive_value("")
        ''
    """
    if not value:
        return ""

    val_str = str(value).strip()
    total_len = len(val_str)

    if total_len == 0:
        return ""

    if total_len <= visible_suffix_len:
        # For very short identifiers, keep only the last character visible
        masked_prefix = "*" * max(0, total_len - 1)
        return masked_prefix + val_str[-1:]

    masked_prefix = "*" * (total_len - visible_suffix_len)
    visible_suffix = val_str[-visible_suffix_len:]
    return f"{masked_prefix}{visible_suffix}"


def mask_account_id(account_id: Optional[str]) -> str:
    """Mask an account identifier to protect customer banking privacy.

    Example:
        '123456789012' -> '********9012'

    Args:
        account_id: The raw banking account identifier.

    Returns:
        Masked account string suitable for application logs.
    """
    return mask_sensitive_value(account_id, visible_suffix_len=4)


def sanitize_log_message(message: str) -> str:
    """Sanitize arbitrary log messages by redacting 10-19 digit numeric account sequences.

    Args:
        message: The log message to inspect and sanitize.

    Returns:
        Safe log message with account sequences masked.
    """
    if not message:
        return ""

    def _replace_match(match: re.Match) -> str:
        raw = match.group(0)
        return mask_account_id(raw)

    return _NUMERIC_ACCOUNT_REGEX.sub(_replace_match, message)
