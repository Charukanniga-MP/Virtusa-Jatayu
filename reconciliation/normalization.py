"""Field Normalization Utilities for Fuzzy Ledger Reconciliation.

Provides safe, deterministic string and identifier normalization functions.
Adheres strictly to financial domain safety:
- Preserves all meaningful numerical digits in account identifiers.
- Canonicalizes reference codes, transaction identifiers, and currencies.
- Stateless, testable, and deterministic.
"""

from __future__ import annotations

import re
from typing import Optional


def normalize_reference(reference_id: Optional[str]) -> str:
    """Normalize a financial reference ID for fuzzy comparison.

    Rules:
    1. None or empty string -> empty string ""
    2. Convert to uppercase
    3. Strip leading and trailing whitespace
    4. Remove hyphens, underscores, dots, and internal spaces
       (e.g., " pay-abc-123 " -> "PAYABC123", "pay abc 123" -> "PAYABC123")

    Args:
        reference_id: Raw reference string or None.

    Returns:
        Canonicalized reference string.
    """
    if not reference_id:
        return ""
    clean = reference_id.strip().upper()
    # Remove separators (hyphens, underscores, dots, whitespace)
    return re.sub(r"[\s\-_.]+", "", clean)


def normalize_transaction_id(transaction_id: Optional[str]) -> str:
    """Normalize a transaction identifier.

    Rules:
    1. None or empty string -> empty string ""
    2. Strip leading/trailing whitespace and uppercase
    3. Maintain structural integrity of transaction prefixes (e.g. TXN-1001)

    Args:
        transaction_id: Raw transaction identifier string.

    Returns:
        Canonicalized transaction identifier.
    """
    if not transaction_id:
        return ""
    return transaction_id.strip().upper()


def normalize_account_id(account_id: Optional[str]) -> str:
    """Normalize an account identifier safely without altering account numbers.

    CRITICAL FINANCIAL SAFETY RULE:
    Do NOT strip or truncate meaningful digits from account numbers!
    Only normalize case, surrounding whitespace, and remove internal space formatting.

    Rules:
    1. None or empty string -> empty string ""
    2. Strip leading/trailing whitespace and uppercase
    3. Remove interior whitespace formatting (e.g., "ACC 100001" -> "ACC-100001" or "ACC100001")
    4. Standardize standard prefixes if separated by space

    Args:
        account_id: Raw account identifier string.

    Returns:
        Safe normalized account identifier.
    """
    if not account_id:
        return ""
    clean = account_id.strip().upper()
    # If formatted with spaces between prefix and digits, collapse spaces
    return re.sub(r"\s+", "", clean)


def normalize_currency(currency: Optional[str]) -> str:
    """Normalize an ISO 4217 currency code.

    Rules:
    1. None or empty string -> empty string ""
    2. Strip whitespace and uppercase to standard 3-character format (e.g. " usd " -> "USD")

    Args:
        currency: Raw currency string.

    Returns:
        Standard 3-letter uppercase currency code.
    """
    if not currency:
        return ""
    return currency.strip().upper()
