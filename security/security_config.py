"""Centralized Security Configuration for ZT-LedgerGuard.

Zero-Trust Principle:
Never trust, always verify. All security boundaries, hash algorithms,
PII classifications, and gateway limits are centralized here.
"""

from __future__ import annotations

from decimal import Decimal
from enum import Enum
import os
from typing import Dict, List, Set


# ------------------------------------------------------------------------------
# Cryptographic & Hashing Constants
# ------------------------------------------------------------------------------
HASH_ALGORITHM: str = "SHA-256"

# Documented 64-character hex Genesis previous hash for block sequence 1
GENESIS_PREVIOUS_HASH: str = "0" * 64

# Tokenization prefix and salt configuration
TOKEN_PREFIX: str = "tok_"
# Read from environment with fallback for local deterministic execution
TOKEN_SALT: str = os.getenv("ZT_SECURITY_SALT", "zt_ledgerguard_zero_trust_salt_v1")


# ------------------------------------------------------------------------------
# Security Gateway & Request Constraints
# ------------------------------------------------------------------------------
# Maximum acceptable request payload size (1 MB)
MAX_REQUEST_SIZE_BYTES: int = 1_000_000

# Maximum single audit event payload size (64 KB)
MAX_AUDIT_EVENT_SIZE_BYTES: int = 65_536

# Permitted HTTP content types
ALLOWED_CONTENT_TYPES: List[str] = ["application/json"]

# Permitted banking currencies
ALLOWED_CURRENCIES: Set[str] = {
    "USD", "EUR", "GBP", "INR", "JPY", "CAD", "AUD", "SGD", "CHF"
}

# Transaction amount sanity bounds
MIN_TRANSACTION_AMOUNT: Decimal = Decimal("0.01")
MAX_TRANSACTION_AMOUNT: Decimal = Decimal("10000000.00")  # 10,000,000 ceiling


# ------------------------------------------------------------------------------
# PII Classification Taxonomy
# ------------------------------------------------------------------------------
class PIIClassificationLevel(str, Enum):
    """Categorical classification levels for data fields."""

    PUBLIC = "PUBLIC"                    # Non-sensitive: can be exposed in logs, public APIs
    SENSITIVE = "SENSITIVE"              # Internal identifiers: must be masked in logs/audit events
    HIGHLY_SENSITIVE = "HIGHLY_SENSITIVE" # Critical PII/PCI: strictly masked/tokenized everywhere


# Field-to-classification mapping
PII_FIELD_CLASSIFICATION: Dict[str, PIIClassificationLevel] = {
    # Public / Non-Sensitive Fields
    "transaction_id": PIIClassificationLevel.PUBLIC,
    "currency": PIIClassificationLevel.PUBLIC,
    "transaction_type": PIIClassificationLevel.PUBLIC,
    "timestamp": PIIClassificationLevel.PUBLIC,
    "transaction_timestamp": PIIClassificationLevel.PUBLIC,
    "source_system": PIIClassificationLevel.PUBLIC,
    "destination_system": PIIClassificationLevel.PUBLIC,
    "status": PIIClassificationLevel.PUBLIC,
    "risk_level": PIIClassificationLevel.PUBLIC,
    "reconciliation_score": PIIClassificationLevel.PUBLIC,
    "fraud_score": PIIClassificationLevel.PUBLIC,
    "decision": PIIClassificationLevel.PUBLIC,
    "confidence": PIIClassificationLevel.PUBLIC,
    "sequence_number": PIIClassificationLevel.PUBLIC,
    "event_type": PIIClassificationLevel.PUBLIC,
    "event_id": PIIClassificationLevel.PUBLIC,
    "correlation_id": PIIClassificationLevel.PUBLIC,
    "causation_id": PIIClassificationLevel.PUBLIC,

    # Sensitive Fields (Account / Customer References)
    "account_id": PIIClassificationLevel.SENSITIVE,
    "customer_id": PIIClassificationLevel.SENSITIVE,
    "merchant_id": PIIClassificationLevel.SENSITIVE,
    "reference_id": PIIClassificationLevel.SENSITIVE,

    # Highly Sensitive Fields (Financial Instruments & Personal Identifiers)
    "card_number": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "pan": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "bank_account_number": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "iban": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "phone_number": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "phone": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "email": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "address": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "government_id": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "national_id": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "ssn": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "tax_id": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "cvv": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "pin": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "password": PIIClassificationLevel.HIGHLY_SENSITIVE,
    "secret": PIIClassificationLevel.HIGHLY_SENSITIVE,
}
