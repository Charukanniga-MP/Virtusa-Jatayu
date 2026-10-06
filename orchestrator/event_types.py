"""Centralized Domain Event Type Definitions for ZT-LedgerGuard.

Provides strongly-typed event category enumeration across the entire platform.
Prevents hardcoded string fragmentation and establishes the canonical event taxonomy.
"""

from enum import Enum


class EventType(str, Enum):
    """Canonical domain event types for the ZT-LedgerGuard financial platform."""

    # Ingestion & Validation Events
    TRANSACTION_RECEIVED = "TRANSACTION_RECEIVED"
    TRANSACTION_VALIDATED = "TRANSACTION_VALIDATED"
    TRANSACTION_INGESTED = "TRANSACTION_INGESTED"  # Day 1 baseline compatibility

    # Reconciliation Pipeline Events (Days 4-5)
    TRANSACTION_RECONCILIATION_REQUESTED = "TRANSACTION_RECONCILIATION_REQUESTED"
    FUZZY_CANDIDATE_IDENTIFIED = "FUZZY_CANDIDATE_IDENTIFIED"
    TRANSACTION_RECONCILED = "TRANSACTION_RECONCILED"

    # Fraud Triage Pipeline Events (Day 6)
    FRAUD_ANALYSIS_REQUESTED = "FRAUD_ANALYSIS_REQUESTED"
    FRAUD_ANALYSIS_COMPLETED = "FRAUD_ANALYSIS_COMPLETED"

    # Risk Assessment Pipeline Events (Day 7)
    RISK_ANALYSIS_REQUESTED = "RISK_ANALYSIS_REQUESTED"
    RISK_ANALYSIS_COMPLETED = "RISK_ANALYSIS_COMPLETED"

    # Final Decision & Disposition Events
    TRANSACTION_APPROVED = "TRANSACTION_APPROVED"
    TRANSACTION_FLAGGED = "TRANSACTION_FLAGGED"
    TRANSACTION_REJECTED = "TRANSACTION_REJECTED"

    # Failure & Operational Recovery Events
    PROCESSING_FAILED = "PROCESSING_FAILED"
    PROCESSING_RETRY = "PROCESSING_RETRY"

    def __str__(self) -> str:
        return self.value
