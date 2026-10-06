"""Core Data Models for Transactions and Ledger Entries.

Defines Pydantic schemas for Day 1 ingestion, validation, and storage.
Reconciliation and fraud detection logic are purposefully decoupled.
"""

from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class Transaction(BaseModel):
    """Core Banking Transaction Model.

    Represents a financial transaction arriving from upstream payment rails,
    mobile banking, ATM networks, or wire transfer gateways.
    """

    transaction_id: str = Field(..., description="Unique transaction identifier")
    account_id: str = Field(..., description="Customer or counterpart account identifier")
    transaction_type: str = Field(..., description="Type of transaction (e.g. TRANSFER, PAYMENT, DEPOSIT)")
    amount: float = Field(..., ge=0.0, description="Transaction amount, must be >= 0")
    currency: str = Field(..., description="ISO 4217 3-letter currency code (e.g., USD, EUR, INR)")
    transaction_timestamp: datetime = Field(..., description="Timestamp when transaction occurred")
    source_system: str = Field(..., description="Originating channel or gateway")
    destination_system: str = Field(..., description="Target system or rail")
    reference_id: Optional[str] = Field(default=None, description="External reference identifier")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="System ingestion timestamp",
    )

    @field_validator("transaction_id", "account_id", "transaction_type", "source_system", "destination_system")
    @classmethod
    def validate_non_empty_strings(cls, value: str, info) -> str:
        """Validate that required string fields are not blank or whitespace-only."""
        if not value or not value.strip():
            raise ValueError(f"Field '{info.field_name}' cannot be empty or whitespace.")
        return value.strip()

    @field_validator("currency")
    @classmethod
    def validate_currency_code(cls, value: str) -> str:
        """Validate that currency is a valid 3-letter uppercase alphabetic code."""
        if not value or len(value) != 3 or not value.isalpha() or not value.isupper():
            raise ValueError("Currency must be a valid 3-letter uppercase alphabetic code (e.g., USD, EUR).")
        return value

    model_config = {
        "json_schema_extra": {
            "example": {
                "transaction_id": "TX-10001",
                "account_id": "ACC-998877665544",
                "transaction_type": "TRANSFER",
                "amount": 2500.50,
                "currency": "USD",
                "transaction_timestamp": "2026-10-06T10:30:00Z",
                "source_system": "CORE_BANKING_API",
                "destination_system": "ACH_GATEWAY",
                "reference_id": "REF-EXT-88712",
            }
        }
    }


class LedgerEntry(BaseModel):
    """Core Ledger Entry Model.

    Represents a double-entry or posted accounting ledger record against which
    in-flight transactions are reconciled. Kept separate from Transaction model.
    """

    ledger_entry_id: str = Field(..., description="Unique ledger record ID")
    transaction_id: str = Field(..., description="Referenced transaction identifier")
    account_id: str = Field(..., description="GL or customer account ID")
    amount: float = Field(..., ge=0.0, description="Entry amount, must be >= 0")
    currency: str = Field(..., description="ISO 4217 3-letter currency code")
    debit_credit: str = Field(..., description="Accounting side: DEBIT or CREDIT")
    reference_id: Optional[str] = Field(default=None, description="External ledger reference")
    timestamp: datetime = Field(..., description="Ledger posting timestamp")
    source_system: str = Field(..., description="General ledger subsystem or core module")

    @field_validator("ledger_entry_id", "transaction_id", "account_id", "source_system")
    @classmethod
    def validate_non_empty_strings(cls, value: str, info) -> str:
        """Validate that required string fields are not blank or whitespace-only."""
        if not value or not value.strip():
            raise ValueError(f"Field '{info.field_name}' cannot be empty or whitespace.")
        return value.strip()

    @field_validator("currency")
    @classmethod
    def validate_currency_code(cls, value: str) -> str:
        """Validate that currency is a valid 3-letter uppercase alphabetic code."""
        if not value or len(value) != 3 or not value.isalpha() or not value.isupper():
            raise ValueError("Currency must be a valid 3-letter uppercase alphabetic code (e.g., USD, EUR).")
        return value

    @field_validator("debit_credit")
    @classmethod
    def validate_debit_credit(cls, value: str) -> str:
        """Validate that debit_credit is either DEBIT or CREDIT (normalized to uppercase)."""
        normalized = value.strip().upper() if value else ""
        if normalized not in {"DEBIT", "CREDIT"}:
            raise ValueError("Field 'debit_credit' must be either 'DEBIT' or 'CREDIT'.")
        return normalized

    model_config = {
        "json_schema_extra": {
            "example": {
                "ledger_entry_id": "LEDGER-20001",
                "transaction_id": "TX-10001",
                "account_id": "ACC-998877665544",
                "amount": 2500.50,
                "currency": "USD",
                "debit_credit": "DEBIT",
                "reference_id": "REF-EXT-88712",
                "timestamp": "2026-10-06T10:30:05Z",
                "source_system": "GENERAL_LEDGER_CORE",
            }
        }
    }
