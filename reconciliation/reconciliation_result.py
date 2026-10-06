"""Reconciliation Result Data Model.

Defines the structured, explainable outcome of a ledger reconciliation operation.
Contains granular field-level matches, mismatches, deterministic evidence,
and scoring metrics.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field, field_validator


class ReconciliationStatus(str, Enum):
    """Controlled reconciliation decision classifications."""

    MATCHED = "MATCHED"
    PARTIAL_MATCH = "PARTIAL_MATCH"
    MISMATCHED = "MISMATCHED"
    NOT_FOUND = "NOT_FOUND"


class ReconciliationResult(BaseModel):
    """Explainable result of a deterministic transaction-to-ledger reconciliation."""

    reconciliation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique identifier for the reconciliation audit record",
    )
    transaction_id: str = Field(
        ...,
        description="Identifier of the transaction under reconciliation",
    )
    ledger_entry_id: Optional[str] = Field(
        default=None,
        description="Identifier of the matched candidate ledger entry, if any",
    )
    correlation_id: Optional[str] = Field(
        default=None,
        description="End-to-end distributed workflow correlation identifier",
    )
    status: str = Field(
        ...,
        description="Reconciliation status: MATCHED, PARTIAL_MATCH, MISMATCHED, or NOT_FOUND",
    )
    score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Explainable weighted match score between 0.0 and 1.0",
    )
    matched_fields: List[str] = Field(
        default_factory=list,
        description="List of fields with exact or valid tolerance matches",
    )
    mismatched_fields: List[str] = Field(
        default_factory=list,
        description="List of fields that failed reconciliation criteria",
    )
    mismatch_reasons: List[str] = Field(
        default_factory=list,
        description="Human-readable explanations for all mismatched criteria",
    )
    evidence: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Deterministic, non-LLM evidence records for each evaluated rule",
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when the reconciliation was performed",
    )

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        """Validate that status belongs to the controlled ReconciliationStatus values."""
        upper_val = value.upper().strip()
        allowed = {s.value for s in ReconciliationStatus}
        if upper_val not in allowed:
            raise ValueError(
                f"Invalid reconciliation status '{value}'. Allowed values: {sorted(allowed)}"
            )
        return upper_val

    @field_validator("score")
    @classmethod
    def validate_score(cls, value: float) -> float:
        """Ensure score is strictly bounded between 0.0 and 1.0 and rounded cleanly."""
        if value < 0.0 or value > 1.0:
            raise ValueError("Reconciliation score must be between 0.0 and 1.0.")
        return round(float(value), 4)

    model_config = {
        "json_schema_extra": {
            "example": {
                "reconciliation_id": "rec-10001",
                "transaction_id": "TXN-000001",
                "ledger_entry_id": "LEDGER-000001",
                "correlation_id": "CORR-10001",
                "status": "MATCHED",
                "score": 1.0,
                "matched_fields": [
                    "transaction_id",
                    "account_id",
                    "amount",
                    "currency",
                    "timestamp",
                    "direction",
                ],
                "mismatched_fields": [],
                "mismatch_reasons": [],
                "evidence": [
                    {
                        "field": "amount",
                        "transaction_value": "1500.00 USD",
                        "ledger_value": "1500.00 USD",
                        "match": True,
                        "difference": "0.00 USD",
                        "message": "Amount matches exactly: 1500.00 USD",
                    }
                ],
            }
        }
    }
