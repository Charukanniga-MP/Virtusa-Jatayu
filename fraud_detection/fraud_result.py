"""Fraud Analysis Result Models and Enums for ZT-LedgerGuard.

Provides strongly-typed Pydantic schemas for deterministic fraud triage,
explainable indicator evidence, and compliance audit logging.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class RiskLevel(str, Enum):
    """Categorical fraud risk classification levels."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class FraudStatus(str, Enum):
    """Operational status resulting from fraud evaluation."""

    CLEAR = "CLEAR"
    ANALYZED = "ANALYZED"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    BLOCK_RECOMMENDED = "BLOCK_RECOMMENDED"


class RuleSeverity(str, Enum):
    """Severity classification for individual rule triggers."""

    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class RuleEvidence(BaseModel):
    """Explainable evidence record for an executed fraud rule."""

    rule_id: str = Field(description="Unique identifier of the fraud rule.")
    triggered: bool = Field(description="Whether the rule threshold was breached.")
    score_contribution: int = Field(
        ge=0, le=100, description="Risk points contributed by this rule."
    )
    severity: RuleSeverity = Field(
        default=RuleSeverity.LOW, description="Severity grade of the rule."
    )
    reason: str = Field(description="Human-readable explanation of why the rule evaluated this way.")
    evidence: Dict[str, Any] = Field(
        default_factory=dict, description="Deterministic numerical or contextual metrics."
    )


class FraudAnalysisResult(BaseModel):
    """Deterministic, explainable fraud triage result for a transaction."""

    transaction_id: str = Field(description="ID of the evaluated transaction.")
    fraud_score: int = Field(
        ge=0,
        le=100,
        description="Aggregate fraud risk score bounded deterministically between 0 and 100.",
    )
    risk_level: RiskLevel = Field(description="Categorical risk rating: LOW, MEDIUM, HIGH, CRITICAL.")
    status: FraudStatus = Field(description="Operational status recommendation.")
    requires_manual_review: bool = Field(
        default=False,
        description="True if human compliance/fraud investigator review is recommended.",
    )
    block_recommended: bool = Field(
        default=False,
        description="True if risk is CRITICAL and downstream execution should freeze/block transaction.",
    )
    indicators: List[str] = Field(
        default_factory=list,
        description="List of triggered fraud rule identifiers.",
    )
    evidence: List[RuleEvidence] = Field(
        default_factory=list,
        description="Detailed explainable audit evidence for all evaluated rules.",
    )
    analysis_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of the fraud assessment.",
    )
    model_version: str = Field(
        default="fraud-rule-engine-v1.0",
        description="Deterministic version string of the fraud engine.",
    )
    correlation_id: Optional[str] = Field(
        default=None,
        description="Pipeline correlation ID for end-to-end distributed tracing.",
    )

    @field_validator("fraud_score")
    @classmethod
    def validate_score_bounds(cls, v: int) -> int:
        if v < 0 or v > 100:
            raise ValueError(f"fraud_score must be between 0 and 100, got {v}")
        return v
