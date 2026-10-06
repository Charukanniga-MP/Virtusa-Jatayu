"""Fraud Detection Models Foundation.

Planned for full expansion in Day 6 (ML Anomaly Scoring & Real-Time Fraud Triage).
Provides schema foundation for risk scores and fraud assessment results.
"""

from typing import Optional
from pydantic import BaseModel, Field


class FraudTriagePlaceholder(BaseModel):
    """Placeholder model for Day 6 fraud detection agent results."""

    transaction_id: str
    risk_score: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    flagged: bool = False
    rule_violations: list[str] = Field(default_factory=list)
