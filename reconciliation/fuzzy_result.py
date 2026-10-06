"""Fuzzy Candidate Matching Data Models.

Defines Pydantic models for fuzzy candidate ranking, field similarity scores,
explainable evidence, and candidate discovery summaries.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class ConfidenceLevel(str, Enum):
    """Graded confidence tiers for candidate ledger entries."""

    HIGH = "HIGH"          # >= 0.90
    MEDIUM = "MEDIUM"      # >= 0.75
    LOW = "LOW"            # >= 0.50
    REJECTED = "REJECTED"  # < 0.50 or hard constraint violation


class CandidateSelectionStatus(str, Enum):
    """Categorical outcome of candidate discovery and ranking."""

    SELECTED = "SELECTED"          # Clear winning candidate identified
    AMBIGUOUS = "AMBIGUOUS"        # Multiple close candidates within ambiguity margin
    NO_CANDIDATES = "NO_CANDIDATES"  # No candidates identified in repository
    REJECTED = "REJECTED"          # All candidate(s) failed hard constraints


class FuzzyCandidate(BaseModel):
    """Ranked candidate ledger entry with field-level similarity metrics."""

    ledger_entry_id: str = Field(..., description="Candidate ledger entry primary key")
    transaction_id: str = Field(..., description="Target transaction ID under reconciliation")
    candidate_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Aggregate similarity score between 0.0 and 1.0",
    )
    confidence_level: str = Field(
        ...,
        description="Confidence classification: HIGH, MEDIUM, LOW, or REJECTED",
    )
    field_scores: Dict[str, float] = Field(
        default_factory=dict,
        description="Individual field similarity scores (0.0 to 1.0)",
    )
    matched_fields: List[str] = Field(
        default_factory=list,
        description="Fields meeting or exceeding similarity thresholds",
    )
    mismatched_fields: List[str] = Field(
        default_factory=list,
        description="Fields that diverged or failed hard constraints",
    )
    reasons: List[str] = Field(
        default_factory=list,
        description="Human-auditable explanations for scores and deductions",
    )
    evidence: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Granular field-by-field verification evidence dictionaries",
    )

    @field_validator("confidence_level")
    @classmethod
    def validate_confidence(cls, v: str) -> str:
        upper = v.upper().strip()
        allowed = {c.value for c in ConfidenceLevel}
        if upper not in allowed:
            raise ValueError(f"Invalid confidence_level '{v}'. Allowed: {sorted(allowed)}")
        return upper

    @field_validator("candidate_score")
    @classmethod
    def round_score(cls, v: float) -> float:
        return round(float(v), 4)


class CandidateDiscoveryResult(BaseModel):
    """Composite outcome of repository discovery and fuzzy candidate ranking."""

    transaction_id: str = Field(..., description="Target transaction identifier")
    candidate_count: int = Field(..., ge=0, description="Total number of ranked candidates discovered")
    selection_status: str = Field(
        ...,
        description="Selection status: SELECTED, AMBIGUOUS, NO_CANDIDATES, or REJECTED",
    )
    top_candidate: Optional[FuzzyCandidate] = Field(
        default=None,
        description="Top-ranked candidate if selected, else None",
    )
    candidates: List[FuzzyCandidate] = Field(
        default_factory=list,
        description="Ranked candidates up to TOP_K limit, sorted descending by score",
    )
    requires_manual_review: bool = Field(
        default=False,
        description="Flag indicating ambiguity, low confidence, or hard constraint alert requiring human review",
    )
    ambiguity_margin: float = Field(
        default=0.03,
        description="Score delta threshold below which top candidates are considered ambiguous",
    )
    reasons: List[str] = Field(
        default_factory=list,
        description="Summary explanations for the selection outcome",
    )

    @field_validator("selection_status")
    @classmethod
    def validate_selection_status(cls, v: str) -> str:
        upper = v.upper().strip()
        allowed = {s.value for s in CandidateSelectionStatus}
        if upper not in allowed:
            raise ValueError(f"Invalid selection_status '{v}'. Allowed: {sorted(allowed)}")
        return upper
