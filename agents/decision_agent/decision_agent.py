"""Decision Agent & Consensus Engine for ZT-LedgerGuard Multi-Agent Architecture.

Combines independent agent results (ReconciliationAgent, FraudDetectionAgent, PolicyAgent)
into an authoritative, explainable DecisionResult using deterministic consensus rules,
strict precedence ordering, and disagreement tracking.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import logging
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field, field_validator

from agents.base_agent import AgentExecutionRecord, BaseAgent
from agents.policy_agent.policy_agent import PolicyAgentResult
from agents.reconciliation_agent.reconciliation_agent import ReconciliationAgentResult
from fraud_detection.fraud_result import FraudAnalysisResult
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus

logger = logging.getLogger("agents.decision_agent")


class DecisionType(str, Enum):
    """Authoritative multi-agent consensus decision classifications."""

    APPROVE = "APPROVE"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    BLOCK_RECOMMENDED = "BLOCK_RECOMMENDED"


class DecisionPrecedence:
    """Strict centralized precedence order: BLOCK_RECOMMENDED > MANUAL_REVIEW > APPROVE."""

    _PRECEDENCE_MAP: Dict[str, int] = {
        DecisionType.BLOCK_RECOMMENDED.value: 3,
        DecisionType.MANUAL_REVIEW.value: 2,
        DecisionType.APPROVE.value: 1,
    }

    @classmethod
    def rank(cls, decision: str) -> int:
        """Return numerical rank of a decision type for comparison."""
        return cls._PRECEDENCE_MAP.get(decision.upper(), 0)

    @classmethod
    def get_highest(cls, decisions: List[str]) -> str:
        """Select the highest-precedence decision from a collection."""
        if not decisions:
            return DecisionType.MANUAL_REVIEW.value
        return max(decisions, key=lambda d: cls.rank(d))


class DecisionContext(BaseModel):
    """Structured multi-agent context passed to the Consensus Engine."""

    transaction_id: str = Field(..., description="Target transaction identifier")
    reconciliation_result: Optional[ReconciliationAgentResult] = Field(
        default=None,
        description="Outcome from ReconciliationAgent",
    )
    fraud_result: Optional[FraudAnalysisResult] = Field(
        default=None,
        description="Outcome from FraudDetectionAgent",
    )
    policy_result: Optional[PolicyAgentResult] = Field(
        default=None,
        description="Outcome from PolicyAgent",
    )
    correlation_id: Optional[str] = Field(
        default=None,
        description="End-to-end distributed trace identifier",
    )
    agent_failures: List[str] = Field(
        default_factory=list,
        description="List of agent names that failed execution",
    )


class DecisionResult(BaseModel):
    """Authoritative, explainable disposition produced by multi-agent consensus."""

    decision_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique identifier for this consensus audit record",
    )
    transaction_id: str = Field(..., description="Target transaction identifier")
    decision: str = Field(
        ...,
        description="Consensus decision: APPROVE, MANUAL_REVIEW, or BLOCK_RECOMMENDED",
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Deterministic decision agreement/confidence score (0.0 - 1.0)",
    )
    requires_manual_review: bool = Field(..., description="Whether manual compliance review is needed")
    block_recommended: bool = Field(..., description="Whether account/transaction freeze is recommended")
    agent_results: Dict[str, Any] = Field(
        default_factory=dict,
        description="Summary outcomes of all participating specialized agents",
    )
    triggered_policies: List[str] = Field(
        default_factory=list,
        description="List of business/risk policy IDs triggered during evaluation",
    )
    decision_reasons: List[str] = Field(
        default_factory=list,
        description="Human-auditable explanations for the final consensus outcome",
    )
    disagreement_detected: bool = Field(
        default=False,
        description="Whether specialized agents reached conflicting postures",
    )
    disagreements: List[str] = Field(
        default_factory=list,
        description="Specific agent posture conflicts detected",
    )
    correlation_id: Optional[str] = Field(default=None, description="Distributed correlation ID")
    decision_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Consensus computation timestamp",
    )
    model_version: str = Field(default="1.0.0-consensus", description="Consensus engine version")
    execution_record: Optional[AgentExecutionRecord] = Field(
        default=None,
        description="Auditable agent execution record",
    )

    @field_validator("decision")
    @classmethod
    def validate_decision(cls, value: str) -> str:
        """Validate decision matches controlled DecisionType values."""
        val = value.upper()
        if val not in {d.value for d in DecisionType}:
            raise ValueError(f"Invalid decision '{value}'. Must be one of {[d.value for d in DecisionType]}")
        return val

    @field_validator("confidence")
    @classmethod
    def validate_confidence(cls, value: float) -> float:
        """Ensure confidence remains strictly bounded in [0.0, 1.0]."""
        if not (0.0 <= value <= 1.0):
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {value}")
        return round(value, 4)


class ConsensusEngine:
    """Deterministic consensus evaluator combining independent agent evaluations."""

    def evaluate(self, context: DecisionContext) -> tuple[str, float, bool, bool, List[str], bool, List[str]]:
        """Apply deterministic decision rules with centralized precedence.

        Returns:
            (decision, confidence, requires_manual_review, block_recommended, decision_reasons, disagreement_detected, disagreements)
        """
        reasons: List[str] = []
        disagreements: List[str] = []
        candidate_decisions: List[str] = []

        # ----------------------------------------------------------------------
        # 1. Inspect Individual Agent Postures & Failures
        # ----------------------------------------------------------------------
        has_failure = bool(context.agent_failures)
        if has_failure:
            for failed_agent in context.agent_failures:
                reasons.append(f"Agent failure detected ({failed_agent}): Financial safety fallback requires human review.")
            candidate_decisions.append(DecisionType.MANUAL_REVIEW.value)

        # Reconciliation Posture
        recon_status = "UNKNOWN"
        recon_score = 0.0
        recon_posture = DecisionType.MANUAL_REVIEW.value
        if context.reconciliation_result:
            recon_status = context.reconciliation_result.status.upper()
            recon_score = context.reconciliation_result.reconciliation_score
            if recon_status == ReconciliationStatus.MATCHED.value:
                recon_posture = DecisionType.APPROVE.value
            elif recon_status in (
                ReconciliationStatus.AMBIGUOUS.value,
                ReconciliationStatus.NOT_FOUND.value,
                ReconciliationStatus.MISMATCHED.value,
                ReconciliationStatus.PARTIAL_MATCH.value,
            ):
                recon_posture = DecisionType.MANUAL_REVIEW.value

        # Fraud Posture
        fraud_score = 0
        fraud_level = "UNKNOWN"
        fraud_posture = DecisionType.APPROVE.value
        if context.fraud_result:
            fraud_score = context.fraud_result.fraud_score
            fraud_level = context.fraud_result.risk_level.value if hasattr(context.fraud_result.risk_level, "value") else str(context.fraud_result.risk_level)
            if fraud_score >= 75:
                fraud_posture = DecisionType.BLOCK_RECOMMENDED.value
            elif fraud_score >= 50:
                fraud_posture = DecisionType.MANUAL_REVIEW.value
            else:
                fraud_posture = DecisionType.APPROVE.value

        # Policy Posture
        policy_posture = DecisionType.APPROVE.value
        if context.policy_result:
            policy_posture = context.policy_result.recommendation

        # ----------------------------------------------------------------------
        # 2. Detect Agent Disagreements
        # ----------------------------------------------------------------------
        postures = [recon_posture, fraud_posture, policy_posture]
        unique_postures = set(postures)
        disagreement_detected = len(unique_postures) > 1

        if disagreement_detected:
            if recon_status == ReconciliationStatus.MATCHED.value and fraud_score >= 75:
                disagreements.append(f"Reconciliation matched perfectly, but Fraud Agent detected CRITICAL threat (Score: {fraud_score}).")
            if recon_status == ReconciliationStatus.AMBIGUOUS.value and fraud_score < 25:
                disagreements.append(f"Fraud Agent scored LOW risk ({fraud_score}), but Reconciliation is AMBIGUOUS.")
            if recon_status == ReconciliationStatus.MISMATCHED.value and fraud_score < 25:
                disagreements.append(f"Fraud Agent scored LOW risk ({fraud_score}), but Reconciliation failed (MISMATCHED).")
            if recon_status == ReconciliationStatus.NOT_FOUND.value and fraud_score < 25:
                disagreements.append(f"Fraud Agent scored LOW risk ({fraud_score}), but Ledger Entry was NOT_FOUND.")
            if not disagreements:
                disagreements.append(f"Agent posture divergence: Recon={recon_posture}, Fraud={fraud_posture}, Policy={policy_posture}.")

        # ----------------------------------------------------------------------
        # 3. Apply Deterministic Consensus Rules
        # ----------------------------------------------------------------------
        # Rule 1: Critical Fraud Risk
        if fraud_score >= 75:
            reasons.append(f"Rule 1 triggered: Critical fraud score ({fraud_score} >= 75) mandates BLOCK_RECOMMENDED.")
            candidate_decisions.append(DecisionType.BLOCK_RECOMMENDED.value)

        # Rule 5: Mismatch + Elevated Fraud
        if recon_status == ReconciliationStatus.MISMATCHED.value and fraud_score >= 50:
            reasons.append(f"Rule 5 triggered: Ledger mismatch coupled with elevated fraud score ({fraud_score} >= 50) mandates BLOCK_RECOMMENDED.")
            candidate_decisions.append(DecisionType.BLOCK_RECOMMENDED.value)

        # Policy Agent Direct Mandate: BLOCK_RECOMMENDED
        if policy_posture == DecisionType.BLOCK_RECOMMENDED.value:
            reasons.append("Policy Agent mandated BLOCK_RECOMMENDED based on governance rules.")
            candidate_decisions.append(DecisionType.BLOCK_RECOMMENDED.value)

        # Rule 2: High Fraud Risk
        if 50 <= fraud_score < 75:
            reasons.append(f"Rule 2 triggered: Elevated fraud score ({fraud_score}) warrants MANUAL_REVIEW.")
            candidate_decisions.append(DecisionType.MANUAL_REVIEW.value)

        # Rule 3: Ambiguous Reconciliation
        if recon_status == ReconciliationStatus.AMBIGUOUS.value:
            reasons.append("Rule 3 triggered: Ambiguous ledger candidate discovery requires compliance triage (MANUAL_REVIEW).")
            candidate_decisions.append(DecisionType.MANUAL_REVIEW.value)

        # Rule 4: Missing Ledger Entry
        if recon_status == ReconciliationStatus.NOT_FOUND.value:
            reasons.append("Rule 4 triggered: Ledger entry NOT_FOUND requires manual investigation.")
            candidate_decisions.append(DecisionType.MANUAL_REVIEW.value)

        # Reconciliation Mismatch (Standard)
        if recon_status == ReconciliationStatus.MISMATCHED.value and fraud_score < 50:
            reasons.append("Reconciliation MISMATCHED requires manual review.")
            candidate_decisions.append(DecisionType.MANUAL_REVIEW.value)

        # Policy Agent Mandate: MANUAL_REVIEW
        if policy_posture == DecisionType.MANUAL_REVIEW.value:
            reasons.append("Policy Agent recommended MANUAL_REVIEW.")
            candidate_decisions.append(DecisionType.MANUAL_REVIEW.value)

        # Rule 6: Clean Transaction
        if recon_status == ReconciliationStatus.MATCHED.value and fraud_score < 25 and policy_posture == DecisionType.APPROVE.value and not has_failure:
            reasons.append("Rule 6 satisfied: Matched ledger, low fraud score (<25), and clear policy checks yield APPROVE.")
            candidate_decisions.append(DecisionType.APPROVE.value)

        # Rule 7: Medium Risk with Clean Reconciliation
        if recon_status == ReconciliationStatus.MATCHED.value and 25 <= fraud_score < 50 and policy_posture == DecisionType.APPROVE.value and not has_failure:
            reasons.append(f"Rule 7 satisfied: Matched ledger with moderate fraud score ({fraud_score}) approved with preserved audit evidence.")
            candidate_decisions.append(DecisionType.APPROVE.value)

        # ----------------------------------------------------------------------
        # 4. Resolve via Centralized Precedence: BLOCK_RECOMMENDED > MANUAL_REVIEW > APPROVE
        # ----------------------------------------------------------------------
        final_decision = DecisionPrecedence.get_highest(candidate_decisions)

        requires_manual_review = final_decision in (DecisionType.MANUAL_REVIEW.value, DecisionType.BLOCK_RECOMMENDED.value)
        block_recommended = final_decision == DecisionType.BLOCK_RECOMMENDED.value

        # ----------------------------------------------------------------------
        # 5. Deterministic Confidence Score Calculation
        # ----------------------------------------------------------------------
        # Unanimous agreement: 0.95 - 1.00
        # Disagreement: drops to 0.70 - 0.85
        # Agent failure / missing data: 0.50 - 0.65
        confidence = 1.0

        if has_failure:
            confidence -= 0.35
        elif disagreement_detected:
            confidence -= 0.20
        
        # Penalize borderline reconciliation
        if recon_status == ReconciliationStatus.PARTIAL_MATCH.value:
            confidence -= 0.10
        elif recon_score < 0.85 and recon_status == ReconciliationStatus.MATCHED.value:
            confidence -= 0.05

        # Penalize boundary fraud scores
        if 45 <= fraud_score <= 55 or 70 <= fraud_score <= 78:
            confidence -= 0.05

        # Strictly clamp within [0.50, 1.0]
        confidence = max(0.50, min(1.0, confidence))

        return (
            final_decision,
            confidence,
            requires_manual_review,
            block_recommended,
            reasons,
            disagreement_detected,
            disagreements,
        )


class DecisionAgent(BaseAgent):
    """Decision Agent evaluating multi-agent consensus and producing final disposition."""

    agent_name: str = "DecisionAgent"
    agent_version: str = "1.0.0"

    def __init__(
        self,
        consensus_engine: Optional[ConsensusEngine] = None,
        agent_name: Optional[str] = None,
        agent_version: Optional[str] = None,
    ) -> None:
        super().__init__(
            agent_name=agent_name or "DecisionAgent",
            agent_version=agent_version or "1.0.0",
        )
        self.consensus_engine = consensus_engine or ConsensusEngine()

    def analyze(self, context: DecisionContext) -> DecisionResult:
        """Run multi-agent consensus analysis on provided DecisionContext."""
        started_at = datetime.now(timezone.utc)
        corr_id = context.correlation_id or f"CORR-DECISION-{context.transaction_id}"

        (
            decision,
            confidence,
            requires_manual_review,
            block_recommended,
            reasons,
            disagreement_detected,
            disagreements,
        ) = self.consensus_engine.evaluate(context)

        # Build agent summaries
        agent_summaries: Dict[str, Any] = {}
        if context.reconciliation_result:
            agent_summaries["reconciliation"] = {
                "status": context.reconciliation_result.status,
                "score": context.reconciliation_result.reconciliation_score,
                "requires_manual_review": context.reconciliation_result.requires_manual_review,
            }
        if context.fraud_result:
            agent_summaries["fraud"] = {
                "score": context.fraud_result.fraud_score,
                "risk_level": context.fraud_result.risk_level.value if hasattr(context.fraud_result.risk_level, "value") else str(context.fraud_result.risk_level),
                "requires_manual_review": context.fraud_result.requires_manual_review,
                "block_recommended": context.fraud_result.block_recommended,
            }
        if context.policy_result:
            agent_summaries["policy"] = {
                "recommendation": context.policy_result.recommendation,
                "requires_manual_review": context.policy_result.requires_manual_review,
                "block_recommended": context.policy_result.block_recommended,
                "triggered_count": len(context.policy_result.triggered_policies),
            }

        triggered_policy_ids: List[str] = []
        if context.policy_result:
            triggered_policy_ids = [p.policy_id for p in context.policy_result.triggered_policies]

        completed_at = datetime.now(timezone.utc)

        exec_record = self.create_execution_record(
            transaction_id=context.transaction_id,
            decision=decision,
            started_at=started_at,
            completed_at=completed_at,
            evidence=[{"reasons": reasons, "disagreements": disagreements}],
            correlation_id=corr_id,
            success=True,
        )

        result = DecisionResult(
            transaction_id=context.transaction_id,
            decision=decision,
            confidence=confidence,
            requires_manual_review=requires_manual_review,
            block_recommended=block_recommended,
            agent_results=agent_summaries,
            triggered_policies=triggered_policy_ids,
            decision_reasons=reasons,
            disagreement_detected=disagreement_detected,
            disagreements=disagreements,
            correlation_id=corr_id,
            decision_timestamp=completed_at,
            execution_record=exec_record,
        )

        logger.info(
            "[%s] Consensus Decision for TX %s -> %s (Confidence: %.2f, Disagreement: %s)",
            self.agent_name,
            context.transaction_id,
            result.decision,
            result.confidence,
            result.disagreement_detected,
        )
        return result
