"""Reconciliation Agent for ZT-LedgerGuard Multi-Agent Architecture.

Wraps the existing two-stage reconciliation services (CandidateService, FuzzyCandidateMatcher,
and deterministic ReconciliationEngine) into an autonomous, explainable BaseAgent.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from agents.base_agent import AgentExecutionRecord, BaseAgent
from reconciliation.models import Transaction
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus
from reconciliation.reconciliation_service import ReconciliationService

logger = logging.getLogger("agents.reconciliation_agent")


class ReconciliationAgentResult(BaseModel):
    """Normalized, explainable decision result produced by ReconciliationAgent."""

    agent_name: str = Field(default="ReconciliationAgent", description="Name of the agent")
    transaction_id: str = Field(..., description="Target transaction ID")
    status: str = Field(
        ...,
        description="Reconciliation status: MATCHED, PARTIAL_MATCH, MISMATCHED, NOT_FOUND, or AMBIGUOUS",
    )
    reconciliation_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Normalized match confidence score between 0.0 and 1.0",
    )
    requires_manual_review: bool = Field(
        default=False,
        description="Whether ambiguous or high-discrepancy reconciliation requires compliance triage",
    )
    matched_ledger_entry_id: Optional[str] = Field(
        default=None,
        description="ID of the matched ledger entry, if any",
    )
    matched_fields: List[str] = Field(default_factory=list, description="Fields meeting matching criteria")
    mismatched_fields: List[str] = Field(default_factory=list, description="Fields failing matching criteria")
    mismatch_reasons: List[str] = Field(default_factory=list, description="Explanations for failed criteria")
    evidence: List[Dict[str, Any]] = Field(default_factory=list, description="Granular verification evidence")
    raw_result: Optional[ReconciliationResult] = Field(default=None, description="Underlying domain result")
    execution_record: AgentExecutionRecord = Field(..., description="Auditable agent execution record")


class ReconciliationAgent(BaseAgent):
    """Orchestration agent wrapping deterministic and fuzzy ledger reconciliation."""

    agent_name: str = "ReconciliationAgent"
    agent_version: str = "1.0.0"

    def __init__(
        self,
        service: Optional[ReconciliationService] = None,
        agent_name: Optional[str] = None,
        agent_version: Optional[str] = None,
    ) -> None:
        super().__init__(
            agent_name=agent_name or "ReconciliationAgent",
            agent_version=agent_version or "1.0.0",
        )
        self.service = service or ReconciliationService()

    def analyze(
        self,
        transaction: Transaction,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> ReconciliationAgentResult:
        """Execute two-stage candidate discovery and deterministic ledger verification."""
        started_at = datetime.now(timezone.utc)
        corr_id = correlation_id or f"CORR-RECON-{transaction.transaction_id}"

        try:
            raw_recon: ReconciliationResult = self.service.reconcile_transaction(
                transaction=transaction,
                correlation_id=corr_id,
                causation_id=causation_id,
            )

            completed_at = datetime.now(timezone.utc)

            # Build explainable evidence payload
            evidence_list: List[Dict[str, Any]] = []
            if raw_recon.evidence:
                evidence_list.extend(raw_recon.evidence)
            evidence_list.append({
                "status": raw_recon.status,
                "score": raw_recon.score,
                "matched_fields": raw_recon.matched_fields,
                "mismatched_fields": raw_recon.mismatched_fields,
                "mismatch_reasons": raw_recon.mismatch_reasons,
                "ledger_entry_id": raw_recon.ledger_entry_id,
                "requires_manual_review": raw_recon.requires_manual_review,
            })

            exec_record = self.create_execution_record(
                transaction_id=transaction.transaction_id,
                decision=raw_recon.status,
                started_at=started_at,
                completed_at=completed_at,
                evidence=evidence_list,
                correlation_id=corr_id,
                success=True,
            )

            result = ReconciliationAgentResult(
                agent_name=self.agent_name,
                transaction_id=transaction.transaction_id,
                status=raw_recon.status,
                reconciliation_score=raw_recon.score,
                requires_manual_review=raw_recon.requires_manual_review,
                matched_ledger_entry_id=raw_recon.ledger_entry_id,
                matched_fields=raw_recon.matched_fields,
                mismatched_fields=raw_recon.mismatched_fields,
                mismatch_reasons=raw_recon.mismatch_reasons,
                evidence=evidence_list,
                raw_result=raw_recon,
                execution_record=exec_record,
            )

            logger.info(
                "[%s] Reconciled TX %s -> Status: %s, Score: %.2f, ManualReview: %s",
                self.agent_name,
                transaction.transaction_id,
                result.status,
                result.reconciliation_score,
                result.requires_manual_review,
            )
            return result

        except Exception as exc:
            completed_at = datetime.now(timezone.utc)
            logger.error(
                "[%s] Error reconciling TX %s: %s",
                self.agent_name,
                transaction.transaction_id,
                exc,
                exc_info=True,
            )
            fail_evidence = [{"error": str(exc), "fallback": "MANUAL_REVIEW"}]
            exec_record = self.create_execution_record(
                transaction_id=transaction.transaction_id,
                decision=ReconciliationStatus.MISMATCHED.value,
                started_at=started_at,
                completed_at=completed_at,
                evidence=fail_evidence,
                correlation_id=corr_id,
                success=False,
                error_message=str(exc),
            )
            return ReconciliationAgentResult(
                agent_name=self.agent_name,
                transaction_id=transaction.transaction_id,
                status=ReconciliationStatus.MISMATCHED.value,
                reconciliation_score=0.0,
                requires_manual_review=True,
                matched_ledger_entry_id=None,
                matched_fields=[],
                mismatched_fields=["system_execution"],
                mismatch_reasons=[f"Agent execution failure: {exc}"],
                evidence=fail_evidence,
                raw_result=None,
                execution_record=exec_record,
            )
