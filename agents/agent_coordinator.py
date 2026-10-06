"""Multi-Agent Decision Coordinator for ZT-LedgerGuard.

Coordinates independent domain agents (ReconciliationAgent, FraudDetectionAgent, PolicyAgent),
aggregates their outcomes into a structured DecisionContext, executes the ConsensusEngine,
persists auditable decision records in SQLite, and emits canonical domain events.
Guarantees financial safety: Agent Failure ≠ Approval (automatic MANUAL_REVIEW fallback).
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional

from agents.decision_agent.decision_agent import (
    DecisionAgent,
    DecisionContext,
    DecisionResult,
    DecisionType,
)
from agents.fraud_agent.fraud_agent import FraudDetectionAgent
from agents.policy_agent.policy_agent import (
    PolicyAgent,
    PolicyAgentResult,
    PolicyEvidence,
    PolicyRecommendation,
)
from agents.reconciliation_agent.reconciliation_agent import (
    ReconciliationAgent,
    ReconciliationAgentResult,
)
from database.database import (
    get_connection,
    get_decision_result_by_transaction_id,
    insert_decision_result,
)
from fraud_detection.fraud_result import (
    FraudAnalysisResult,
    FraudStatus,
    RiskLevel,
)
from orchestrator.event_orchestrator import Event
from orchestrator.event_types import EventType
from orchestrator.workflow_state import StageStatus, WorkflowStatus, WorkflowTracker
from reconciliation.models import Transaction
from reconciliation.reconciliation_result import ReconciliationStatus
from security.security_utils import mask_account_id

logger = logging.getLogger("agents.agent_coordinator")


class AgentCoordinator:
    """Orchestrator coordinating multi-agent consensus decisioning."""

    def __init__(
        self,
        reconciliation_agent: Optional[ReconciliationAgent] = None,
        fraud_agent: Optional[FraudDetectionAgent] = None,
        policy_agent: Optional[PolicyAgent] = None,
        decision_agent: Optional[DecisionAgent] = None,
        publisher: Optional[Any] = None,
        workflow_tracker: Optional[WorkflowTracker] = None,
        db_path: Optional[str] = None,
    ) -> None:
        self.db_path = db_path
        self.reconciliation_agent = reconciliation_agent or ReconciliationAgent()
        self.fraud_agent = fraud_agent or FraudDetectionAgent()
        self.policy_agent = policy_agent or PolicyAgent()
        self.decision_agent = decision_agent or DecisionAgent()

        if publisher is None:
            from kafka.local_event_publisher import LocalEventPublisher
            self.publisher = LocalEventPublisher()
        else:
            self.publisher = publisher

        self.workflow_tracker = workflow_tracker

    def coordinate_decision(
        self,
        transaction: Transaction,
        recent_transactions: Optional[List[Transaction]] = None,
        prior_failed_count: int = 0,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
        force_recompute: bool = False,
    ) -> DecisionResult:
        """Execute full multi-agent consensus coordination for a transaction."""
        corr_id = correlation_id or f"CORR-COORD-{transaction.transaction_id}"

        # ----------------------------------------------------------------------
        # 1. Idempotency Check: Return existing decision if already computed
        # ----------------------------------------------------------------------
        if not force_recompute:
            try:
                with get_connection(self.db_path) as conn:
                    existing = get_decision_result_by_transaction_id(conn, transaction.transaction_id)
                if existing:
                    logger.info(
                        "[AgentCoordinator] Idempotent hit: returning cached decision for TX %s",
                        transaction.transaction_id,
                    )
                    return DecisionResult(
                        decision_id=existing["id"],
                        transaction_id=existing["transaction_id"],
                        decision=existing["decision"],
                        confidence=existing["confidence"],
                        requires_manual_review=existing["requires_manual_review"],
                        block_recommended=existing["block_recommended"],
                        agent_results=existing.get("agent_results", {}),
                        triggered_policies=existing.get("triggered_policies", []),
                        decision_reasons=existing.get("decision_reasons", []),
                        disagreement_detected=bool(existing.get("disagreements")),
                        disagreements=existing.get("disagreements", []),
                        correlation_id=existing.get("correlation_id"),
                        decision_timestamp=datetime.fromisoformat(existing["created_at"]) if isinstance(existing["created_at"], str) else existing["created_at"],
                    )
            except Exception as e:
                logger.warning("[AgentCoordinator] DB check for existing decision failed: %s", e)

        # ----------------------------------------------------------------------
        # 2. Update Workflow State: consensus_decision -> IN_PROGRESS
        # ----------------------------------------------------------------------
        if self.workflow_tracker:
            self.workflow_tracker.update_stage(
                correlation_id=corr_id,
                stage="consensus_decision",
                status=StageStatus.CONSENSUS_IN_PROGRESS.value,
            )

        agent_failures: List[str] = []
        masked_acc = mask_account_id(transaction.account_id)

        # ----------------------------------------------------------------------
        # 3. Step 1: Reconciliation Agent Execution
        # ----------------------------------------------------------------------
        recon_result: Optional[ReconciliationAgentResult] = None
        try:
            recon_result = self.reconciliation_agent.analyze(
                transaction=transaction,
                correlation_id=corr_id,
                causation_id=causation_id,
            )
            # Emit RECONCILIATION_AGENT_COMPLETED event
            self._publish_event(
                event_type=EventType.RECONCILIATION_AGENT_COMPLETED.value,
                source="reconciliation_agent",
                correlation_id=corr_id,
                causation_id=causation_id,
                payload={
                    "transaction_id": transaction.transaction_id,
                    "masked_account_id": masked_acc,
                    "status": recon_result.status,
                    "reconciliation_score": recon_result.reconciliation_score,
                    "requires_manual_review": recon_result.requires_manual_review,
                },
            )
        except Exception as exc:
            logger.error("[AgentCoordinator] ReconciliationAgent failed for TX %s: %s", transaction.transaction_id, exc)
            agent_failures.append("ReconciliationAgent")

        # ----------------------------------------------------------------------
        # 4. Step 2: Fraud Detection Agent Execution
        # ----------------------------------------------------------------------
        fraud_result: Optional[FraudAnalysisResult] = None
        try:
            fraud_result = self.fraud_agent.analyze_transaction(
                transaction=transaction,
                recent_transactions=recent_transactions,
                reconciliation_result=recon_result.raw_result if recon_result else None,
                prior_failed_count=prior_failed_count,
                correlation_id=corr_id,
            )
            # Emit FRAUD_AGENT_COMPLETED event
            self._publish_event(
                event_type=EventType.FRAUD_AGENT_COMPLETED.value,
                source="fraud_agent",
                correlation_id=corr_id,
                causation_id=causation_id,
                payload={
                    "transaction_id": transaction.transaction_id,
                    "masked_account_id": masked_acc,
                    "fraud_score": fraud_result.fraud_score,
                    "risk_level": fraud_result.risk_level.value if hasattr(fraud_result.risk_level, "value") else str(fraud_result.risk_level),
                    "requires_manual_review": fraud_result.requires_manual_review,
                    "block_recommended": fraud_result.block_recommended,
                },
            )
        except Exception as exc:
            logger.error("[AgentCoordinator] FraudDetectionAgent failed for TX %s: %s", transaction.transaction_id, exc)
            agent_failures.append("FraudDetectionAgent")

        # ----------------------------------------------------------------------
        # 5. Step 3: Policy / Risk Agent Execution
        # ----------------------------------------------------------------------
        policy_result: Optional[PolicyAgentResult] = None
        try:
            policy_result = self.policy_agent.analyze(
                transaction=transaction,
                fraud_result=fraud_result,
                reconciliation_status=recon_result.status if recon_result else ReconciliationStatus.MISMATCHED.value,
                reconciliation_score=recon_result.reconciliation_score if recon_result else 0.0,
                correlation_id=corr_id,
            )
            # Emit POLICY_AGENT_COMPLETED event
            self._publish_event(
                event_type=EventType.POLICY_AGENT_COMPLETED.value,
                source="policy_agent",
                correlation_id=corr_id,
                causation_id=causation_id,
                payload={
                    "transaction_id": transaction.transaction_id,
                    "masked_account_id": masked_acc,
                    "recommendation": policy_result.recommendation,
                    "triggered_count": len(policy_result.triggered_policies),
                    "requires_manual_review": policy_result.requires_manual_review,
                    "block_recommended": policy_result.block_recommended,
                },
            )
        except Exception as exc:
            logger.error("[AgentCoordinator] PolicyAgent failed for TX %s: %s", transaction.transaction_id, exc)
            agent_failures.append("PolicyAgent")

        # ----------------------------------------------------------------------
        # 6. Step 4: Multi-Agent Consensus Evaluation
        # ----------------------------------------------------------------------
        decision_context = DecisionContext(
            transaction_id=transaction.transaction_id,
            reconciliation_result=recon_result,
            fraud_result=fraud_result,
            policy_result=policy_result,
            correlation_id=corr_id,
            agent_failures=agent_failures,
        )

        self._publish_event(
            event_type=EventType.CONSENSUS_DECISION_REQUESTED.value,
            source="agent_coordinator",
            correlation_id=corr_id,
            causation_id=causation_id,
            payload={
                "transaction_id": transaction.transaction_id,
                "masked_account_id": masked_acc,
                "failed_agents": agent_failures,
            },
        )

        decision_result = self.decision_agent.analyze(decision_context)

        # ----------------------------------------------------------------------
        # 7. Step 5: Persist Decision in SQLite
        # ----------------------------------------------------------------------
        try:
            with get_connection(self.db_path) as conn:
                insert_decision_result(
                    conn,
                    {
                        "id": decision_result.decision_id,
                        "transaction_id": decision_result.transaction_id,
                        "correlation_id": decision_result.correlation_id,
                        "decision": decision_result.decision,
                        "confidence": decision_result.confidence,
                        "requires_manual_review": decision_result.requires_manual_review,
                        "block_recommended": decision_result.block_recommended,
                        "agent_results": decision_result.agent_results,
                        "triggered_policies": decision_result.triggered_policies,
                        "decision_reasons": decision_result.decision_reasons,
                        "disagreements": decision_result.disagreements,
                        "created_at": decision_result.decision_timestamp.isoformat(),
                    },
                )
        except Exception as e:
            logger.error("[AgentCoordinator] Failed to persist decision for TX %s: %s", transaction.transaction_id, e)

        # ----------------------------------------------------------------------
        # 8. Step 6: Publish CONSENSUS_DECISION_COMPLETED Event
        # ----------------------------------------------------------------------
        self._publish_event(
            event_type=EventType.CONSENSUS_DECISION_COMPLETED.value,
            source="agent_coordinator",
            correlation_id=corr_id,
            causation_id=causation_id,
            payload={
                "decision_id": decision_result.decision_id,
                "transaction_id": transaction.transaction_id,
                "masked_account_id": masked_acc,
                "decision": decision_result.decision,
                "confidence": decision_result.confidence,
                "requires_manual_review": decision_result.requires_manual_review,
                "block_recommended": decision_result.block_recommended,
                "disagreement_detected": decision_result.disagreement_detected,
                "decision_reasons": decision_result.decision_reasons,
            },
        )

        # ----------------------------------------------------------------------
        # 9. Step 7: Update Workflow State to Completed
        # ----------------------------------------------------------------------
        if self.workflow_tracker:
            self.workflow_tracker.update_stage(
                correlation_id=corr_id,
                stage="consensus_decision",
                status=StageStatus.CONSENSUS_DECISION_COMPLETED.value,
                metadata={"decision": decision_result.decision, "confidence": decision_result.confidence},
            )
            self.workflow_tracker.mark_completed(
                correlation_id=corr_id,
                final_disposition=decision_result.decision,
            )

        logger.info(
            "[AgentCoordinator] Completed consensus for TX %s: Decision=%s (Conf=%.2f, Review=%s, Block=%s)",
            transaction.transaction_id,
            decision_result.decision,
            decision_result.confidence,
            decision_result.requires_manual_review,
            decision_result.block_recommended,
        )
        return decision_result

    def _publish_event(
        self,
        event_type: str,
        source: str,
        correlation_id: str,
        payload: Dict[str, Any],
        causation_id: Optional[str] = None,
    ) -> None:
        """Helper to create and dispatch a domain event to the configured publisher."""
        event = Event(
            event_type=event_type,
            source=source,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload=payload,
        )
        try:
            self.publisher.publish(event)
        except Exception as exc:
            logger.warning("[AgentCoordinator] Event publishing warning (%s): %s", event_type, exc)
