"""Production-Style Event-Driven Processing Pipeline Coordinator.

Coordinates decoupled lifecycle progression across banking transaction events:
TRANSACTION_RECEIVED
    ↓
TRANSACTION_VALIDATED
    ↓
TRANSACTION_RECONCILIATION_REQUESTED  &  FRAUD_ANALYSIS_REQUESTED
    ↓                                        ↓
TRANSACTION_RECONCILED                   FRAUD_ANALYSIS_COMPLETED
                                             ↓
                                         RISK_ANALYSIS_REQUESTED
                                             ↓
                                         RISK_ANALYSIS_COMPLETED
                                             ↓
                                         TRANSACTION_APPROVED (Workflow COMPLETED)

Maintains shared correlation_id and rigorous parent causation_id tracing.
Uses placeholder execution results ready for Days 4–7 domain agent plug-ins.
"""

import logging
from typing import Any, Dict, Optional, Union
from pydantic import BaseModel

from orchestrator.event_orchestrator import Event, EventOrchestrator
from orchestrator.event_types import EventType
from orchestrator.workflow_state import StageStatus, WorkflowState, WorkflowTracker
from reconciliation.models import Transaction

logger = logging.getLogger("orchestrator.processing_pipeline")


class ProcessingPipeline:
    """Coordinates event flow across autonomous processing components."""

    def __init__(
        self,
        orchestrator: EventOrchestrator,
        workflow_tracker: Optional[WorkflowTracker] = None,
        reconciliation_service: Optional[Any] = None,
        fraud_service: Optional[Any] = None,
        agent_coordinator: Optional[Any] = None,
    ) -> None:
        self.orchestrator = orchestrator
        self.tracker = workflow_tracker or WorkflowTracker()
        self.reconciliation_service = reconciliation_service
        self.fraud_service = fraud_service
        self.agent_coordinator = agent_coordinator
        self._register_pipeline_handlers()

    def _register_pipeline_handlers(self) -> None:
        """Register the autonomous event propagation handlers."""

        # 1. TRANSACTION_RECEIVED -> Validate and advance
        def on_transaction_received(event: Event) -> Dict[str, Any]:
            tx_id = event.payload.get("transaction_id", "UNKNOWN")
            self.tracker.update_stage(event.correlation_id, "validation", StageStatus.COMPLETED.value)
            logger.info("[Pipeline] Validated transaction %s", tx_id)

            # Advance to validated event
            validated_event = Event(
                event_type=EventType.TRANSACTION_VALIDATED.value,
                source="pipeline.validation",
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                payload=dict(event.payload, stage="VALIDATION_COMPLETED"),
            )
            self.orchestrator.publish(validated_event)
            return {"stage": "VALIDATION", "status": "COMPLETED"}

        # 2. TRANSACTION_VALIDATED -> Dispatch Reconciliation & Fraud in parallel
        def on_transaction_validated(event: Event) -> Dict[str, Any]:
            logger.info("[Pipeline] Triggering Reconciliation & Fraud for %s", event.correlation_id)

            # Trigger reconciliation branch
            recon_event = Event(
                event_type=EventType.TRANSACTION_RECONCILIATION_REQUESTED.value,
                source="pipeline.coordinator",
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                payload=dict(event.payload, stage="RECONCILIATION_REQUESTED"),
            )
            self.orchestrator.publish(recon_event)

            # Trigger fraud analysis branch
            fraud_event = Event(
                event_type=EventType.FRAUD_ANALYSIS_REQUESTED.value,
                source="pipeline.coordinator",
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                payload=dict(event.payload, stage="FRAUD_REQUESTED"),
            )
            self.orchestrator.publish(fraud_event)
            return {"stage": "DISPATCH", "status": "COMPLETED"}

        # 3. TRANSACTION_RECONCILIATION_REQUESTED -> Reconciled
        def on_reconciliation_requested(event: Event) -> Dict[str, Any]:
            if self.reconciliation_service:
                return self.reconciliation_service.on_reconciliation_requested(event)

            self.tracker.update_stage(event.correlation_id, "reconciliation", StageStatus.COMPLETED.value)
            reconciled_event = Event(
                event_type=EventType.TRANSACTION_RECONCILED.value,
                source="pipeline.reconciliation_agent",
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                payload=dict(event.payload, reconciled=True, stage="RECONCILED"),
            )
            self.orchestrator.publish(reconciled_event)
            return {"stage": "RECONCILIATION", "status": "COMPLETED"}

        # 4. FRAUD_ANALYSIS_REQUESTED -> Fraud Completed
        def on_fraud_requested(event: Event) -> Dict[str, Any]:
            if self.fraud_service:
                return self.fraud_service.on_fraud_requested(event)

            self.tracker.update_stage(event.correlation_id, "fraud_analysis", StageStatus.COMPLETED.value)
            fraud_completed_event = Event(
                event_type=EventType.FRAUD_ANALYSIS_COMPLETED.value,
                source="pipeline.fraud_agent",
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                payload=dict(event.payload, risk_score=0.08, flagged=False, stage="FRAUD_COMPLETED"),
            )
            self.orchestrator.publish(fraud_completed_event)
            return {"stage": "FRAUD_ANALYSIS", "status": "COMPLETED"}

        # 5. FRAUD_ANALYSIS_COMPLETED -> Trigger Risk Analysis
        def on_fraud_completed(event: Event) -> Dict[str, Any]:
            risk_event = Event(
                event_type=EventType.RISK_ANALYSIS_REQUESTED.value,
                source="pipeline.risk_dispatcher",
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                payload=dict(event.payload, stage="RISK_REQUESTED"),
            )
            self.orchestrator.publish(risk_event)
            return {"stage": "RISK_REQUEST", "status": "COMPLETED"}

        # 6. RISK_ANALYSIS_REQUESTED -> Risk Completed
        def on_risk_requested(event: Event) -> Dict[str, Any]:
            self.tracker.update_stage(event.correlation_id, "risk_analysis", StageStatus.COMPLETED.value)
            risk_completed_event = Event(
                event_type=EventType.RISK_ANALYSIS_COMPLETED.value,
                source="pipeline.risk_agent",
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                payload=dict(event.payload, aggregate_risk="LOW", stage="RISK_COMPLETED"),
            )
            self.orchestrator.publish(risk_completed_event)
            return {"stage": "RISK_ANALYSIS", "status": "COMPLETED"}

        # 7. RISK_ANALYSIS_COMPLETED -> Final Disposition Decision
        def on_risk_completed(event: Event) -> Dict[str, Any]:
            decision_event = Event(
                event_type=EventType.TRANSACTION_APPROVED.value,
                source="pipeline.decision_engine",
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                payload=dict(event.payload, disposition="APPROVED"),
            )
            self.orchestrator.publish(decision_event)
            self.tracker.mark_completed(event.correlation_id, final_disposition="APPROVED")
            logger.info("[Pipeline] Workflow %s marked COMPLETED (APPROVED)", event.correlation_id)
            return {"stage": "FINAL_DECISION", "status": "COMPLETED"}

        # 8. CONSENSUS_DECISION_REQUESTED -> Consensus In Progress
        def on_consensus_requested(event: Event) -> Dict[str, Any]:
            self.tracker.update_stage(event.correlation_id, "consensus_decision", StageStatus.CONSENSUS_IN_PROGRESS.value)
            logger.info("[Pipeline] Consensus decision requested for %s", event.correlation_id)
            return {"stage": "CONSENSUS_DECISION", "status": "IN_PROGRESS"}

        # 9. CONSENSUS_DECISION_COMPLETED -> Workflow completion
        def on_consensus_completed(event: Event) -> Dict[str, Any]:
            decision = event.payload.get("decision", "MANUAL_REVIEW")
            self.tracker.update_stage(
                event.correlation_id,
                "consensus_decision",
                StageStatus.CONSENSUS_DECISION_COMPLETED.value,
                metadata={"decision": decision},
            )
            self.tracker.mark_completed(event.correlation_id, final_disposition=decision)
            logger.info("[Pipeline] Workflow %s marked COMPLETED (%s)", event.correlation_id, decision)
            return {"stage": "CONSENSUS_DECISION", "status": "COMPLETED", "decision": decision}

        # Register to the orchestrator instance
        self.orchestrator.register_handler(
            EventType.TRANSACTION_RECEIVED.value, on_transaction_received
        )
        self.orchestrator.register_handler(
            EventType.TRANSACTION_VALIDATED.value, on_transaction_validated
        )
        self.orchestrator.register_handler(
            EventType.TRANSACTION_RECONCILIATION_REQUESTED.value, on_reconciliation_requested
        )
        self.orchestrator.register_handler(
            EventType.FRAUD_ANALYSIS_REQUESTED.value, on_fraud_requested
        )
        self.orchestrator.register_handler(
            EventType.FRAUD_ANALYSIS_COMPLETED.value, on_fraud_completed
        )
        self.orchestrator.register_handler(
            EventType.RISK_ANALYSIS_REQUESTED.value, on_risk_requested
        )
        self.orchestrator.register_handler(
            EventType.RISK_ANALYSIS_COMPLETED.value, on_risk_completed
        )
        self.orchestrator.register_handler(
            EventType.CONSENSUS_DECISION_REQUESTED.value, on_consensus_requested
        )
        self.orchestrator.register_handler(
            EventType.CONSENSUS_DECISION_COMPLETED.value, on_consensus_completed
        )

    def execute_transaction_workflow(
        self,
        transaction: Union[Transaction, Dict[str, Any]],
        correlation_id: Optional[str] = None,
    ) -> WorkflowState:
        """Execute the full end-to-end pipeline for a financial transaction."""
        import uuid

        corr_id = correlation_id or f"CORR-{uuid.uuid4().hex[:12].upper()}"

        if isinstance(transaction, Transaction):
            tx_data = {
                "transaction_id": transaction.transaction_id,
                "account_id": transaction.account_id,
                "amount": transaction.amount,
                "currency": transaction.currency,
                "source_system": transaction.source_system,
            }
        else:
            tx_data = dict(transaction)

        # Initialize workflow tracking
        self.tracker.create_workflow(corr_id, metadata={"transaction_id": tx_data.get("transaction_id")})

        # Emit the entry-point event to start the pipeline
        entry_event = Event(
            event_type=EventType.TRANSACTION_RECEIVED.value,
            source="pipeline.entrypoint",
            correlation_id=corr_id,
            causation_id=None,
            payload=tx_data,
        )
        self.orchestrator.publish(entry_event)

        state = self.tracker.get_workflow(corr_id)
        return state or WorkflowState(correlation_id=corr_id)
