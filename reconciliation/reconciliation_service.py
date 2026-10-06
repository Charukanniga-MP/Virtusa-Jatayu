"""Reconciliation Service Layer.

Coordinates the end-to-end reconciliation workflow:
1. Transaction lookup or ingestion
2. Candidate ledger entry discovery via LedgerRepository
3. Invocation of the deterministic ReconciliationEngine
4. Audit result persistence in SQLite
5. Event emission of TRANSACTION_RECONCILED with correlation and causation tracing
6. Workflow state progression tracking
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from database.database import get_transaction
from orchestrator.event_orchestrator import Event, EventOrchestrator
from orchestrator.event_types import EventType
from orchestrator.workflow_state import StageStatus, WorkflowTracker
from reconciliation.candidate_service import CandidateService
from reconciliation.engine import ReconciliationEngine
from reconciliation.fuzzy_result import CandidateDiscoveryResult, CandidateSelectionStatus
from reconciliation.ledger_repository import LedgerRepository
from reconciliation.models import LedgerEntry, Transaction
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus
from security.security_utils import mask_account_id

if TYPE_CHECKING:
    from kafka.local_event_publisher import EventPublisher

logger = logging.getLogger("reconciliation.reconciliation_service")


class ReconciliationService:
    """Service coordinating two-stage reconciliation: fuzzy candidate discovery + deterministic verification."""

    def __init__(
        self,
        repository: Optional[LedgerRepository] = None,
        engine: Optional[ReconciliationEngine] = None,
        candidate_service: Optional[CandidateService] = None,
        publisher: Optional[EventPublisher] = None,
        workflow_tracker: Optional[WorkflowTracker] = None,
        db_path: Optional[str] = None,
    ) -> None:
        """Initialize ReconciliationService with repository, engine, and candidate service."""
        self.db_path = db_path
        self.repository = repository or LedgerRepository(db_path=db_path)
        self.engine = engine or ReconciliationEngine()
        self.candidate_service = candidate_service or CandidateService(repository=self.repository)
        if publisher is None:
            from kafka.local_event_publisher import LocalEventPublisher
            self.publisher = LocalEventPublisher()
        else:
            self.publisher = publisher
        self.workflow_tracker = workflow_tracker

    def discover_candidates(self, transaction: Transaction) -> CandidateDiscoveryResult:
        """Discover and rank candidate ledger entries for a transaction without deciding reconciliation."""
        return self.candidate_service.discover_candidates(transaction)

    def discover_candidates_by_transaction_id(
        self, transaction_id: str
    ) -> Optional[CandidateDiscoveryResult]:
        """Fetch transaction by ID from DB and execute candidate discovery."""
        tx = self._fetch_transaction_by_id(transaction_id)
        if not tx:
            return None
        return self.discover_candidates(tx)

    def reconcile_transaction(
        self,
        transaction: Transaction,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> ReconciliationResult:
        """Execute two-stage reconciliation: candidate discovery followed by deterministic verification.

        Args:
            transaction: Transaction instance to reconcile.
            correlation_id: End-to-end distributed tracing correlation ID.
            causation_id: Parent triggering event ID.

        Returns:
            ReconciliationResult containing final financial disposition.
        """
        corr_id = correlation_id or f"CORR-{transaction.transaction_id}"

        # 1. Update workflow stage to PROCESSING
        if self.workflow_tracker:
            self.workflow_tracker.update_stage(
                correlation_id=corr_id,
                stage="reconciliation",
                status=StageStatus.PROCESSING.value,
            )

        # ----------------------------------------------------------------------
        # STAGE 1: Candidate Discovery & Fuzzy Ranking
        # ----------------------------------------------------------------------
        discovery = self.candidate_service.discover_candidates(transaction)

        # Emit FUZZY_CANDIDATE_IDENTIFIED Event
        top_cand = discovery.top_candidate
        masked_acc = mask_account_id(transaction.account_id)
        fuzzy_event = Event(
            event_type=EventType.FUZZY_CANDIDATE_IDENTIFIED.value,
            source="candidate_service",
            correlation_id=corr_id,
            causation_id=causation_id,
            payload={
                "transaction_id": transaction.transaction_id,
                "masked_account_id": masked_acc,
                "candidate_count": discovery.candidate_count,
                "top_candidate_id": top_cand.ledger_entry_id if top_cand else None,
                "candidate_score": top_cand.candidate_score if top_cand else 0.0,
                "confidence_level": top_cand.confidence_level if top_cand else "REJECTED",
                "ambiguity_status": discovery.selection_status,
                "requires_manual_review": discovery.requires_manual_review,
            },
        )
        self.publisher.publish(fuzzy_event)

        # ----------------------------------------------------------------------
        # STAGE 2: Deterministic Financial Verification
        # ----------------------------------------------------------------------
        if discovery.selection_status == CandidateSelectionStatus.AMBIGUOUS.value:
            # Ambiguity detected: do NOT approve automatically
            result = ReconciliationResult(
                transaction_id=transaction.transaction_id,
                ledger_entry_id=top_cand.ledger_entry_id if top_cand else None,
                correlation_id=corr_id,
                status=ReconciliationStatus.AMBIGUOUS.value,
                score=top_cand.candidate_score if top_cand else 0.0,
                matched_fields=top_cand.matched_fields if top_cand else [],
                mismatched_fields=top_cand.mismatched_fields if top_cand else ["candidate_ambiguity"],
                mismatch_reasons=discovery.reasons,
                evidence=top_cand.evidence if top_cand else [],
                requires_manual_review=True,
            )
        else:
            # Clear candidate identified or candidate rejected/missing
            candidate_entry: Optional[LedgerEntry] = None
            if top_cand:
                candidate_entry = self.repository.get_ledger_entry(top_cand.ledger_entry_id)

            if candidate_entry is None:
                # Direct repository lookup fallback
                candidate_entry = self.repository.find_by_transaction_id(transaction.transaction_id)
                if candidate_entry is None and transaction.reference_id:
                    candidate_entry = self.repository.find_by_reference_id(transaction.reference_id)

            # Authoritative deterministic verification
            result = self.engine.reconcile(
                transaction=transaction,
                ledger_entry=candidate_entry,
                correlation_id=corr_id,
            )

            # Attach manual review requirement if candidate confidence was low or verification failed
            if discovery.requires_manual_review or result.status in (
                ReconciliationStatus.MISMATCHED.value,
                ReconciliationStatus.AMBIGUOUS.value,
            ):
                result.requires_manual_review = True

        # 4. Persist Reconciliation Result Audit Record
        self.repository.save_reconciliation_result(result)

        # 5. Update Workflow State
        if self.workflow_tracker:
            status_val = (
                StageStatus.FAILED.value
                if result.status == ReconciliationStatus.MISMATCHED.value
                else StageStatus.COMPLETED.value
            )
            self.workflow_tracker.update_stage(
                correlation_id=corr_id,
                stage="reconciliation",
                status=status_val,
                metadata={
                    "reconciliation_status": result.status,
                    "reconciliation_score": result.score,
                    "requires_manual_review": result.requires_manual_review,
                },
            )

        # 6. Emit TRANSACTION_RECONCILED Event
        reconciled_event = Event(
            event_type=EventType.TRANSACTION_RECONCILED.value,
            source="reconciliation_service",
            correlation_id=corr_id,
            causation_id=causation_id,
            payload={
                "transaction_id": transaction.transaction_id,
                "masked_account_id": masked_acc,
                "ledger_entry_id": result.ledger_entry_id,
                "reconciliation_id": result.reconciliation_id,
                "status": result.status,
                "score": result.score,
                "correlation_id": corr_id,
                "matched_fields": result.matched_fields,
                "mismatched_fields": result.mismatched_fields,
                "requires_manual_review": result.requires_manual_review,
            },
        )
        self.publisher.publish(reconciled_event)

        logger.info(
            "Reconciliation completed for %s: status=%s score=%.2f manual_review=%s (event=%s)",
            transaction.transaction_id,
            result.status,
            result.score,
            result.requires_manual_review,
            reconciled_event.event_id,
        )

        return result

    def _fetch_transaction_by_id(self, transaction_id: str) -> Optional[Transaction]:
        """Fetch and reconstruct domain Transaction model from SQLite."""
        tx_row = get_transaction(transaction_id, db_path=self.db_path)
        if not tx_row:
            return None

        ts = tx_row["transaction_timestamp"]
        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts)
        else:
            dt = ts

        return Transaction(
            transaction_id=tx_row["transaction_id"],
            account_id=tx_row["account_id"],
            transaction_type=tx_row["transaction_type"],
            amount=float(tx_row["amount"]),
            currency=tx_row["currency"],
            transaction_timestamp=dt,
            source_system=tx_row["source_system"],
            destination_system=tx_row["destination_system"],
            reference_id=tx_row.get("reference_id"),
        )

    def reconcile_by_transaction_id(
        self,
        transaction_id: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> Optional[ReconciliationResult]:
        """Fetch transaction from DB and run reconciliation. Returns None if transaction not found."""
        tx = self._fetch_transaction_by_id(transaction_id)
        if not tx:
            return None

        return self.reconcile_transaction(
            transaction=tx,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    def on_reconciliation_requested(self, event: Event) -> Dict[str, Any]:
        """Event handler for TRANSACTION_RECONCILED_REQUESTED events."""
        tx_id = event.payload.get("transaction_id")
        if not tx_id:
            logger.error("TRANSACTION_RECONCILIATION_REQUESTED event missing transaction_id: %s", event.event_id)
            return {"error": "Missing transaction_id in event payload"}

        result = self.reconcile_by_transaction_id(
            transaction_id=tx_id,
            correlation_id=event.correlation_id,
            causation_id=event.event_id,
        )

        if result is None:
            # If transaction is not yet persisted in DB, build temporary object from event payload if available
            amt = event.payload.get("amount")
            curr = event.payload.get("currency")
            if amt is not None and curr:
                synthetic_tx = Transaction(
                    transaction_id=tx_id,
                    account_id=event.payload.get("account_id") or "ACC-UNKNOWN",
                    transaction_type=event.payload.get("transaction_type") or "TRANSFER",
                    amount=float(amt),
                    currency=str(curr),
                    transaction_timestamp=datetime.now(timezone.utc),
                    source_system=event.payload.get("source_system") or "UNKNOWN",
                    destination_system=event.payload.get("destination_system") or "UNKNOWN",
                    reference_id=event.payload.get("reference_id"),
                )
                result = self.reconcile_transaction(
                    transaction=synthetic_tx,
                    correlation_id=event.correlation_id,
                    causation_id=event.event_id,
                )
            else:
                return {"error": f"Transaction {tx_id} not found for reconciliation"}

        return {
            "status": result.status,
            "score": result.score,
            "reconciliation_id": result.reconciliation_id,
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Retrieve aggregated reconciliation metrics from repository."""
        return self.repository.get_metrics()
