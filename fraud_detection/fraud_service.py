"""Fraud Detection Service Layer for ZT-LedgerGuard.

Coordinates fraud agent analysis, database audit persistence,
event emission of FRAUD_ANALYSIS_COMPLETED, and workflow state tracking.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from agents.fraud_agent.fraud_agent import FraudDetectionAgent
from database.database import (
    get_connection,
    get_fraud_analysis_by_transaction_id,
    get_fraud_metrics,
    get_transaction,
    insert_fraud_analysis,
)
from fraud_detection.profile import AccountProfile
from fraud_detection.fraud_result import (
    FraudAnalysisResult,
    FraudStatus,
    RiskLevel,
    RuleEvidence,
)
from orchestrator.event_orchestrator import Event
from orchestrator.event_types import EventType
from orchestrator.workflow_state import StageStatus, WorkflowTracker
from reconciliation.models import Transaction
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus
from security.security_utils import mask_account_id

if TYPE_CHECKING:
    from kafka.local_event_publisher import EventPublisher

logger = logging.getLogger("fraud_detection.fraud_service")


class FraudService:
    """Service coordinating end-to-end fraud triage, persistence, and event progression."""

    def __init__(
        self,
        agent: Optional[FraudDetectionAgent] = None,
        publisher: Optional[EventPublisher] = None,
        workflow_tracker: Optional[WorkflowTracker] = None,
        db_path: Optional[str] = None,
    ) -> None:
        self.db_path = db_path
        self.agent = agent or FraudDetectionAgent()
        if publisher is None:
            from kafka.local_event_publisher import LocalEventPublisher

            self.publisher = LocalEventPublisher()
        else:
            self.publisher = publisher
        self.workflow_tracker = workflow_tracker

    def analyze_transaction(
        self,
        transaction: Transaction,
        profile: Optional[AccountProfile] = None,
        reconciliation_result: Optional[ReconciliationResult] = None,
        recent_transactions: Optional[List[Transaction]] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> FraudAnalysisResult:
        """Execute fraud evaluation, persist audit record, and dispatch domain events."""
        corr_id = correlation_id or f"CORR-{uuid.uuid4().hex[:12].upper()}"

        # 1. Fetch recent transactions for velocity/repetition if not supplied
        if recent_transactions is None:
            recent_transactions = self._fetch_recent_transactions_for_account(
                transaction.account_id
            )

        # 2. Execute Fraud Detection Agent
        result = self.agent.analyze_transaction(
            transaction=transaction,
            profile=profile,
            recent_transactions=recent_transactions,
            reconciliation_result=reconciliation_result,
            correlation_id=corr_id,
        )

        # 3. Persist Analysis to SQLite
        self._persist_fraud_analysis(result, corr_id)

        # 4. Update Workflow State if tracker configured
        if self.workflow_tracker:
            self.workflow_tracker.update_stage(
                correlation_id=corr_id,
                stage="fraud_analysis",
                status=StageStatus.COMPLETED.value,
                metadata={
                    "fraud_score": result.fraud_score,
                    "risk_level": result.risk_level.value,
                    "status": result.status.value,
                    "requires_manual_review": result.requires_manual_review,
                    "block_recommended": result.block_recommended,
                },
            )

        # 5. Emit FRAUD_ANALYSIS_COMPLETED Event
        masked_acc = mask_account_id(transaction.account_id)
        fraud_completed_event = Event(
            event_type=EventType.FRAUD_ANALYSIS_COMPLETED.value,
            source="fraud_service",
            correlation_id=corr_id,
            causation_id=causation_id,
            payload={
                "transaction_id": transaction.transaction_id,
                "masked_account_id": masked_acc,
                "fraud_score": result.fraud_score,
                "risk_level": result.risk_level.value,
                "status": result.status.value,
                "requires_manual_review": result.requires_manual_review,
                "block_recommended": result.block_recommended,
                "indicators": result.indicators,
                "correlation_id": corr_id,
            },
        )
        self.publisher.publish(fraud_completed_event)

        logger.info(
            "Fraud analysis completed for %s: score=%d (%s) manual_review=%s block=%s (event=%s)",
            transaction.transaction_id,
            result.fraud_score,
            result.risk_level.value,
            result.requires_manual_review,
            result.block_recommended,
            fraud_completed_event.event_id,
        )
        return result

    def analyze_by_transaction_id(
        self,
        transaction_id: str,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> Optional[FraudAnalysisResult]:
        """Fetch transaction and optional reconciliation result by ID and execute analysis."""
        tx = self._fetch_transaction_by_id(transaction_id)
        if not tx:
            return None

        # Look up any existing reconciliation record
        recon_result = self._fetch_reconciliation_result_by_tx_id(transaction_id)
        return self.analyze_transaction(
            transaction=tx,
            reconciliation_result=recon_result,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    def on_fraud_requested(self, event: Event) -> Dict[str, Any]:
        """Event handler callback for FRAUD_ANALYSIS_REQUESTED events."""
        corr_id = event.correlation_id
        causation_id = event.event_id
        payload = event.payload or {}
        tx_id = payload.get("transaction_id")

        if not tx_id:
            logger.warning("[FraudService] Missing transaction_id in FRAUD_ANALYSIS_REQUESTED payload")
            return {"status": "FAILED", "reason": "Missing transaction_id"}

        result = self.analyze_by_transaction_id(
            transaction_id=tx_id,
            correlation_id=corr_id,
            causation_id=causation_id,
        )
        if not result:
            # Construct transaction on-the-fly from event payload if not yet in DB
            try:
                tx = Transaction(
                    transaction_id=tx_id,
                    account_id=payload.get("account_id", "ACC-UNKNOWN"),
                    transaction_type=payload.get("transaction_type", "DEBIT"),
                    amount=float(payload.get("amount", 0.0)),
                    currency=payload.get("currency", "USD"),
                    transaction_timestamp=datetime.now(timezone.utc),
                    source_system=payload.get("source_system", "API"),
                    destination_system=payload.get("destination_system", "CORE_LEDGER"),
                )
                result = self.analyze_transaction(
                    transaction=tx,
                    correlation_id=corr_id,
                    causation_id=causation_id,
                )
            except Exception as exc:
                logger.error("[FraudService] Failed to analyze transaction from payload: %s", exc)
                return {"status": "FAILED", "reason": str(exc)}

        return {
            "stage": "FRAUD_ANALYSIS",
            "status": "COMPLETED",
            "fraud_score": result.fraud_score,
            "risk_level": result.risk_level.value,
        }

    def get_fraud_analysis(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve stored fraud analysis from SQLite."""
        conn = get_connection(self.db_path)
        row = get_fraud_analysis_by_transaction_id(conn, transaction_id)
        conn.close()
        if not row:
            return None

        # Parse JSON fields safely
        indicators = []
        evidence = []
        try:
            indicators = json.loads(row.get("indicators_json") or "[]")
        except Exception:
            pass
        try:
            evidence = json.loads(row.get("evidence_json") or "[]")
        except Exception:
            pass

        return {
            "id": row["id"],
            "transaction_id": row["transaction_id"],
            "fraud_score": row["fraud_score"],
            "risk_level": row["risk_level"],
            "status": row["status"],
            "requires_manual_review": bool(row["requires_manual_review"]),
            "block_recommended": bool(row["block_recommended"]),
            "indicators": indicators,
            "evidence": evidence,
            "created_at": row["created_at"],
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Retrieve aggregate fraud metrics."""
        conn = get_connection(self.db_path)
        metrics = get_fraud_metrics(conn)
        conn.close()
        return metrics

    def _persist_fraud_analysis(
        self, result: FraudAnalysisResult, correlation_id: str
    ) -> None:
        """Save fraud analysis record into SQLite database."""
        analysis_id = f"FRAUD-{uuid.uuid4().hex[:12].upper()}"
        evidence_dicts = [e.model_dump() for e in result.evidence]

        analysis_data = {
            "id": analysis_id,
            "transaction_id": result.transaction_id,
            "fraud_score": result.fraud_score,
            "risk_level": result.risk_level.value,
            "status": result.status.value,
            "requires_manual_review": result.requires_manual_review,
            "block_recommended": result.block_recommended,
            "indicators_json": json.dumps(result.indicators),
            "evidence_json": json.dumps(evidence_dicts),
            "created_at": result.analysis_timestamp.isoformat(),
        }

        conn = get_connection(self.db_path)
        insert_fraud_analysis(conn, analysis_data)
        conn.close()

    def _fetch_transaction_by_id(self, transaction_id: str) -> Optional[Transaction]:
        """Fetch transaction from DB."""
        tx_row = get_transaction(transaction_id, db_path=self.db_path)
        if not tx_row:
            return None
        ts = tx_row["transaction_timestamp"]
        dt = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
        return Transaction(
            transaction_id=tx_row["transaction_id"],
            account_id=tx_row["account_id"],
            transaction_type=tx_row["transaction_type"],
            amount=float(tx_row["amount"]),
            currency=tx_row["currency"],
            transaction_timestamp=dt,
            source_system=tx_row["source_system"],
            destination_system=tx_row.get("destination_system", "CORE_LEDGER"),
            reference_id=tx_row.get("reference_id"),
        )

    def _fetch_recent_transactions_for_account(
        self, account_id: str
    ) -> List[Transaction]:
        """Fetch transactions for account to calculate velocity/repeated patterns."""
        conn = get_connection(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM transactions WHERE account_id = ? ORDER BY transaction_timestamp DESC LIMIT 50",
            (account_id,),
        )
        rows = cursor.fetchall()
        conn.close()

        transactions = []
        for row in rows:
            r = dict(row)
            ts = r["transaction_timestamp"]
            dt = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
            transactions.append(
                Transaction(
                    transaction_id=r["transaction_id"],
                    account_id=r["account_id"],
                    transaction_type=r["transaction_type"],
                    amount=float(r["amount"]),
                    currency=r["currency"],
                    transaction_timestamp=dt,
                    source_system=r["source_system"],
                    destination_system=r.get("destination_system", "CORE_LEDGER"),
                    reference_id=r.get("reference_id"),
                )
            )
        return transactions

    def _fetch_reconciliation_result_by_tx_id(
        self, transaction_id: str
    ) -> Optional[ReconciliationResult]:
        """Fetch reconciliation result for transaction if available."""
        conn = get_connection(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM reconciliation_results WHERE transaction_id = ? ORDER BY timestamp DESC LIMIT 1",
            (transaction_id,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None

        # Parse matched / mismatched lists
        matched = (
            [m.strip() for m in row["matched_fields"].split(",") if m.strip()]
            if row["matched_fields"]
            else []
        )
        mismatched = (
            [m.strip() for m in row["mismatched_fields"].split(",") if m.strip()]
            if row["mismatched_fields"]
            else []
        )
        reasons = (
            [r.strip() for r in row["mismatch_reasons"].split(";") if r.strip()]
            if row["mismatch_reasons"]
            else []
        )

        return ReconciliationResult(
            reconciliation_id=row["reconciliation_id"],
            transaction_id=row["transaction_id"],
            ledger_entry_id=row["ledger_entry_id"],
            correlation_id=row["correlation_id"],
            status=ReconciliationStatus(row["status"]),
            score=float(row["score"]),
            matched_fields=matched,
            mismatched_fields=mismatched,
            mismatch_reasons=reasons,
            evidence=row["evidence"] or "",
            requires_manual_review=bool(row["requires_manual_review"]),
        )
