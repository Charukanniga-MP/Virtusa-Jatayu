"""Intelligent Deterministic Ledger Reconciliation Engine.

Provides stateless, explainable matching between incoming banking transactions
and core ledger entries.
Strictly deterministic: zero LLMs, zero ML models, zero non-deterministic heuristics.
"""

from typing import List, Optional
import uuid

from reconciliation.models import LedgerEntry, Transaction
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus
from reconciliation.rules import (
    ReconciliationWeights,
    RuleEvaluation,
    evaluate_account_id,
    evaluate_amount,
    evaluate_currency,
    evaluate_direction,
    evaluate_id_reference,
    evaluate_timestamp,
)


class ReconciliationEngine:
    """Stateless, deterministic reconciliation engine calculating weighted match scores."""

    def __init__(
        self,
        weights: Optional[ReconciliationWeights] = None,
        timestamp_tolerance_seconds: Optional[int] = None,
        match_threshold: float = 0.95,
        partial_match_threshold: float = 0.50,
    ) -> None:
        """Initialize engine with configurable scoring weights and thresholds.

        Args:
            weights: ReconciliationWeights assigning relative importance to fields.
            timestamp_tolerance_seconds: Maximum delta allowed between tx and ledger timestamps.
            match_threshold: Minimum score required for MATCHED status when no critical mismatches occur.
            partial_match_threshold: Minimum score required for PARTIAL_MATCH status.
        """
        self.weights = weights or ReconciliationWeights()
        self.timestamp_tolerance_seconds = timestamp_tolerance_seconds
        self.match_threshold = match_threshold
        self.partial_match_threshold = partial_match_threshold

    def reconcile(
        self,
        transaction: Transaction,
        ledger_entry: Optional[LedgerEntry],
        correlation_id: Optional[str] = None,
        reconciliation_id: Optional[str] = None,
    ) -> ReconciliationResult:
        """Deterministically compare a transaction against a candidate ledger entry.

        Args:
            transaction: Input domain Transaction under review.
            ledger_entry: Candidate LedgerEntry to reconcile against (or None if missing).
            correlation_id: Distributed trace correlation token.
            reconciliation_id: Explicit audit ID (auto-generated if None).

        Returns:
            ReconciliationResult with detailed scores, fields, and explainable evidence.
        """
        rec_id = reconciliation_id or str(uuid.uuid4())

        # ----------------------------------------------------------------------
        # Scenario: Candidate Ledger Entry Not Found
        # ----------------------------------------------------------------------
        if ledger_entry is None:
            return ReconciliationResult(
                reconciliation_id=rec_id,
                transaction_id=transaction.transaction_id,
                ledger_entry_id=None,
                correlation_id=correlation_id,
                status=ReconciliationStatus.NOT_FOUND.value,
                score=0.0,
                matched_fields=[],
                mismatched_fields=["ledger_entry"],
                mismatch_reasons=["Candidate ledger entry not found"],
                evidence=[
                    {
                        "field": "ledger_entry",
                        "match": False,
                        "message": f"No candidate ledger entry found for transaction {transaction.transaction_id}",
                    }
                ],
            )

        # ----------------------------------------------------------------------
        # Execute Deterministic Rule Suite
        # ----------------------------------------------------------------------
        evaluations: List[RuleEvaluation] = [
            evaluate_id_reference(transaction, ledger_entry, weight=self.weights.id_reference),
            evaluate_account_id(transaction, ledger_entry, weight=self.weights.account),
            evaluate_amount(transaction, ledger_entry, weight=self.weights.amount),
            evaluate_currency(transaction, ledger_entry, weight=self.weights.currency),
            evaluate_timestamp(
                transaction,
                ledger_entry,
                tolerance_seconds=self.timestamp_tolerance_seconds,
                weight=self.weights.timestamp,
            ),
            evaluate_direction(transaction, ledger_entry, weight=self.weights.direction),
        ]

        # Aggregate fields, score, and reasons
        total_score = 0.0
        matched_fields: List[str] = []
        mismatched_fields: List[str] = []
        mismatch_reasons: List[str] = []
        evidence_list = []

        for ev in evaluations:
            total_score += ev.score_contribution
            if ev.matched:
                matched_fields.append(ev.field_name)
            else:
                mismatched_fields.append(ev.field_name)
                if ev.mismatch_reason:
                    mismatch_reasons.append(ev.mismatch_reason)
            evidence_list.append(ev.evidence)

        # Normalize total score within [0.0, 1.0]
        final_score = min(1.0, max(0.0, round(total_score, 4)))

        # ----------------------------------------------------------------------
        # Classify Status Based on Deterministic Thresholds
        # ----------------------------------------------------------------------
        if len(mismatched_fields) == 0 and final_score >= self.match_threshold:
            status = ReconciliationStatus.MATCHED.value
        elif final_score >= self.partial_match_threshold:
            status = ReconciliationStatus.PARTIAL_MATCH.value
        else:
            status = ReconciliationStatus.MISMATCHED.value

        return ReconciliationResult(
            reconciliation_id=rec_id,
            transaction_id=transaction.transaction_id,
            ledger_entry_id=ledger_entry.ledger_entry_id,
            correlation_id=correlation_id,
            status=status,
            score=final_score,
            matched_fields=matched_fields,
            mismatched_fields=mismatched_fields,
            mismatch_reasons=mismatch_reasons,
            evidence=evidence_list,
        )
