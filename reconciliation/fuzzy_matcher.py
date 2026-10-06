"""Fuzzy Ledger Candidate Matcher.

Provides deterministic, explainable candidate ranking and field-level similarity
calculation between transactions and ledger entries.
Strictly adheres to financial safety principles:
- Candidate discovery only; never directly modifies financial values or approves transactions.
- Enforces non-negotiable hard constraints (Account & Currency mismatch rejection).
- Detects ambiguous candidates within a safety margin.
- Zero LLMs, zero ML models, 100% deterministic and explainable.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import difflib
import logging
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from reconciliation.fuzzy_result import (
    CandidateDiscoveryResult,
    CandidateSelectionStatus,
    ConfidenceLevel,
    FuzzyCandidate,
)
from reconciliation.models import LedgerEntry, Transaction
from reconciliation.normalization import (
    normalize_account_id,
    normalize_currency,
    normalize_reference,
    normalize_transaction_id,
)
from reconciliation.rules import to_decimal
from security.security_utils import mask_account_id

logger = logging.getLogger("reconciliation.fuzzy_matcher")


class FuzzyMatchWeights(BaseModel):
    """Configurable scoring weights for candidate similarity ranking (sum = 1.0)."""

    reference: float = Field(default=0.30, ge=0.0, le=1.0)
    account: float = Field(default=0.25, ge=0.0, le=1.0)
    amount: float = Field(default=0.25, ge=0.0, le=1.0)
    timestamp: float = Field(default=0.15, ge=0.0, le=1.0)
    currency: float = Field(default=0.05, ge=0.0, le=1.0)


class FuzzyCandidateMatcher:
    """Stateless, deterministic fuzzy candidate matcher and ranker."""

    def __init__(
        self,
        weights: Optional[FuzzyMatchWeights] = None,
        high_threshold: float = 0.90,
        medium_threshold: float = 0.75,
        low_threshold: float = 0.50,
        ambiguity_margin: float = 0.03,
        amount_tolerance: float = 0.01,
        fuzzy_timestamp_window_seconds: int = 900,
        top_k: int = 5,
        enforce_hard_constraints: bool = True,
    ) -> None:
        """Initialize matcher with configurable scoring rules, tolerances, and thresholds."""
        self.weights = weights or FuzzyMatchWeights()
        self.high_threshold = high_threshold
        self.medium_threshold = medium_threshold
        self.low_threshold = low_threshold
        self.ambiguity_margin = ambiguity_margin
        self.amount_tolerance = Decimal(str(amount_tolerance))
        self.fuzzy_timestamp_window_seconds = fuzzy_timestamp_window_seconds
        self.top_k = top_k
        self.enforce_hard_constraints = enforce_hard_constraints

    def calculate_candidate_similarity(
        self,
        transaction: Transaction,
        ledger_entry: LedgerEntry,
    ) -> FuzzyCandidate:
        """Compute field-level similarities, aggregate score, and evidence for a candidate."""
        evidence: List[Dict[str, Any]] = []
        matched_fields: List[str] = []
        mismatched_fields: List[str] = []
        reasons: List[str] = []
        field_scores: Dict[str, float] = {}

        # ----------------------------------------------------------------------
        # 1. Hard Constraints Evaluation (Account & Currency)
        # ----------------------------------------------------------------------
        norm_tx_acc = normalize_account_id(transaction.account_id)
        norm_ledger_acc = normalize_account_id(ledger_entry.account_id)
        acc_match = (norm_tx_acc == norm_ledger_acc) and bool(norm_tx_acc)

        norm_tx_curr = normalize_currency(transaction.currency)
        norm_ledger_curr = normalize_currency(ledger_entry.currency)
        curr_match = (norm_tx_curr == norm_ledger_curr) and bool(norm_tx_curr)

        masked_tx_acc = mask_account_id(transaction.account_id)
        masked_ledger_acc = mask_account_id(ledger_entry.account_id)

        if self.enforce_hard_constraints:
            hard_constraint_violated = False

            if not acc_match:
                hard_constraint_violated = True
                mismatched_fields.append("account_id")
                msg = f"Hard constraint violation: Account mismatch ({masked_tx_acc} vs {masked_ledger_acc})"
                reasons.append(msg)
                evidence.append({
                    "field": "account_id",
                    "match": False,
                    "score": 0.0,
                    "message": msg,
                })

            if not curr_match:
                hard_constraint_violated = True
                mismatched_fields.append("currency")
                msg = f"Hard constraint violation: Currency mismatch ({norm_tx_curr} != {norm_ledger_curr})"
                reasons.append(msg)
                evidence.append({
                    "field": "currency",
                    "match": False,
                    "score": 0.0,
                    "message": msg,
                })

            if hard_constraint_violated:
                field_scores["account_id"] = 1.0 if acc_match else 0.0
                field_scores["currency"] = 1.0 if curr_match else 0.0
                return FuzzyCandidate(
                    ledger_entry_id=ledger_entry.ledger_entry_id,
                    transaction_id=transaction.transaction_id,
                    candidate_score=0.0,
                    confidence_level=ConfidenceLevel.REJECTED.value,
                    field_scores=field_scores,
                    matched_fields=matched_fields,
                    mismatched_fields=mismatched_fields,
                    reasons=reasons,
                    evidence=evidence,
                )

        # ----------------------------------------------------------------------
        # 2. Account ID Score (Soft evaluation if hard constraints disabled/passed)
        # ----------------------------------------------------------------------
        if acc_match:
            field_scores["account_id"] = 1.0
            matched_fields.append("account_id")
            evidence.append({
                "field": "account_id",
                "match": True,
                "score": 1.0,
                "message": f"Account match: {masked_tx_acc}",
            })
        else:
            field_scores["account_id"] = 0.0
            mismatched_fields.append("account_id")
            evidence.append({
                "field": "account_id",
                "match": False,
                "score": 0.0,
                "message": f"Account mismatch: {masked_tx_acc} != {masked_ledger_acc}",
            })

        # ----------------------------------------------------------------------
        # 3. Currency Match Score
        # ----------------------------------------------------------------------
        if curr_match:
            field_scores["currency"] = 1.0
            matched_fields.append("currency")
            evidence.append({
                "field": "currency",
                "match": True,
                "score": 1.0,
                "message": f"Currency match: {norm_tx_curr}",
            })
        else:
            field_scores["currency"] = 0.0
            mismatched_fields.append("currency")
            evidence.append({
                "field": "currency",
                "match": False,
                "score": 0.0,
                "message": f"Currency mismatch: {norm_tx_curr} != {norm_ledger_curr}",
            })

        # ----------------------------------------------------------------------
        # 4. Reference ID Similarity
        # ----------------------------------------------------------------------
        norm_tx_ref = normalize_reference(transaction.reference_id)
        norm_ledger_ref = normalize_reference(ledger_entry.reference_id)

        if norm_tx_ref and norm_ledger_ref:
            if norm_tx_ref == norm_ledger_ref:
                ref_sim = 1.0
            else:
                ref_sim = difflib.SequenceMatcher(None, norm_tx_ref, norm_ledger_ref).ratio()
        elif not norm_tx_ref and not norm_ledger_ref:
            ref_sim = 0.5  # Neutral baseline when reference absent on both
        else:
            ref_sim = 0.0

        field_scores["reference_id"] = round(ref_sim, 4)
        if ref_sim >= 0.80:
            matched_fields.append("reference_id")
            evidence.append({
                "field": "reference_id",
                "match": True,
                "score": round(ref_sim, 4),
                "message": f"Reference similarity: {ref_sim:.2f} ('{transaction.reference_id}' ~ '{ledger_entry.reference_id}')",
            })
        else:
            mismatched_fields.append("reference_id")
            reasons.append(f"Low reference similarity: {ref_sim:.2f}")
            evidence.append({
                "field": "reference_id",
                "match": False,
                "score": round(ref_sim, 4),
                "message": f"Reference divergence: {ref_sim:.2f}",
            })

        # ----------------------------------------------------------------------
        # 5. Amount Similarity (Strict Decimal Arithmetic)
        # ----------------------------------------------------------------------
        tx_amt = to_decimal(transaction.amount)
        ledger_amt = to_decimal(ledger_entry.amount)
        diff = abs(tx_amt - ledger_amt)

        if diff == Decimal("0.00"):
            amt_score = 1.0
            matched_fields.append("amount")
            evidence.append({
                "field": "amount",
                "match": True,
                "score": 1.0,
                "difference": f"0.00 {norm_tx_curr}",
                "message": f"Exact amount match: {tx_amt} {norm_tx_curr}",
            })
        elif diff <= self.amount_tolerance:
            amt_score = 0.98
            matched_fields.append("amount")
            evidence.append({
                "field": "amount",
                "match": True,
                "score": 0.98,
                "difference": f"{diff} {norm_tx_curr}",
                "message": f"Amount within tolerance {self.amount_tolerance}: diff={diff} {norm_tx_curr}",
            })
        else:
            # Relative difference scaling
            rel_diff = diff / max(tx_amt, Decimal("0.01"))
            if diff <= Decimal("1.00"):
                amt_score = max(0.0, 1.0 - float(diff) * 0.1)
            elif rel_diff < Decimal("0.05"):
                amt_score = max(0.0, 1.0 - float(rel_diff) * 5.0)
            else:
                amt_score = 0.0

            if amt_score >= 0.70:
                matched_fields.append("amount")
            else:
                mismatched_fields.append("amount")
                reasons.append(f"Amount difference: tx={tx_amt}, ledger={ledger_amt} (diff: {diff})")

            evidence.append({
                "field": "amount",
                "match": amt_score >= 0.70,
                "score": round(amt_score, 4),
                "difference": f"{diff} {norm_tx_curr}",
                "message": f"Amount similarity {amt_score:.2f}: diff={diff} {norm_tx_curr}",
            })

        field_scores["amount"] = round(amt_score, 4)

        # ----------------------------------------------------------------------
        # 6. Timestamp Similarity
        # ----------------------------------------------------------------------
        tx_ts = transaction.transaction_timestamp
        ledger_ts = ledger_entry.timestamp

        if tx_ts.tzinfo is None:
            tx_ts = tx_ts.replace(tzinfo=timezone.utc)
        if ledger_ts.tzinfo is None:
            ledger_ts = ledger_ts.replace(tzinfo=timezone.utc)

        delta_sec = abs((tx_ts - ledger_ts).total_seconds())

        if delta_sec <= 0:
            ts_score = 1.0
        elif delta_sec <= self.fuzzy_timestamp_window_seconds:
            ts_score = max(0.0, 1.0 - (delta_sec / self.fuzzy_timestamp_window_seconds))
        else:
            ts_score = 0.0

        field_scores["timestamp"] = round(ts_score, 4)
        if ts_score >= 0.50:
            matched_fields.append("timestamp")
            evidence.append({
                "field": "timestamp",
                "match": True,
                "score": round(ts_score, 4),
                "delta_seconds": round(delta_sec, 1),
                "message": f"Timestamp within fuzzy window: delta={delta_sec:.1f}s (window={self.fuzzy_timestamp_window_seconds}s)",
            })
        else:
            mismatched_fields.append("timestamp")
            reasons.append(f"Timestamp outside fuzzy window: delta={delta_sec:.1f}s")
            evidence.append({
                "field": "timestamp",
                "match": False,
                "score": round(ts_score, 4),
                "delta_seconds": round(delta_sec, 1),
                "message": f"Timestamp delta {delta_sec:.1f}s exceeds window {self.fuzzy_timestamp_window_seconds}s",
            })

        # ----------------------------------------------------------------------
        # 7. Weighted Composite Score
        # ----------------------------------------------------------------------
        raw_score = (
            field_scores["reference_id"] * self.weights.reference
            + field_scores["account_id"] * self.weights.account
            + field_scores["amount"] * self.weights.amount
            + field_scores["timestamp"] * self.weights.timestamp
            + field_scores["currency"] * self.weights.currency
        )
        final_score = min(1.0, max(0.0, round(raw_score, 4)))

        # ----------------------------------------------------------------------
        # 8. Graded Confidence Classification
        # ----------------------------------------------------------------------
        if final_score >= self.high_threshold:
            confidence = ConfidenceLevel.HIGH.value
        elif final_score >= self.medium_threshold:
            confidence = ConfidenceLevel.MEDIUM.value
        elif final_score >= self.low_threshold:
            confidence = ConfidenceLevel.LOW.value
        else:
            confidence = ConfidenceLevel.REJECTED.value
            reasons.append(f"Score {final_score:.2f} below low confidence threshold ({self.low_threshold})")

        return FuzzyCandidate(
            ledger_entry_id=ledger_entry.ledger_entry_id,
            transaction_id=transaction.transaction_id,
            candidate_score=final_score,
            confidence_level=confidence,
            field_scores=field_scores,
            matched_fields=matched_fields,
            mismatched_fields=mismatched_fields,
            reasons=reasons,
            evidence=evidence,
        )

    def rank_candidates(
        self,
        transaction: Transaction,
        candidates: List[LedgerEntry],
    ) -> CandidateDiscoveryResult:
        """Evaluate and rank candidate ledger entries, identifying winners or ambiguities."""
        if not candidates:
            return CandidateDiscoveryResult(
                transaction_id=transaction.transaction_id,
                candidate_count=0,
                selection_status=CandidateSelectionStatus.NO_CANDIDATES.value,
                top_candidate=None,
                candidates=[],
                requires_manual_review=False,
                ambiguity_margin=self.ambiguity_margin,
                reasons=["No candidate ledger entries discovered in repository"],
            )

        # 1. Score each candidate
        scored: List[FuzzyCandidate] = [
            self.calculate_candidate_similarity(transaction, entry)
            for entry in candidates
        ]

        # 2. Sort descending by score
        scored.sort(key=lambda c: c.candidate_score, reverse=True)

        # 3. Restrict to top_k
        top_candidates = scored[: self.top_k]

        # Filter out hard-rejected candidates when checking for winners
        valid_candidates = [c for c in top_candidates if c.confidence_level != ConfidenceLevel.REJECTED.value]

        reasons: List[str] = []
        requires_manual_review = False
        top_candidate: Optional[FuzzyCandidate] = None

        if not valid_candidates:
            selection_status = CandidateSelectionStatus.REJECTED.value
            requires_manual_review = True
            reasons.append("All discovered candidates were rejected due to hard constraint violations or low score")
            top_candidate = top_candidates[0] if top_candidates else None
        elif len(valid_candidates) == 1:
            selection_status = CandidateSelectionStatus.SELECTED.value
            top_candidate = valid_candidates[0]
            if top_candidate.confidence_level == ConfidenceLevel.LOW.value:
                requires_manual_review = True
                reasons.append("Selected candidate has LOW confidence; manual compliance review recommended")
            else:
                reasons.append(f"Clear candidate identified: {top_candidate.ledger_entry_id} (confidence: {top_candidate.confidence_level})")
        else:
            # 4. Check for ambiguity between top two candidates
            top = valid_candidates[0]
            runner_up = valid_candidates[1]
            score_diff = top.candidate_score - runner_up.candidate_score

            if score_diff < self.ambiguity_margin:
                selection_status = CandidateSelectionStatus.AMBIGUOUS.value
                requires_manual_review = True
                top_candidate = top
                reasons.append(
                    f"Ambiguous candidates: Top entry '{top.ledger_entry_id}' (score={top.candidate_score:.3f}) "
                    f"and runner-up '{runner_up.ledger_entry_id}' (score={runner_up.candidate_score:.3f}) "
                    f"differ by {score_diff:.4f}, below ambiguity margin {self.ambiguity_margin}."
                )
            else:
                selection_status = CandidateSelectionStatus.SELECTED.value
                top_candidate = top
                if top_candidate.confidence_level == ConfidenceLevel.LOW.value:
                    requires_manual_review = True
                    reasons.append("Selected candidate has LOW confidence; manual compliance review recommended")
                else:
                    reasons.append(f"Clear candidate identified: {top.ledger_entry_id} (score={top.candidate_score:.3f})")

        return CandidateDiscoveryResult(
            transaction_id=transaction.transaction_id,
            candidate_count=len(candidates),
            selection_status=selection_status,
            top_candidate=top_candidate,
            candidates=top_candidates,
            requires_manual_review=requires_manual_review,
            ambiguity_margin=self.ambiguity_margin,
            reasons=reasons,
        )
