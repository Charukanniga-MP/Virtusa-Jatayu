"""Deterministic Financial Reconciliation Rules.

Implements rule evaluations for:
1. Transaction ID & Reference ID correlation
2. Account identifier equivalence
3. Exact monetary amount equality using Decimal precision (no float equality)
4. Currency code matching (ISO 4217)
5. Timestamp window tolerance (configurable)
6. Accounting debit/credit direction consistency

Each rule generates explainable, deterministic evidence without requiring LLMs or heuristic ML.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import os
from typing import Any, Dict, Optional, Tuple

from reconciliation.models import LedgerEntry, Transaction


@dataclass(frozen=True)
class ReconciliationWeights:
    """Configurable scoring weights for individual reconciliation dimensions. Sums to 1.0."""

    id_reference: float = 0.30
    account: float = 0.20
    amount: float = 0.25
    currency: float = 0.10
    timestamp: float = 0.10
    direction: float = 0.05


@dataclass
class RuleEvaluation:
    """Outcome of evaluating a single deterministic reconciliation rule."""

    field_name: str
    matched: bool
    score_contribution: float
    max_weight: float
    evidence: Dict[str, Any]
    mismatch_reason: Optional[str] = None


# ------------------------------------------------------------------------------
# Monetary Helper: Decimal Precision
# ------------------------------------------------------------------------------
def to_decimal(value: Any) -> Decimal:
    """Convert amount to Decimal rounded to 2 decimal places using bankers' rounding."""
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# ------------------------------------------------------------------------------
# Rule 1 & 2: Identifier and Reference Matching
# ------------------------------------------------------------------------------
def evaluate_id_reference(
    transaction: Transaction,
    ledger_entry: LedgerEntry,
    weight: float = 0.30,
) -> RuleEvaluation:
    """Evaluate direct Transaction ID and External Reference ID alignment."""
    tx_id_match = transaction.transaction_id == ledger_entry.transaction_id
    ref_match = bool(
        transaction.reference_id
        and ledger_entry.reference_id
        and transaction.reference_id == ledger_entry.reference_id
    )

    matched = tx_id_match or ref_match
    score = weight if matched else 0.0

    evidence = {
        "field": "transaction_id",
        "transaction_id": transaction.transaction_id,
        "ledger_transaction_id": ledger_entry.transaction_id,
        "transaction_ref": transaction.reference_id,
        "ledger_ref": ledger_entry.reference_id,
        "tx_id_match": tx_id_match,
        "reference_match": ref_match,
        "match": matched,
        "message": (
            f"Transaction ID / Reference matched: tx_id={tx_id_match}, ref={ref_match}"
            if matched
            else f"Transaction ID mismatch: '{transaction.transaction_id}' != '{ledger_entry.transaction_id}'"
        ),
    }

    reason = None if matched else "Transaction ID and Reference ID mismatch"
    return RuleEvaluation(
        field_name="transaction_id",
        matched=matched,
        score_contribution=score,
        max_weight=weight,
        evidence=evidence,
        mismatch_reason=reason,
    )


# ------------------------------------------------------------------------------
# Rule 3: Account Identifier Matching
# ------------------------------------------------------------------------------
def evaluate_account_id(
    transaction: Transaction,
    ledger_entry: LedgerEntry,
    weight: float = 0.20,
) -> RuleEvaluation:
    """Evaluate whether account identifiers match identically."""
    matched = transaction.account_id == ledger_entry.account_id
    score = weight if matched else 0.0

    evidence = {
        "field": "account_id",
        "transaction_account": transaction.account_id,
        "ledger_account": ledger_entry.account_id,
        "match": matched,
        "message": (
            f"Account ID match: {transaction.account_id}"
            if matched
            else f"Account mismatch: Transaction account '{transaction.account_id}' != Ledger account '{ledger_entry.account_id}'"
        ),
    }

    reason = None if matched else "Account mismatch"
    return RuleEvaluation(
        field_name="account_id",
        matched=matched,
        score_contribution=score,
        max_weight=weight,
        evidence=evidence,
        mismatch_reason=reason,
    )


# ------------------------------------------------------------------------------
# Rule 4: Monetary Amount Equality (Decimal Precision)
# ------------------------------------------------------------------------------
def evaluate_amount(
    transaction: Transaction,
    ledger_entry: LedgerEntry,
    weight: float = 0.25,
) -> RuleEvaluation:
    """Evaluate exact monetary amount matching using Decimal arithmetic to prevent floating-point drift."""
    tx_dec = to_decimal(transaction.amount)
    ledger_dec = to_decimal(ledger_entry.amount)
    diff = abs(tx_dec - ledger_dec)

    matched = (tx_dec == ledger_dec)
    score = weight if matched else 0.0

    evidence = {
        "field": "amount",
        "transaction_amount": str(tx_dec),
        "ledger_amount": str(ledger_dec),
        "difference": str(diff),
        "match": matched,
        "message": (
            f"Amount match: {tx_dec} {transaction.currency} == {ledger_dec} {ledger_entry.currency}"
            if matched
            else f"Amount mismatch: Transaction={tx_dec} vs Ledger={ledger_dec} (difference: {diff} {transaction.currency})"
        ),
    }

    reason = None if matched else "Amount mismatch"
    return RuleEvaluation(
        field_name="amount",
        matched=matched,
        score_contribution=score,
        max_weight=weight,
        evidence=evidence,
        mismatch_reason=reason,
    )


# ------------------------------------------------------------------------------
# Rule 5: Currency Code Matching (ISO 4217)
# ------------------------------------------------------------------------------
def evaluate_currency(
    transaction: Transaction,
    ledger_entry: LedgerEntry,
    weight: float = 0.10,
) -> RuleEvaluation:
    """Evaluate ISO 4217 currency match."""
    tx_curr = transaction.currency.upper().strip()
    ledger_curr = ledger_entry.currency.upper().strip()
    matched = tx_curr == ledger_curr
    score = weight if matched else 0.0

    evidence = {
        "field": "currency",
        "transaction_currency": tx_curr,
        "ledger_currency": ledger_curr,
        "match": matched,
        "message": (
            f"Currency match: {tx_curr}"
            if matched
            else f"Currency mismatch: Transaction '{tx_curr}' != Ledger '{ledger_curr}'"
        ),
    }

    reason = None if matched else "Currency mismatch"
    return RuleEvaluation(
        field_name="currency",
        matched=matched,
        score_contribution=score,
        max_weight=weight,
        evidence=evidence,
        mismatch_reason=reason,
    )


# ------------------------------------------------------------------------------
# Rule 6: Timestamp Tolerance
# ------------------------------------------------------------------------------
def evaluate_timestamp(
    transaction: Transaction,
    ledger_entry: LedgerEntry,
    tolerance_seconds: Optional[int] = None,
    weight: float = 0.10,
) -> RuleEvaluation:
    """Evaluate timestamp proximity within configurable tolerance seconds (default 300s)."""
    if tolerance_seconds is None:
        tolerance_seconds = int(os.getenv("TIMESTAMP_TOLERANCE_SECONDS", "300"))

    # Ensure tz-aware comparisons
    t1 = transaction.transaction_timestamp
    if t1.tzinfo is None:
        t1 = t1.replace(tzinfo=timezone.utc)

    t2 = ledger_entry.timestamp
    if t2.tzinfo is None:
        t2 = t2.replace(tzinfo=timezone.utc)

    delta_seconds = abs((t1 - t2).total_seconds())
    matched = delta_seconds <= tolerance_seconds
    score = weight if matched else 0.0

    evidence = {
        "field": "timestamp",
        "transaction_timestamp": t1.isoformat(),
        "ledger_timestamp": t2.isoformat(),
        "delta_seconds": round(delta_seconds, 2),
        "tolerance_seconds": tolerance_seconds,
        "match": matched,
        "message": (
            f"Timestamp within tolerance: delta {delta_seconds:.1f}s <= {tolerance_seconds}s"
            if matched
            else f"Timestamp mismatch: delta {delta_seconds:.1f}s exceeds tolerance {tolerance_seconds}s"
        ),
    }

    reason = None if matched else "Timestamp mismatch"
    return RuleEvaluation(
        field_name="timestamp",
        matched=matched,
        score_contribution=score,
        max_weight=weight,
        evidence=evidence,
        mismatch_reason=reason,
    )


# ------------------------------------------------------------------------------
# Rule 7: Debit/Credit Accounting Direction
# ------------------------------------------------------------------------------
def evaluate_direction(
    transaction: Transaction,
    ledger_entry: LedgerEntry,
    weight: float = 0.05,
) -> RuleEvaluation:
    """Evaluate accounting debit/credit compatibility with transaction type.

    Assumptions:
    - In double-entry banking:
      - DEPOSIT -> Customer account is CREDITED (liability increases from bank perspective)
      - WITHDRAWAL, PAYMENT, CARD_PAYMENT, TRANSFER, INTERNATIONAL_TRANSFER -> Customer account is DEBITED
    """
    tx_type = transaction.transaction_type.upper().strip()
    side = ledger_entry.debit_credit.upper().strip()

    credit_types = {"DEPOSIT", "REFUND", "CREDIT_ADJUSTMENT"}
    debit_types = {
        "WITHDRAWAL",
        "PAYMENT",
        "CARD_PAYMENT",
        "TRANSFER",
        "INTERNATIONAL_TRANSFER",
        "FEE",
    }

    if tx_type in credit_types:
        matched = (side == "CREDIT")
    elif tx_type in debit_types:
        matched = (side == "DEBIT")
    else:
        # Fallback for generic custom types: accept if side is valid
        matched = side in {"DEBIT", "CREDIT"}

    score = weight if matched else 0.0

    evidence = {
        "field": "direction",
        "transaction_type": tx_type,
        "ledger_debit_credit": side,
        "match": matched,
        "message": (
            f"Accounting direction valid: {tx_type} -> {side}"
            if matched
            else f"Direction mismatch: {tx_type} incompatible with {side}"
        ),
    }

    reason = None if matched else "Direction mismatch"
    return RuleEvaluation(
        field_name="direction",
        matched=matched,
        score_contribution=score,
        max_weight=weight,
        evidence=evidence,
        mismatch_reason=reason,
    )
