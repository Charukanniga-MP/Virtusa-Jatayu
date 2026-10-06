"""Synthetic Ledger Entry Generator.

Produces realistic, deterministic ledger records corresponding to banking transactions
under controlled test scenarios:
- Exact Match (100% field alignment)
- Partial Match (minor delta, e.g. timestamp latency or reference difference)
- Amount Mismatch (fee discrepancy or posting difference)
- Currency Mismatch (conversion rail mismatch)
- Account Mismatch (misrouted general ledger posting)
- Timestamp Mismatch (settlement outside tolerance window)
- Direction Mismatch (inverted debit/credit posting)
- Missing Entry (unposted or lost ledger record)
"""

from datetime import datetime, timedelta, timezone
import random
from typing import Dict, List, Optional, Tuple

from reconciliation.models import LedgerEntry, Transaction


class LedgerGenerator:
    """Deterministic generator for synthetic core ledger accounting entries."""

    def __init__(self, seed: Optional[int] = None) -> None:
        """Initialize generator with an optional pseudorandom seed."""
        self.seed = seed
        self._rng = random.Random(seed)
        self._counter = 1000

    def generate_entry_for_transaction(
        self,
        transaction: Transaction,
        scenario: str = "exact",
        entry_id: Optional[str] = None,
    ) -> Optional[LedgerEntry]:
        """Generate a corresponding LedgerEntry under a specific test scenario.

        Args:
            transaction: Base Transaction to mirror or alter.
            scenario: One of 'exact', 'amount_mismatch', 'currency_mismatch',
                      'account_mismatch', 'timestamp_mismatch', 'direction_mismatch',
                      'reference_mismatch', or 'missing'.
            entry_id: Explicit ledger record identifier.

        Returns:
            LedgerEntry instance, or None if scenario is 'missing'.
        """
        scenario = scenario.lower().strip()
        if scenario == "missing":
            return None

        self._counter += 1
        ledger_id = entry_id or f"LEDGER-{self._counter:06d}"

        # Baseline: Exact Match Values
        tx_id = transaction.transaction_id
        acc_id = transaction.account_id
        amt = float(transaction.amount)
        curr = transaction.currency
        ref_id = transaction.reference_id or f"REF-{self._counter:06d}"
        source_system = "GENERAL_LEDGER_CORE"

        # Determine conventional debit/credit direction
        tx_type = transaction.transaction_type.upper()
        if tx_type in {"DEPOSIT", "REFUND", "CREDIT_ADJUSTMENT"}:
            side = "CREDIT"
        else:
            side = "DEBIT"

        # Baseline timestamp: posted 30 seconds after transaction
        base_ts = transaction.transaction_timestamp
        if base_ts.tzinfo is None:
            base_ts = base_ts.replace(tzinfo=timezone.utc)
        ts = base_ts + timedelta(seconds=30)

        # Apply Scenario Variations
        if scenario == "amount_mismatch":
            # Amount differs by $50.00
            amt = round(amt + 50.00, 2)
        elif scenario == "currency_mismatch":
            curr = "EUR" if curr != "EUR" else "USD"
        elif scenario == "account_mismatch":
            acc_id = f"ACC-{self._rng.randint(800000, 899999)}"
        elif scenario == "timestamp_mismatch":
            # 2 hours later (7200s, well outside 300s tolerance)
            ts = base_ts + timedelta(hours=2)
        elif scenario == "direction_mismatch":
            side = "DEBIT" if side == "CREDIT" else "CREDIT"
        elif scenario == "reference_mismatch":
            ref_id = f"REF-MISMATCH-{self._counter:06d}"
        elif scenario in {"severe_mismatch", "mismatched"}:
            amt = round(amt + 500.00, 2)
            acc_id = f"ACC-{self._rng.randint(800000, 899999)}"
            curr = "EUR" if curr != "EUR" else "GBP"
            side = "DEBIT" if side == "CREDIT" else "CREDIT"

        return LedgerEntry(
            ledger_entry_id=ledger_id,
            transaction_id=tx_id,
            account_id=acc_id,
            amount=amt,
            currency=curr,
            debit_credit=side,
            reference_id=ref_id,
            timestamp=ts,
            source_system=source_system,
        )

    def generate_controlled_dataset(
        self,
        transactions: List[Transaction],
        exact_ratio: float = 0.70,
        partial_ratio: float = 0.15,
        mismatch_ratio: float = 0.10,
        missing_ratio: float = 0.05,
    ) -> List[Tuple[Transaction, Optional[LedgerEntry], str]]:
        """Generate a realistic, controlled distribution of ledger entries for a list of transactions."""
        results = []
        for tx in transactions:
            roll = self._rng.random()
            if roll < exact_ratio:
                scenario = "exact"
            elif roll < exact_ratio + partial_ratio:
                scenario = self._rng.choice(["timestamp_mismatch", "reference_mismatch"])
            elif roll < exact_ratio + partial_ratio + mismatch_ratio:
                scenario = self._rng.choice(["severe_mismatch", "mismatched"])
            else:
                scenario = "missing"

            entry = self.generate_entry_for_transaction(tx, scenario=scenario)
            results.append((tx, entry, scenario))
        return results
