"""Legacy Banking System Simulator.

Simulates a traditional legacy core-banking host (e.g., COBOL/Mainframe OLTP batch engine).
Used on Day 1 for deterministic transaction validation and response generation.
This simulator provides the baseline host behavior for future Shadow Deployment (Day 9),
allowing parallel validation between legacy batch output and real-time AI agents.

NOTE:
This is an in-memory simulation designed for local developer workstations without
requiring external emulators or containerized mainframe mocks.
"""

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Union
from reconciliation.models import Transaction


class LegacyBankingSystem:
    """Deterministic legacy core-banking simulator."""

    LEGACY_MAX_SINGLE_AMOUNT = 1_000_000.0  # Legacy host per-transaction ceiling

    def __init__(self, host_id: str = "HOST-CORE-01") -> None:
        self.host_id = host_id

    def process_transaction(self, transaction: Union[Transaction, Dict[str, Any]]) -> Dict[str, Any]:
        """Process a transaction deterministically simulating legacy host behavior.

        Deterministic Rules:
        - Rejection if amount > LEGACY_MAX_SINGLE_AMOUNT (Reason: EXCEEDS_LEGACY_LIMIT, Code: 51)
        - Rejection if account_id begins with 'RESTRICTED' (Reason: ACCOUNT_RESTRICTED, Code: 05)
        - Otherwise: Approved (Status: PROCESSED, Code: 00)

        Args:
            transaction: Transaction instance or dictionary representation.

        Returns:
            Dictionary containing legacy processing response and host reference.
        """
        if isinstance(transaction, Transaction):
            tx_id = transaction.transaction_id
            account_id = transaction.account_id
            amount = transaction.amount
            currency = transaction.currency
        else:
            tx_id = str(transaction.get("transaction_id", ""))
            account_id = str(transaction.get("account_id", ""))
            amount = float(transaction.get("amount", 0.0))
            currency = str(transaction.get("currency", "USD"))

        timestamp = datetime.now(timezone.utc).isoformat()

        # Deterministic check 1: Restricted accounts
        if account_id.upper().startswith("RESTRICTED"):
            return {
                "host_id": self.host_id,
                "transaction_id": tx_id,
                "status": "REJECTED",
                "response_code": "05",
                "reason": "ACCOUNT_RESTRICTED",
                "authorization_code": None,
                "processed_at": timestamp,
            }

        # Deterministic check 2: Legacy limit ceiling
        if amount > self.LEGACY_MAX_SINGLE_AMOUNT:
            return {
                "host_id": self.host_id,
                "transaction_id": tx_id,
                "status": "REJECTED",
                "response_code": "51",
                "reason": "EXCEEDS_LEGACY_LIMIT",
                "authorization_code": None,
                "processed_at": timestamp,
            }

        # Deterministic authorization code generation based on transaction identity
        hash_digest = hashlib.sha256(f"{tx_id}:{account_id}:{amount}:{currency}".encode()).hexdigest()[:8].upper()
        auth_code = f"AUTH-{hash_digest}"

        return {
            "host_id": self.host_id,
            "transaction_id": tx_id,
            "status": "PROCESSED",
            "response_code": "00",
            "reason": "APPROVED",
            "authorization_code": auth_code,
            "processed_at": timestamp,
        }
