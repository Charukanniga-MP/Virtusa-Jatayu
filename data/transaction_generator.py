"""Synthetic Transaction Generator for ZT-LedgerGuard.

Produces realistic, validated synthetic banking transactions for streaming ingestion,
testing, and benchmark simulations without exposing real customer information.
Fully supports deterministic seeded pseudo-random generation for reproducible test suites.
"""

import random
import time
from datetime import datetime, timezone, timedelta
from typing import Iterator, List, Optional
from reconciliation.models import Transaction


class TransactionGenerator:
    """Deterministic, streaming-capable synthetic banking transaction generator."""

    DEFAULT_TRANSACTION_TYPES: List[str] = [
        "TRANSFER",
        "PAYMENT",
        "WITHDRAWAL",
        "DEPOSIT",
        "CARD_PAYMENT",
        "INTERNATIONAL_TRANSFER",
    ]

    DEFAULT_CURRENCIES: List[str] = ["USD", "EUR", "GBP", "INR"]

    DEFAULT_SOURCE_SYSTEMS: List[str] = [
        "MOBILE_APP",
        "WEB_PORTAL",
        "ATM_NETWORK",
        "POS_TERMINAL",
        "CORE_BANKING",
        "SWIFT_GATEWAY",
    ]

    DEFAULT_DESTINATION_SYSTEMS: List[str] = [
        "ACH_NETWORK",
        "FEDWIRE",
        "SEPA",
        "INTERNAL_LEDGER",
        "CARD_RAILS",
    ]

    def __init__(
        self,
        seed: Optional[int] = None,
        start_tx_id: int = 1,
        account_pool_size: int = 50,
        base_timestamp: Optional[datetime] = None,
        auto_sequence: bool = False,
        db_path: Optional[str] = None,
    ) -> None:
        """Initialize the transaction generator.

        Args:
            seed: Optional integer seed for deterministic pseudo-random sequences.
            start_tx_id: Starting integer for sequential transaction IDs.
            account_pool_size: Number of synthetic accounts to cycle between (ACC-100001...).
            base_timestamp: Starting base timestamp for generated transactions.
            auto_sequence: If True, resumes sequence from the highest existing ID in DB.
            db_path: Target database path when auto_sequence is True.
        """
        self.seed = seed
        self.rng = random.Random(seed)
        self.account_pool_size = max(1, account_pool_size)
        self.base_timestamp = base_timestamp or datetime.now(timezone.utc)
        self._elapsed_seconds = 0

        if auto_sequence:
            from database.database import get_max_transaction_counter
            max_existing = get_max_transaction_counter(db_path)
            self.current_tx_counter = max(start_tx_id, max_existing + 1)
        else:
            self.current_tx_counter = start_tx_id

        # Pre-generate synthetic account pool
        self.account_pool = [
            f"ACC-{100000 + i}" for i in range(1, self.account_pool_size + 1)
        ]


    def reset(self, seed: Optional[int] = None) -> None:
        """Reset internal generator state to its initial seed."""
        target_seed = seed if seed is not None else self.seed
        self.rng = random.Random(target_seed)
        self.current_tx_counter = 1
        self._elapsed_seconds = 0

    def generate_transaction(self) -> Transaction:
        """Generate a single realistic synthetic transaction conforming to the Transaction schema."""
        tx_id_str = f"TXN-{self.current_tx_counter:06d}"
        ref_id_str = f"REF-{self.current_tx_counter:06d}"
        self.current_tx_counter += 1

        account_id = self.rng.choice(self.account_pool)
        tx_type = self.rng.choice(self.DEFAULT_TRANSACTION_TYPES)
        currency = self.rng.choice(self.DEFAULT_CURRENCIES)
        source_system = self.rng.choice(self.DEFAULT_SOURCE_SYSTEMS)
        destination_system = self.rng.choice(self.DEFAULT_DESTINATION_SYSTEMS)

        # Realistic amounts: weighted toward everyday banking transactions
        if tx_type == "CARD_PAYMENT":
            amount = round(self.rng.uniform(5.00, 350.00), 2)
        elif tx_type in ("WITHDRAWAL", "DEPOSIT"):
            amount = round(self.rng.uniform(20.00, 1500.00), 2)
        elif tx_type == "INTERNATIONAL_TRANSFER":
            amount = round(self.rng.uniform(500.00, 15000.00), 2)
        else:
            amount = round(self.rng.uniform(25.00, 5000.00), 2)

        # Increment simulated time slightly per transaction
        self._elapsed_seconds += self.rng.randint(1, 10)
        tx_timestamp = self.base_timestamp + timedelta(seconds=self._elapsed_seconds)

        return Transaction(
            transaction_id=tx_id_str,
            account_id=account_id,
            transaction_type=tx_type,
            amount=amount,
            currency=currency,
            transaction_timestamp=tx_timestamp,
            source_system=source_system,
            destination_system=destination_system,
            reference_id=ref_id_str,
        )

    def generate_transactions(
        self,
        count: int = 100,
        interval_seconds: float = 0.0,
    ) -> Iterator[Transaction]:
        """Yield synthetic transactions one-by-one as a stream iterator.

        Args:
            count: Total number of transactions to produce.
            interval_seconds: Optional sleep interval to simulate real-time streaming delay.

        Yields:
            Validated Transaction instances.
        """
        for _ in range(count):
            tx = self.generate_transaction()
            if interval_seconds > 0.0:
                time.sleep(interval_seconds)
            yield tx

    def generate_fraud_scenario(
        self, scenario_type: str, account_id: Optional[str] = None
    ) -> List[Transaction]:
        """Generate transactions tailored to trigger specific fraud behavioral patterns.

        Supported scenarios:
            'normal': Single typical everyday transaction (LOW risk).
            'amount_anomaly': Transaction with exceptionally high amount (MEDIUM/HIGH risk).
            'velocity_burst': High frequency burst of transactions within 60s (HIGH/CRITICAL).
            'rapid_repeated': Rapid identical amount transactions within repeat window.
            'unusual_time': Transaction occurring in the middle of the night (03:00 AM).
            'multi_indicator': Extreme anomaly combining high amount, off-hours, and high velocity.
        """
        acc = account_id or self.account_pool[0]
        base_time = datetime.now(timezone.utc)

        if scenario_type == "normal":
            return [
                Transaction(
                    transaction_id=f"TXN-NORM-{self.rng.randint(1000, 9999)}",
                    account_id=acc,
                    transaction_type="PAYMENT",
                    amount=150.00,
                    currency="USD",
                    transaction_timestamp=base_time.replace(hour=14, minute=30),
                    source_system="WEB_PORTAL",
                    destination_system="INTERNAL_LEDGER",
                    reference_id=f"REF-NORM-{self.rng.randint(1000, 9999)}",
                )
            ]

        if scenario_type == "amount_anomaly":
            return [
                Transaction(
                    transaction_id=f"TXN-ANOM-{self.rng.randint(1000, 9999)}",
                    account_id=acc,
                    transaction_type="TRANSFER",
                    amount=125000.00,  # 25x normal baseline
                    currency="USD",
                    transaction_timestamp=base_time.replace(hour=14, minute=30),
                    source_system="ONLINE_BANKING",
                    destination_system="ACH_NETWORK",
                    reference_id=f"REF-ANOM-{self.rng.randint(1000, 9999)}",
                )
            ]

        if scenario_type == "velocity_burst":
            txs = []
            burst_start = base_time.replace(hour=14, minute=30)
            for i in range(8):
                txs.append(
                    Transaction(
                        transaction_id=f"TXN-VEL-{i:03d}-{self.rng.randint(1000, 9999)}",
                        account_id=acc,
                        transaction_type="CARD_PAYMENT",
                        amount=round(self.rng.uniform(20.0, 100.0), 2),
                        currency="USD",
                        transaction_timestamp=burst_start + timedelta(seconds=i * 5),
                        source_system="POS_TERMINAL",
                        destination_system="CARD_RAILS",
                        reference_id=f"REF-VEL-{i:03d}",
                    )
                )
            return txs

        if scenario_type == "rapid_repeated":
            txs = []
            rep_start = base_time.replace(hour=14, minute=30)
            for i in range(4):
                txs.append(
                    Transaction(
                        transaction_id=f"TXN-REP-{i:03d}-{self.rng.randint(1000, 9999)}",
                        account_id=acc,
                        transaction_type="TRANSFER",
                        amount=5000.00,
                        currency="USD",
                        transaction_timestamp=rep_start + timedelta(seconds=i * 15),
                        source_system="MOBILE_APP",
                        destination_system="SEPA",
                        reference_id=f"REF-REP-{i:03d}",
                    )
                )
            return txs

        if scenario_type == "unusual_time":
            return [
                Transaction(
                    transaction_id=f"TXN-TIME-{self.rng.randint(1000, 9999)}",
                    account_id=acc,
                    transaction_type="WITHDRAWAL",
                    amount=200.00,
                    currency="USD",
                    transaction_timestamp=base_time.replace(hour=3, minute=15, second=0),
                    source_system="ATM_NETWORK",
                    destination_system="INTERNAL_LEDGER",
                    reference_id=f"REF-TIME-{self.rng.randint(1000, 9999)}",
                )
            ]

        if scenario_type == "multi_indicator":
            txs = []
            night_time = base_time.replace(hour=2, minute=45, second=0)
            # Burst of huge amount transactions at night
            for i in range(5):
                txs.append(
                    Transaction(
                        transaction_id=f"TXN-MULTI-{i:03d}-{self.rng.randint(1000, 9999)}",
                        account_id=acc,
                        transaction_type="INTERNATIONAL_TRANSFER",
                        amount=95000.00,
                        currency="EUR",  # Deviates from USD baseline
                        transaction_timestamp=night_time + timedelta(seconds=i * 10),
                        source_system="SWIFT_GATEWAY",
                        destination_system="FEDWIRE",
                        reference_id=f"REF-MULTI-{i:03d}",
                    )
                )
            return txs

        return [self.generate_transaction()]
