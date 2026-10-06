"""CLI Transaction Stream Simulation Utility.

Executes a local synthetic banking transaction stream:
- Generates realistic synthetic transactions
- Ingests and validates schemas
- Persists records to the SQLite database
- Publishes canonical events through LocalEventPublisher to the EventOrchestrator
- Displays a concise summary with zero-trust masked account information
"""

import argparse
import sys
from typing import Optional

# Ensure project root is in sys.path when script is executed directly
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.transaction_generator import TransactionGenerator
from database.database import init_db
from kafka.local_event_publisher import (
    EVENT_TYPE_TRANSACTION_RECEIVED,
    LocalEventPublisher,
)
from orchestrator.event_orchestrator import (
    EventOrchestrator,
    transaction_event_handler,
)
from reconciliation.transaction_service import TransactionService
from security.security_utils import mask_account_id


def run_stream(
    count: int = 20,
    seed: Optional[int] = None,
    interval_seconds: float = 0.0,
    verbose: bool = True,
) -> None:
    """Run transaction generation and event streaming pipeline."""
    # 1. Initialize DB and orchestration infrastructure
    init_db()
    orchestrator = EventOrchestrator()
    orchestrator.register_handler(EVENT_TYPE_TRANSACTION_RECEIVED, transaction_event_handler)

    publisher = LocalEventPublisher(orchestrator)
    service = TransactionService(publisher=publisher)
    generator = TransactionGenerator(seed=seed, auto_sequence=True)

    print("=" * 45)
    print("ZT-LedgerGuard Transaction Stream Generator")
    print("=" * 45)
    print(f"Target count: {count} | Seed: {seed or 'random'}")
    print("-" * 45)

    accepted = 0
    rejected = 0

    for tx in generator.generate_transactions(count=count, interval_seconds=interval_seconds):
        try:
            service.process_transaction(tx)
            accepted += 1
            if verbose:
                masked_acc = mask_account_id(tx.account_id)
                print(
                    f"[OK] [{tx.transaction_id}] {tx.transaction_type:<15} "
                    f"{tx.amount:>9.2f} {tx.currency} | Account: {masked_acc}"
                )
        except Exception as exc:
            rejected += 1
            if verbose:
                print(f"[REJECTED] [{tx.transaction_id}]: {exc}")


    metrics = orchestrator.get_metrics()

    print("\nZT-LedgerGuard Transaction Stream")
    print("---------------------------------")
    print(f"Generated: {count}")
    print(f"Accepted: {accepted}")
    print(f"Rejected: {rejected}")
    print(f"Events Published: {metrics['events_published']}")
    print(f"Events Handled: {metrics['events_handled']}")
    print("---------------------------------\n")


def main() -> None:
    """CLI argument parser entry point."""
    parser = argparse.ArgumentParser(
        description="Stream synthetic banking transactions through ZT-LedgerGuard."
    )
    parser.add_argument(
        "--count",
        "-c",
        type=int,
        default=20,
        help="Number of transactions to generate (default: 20)",
    )
    parser.add_argument(
        "--seed",
        "-s",
        type=int,
        default=None,
        help="Optional seed for deterministic generation",
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=float,
        default=0.0,
        help="Delay in seconds between generated transactions (default: 0.0)",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress individual per-transaction output lines",
    )

    args = parser.parse_args()

    if args.count < 1:
        print("Error: --count must be at least 1.")
        sys.exit(1)

    run_stream(
        count=args.count,
        seed=args.seed,
        interval_seconds=args.interval,
        verbose=not args.quiet,
    )


if __name__ == "__main__":
    main()
