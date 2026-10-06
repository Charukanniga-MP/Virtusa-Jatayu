"""Day 6 Performance Benchmark — 10,000-Record Deterministic Fraud Detection.

LOCAL DEVELOPMENT BENCHMARK:
Generates 10,000 synthetic transactions under realistic financial distributions:
- Normal transactions (~75%)
- Amount anomalies (~10%)
- Velocity bursts (~5%)
- Rapid repeated transactions (~5%)
- Unusual transaction time / off-hours (~5%)

Evaluates each transaction using the FraudDetectionAgent:
- Profile lookup / historical baseline comparison
- 7-feature deterministic extraction
- 7 independent fraud rules evaluation
- Anti-double-counting safeguards
- Bounded risk scoring (0-100)
- Risk level classification (LOW, MEDIUM, HIGH, CRITICAL)
- Explainable evidence generation
- Manual review recommendation

Measures:
- Total records processed
- Total elapsed time (seconds)
- Average analysis latency (milliseconds/transaction)
- Fraud evaluation throughput (transactions/second)
- Risk level breakdown (LOW, MEDIUM, HIGH, CRITICAL)
- Manual review recommendation count
- Average fraud risk score

Runs entirely locally: Zero Docker, Zero Kafka, Zero LLMs, Zero external network calls.
"""

from __future__ import annotations

import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.fraud_agent.fraud_agent import FraudDetectionAgent
from data.transaction_generator import TransactionGenerator
from fraud_detection.fraud_result import RiskLevel
from fraud_detection.profile import AccountProfile, AccountProfileStore
from reconciliation.models import Transaction
from reconciliation.reconciliation_result import ReconciliationResult, ReconciliationStatus


def generate_benchmark_dataset(
    record_count: int = 10000,
    seed: int = 202610,
) -> tuple[List[Transaction], Dict[str, List[Transaction]], Dict[str, ReconciliationResult], Dict[str, int], AccountProfileStore]:
    """Generates 10,000 transactions across multiple accounts with realistic fraud distributions."""
    rng = random.Random(seed)
    profile_store = AccountProfileStore()

    # Pre-generate 100 account profiles with baseline statistics
    account_ids = [f"ACC-BENCH-{i:04d}" for i in range(1, 101)]
    for acc in account_ids:
        avg_amt = Decimal(str(rng.randint(2000, 15000)))
        max_amt = avg_amt * Decimal("3")
        profile = AccountProfile(
            account_id=acc,
            transaction_count=rng.randint(50, 200),
            average_amount=avg_amt,
            max_amount=max_amt,
            min_amount=Decimal("100"),
            average_interval_seconds=3600.0,
            common_transaction_hours=[9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
            common_currencies=["INR", "USD"],
            common_transaction_directions=["TRANSFER", "PAYMENT"],
            last_updated=datetime.now(timezone.utc),
        )
        profile_store.set_profile(profile)

    base_time = datetime(2026, 10, 6, 12, 0, 0, tzinfo=timezone.utc)
    transactions: List[Transaction] = []
    prior_history_map: Dict[str, List[Transaction]] = {}
    recon_results: Dict[str, ReconciliationResult] = {}
    prior_failed_map: Dict[str, int] = {}

    for i in range(record_count):
        acc = rng.choice(account_ids)
        profile = profile_store.get_profile(acc)
        avg = profile.average_amount if profile else Decimal("5000")

        # Distribution: 70% Normal, 15% Medium, 10% High, 5% Critical
        roll = rng.random()
        tx_id = f"TXN-BENCH-{i+1:06d}"
        failed_count = 0

        if roll < 0.70:
            # Scenario A: Normal transaction (LOW)
            amt = float((avg * Decimal(str(rng.uniform(0.6, 1.4)))).quantize(Decimal("0.01")))
            tx_time = base_time + timedelta(seconds=i * 60)
            status_recon = ReconciliationStatus.MATCHED
            # Normal spaced prior history
            history = [
                Transaction(
                    transaction_id=f"PREV-A-{tx_id}-1",
                    account_id=acc,
                    amount=float(avg),
                    currency="INR",
                    transaction_type="TRANSFER",
                    transaction_timestamp=tx_time - timedelta(hours=2),
                    source_system="CHANNEL_GATEWAY",
                    destination_system="CORE_LEDGER",
                )
            ]
        elif roll < 0.85:
            # Scenario B: Amount Anomaly or Rapid Repeated (MEDIUM, score 25-40)
            if rng.random() < 0.6:
                # 15x Amount spike
                amt = float((avg * Decimal("15.0")).quantize(Decimal("0.01")))
                tx_time = base_time + timedelta(seconds=i * 60)
                status_recon = ReconciliationStatus.MATCHED
                history = []
            else:
                # Rapid repeated (same amount in 30s)
                amt = 9999.00
                tx_time = base_time + timedelta(seconds=i * 60)
                status_recon = ReconciliationStatus.MATCHED
                history = [
                    Transaction(
                        transaction_id=f"PREV-R-{tx_id}-{k}",
                        account_id=acc,
                        amount=9999.00,
                        currency="INR",
                        transaction_type="TRANSFER",
                        transaction_timestamp=tx_time - timedelta(seconds=15 * k),
                        source_system="CHANNEL_GATEWAY",
                        destination_system="CORE_LEDGER",
                    )
                    for k in range(1, 3)
                ]
        elif roll < 0.95:
            # Scenario C: High Risk (Score 50-74 -> HIGH)
            # Velocity burst + Reconciliation mismatch
            amt = float((avg * Decimal(str(rng.uniform(1.0, 2.0)))).quantize(Decimal("0.01")))
            tx_time = base_time + timedelta(seconds=i * 60)
            status_recon = ReconciliationStatus.MISMATCHED
            history = [
                Transaction(
                    transaction_id=f"PREV-V-{tx_id}-{k}",
                    account_id=acc,
                    amount=amt,
                    currency="INR",
                    transaction_type="TRANSFER",
                    transaction_timestamp=tx_time - timedelta(seconds=5 * k),
                    source_system="CHANNEL_GATEWAY",
                    destination_system="CORE_LEDGER",
                )
                for k in range(1, 7)
            ]
            failed_count = 2
        else:
            # Scenario D: Multi-indicator critical attack (Score >= 75 -> CRITICAL)
            # Amount anomaly (25) + Velocity burst (25) + Night hours (10) + Recon mismatch (10) + Failures (10)
            amt = float((avg * Decimal("20.0")).quantize(Decimal("0.01")))
            tx_time = base_time.replace(hour=3, minute=15) + timedelta(seconds=i)
            status_recon = ReconciliationStatus.MISMATCHED
            history = [
                Transaction(
                    transaction_id=f"PREV-C-{tx_id}-{k}",
                    account_id=acc,
                    amount=amt,
                    currency="INR",
                    transaction_type="TRANSFER",
                    transaction_timestamp=tx_time - timedelta(seconds=4 * k),
                    source_system="CHANNEL_GATEWAY",
                    destination_system="CORE_LEDGER",
                )
                for k in range(1, 7)
            ]
            failed_count = 4

        tx = Transaction(
            transaction_id=tx_id,
            account_id=acc,
            amount=amt,
            currency="INR",
            transaction_type="TRANSFER",
            transaction_timestamp=tx_time,
            source_system="CHANNEL_GATEWAY",
            destination_system="CORE_LEDGER",
            reference_id=f"REF-BENCH-{i+1:06d}",
        )
        transactions.append(tx)
        prior_history_map[tx_id] = history
        prior_failed_map[tx_id] = failed_count

        # Build reconciliation result
        recon_results[tx_id] = ReconciliationResult(
            transaction_id=tx_id,
            status=status_recon.value,
            score=0.95 if status_recon == ReconciliationStatus.MATCHED else 0.40,
            evidence=[{"reason": "Reconciliation outcome"}],
        )

    return transactions, prior_history_map, recon_results, prior_failed_map, profile_store


def run_benchmark(record_count: int = 10000) -> Dict[str, Any]:
    print("=" * 76)
    print("ZT-LedgerGuard Day 6: 10,000-Record Fraud Detection Agent Benchmark")
    print("LOCAL DEVELOPMENT BENCHMARK (Windows Native, Zero LLM / Zero External APIs)")
    print("=" * 76)
    print(f"[*] Generating {record_count:,} synthetic transactions & multi-scenario distributions...")

    transactions, prior_history_map, recon_map, prior_failed_map, profile_store = generate_benchmark_dataset(
        record_count=record_count
    )

    print("[*] Initializing FraudDetectionAgent...")
    agent = FraudDetectionAgent(profile_store=profile_store)

    low_count = 0
    medium_count = 0
    high_count = 0
    critical_count = 0
    manual_review_count = 0
    block_recommended_count = 0
    total_score = 0.0

    print(f"[*] Starting high-throughput deterministic fraud evaluation for {record_count:,} records...")
    benchmark_start = time.perf_counter()

    for tx in transactions:
        history = prior_history_map.get(tx.transaction_id, [])
        recon = recon_map.get(tx.transaction_id)
        failed_count = prior_failed_map.get(tx.transaction_id, 0)

        result = agent.analyze_transaction(
            transaction=tx,
            recent_transactions=history,
            reconciliation_result=recon,
            prior_failed_count=failed_count,
            correlation_id=f"CORR-BENCH6-{tx.transaction_id}",
        )

        total_score += result.fraud_score

        if result.risk_level == RiskLevel.LOW.value:
            low_count += 1
        elif result.risk_level == RiskLevel.MEDIUM.value:
            medium_count += 1
        elif result.risk_level == RiskLevel.HIGH.value:
            high_count += 1
        elif result.risk_level == RiskLevel.CRITICAL.value:
            critical_count += 1

        if result.requires_manual_review:
            manual_review_count += 1
        if result.block_recommended:
            block_recommended_count += 1

    total_time = time.perf_counter() - benchmark_start
    throughput = round(record_count / total_time, 2) if total_time > 0 else 0.0
    avg_latency_ms = round((total_time / record_count) * 1000, 4) if record_count > 0 else 0.0
    avg_score = round(total_score / record_count, 2) if record_count > 0 else 0.0

    print("-" * 76)
    print(f"Total Transactions Analyzed    : {record_count:,}")
    print(f"Total Elapsed Time             : {total_time:.4f} seconds")
    print(f"Average Analysis Latency       : {avg_latency_ms:.4f} ms / transaction")
    print(f"Fraud Detection Throughput     : {throughput:,.2f} transactions / second")
    print("-" * 76)
    print("Risk Classification Breakdown   :")
    print(f"  - LOW (Score 0-24)           : {low_count:,} ({low_count / record_count * 100:.1f}%)")
    print(f"  - MEDIUM (Score 25-49)       : {medium_count:,} ({medium_count / record_count * 100:.1f}%)")
    print(f"  - HIGH (Score 50-74)         : {high_count:,} ({high_count / record_count * 100:.1f}%)")
    print(f"  - CRITICAL (Score 75-100)    : {critical_count:,} ({critical_count / record_count * 100:.1f}%)")
    print("-" * 76)
    print(f"Manual Review Recommended      : {manual_review_count:,} ({manual_review_count / record_count * 100:.1f}%)")
    print(f"Block Recommended (Hold)       : {block_recommended_count:,} ({block_recommended_count / record_count * 100:.1f}%)")
    print(f"Average Fraud Risk Score       : {avg_score:.2f} / 100")
    print("=" * 76)
    print("[OK] Day 6 Fraud Detection Benchmark completed successfully.")

    return {
        "record_count": record_count,
        "total_elapsed_time_sec": round(total_time, 4),
        "avg_latency_ms": avg_latency_ms,
        "throughput_tx_per_sec": throughput,
        "low_count": low_count,
        "medium_count": medium_count,
        "high_count": high_count,
        "critical_count": critical_count,
        "manual_review_count": manual_review_count,
        "block_recommended_count": block_recommended_count,
        "average_fraud_score": avg_score,
    }


if __name__ == "__main__":
    run_benchmark(10000)
