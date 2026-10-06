"""Day 7 Performance Benchmark — 10,000-Record Multi-Agent Consensus Coordination.

LOCAL DEVELOPMENT BENCHMARK:
Evaluates 10,000 synthetic transactions through the full Day 7 Multi-Agent Coordination stack:
- ReconciliationAgent (Two-stage candidate discovery & verification)
- FraudDetectionAgent (7-feature deterministic risk grading)
- PolicyAgent (Governance and policy enforcement)
- DecisionAgent & ConsensusEngine (Precedence ranking: BLOCK > MANUAL_REVIEW > APPROVE)
- AgentCoordinator (Audit trail, disagreement detection, and SQLite persistence)

Measures:
- Total records processed
- Total elapsed time (seconds)
- Average analysis latency (milliseconds/transaction)
- Multi-agent throughput (transactions/second)
- Final disposition breakdown (APPROVE, MANUAL_REVIEW, BLOCK_RECOMMENDED)
- Average consensus confidence (0.0 - 1.0)
- Agent disagreement count
- Agent failure count

Runs entirely locally: Zero Docker, Zero Kafka, Zero LLMs, Zero external network calls.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
import random
import sys
import tempfile
import time
from typing import Dict, List, Tuple

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.agent_coordinator import AgentCoordinator
from agents.decision_agent.decision_agent import DecisionAgent, DecisionType
from agents.fraud_agent.fraud_agent import FraudDetectionAgent
from agents.policy_agent.policy_agent import PolicyAgent
from agents.reconciliation_agent.reconciliation_agent import ReconciliationAgent
from database.database import (
    get_connection,
    init_db,
    insert_ledger_entry,
    insert_transaction,
)
from fraud_detection.profile import AccountProfile, AccountProfileStore
from reconciliation.ledger_repository import LedgerRepository
from reconciliation.models import LedgerEntry, Transaction
from reconciliation.reconciliation_service import ReconciliationService


def generate_benchmark_dataset(
    record_count: int = 10000,
    seed: int = 20261007,
) -> Tuple[List[Transaction], List[LedgerEntry], AccountProfileStore]:
    """Generate 10,000 transactions and corresponding ledger entries with realistic distributions."""
    rng = random.Random(seed)
    profile_store = AccountProfileStore()

    # Pre-generate 100 account profiles with baseline statistics
    account_ids = [f"ACC-BENCH-{i:04d}" for i in range(1, 101)]
    for acc in account_ids:
        avg_amt = Decimal(str(rng.randint(2000, 15000)))
        profile = AccountProfile(
            account_id=acc,
            transaction_count=rng.randint(50, 200),
            average_amount=avg_amt,
            max_amount=avg_amt * Decimal("3"),
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
    ledger_entries: List[LedgerEntry] = []

    for i in range(record_count):
        acc = rng.choice(account_ids)
        profile = profile_store.get_profile(acc)
        avg = profile.average_amount if profile else Decimal("5000")

        # Distribution:
        # 70% Clean & Matched -> APPROVE
        # 15% Unmatched/Ambiguous Ledger -> MANUAL_REVIEW
        # 10% High Fraud / Limit -> MANUAL_REVIEW or BLOCK
        # 5% Critical Fraud -> BLOCK_RECOMMENDED
        roll = rng.random()
        tx_id = f"TXN-BENCH7-{i+1:06d}"
        ref_id = f"REF-BENCH7-{i+1:06d}"

        if roll < 0.70:
            # Scenario A: Clean matched transaction
            amt = float((avg * Decimal(str(rng.uniform(0.7, 1.3)))).quantize(Decimal("0.01")))
            tx_time = base_time + timedelta(seconds=i * 30)
            tx = Transaction(
                transaction_id=tx_id,
                account_id=acc,
                transaction_type="TRANSFER",
                amount=amt,
                currency="INR",
                transaction_timestamp=tx_time,
                source_system="CHANNEL_GATEWAY",
                destination_system="CORE_LEDGER",
                reference_id=ref_id,
            )
            entry = LedgerEntry(
                ledger_entry_id=f"LEDGER-BENCH7-{i+1:06d}",
                transaction_id=tx_id,
                account_id=acc,
                amount=amt,
                currency="INR",
                debit_credit="DEBIT",
                reference_id=ref_id,
                timestamp=tx_time + timedelta(seconds=2),
                source_system="CORE_BANKING",
            )
            transactions.append(tx)
            ledger_entries.append(entry)

        elif roll < 0.85:
            # Scenario B: Missing or delayed ledger entry (unposted) -> MANUAL_REVIEW
            amt = float((avg * Decimal(str(rng.uniform(0.8, 1.2)))).quantize(Decimal("0.01")))
            tx_time = base_time + timedelta(seconds=i * 30)
            tx = Transaction(
                transaction_id=tx_id,
                account_id=acc,
                transaction_type="PAYMENT",
                amount=amt,
                currency="INR",
                transaction_timestamp=tx_time,
                source_system="MOBILE_APP",
                destination_system="CORE_LEDGER",
                reference_id=ref_id,
            )
            transactions.append(tx)
            # No ledger entry inserted!

        elif roll < 0.95:
            # Scenario C: Elevated fraud / mismatch
            amt = float((avg * Decimal(str(rng.uniform(2.5, 4.0)))).quantize(Decimal("0.01")))
            tx_time = base_time + timedelta(seconds=i * 30)
            tx = Transaction(
                transaction_id=tx_id,
                account_id=acc,
                transaction_type="WITHDRAWAL",
                amount=amt,
                currency="INR",
                transaction_timestamp=tx_time,
                source_system="ATM_NET",
                destination_system="CORE_LEDGER",
                reference_id=ref_id,
            )
            entry = LedgerEntry(
                ledger_entry_id=f"LEDGER-BENCH7-{i+1:06d}",
                transaction_id=tx_id,
                account_id=acc,
                amount=amt * 0.90,  # Amount mismatch
                currency="INR",
                debit_credit="DEBIT",
                reference_id=ref_id,
                timestamp=tx_time + timedelta(seconds=5),
                source_system="CORE_BANKING",
            )
            transactions.append(tx)
            ledger_entries.append(entry)

        else:
            # Scenario D: Critical Fraud / Extreme Spike -> BLOCK_RECOMMENDED
            amt = float((avg * Decimal(str(rng.uniform(6.0, 15.0)))).quantize(Decimal("0.01")))
            tx_time = base_time + timedelta(seconds=i * 30)
            tx = Transaction(
                transaction_id=tx_id,
                account_id=acc,
                transaction_type="WIRE_TRANSFER",
                amount=amt,
                currency="INR",
                transaction_timestamp=tx_time,
                source_system="DARK_CHANNEL",
                destination_system="CORE_LEDGER",
                reference_id=ref_id,
            )
            entry = LedgerEntry(
                ledger_entry_id=f"LEDGER-BENCH7-{i+1:06d}",
                transaction_id=tx_id,
                account_id=acc,
                amount=amt,
                currency="INR",
                debit_credit="DEBIT",
                reference_id=ref_id,
                timestamp=tx_time,
                source_system="CORE_BANKING",
            )
            transactions.append(tx)
            ledger_entries.append(entry)

    return transactions, ledger_entries, profile_store


def run_benchmark(record_count: int = 10000) -> Dict[str, Any]:
    """Execute Day 7 Multi-Agent Consensus benchmark on 10,000 transactions."""
    print("=" * 80)
    print(f"ZT-LedgerGuard DAY 7 BENCHMARK — {record_count:,} Multi-Agent Consensus Records")
    print("Zero-Trust Architecture | 4 Specialized Agents | Consensus Precedence")
    print("=" * 80)

    db_file = os.path.join(tempfile.gettempdir(), f"benchmark_day7_{int(time.time())}.db")
    try:
        init_db(db_file)

        # Optimize SQLite for local in-memory/high-throughput benchmark
        with get_connection(db_file) as conn:
            conn.execute("PRAGMA synchronous = OFF")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA cache_size = 20000")

        print(f"\n[1/4] Generating synthetic benchmark dataset ({record_count:,} records)...")
        gen_start = time.perf_counter()
        transactions, ledger_entries, profile_store = generate_benchmark_dataset(
            record_count=record_count
        )
        gen_elapsed = time.perf_counter() - gen_start
        print(f"      Generated {len(transactions):,} transactions & {len(ledger_entries):,} ledger entries in {gen_elapsed:.2f}s")

        print("\n[2/4] Seeding SQLite database for reconciliation candidate lookup...")
        seed_start = time.perf_counter()
        with get_connection(db_file) as conn:
            query = """
                INSERT OR REPLACE INTO ledger_entries (
                    ledger_entry_id, transaction_id, account_id, amount, currency,
                    debit_credit, reference_id, timestamp, source_system
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params_list = [
                (
                    e.ledger_entry_id, e.transaction_id, e.account_id, e.amount, e.currency,
                    e.debit_credit, e.reference_id, e.timestamp.isoformat(), e.source_system
                )
                for e in ledger_entries
            ]
            with conn:
                conn.executemany(query, params_list)
        seed_elapsed = time.perf_counter() - seed_start
        print(f"      Seeded {len(ledger_entries):,} ledger entries in {seed_elapsed:.3f}s")

        print("\n[3/4] Initializing Multi-Agent Coordination Infrastructure...")
        repo = LedgerRepository(db_path=db_file)
        recon_service = ReconciliationService(repository=repo, db_path=db_file)
        recon_agent = ReconciliationAgent(service=recon_service)
        fraud_agent = FraudDetectionAgent(profile_store=profile_store)
        policy_agent = PolicyAgent()
        decision_agent = DecisionAgent()

        coordinator = AgentCoordinator(
            reconciliation_agent=recon_agent,
            fraud_agent=fraud_agent,
            policy_agent=policy_agent,
            decision_agent=decision_agent,
            db_path=db_file,
        )

        print("\n[4/4] Executing Multi-Agent Consensus on 10,000 transactions...")
        decision_counts = {
            DecisionType.APPROVE.value: 0,
            DecisionType.MANUAL_REVIEW.value: 0,
            DecisionType.BLOCK_RECOMMENDED.value: 0,
        }
        total_confidence = 0.0
        disagreement_count = 0
        failure_count = 0

        t_start = time.perf_counter()

        for idx, tx in enumerate(transactions):
            res = coordinator.coordinate_decision(tx)

            decision_counts[res.decision] = decision_counts.get(res.decision, 0) + 1
            total_confidence += res.confidence
            if res.disagreement_detected:
                disagreement_count += 1
            if res.decision_reasons and any("failure" in r.lower() for r in res.decision_reasons):
                failure_count += 1

            if (idx + 1) % 2000 == 0 or (idx + 1) == record_count:
                current_elapsed = time.perf_counter() - t_start
                rate = (idx + 1) / current_elapsed if current_elapsed > 0 else 0
                print(f"      Progress: {idx + 1:6,d}/{record_count:,} ({(idx + 1)/record_count*100:5.1f}%) | Throughput: {rate:7.2f} tx/s", flush=True)

        t_total = time.perf_counter() - t_start
        avg_latency_ms = (t_total / record_count) * 1000.0
        throughput = record_count / t_total if t_total > 0 else 0.0
        avg_confidence = total_confidence / record_count if record_count > 0 else 0.0

        metrics = {
            "record_count": record_count,
            "total_elapsed_seconds": round(t_total, 3),
            "average_latency_ms": round(avg_latency_ms, 3),
            "throughput_tx_per_sec": round(throughput, 2),
            "approve_count": decision_counts[DecisionType.APPROVE.value],
            "approve_percent": round(decision_counts[DecisionType.APPROVE.value] / record_count * 100, 2),
            "manual_review_count": decision_counts[DecisionType.MANUAL_REVIEW.value],
            "manual_review_percent": round(decision_counts[DecisionType.MANUAL_REVIEW.value] / record_count * 100, 2),
            "block_recommended_count": decision_counts[DecisionType.BLOCK_RECOMMENDED.value],
            "block_recommended_percent": round(decision_counts[DecisionType.BLOCK_RECOMMENDED.value] / record_count * 100, 2),
            "average_confidence": round(avg_confidence, 4),
            "disagreement_count": disagreement_count,
            "disagreement_percent": round(disagreement_count / record_count * 100, 2),
            "failure_count": failure_count,
        }

        print("\n" + "=" * 80)
        print("DAY 7 BENCHMARK PERFORMANCE RESULTS")
        print("=" * 80)
        print(f"Total Transactions Processed : {metrics['record_count']:,}")
        print(f"Total Elapsed Time           : {metrics['total_elapsed_seconds']:.3f} seconds")
        print(f"Average Latency              : {metrics['average_latency_ms']:.3f} ms / transaction")
        print(f"Multi-Agent Throughput       : {metrics['throughput_tx_per_sec']:,.2f} transactions / second")
        print("-" * 80)
        print("CONSENSUS DISPOSITION BREAKDOWN:")
        print(f"  • APPROVE                  : {metrics['approve_count']:,} ({metrics['approve_percent']}%)")
        print(f"  • MANUAL_REVIEW            : {metrics['manual_review_count']:,} ({metrics['manual_review_percent']}%)")
        print(f"  • BLOCK_RECOMMENDED        : {metrics['block_recommended_count']:,} ({metrics['block_recommended_percent']}%)")
        print("-" * 80)
        print("RISK CONSENSUS & GOVERNANCE METRICS:")
        print(f"  • Average Confidence Score : {metrics['average_confidence']:.4f} (scale: 0.0 - 1.0)")
        print(f"  • Agent Disagreements      : {metrics['disagreement_count']:,} ({metrics['disagreement_percent']}%)")
        print(f"  • System Execution Failures: {metrics['failure_count']}")
        print("=" * 80)

        return metrics
    finally:
        try:
            if os.path.exists(db_file):
                os.remove(db_file)
            wal_file = f"{db_file}-wal"
            if os.path.exists(wal_file):
                os.remove(wal_file)
            shm_file = f"{db_file}-shm"
            if os.path.exists(shm_file):
                os.remove(shm_file)
        except Exception:
            pass


if __name__ == "__main__":
    count = 10000
    if len(sys.argv) > 1:
        try:
            count = int(sys.argv[1])
        except ValueError:
            pass
    run_benchmark(record_count=count)
