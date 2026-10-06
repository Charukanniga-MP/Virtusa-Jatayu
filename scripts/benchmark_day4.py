"""Day 4 Performance Benchmark — 1,000-Record Deterministic Ledger Reconciliation.

LOCAL DEVELOPMENT BENCHMARK:
Generates 1,000 synthetic transactions and corresponding core ledger entries
with controlled scenario distributions (exact, partial, mismatched, missing).
Executes deterministic reconciliation through the engine and measures:
- Total records processed
- MATCHED count
- PARTIAL_MATCH count
- MISMATCHED count
- NOT_FOUND count
- Total processing time (seconds)
- Reconciliation throughput (records/sec)
- Average reconciliation score
"""

import os
import sys
import time
from typing import Any, Dict

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.ledger_generator import LedgerGenerator
from data.transaction_generator import TransactionGenerator
from reconciliation.engine import ReconciliationEngine
from reconciliation.reconciliation_result import ReconciliationStatus


def run_benchmark(record_count: int = 1000) -> Dict[str, Any]:
    print("=" * 70)
    print("ZT-LedgerGuard Day 4: 1,000-Record Deterministic Reconciliation Benchmark")
    print("LOCAL DEVELOPMENT BENCHMARK (Windows Native, Zero LLM / Zero ML)")
    print("=" * 70)
    print(f"[*] Generating {record_count} synthetic transactions...")

    tx_generator = TransactionGenerator(seed=202610, auto_sequence=True)
    transactions = list(tx_generator.generate_transactions(count=record_count))

    print(f"[*] Generating controlled ledger distributions for {len(transactions)} transactions...")
    ledger_generator = LedgerGenerator(seed=202610)
    dataset = ledger_generator.generate_controlled_dataset(
        transactions=transactions,
        exact_ratio=0.70,     # ~700 expected
        partial_ratio=0.15,   # ~150 expected
        mismatch_ratio=0.10,  # ~100 expected
        missing_ratio=0.05,   # ~50 expected
    )

    print(f"[*] Initializing deterministic ReconciliationEngine...")
    engine = ReconciliationEngine()

    results = []
    start_time = time.perf_counter()

    for tx, entry, _scenario in dataset:
        corr_id = f"CORR-BENCH-{tx.transaction_id}"
        result = engine.reconcile(
            transaction=tx,
            ledger_entry=entry,
            correlation_id=corr_id,
        )
        results.append(result)

    elapsed_time = time.perf_counter() - start_time

    # Aggregate Metrics
    total = len(results)
    matched = sum(1 for r in results if r.status == ReconciliationStatus.MATCHED.value)
    partial = sum(1 for r in results if r.status == ReconciliationStatus.PARTIAL_MATCH.value)
    mismatched = sum(1 for r in results if r.status == ReconciliationStatus.MISMATCHED.value)
    not_found = sum(1 for r in results if r.status == ReconciliationStatus.NOT_FOUND.value)
    avg_score = round(sum(r.score for r in results) / total, 4) if total > 0 else 0.0
    throughput = round(total / elapsed_time, 2) if elapsed_time > 0 else 0.0

    print("-" * 70)
    print(f"Total Records Reconciled    : {total}")
    print(f"  - MATCHED                 : {matched} ({matched / total * 100:.1f}%)")
    print(f"  - PARTIAL_MATCH           : {partial} ({partial / total * 100:.1f}%)")
    print(f"  - MISMATCHED              : {mismatched} ({mismatched / total * 100:.1f}%)")
    print(f"  - NOT_FOUND (Missing GL)  : {not_found} ({not_found / total * 100:.1f}%)")
    print(f"Average Match Score         : {avg_score:.4f}")
    print(f"Total Processing Time       : {elapsed_time:.4f} seconds")
    print(f"Reconciliation Throughput   : {throughput:.2f} records/second")
    print("=" * 70)
    print("[OK] Benchmark completed successfully.")

    return {
        "total_records": total,
        "matched": matched,
        "partial_match": partial,
        "mismatched": mismatched,
        "not_found": not_found,
        "average_score": avg_score,
        "total_time_seconds": round(elapsed_time, 4),
        "throughput_records_per_sec": throughput,
    }


if __name__ == "__main__":
    run_benchmark(1000)
