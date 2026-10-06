"""Day 5 Performance Benchmark — 10,000-Record Fuzzy Candidate Reconciliation.

LOCAL DEVELOPMENT BENCHMARK:
Generates 10,000 synthetic transactions and corresponding core ledger entries
under controlled multi-scenario distributions:
- Exact matches (~50%)
- Fuzzy matches (~25% - formatting differences, fee variances, timestamp delays)
- Ambiguous candidates (~10% - competing close candidates)
- Hard mismatches (~10% - severe account/currency/amount divergence)
- Missing ledger entries (~5% - unposted transactions)

Measures:
- Candidate discovery time (seconds)
- Fuzzy scoring time (seconds)
- Deterministic verification time (seconds)
- Total processing time (seconds)
- Overall candidate throughput (records/second)
- Classification breakdown (SELECTED, AMBIGUOUS, REJECTED, NO_CANDIDATES)
- Manual review requirement count
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.ledger_generator import LedgerGenerator
from data.transaction_generator import TransactionGenerator
from reconciliation.engine import ReconciliationEngine
from reconciliation.fuzzy_matcher import FuzzyCandidateMatcher
from reconciliation.fuzzy_result import CandidateSelectionStatus
from reconciliation.reconciliation_result import ReconciliationStatus


def run_benchmark(record_count: int = 10000) -> Dict[str, Any]:
    print("=" * 76)
    print("ZT-LedgerGuard Day 5: 10,000-Record Fuzzy Candidate Reconciliation Benchmark")
    print("LOCAL DEVELOPMENT BENCHMARK (Windows Native, Zero LLM / Zero ML)")
    print("=" * 76)
    print(f"[*] Generating {record_count:,} synthetic transactions...")

    tx_gen = TransactionGenerator(seed=202610, auto_sequence=True)
    transactions = list(tx_gen.generate_transactions(count=record_count))

    print(f"[*] Generating controlled ledger candidate distributions for {len(transactions):,} transactions...")
    ledger_gen = LedgerGenerator(seed=202610)
    dataset = ledger_gen.generate_day5_dataset(
        transactions=transactions,
        exact_ratio=0.50,     # ~5,000 expected
        fuzzy_ratio=0.25,     # ~2,500 expected
        ambiguous_ratio=0.10, # ~1,000 expected
        mismatch_ratio=0.10,  # ~1,000 expected
        missing_ratio=0.05,   # ~500 expected
    )

    print("[*] Initializing FuzzyCandidateMatcher and deterministic ReconciliationEngine...")
    matcher = FuzzyCandidateMatcher()
    engine = ReconciliationEngine()

    total_fuzzy_scoring_time = 0.0
    total_deterministic_time = 0.0

    selected_count = 0
    ambiguous_count = 0
    rejected_count = 0
    no_candidates_count = 0
    manual_review_count = 0

    final_matched = 0
    final_partial = 0
    final_mismatched = 0
    final_not_found = 0
    final_ambiguous = 0

    benchmark_start = time.perf_counter()

    for tx, candidate_entries, _scenario in dataset:
        corr_id = f"CORR-BENCH5-{tx.transaction_id}"

        # ----------------------------------------------------------------------
        # Stage 1: Candidate Ranking & Fuzzy Scoring
        # ----------------------------------------------------------------------
        t0_fuzzy = time.perf_counter()
        discovery = matcher.rank_candidates(tx, candidate_entries)
        t1_fuzzy = time.perf_counter()
        total_fuzzy_scoring_time += (t1_fuzzy - t0_fuzzy)

        if discovery.requires_manual_review:
            manual_review_count += 1

        if discovery.selection_status == CandidateSelectionStatus.SELECTED.value:
            selected_count += 1
        elif discovery.selection_status == CandidateSelectionStatus.AMBIGUOUS.value:
            ambiguous_count += 1
        elif discovery.selection_status == CandidateSelectionStatus.REJECTED.value:
            rejected_count += 1
        else:
            no_candidates_count += 1

        # ----------------------------------------------------------------------
        # Stage 2: Financial Verification (Deterministic Engine)
        # ----------------------------------------------------------------------
        t0_det = time.perf_counter()
        if discovery.selection_status == CandidateSelectionStatus.AMBIGUOUS.value:
            final_ambiguous += 1
        else:
            cand_entry = None
            if discovery.top_candidate and candidate_entries:
                # Find the matched candidate entry
                for c in candidate_entries:
                    if c.ledger_entry_id == discovery.top_candidate.ledger_entry_id:
                        cand_entry = c
                        break

            final_res = engine.reconcile(tx, cand_entry, correlation_id=corr_id)

            if final_res.status == ReconciliationStatus.MATCHED.value:
                final_matched += 1
            elif final_res.status == ReconciliationStatus.PARTIAL_MATCH.value:
                final_partial += 1
            elif final_res.status == ReconciliationStatus.MISMATCHED.value:
                final_mismatched += 1
            elif final_res.status == ReconciliationStatus.NOT_FOUND.value:
                final_not_found += 1

        t1_det = time.perf_counter()
        total_deterministic_time += (t1_det - t0_det)

    total_time = time.perf_counter() - benchmark_start
    throughput = round(record_count / total_time, 2) if total_time > 0 else 0.0

    print("-" * 76)
    print(f"Total Transactions Processed   : {record_count:,}")
    print(f"Candidate Discovery Statuses   :")
    print(f"  - SELECTED (Clear Winner)    : {selected_count:,} ({selected_count / record_count * 100:.1f}%)")
    print(f"  - AMBIGUOUS (Close Competing): {ambiguous_count:,} ({ambiguous_count / record_count * 100:.1f}%)")
    print(f"  - REJECTED (Hard Constraints): {rejected_count:,} ({rejected_count / record_count * 100:.1f}%)")
    print(f"  - NO_CANDIDATES (Missing GL) : {no_candidates_count:,} ({no_candidates_count / record_count * 100:.1f}%)")
    print(f"Manual Compliance Review Flag  : {manual_review_count:,} ({manual_review_count / record_count * 100:.1f}%)")
    print("-" * 76)
    print(f"Final Financial Dispositions   :")
    print(f"  - MATCHED                    : {final_matched:,} ({final_matched / record_count * 100:.1f}%)")
    print(f"  - PARTIAL_MATCH              : {final_partial:,} ({final_partial / record_count * 100:.1f}%)")
    print(f"  - MISMATCHED                 : {final_mismatched:,} ({final_mismatched / record_count * 100:.1f}%)")
    print(f"  - NOT_FOUND                  : {final_not_found:,} ({final_not_found / record_count * 100:.1f}%)")
    print(f"  - AMBIGUOUS (Triage Hold)    : {final_ambiguous:,} ({final_ambiguous / record_count * 100:.1f}%)")
    print("-" * 76)
    print(f"Stage 1 Fuzzy Scoring Time     : {total_fuzzy_scoring_time:.4f} s")
    print(f"Stage 2 Deterministic Ver Time : {total_deterministic_time:.4f} s")
    print(f"Total End-to-End Elapsed Time  : {total_time:.4f} seconds")
    print(f"Reconciliation Engine Throughput: {throughput:,.2f} records/second")
    print("=" * 76)
    print("[OK] Day 5 Benchmark completed successfully.")

    return {
        "record_count": record_count,
        "selected": selected_count,
        "ambiguous": ambiguous_count,
        "rejected": rejected_count,
        "no_candidates": no_candidates_count,
        "manual_review_count": manual_review_count,
        "final_matched": final_matched,
        "final_partial": final_partial,
        "final_mismatched": final_mismatched,
        "final_not_found": final_not_found,
        "final_ambiguous": final_ambiguous,
        "fuzzy_scoring_time_sec": round(total_fuzzy_scoring_time, 4),
        "deterministic_time_sec": round(total_deterministic_time, 4),
        "total_time_sec": round(total_time, 4),
        "throughput_records_per_sec": throughput,
    }


if __name__ == "__main__":
    run_benchmark(10000)
